impl WriteCoordinator {
    /// 把一条记录喂进**派生检索索引**：BM25 中文倒排 + 过滤属性边车。
    /// ingest、WAL 重放、从段重建索引三处共用 —— 派生索引的喂法只此一份。
    fn index_record(&self, r: &WalRecord) {
        self.index_record_inner(r, true, true);
    }

    fn index_record_without_rollup(&self, r: &WalRecord) {
        self.index_record_inner(r, false, true);
    }

    fn index_record_without_rollup_and_filter_attrs(&self, r: &WalRecord) {
        self.index_record_inner(r, false, false);
    }

    fn index_record_without_filter_attrs(&self, r: &WalRecord) {
        self.index_record_inner(r, true, false);
    }

    fn index_record_inner(&self, r: &WalRecord, update_rollup: bool, update_filter_attrs: bool) {
        // 中文倒排：把该 span 的**可检索文本**喂进 BM25。检索的主对象是 LLM 的输入/输出原文
        // （input_text/output_text），span_name / logs 作补充。它们拼起来索引——真实 SDK 灌进来的
        // input/output 文本会被索引，而不是只索引 logs（否则真实数据上"中文检索"会突然失效）。
        let mut parts: Vec<&str> = Vec::new();
        if let Some(t) = r.fields.input_text.as_deref() {
            parts.push(t);
        }
        if let Some(t) = r.fields.output_text.as_deref() {
            parts.push(t);
        }
        if let Some(t) = r.fields.span_name.as_deref() {
            parts.push(t);
        }
        // agent/tool/model 名也索引——用户会按"搜某个 agent/tool 的 trace"（如"搜风控 agent 的报错"）。
        for field in [&r.fields.agent_name, &r.fields.tool_name, &r.fields.model] {
            if let Some(t) = field.as_deref() {
                parts.push(t);
            }
        }
        for l in &r.fields.logs {
            parts.push(l);
        }
        let event_id = r.identity.event_id().0;
        if parts.is_empty() {
            self.bm25.mark_event_scoped(r.fields.tenant_id, event_id);
        } else {
            self.bm25.index_event_scoped(
                r.fields.tenant_id,
                event_id,
                r.trace_id,
                r.span_id,
                &parts.join(" "),
            );
        }
        if update_filter_attrs {
            self.filter_attrs.lock().unwrap().apply_record(r);
        }

        if update_rollup {
            self.trace_rollup.lock().unwrap().apply_record(r);
        }

        self.session_idx.lock().unwrap().apply_record(r);
    }

    /// 写入：先进 WAL（ack 后才算持久），同步进活 MemTable，再推进已提交尾。
    /// 折叠在读时做，所以写路径不需要「脏队列」（决策文档已去掉 fold_dirty）。
    /// 整个过 write_lock 串行（单写者）。
    pub fn ingest(&self, records: Vec<WalRecord>) -> WalLsn {
        self.try_ingest(records).expect("yiTrace ingest failed")
    }

    pub fn try_ingest(&self, records: Vec<WalRecord>) -> std::io::Result<WalLsn> {
        if records.iter().any(|r| r.fields.tenant_id.is_some())
            && !self.bm25.supports_tenant_scope()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "BM25 adapter does not support tenant scope",
            ));
        }
        let _process = self.try_acquire_process_lock("write")?;
        let _w = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        // clean reopen 不预载派生索引；第一次写入前补齐，保证历史索引和新事件在同一状态上增量更新。
        self.ensure_all_read_models_current_locked();
        let mut wal = self.wal.lock().unwrap();
        // 这批的起始 LSN（在 append 之前确定），逐条分配 commit_lsn。
        let first = wal.committed_tail().get() + 1;
        // WAL 写入与同步成功之后，才允许可见行进入 MemTable 和派生索引。
        let last = wal.try_append_committed_records(&records)?;
        drop(wal);
        {
            let mut mt = self.memtable.lock().unwrap();
            for (i, r) in records.iter().enumerate() {
                self.index_record(r); // 喂检索索引（BM25 + 属性边车）
                mt.append(MemRow {
                    commit_lsn: first + i as u64,
                    trace_id: r.trace_id,
                    span_id: r.span_id,
                    ts: r.ts,
                    identity: r.identity.clone(),
                    fields: r.fields.clone(),
                });
            }
        }

        // ack 之后才推进 committed_tail（读者据此取 live_lsn 上界）。
        self.current.advance_committed_tail(last);

        // 内存表超阈值就自动刷盘，兜住内存上界（OPEN-2）。仍在 write_lock 下。
        if self.memtable.lock().unwrap().len() >= self.flush_threshold.load(Ordering::Relaxed) {
            // 已同步 WAL 的批次已经持久。自动 flush 失败保留内存行，显式 flush 可返回错误。
            if let Err(err) = self.flush_memtable_locked() {
                olog::log(
                    olog::Level::Warn,
                    "automatic_flush_failed",
                    &[("error", &err.to_string())],
                );
            }
        }
        // 会话边车已在 index_record 里逐事件增量维护，这里无需额外动作。
        let n = first;
        let cnt = last.get() - first + 1;
        let tail = last.get();
        olog::log(
            olog::Level::Debug,
            "ingest",
            &[("lsn", &n), ("count", &cnt), ("tail", &tail)],
        );
        Ok(last)
    }

    /// 摄入 SDK 线格式记录：转成内部 WalRecord（引擎自算 event_id）后走正常 `ingest`。
    /// 这是「打点 → 引擎存」的数据契约入口；上面再套一层 HTTP/OTLP 网关即闭环（网关是纯管道）。
    pub fn ingest_wire(&self, records: Vec<WireRecord>) -> WalLsn {
        self.try_ingest_wire(records)
            .expect("yiTrace wire ingest failed")
    }

    pub fn try_ingest_wire(&self, records: Vec<WireRecord>) -> std::io::Result<WalLsn> {
        self.try_ingest(
            records
                .into_iter()
                .map(WireRecord::into_wal_record)
                .collect(),
        )
    }

    pub fn try_ingest_wire_for_tenant(
        &self,
        mut records: Vec<WireRecord>,
        tenant: Option<u64>,
    ) -> std::io::Result<WalLsn> {
        for r in &mut records {
            r.tenant_id = tenant;
        }
        self.try_ingest_wire(records)
    }

    /// HTTP 网关专用摄入：租户来自鉴权上下文（如 `X-Tenant-Id`），覆盖 wire body 里的 tenant_id。
    /// 这是多租户安全边界；SDK/客户端可以重复发送 body，但不能自选或伪造租户。
    pub fn ingest_wire_for_tenant(
        &self,
        mut records: Vec<WireRecord>,
        tenant: Option<u64>,
    ) -> WalLsn {
        for r in &mut records {
            r.tenant_id = tenant;
        }
        self.ingest_wire(records)
    }

    /// 摄入 OTLP/OpenInference 标准 trace（OTLP/HTTP JSON）：经适配器映射成 WireRecord 后走正常摄入。
    /// 这是「生态入口」——已用 OpenTelemetry / OpenInference 埋点的 agent 应用不改打点即可灌进来。
    /// 解析失败返回 Err（调用方/HTTP 网关据此回 400）。
    pub fn ingest_otlp(&self, body: &str) -> Result<WalLsn, String> {
        let wires = parse_otlp_traces(body)?;
        Ok(self.ingest_wire(wires))
    }

    /// HTTP 网关专用 OTLP 摄入：OTLP attributes 里带的 tenant 也不作为安全边界，统一由请求上下文覆盖。
    pub fn ingest_otlp_for_tenant(
        &self,
        body: &str,
        tenant: Option<u64>,
    ) -> Result<WalLsn, String> {
        let wires = parse_otlp_traces(body)?;
        Ok(self.ingest_wire_for_tenant(wires, tenant))
    }

    /// 设置内存表自动刷盘阈值（行数）。
    pub fn set_flush_threshold(&self, n: usize) {
        self.flush_threshold.store(n.max(1), Ordering::Relaxed);
    }

    /// 当前内存表行数（可观测 / 测试用）。
    pub fn memtable_len(&self) -> usize {
        self.memtable.lock().unwrap().len()
    }

    /// 主动把内存表当前内容封成一个段（周期刷盘 / 关机前）。
    pub fn flush_memtable(&self) {
        self.try_flush_memtable().expect("yiTrace flush failed");
    }

    pub fn try_flush_memtable(&self) -> std::io::Result<()> {
        let _process = self.try_acquire_process_lock("write")?;
        let _w = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        let before = self.memtable.lock().unwrap().len();
        let v_before = self.current.version();
        self.flush_memtable_locked()?;
        // 显式 flush 是派生缓存的持久化点。自动 flush 只保证主数据落盘，避免每个小段都重写
        // 全量 BM25/attrs/rollup sidecar；崩溃时旧 cache 会因 manifest 版本不符而重建。
        self.persist_read_model_sidecars();
        let seg = v_before;
        olog::log(
            olog::Level::Info,
            "flush",
            &[
                ("seg", &seg),
                ("rows", &before),
                ("version", &self.current.version()),
            ],
        );
        Ok(())
    }

    /// 把内存表内容封段（调用方须已持 write_lock）。watermark 推进到内存表最新 LSN。
    fn flush_memtable_locked(&self) -> std::io::Result<()> {
        let (records, max_lsn) = {
            let mut mt = self.memtable.lock().unwrap();
            if mt.newest_lsn().unwrap_or(0) <= self.current.memtable_watermark() {
                // 只有旧快照保留行时不重复封段；快照已释放的前缀仍须按 gate 回收。
                mt.evict_up_to(WalLsn::new(self.current.min_retained_watermark()));
                return Ok(());
            }
            let records: Vec<WalRecord> = mt
                .read_range(
                    WalLsn::new(self.current.memtable_watermark()),
                    WalLsn::new(self.current.committed_tail()),
                )
                .map(|r| WalRecord {
                    trace_id: r.trace_id,
                    span_id: r.span_id,
                    ts: r.ts,
                    identity: r.identity.clone(),
                    fields: r.fields.clone(),
                })
                .collect();
            (records, mt.newest_lsn().unwrap())
        };
        let seg = self.alloc_segment_id();
        self.segments.try_flush_to_segment(seg, &records)?;
        // 段级 key bloom：从这批记录的 (trace,span) 建，供检索折叠定位跳过无关段。
        let bloom = KeyBloom::build(
            records.iter().map(|r| (r.trace_id, r.span_id)),
            records.len(),
        );
        self.seg_key_bloom
            .lock()
            .unwrap()
            .insert(seg.get(), Arc::new(bloom));
        let (min_ts, max_ts) = ts_range(&records);
        let mut draft = self.current.cow_next();
        draft.memtable_watermark = WalLsn::new(max_lsn);
        draft.segments.insert(
            seg.get(),
            SegmentEntry {
                segment_id: seg,
                level: 0,
                state: SegState::Live,
                min_ts,
                max_ts,
                deletion_vec: Arc::new(DeletionVec::empty()),
                deletion_seq: 0,
                upgrade_ref: None,
                upgrade_seq: 0,
            },
        );
        self.commit_and_persist(draft)?;
        if let Err(err) = self.wal.lock().unwrap().checkpoint(WalLsn::new(max_lsn)) {
            olog::log(
                olog::Level::Warn,
                "wal_checkpoint_save_failed",
                &[("error", &err.to_string())],
            );
        }
        let gate = WalLsn::new(self.current.min_retained_watermark());
        self.memtable.lock().unwrap().evict_up_to(gate);
        Ok(())
    }

    /// 读 MemTable 源：某快照可见的半开区间 `(retained_watermark, live_lsn]`（测试/折叠用）。
    pub fn read_memtable_lsns(&self, snap: &Snapshot) -> Vec<u64> {
        self.memtable
            .lock()
            .unwrap()
            .read_range(snap.retained_watermark, snap.live_lsn)
            .map(|r| r.commit_lsn)
            .collect()
    }
}
