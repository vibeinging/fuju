impl WriteCoordinator {
    /// 崩溃恢复：从 WAL checkpoint 后重放 MemTable（§M.6），派生读模型按需加载。
    /// 重放点 = 当前 manifest 的 memtable_watermark（已吸收进段的最大 LSN）。
    /// 重放只取 watermark 之后的记录；即便段与重放有重叠（崩溃窗口里段已落、水位未推进），
    /// 读时的确定性 event_id 去重也保证不重复折叠 —— 这正是「seq 原样持久化、不重补」的意义。
    ///
    /// 检索索引(BM25/属性边车/向量)是派生态：
    /// - clean reopen 不解码 rollup/filter/BM25/bloom，数据库先进入可用状态。
    /// - 第一次相关查询加载对应缓存；第一次写入前全部补齐。
    /// - 缓存缺失、损坏或段有删除/补写时，第一次使用才扫持久段重建。
    /// - WAL 有未 flush 尾部时，恢复会先补齐历史读模型再叠加尾部，保证增量不被后加载缓存覆盖。
    /// - 向量**段里推不出来**：从独立向量文件重载,喂回图索引(后写覆盖先写)。
    pub fn recover(&self) {
        self.try_recover().expect("yiTrace recovery failed");
    }

    pub fn try_recover(&self) -> std::io::Result<()> {
        let _process = self.try_acquire_process_lock("write")?;
        let _local = self.write_lock.lock().unwrap();
        let started = std::time::Instant::now();
        olog::log(
            olog::Level::Info,
            "recover_start",
            &[("version", &self.current.version())],
        );
        if let Some(state) = self
            .manifest_path
            .as_ref()
            .map(persist::try_load)
            .transpose()?
            .flatten()
        {
            self.current.replace_from_disk(state.manifest);
            *self.next_segment_id.lock().unwrap() = state.next_segment_id;
            *self.next_chunk_id.lock().unwrap() = state.next_chunk_id;
        }
        self.wal.lock().unwrap().try_refresh_from_disk()?;
        self.refresh_metadata_from_disk_locked();
        let seg_count = self.rebuild_volatile_from_current_locked()?;
        let wal_count = self.memtable.lock().unwrap().len() as u64;
        let tail = self.current.committed_tail();
        let duration_us = started.elapsed().as_micros() as u64;
        let duration_ms = started.elapsed().as_millis() as u64;
        olog::log(
            olog::Level::Info,
            "recover_done",
            &[
                ("segs_scanned", &seg_count),
                ("vectors_reloaded", &0u64),
                ("wal_replayed", &wal_count),
                ("committed_tail", &tail),
                ("duration_us", &duration_us),
                ("duration_ms", &duration_ms),
            ],
        );
        Ok(())
    }

    /// 测试/演示：模拟崩溃，丢弃易失的 MemTable。WAL 与 manifest 是持久的，保留不动。
    pub fn simulate_crash_lose_memtable(&self) {
        *self.memtable.lock().unwrap() = MemTable::new();
    }

    fn alloc_segment_id(&self) -> SegmentId {
        let mut g = self.next_segment_id.lock().unwrap();
        let id = *g;
        *g += 1;
        SegmentId::new(id)
    }

    fn alloc_chunk_id(&self) -> yt_core::ids::ChunkId {
        let mut g = self.next_chunk_id.lock().unwrap();
        let id = *g;
        *g += 1;
        yt_core::ids::ChunkId::new(id)
    }

    /// flush 提交（sealed → live）：把一批已 ack 事件封段，新段 Live 进新版本，watermark 推进。
    /// 段加入 + watermark 推进必须在**同一次** commit 里原子生效（堵「既不在 memtable 又不在段」空窗）。
    pub fn commit_flush(&self, records: &[WalRecord], up_to_lsn: WalLsn) {
        self.try_commit_flush(records, up_to_lsn)
            .expect("yiTrace flush commit failed");
    }

    pub fn try_commit_flush(
        &self,
        records: &[WalRecord],
        up_to_lsn: WalLsn,
    ) -> std::io::Result<()> {
        let _process = self.try_acquire_process_lock("write")?;
        let _w = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        let seg = self.alloc_segment_id();
        self.segments.try_flush_to_segment(seg, records)?; // building→sealed（写完 fsync）
        let bloom = KeyBloom::build(
            records.iter().map(|r| (r.trace_id, r.span_id)),
            records.len(),
        );
        self.seg_key_bloom
            .lock()
            .unwrap()
            .insert(seg.get(), Arc::new(bloom));
        let (min_ts, max_ts) = ts_range(records);
        let mut draft = self.current.cow_next();
        draft.memtable_watermark = up_to_lsn; // 与下面加段同事务
        draft.segments.insert(
            seg.get(),
            SegmentEntry {
                segment_id: seg,
                level: 0,
                state: SegState::Live,
                min_ts, // zone-map：读路径据此做时间窗剪枝
                max_ts,
                deletion_vec: Arc::new(DeletionVec::empty()),
                deletion_seq: 0,
                upgrade_ref: None,
                upgrade_seq: 0,
            },
        );
        self.commit_and_persist(draft)?; // 原子换指针：sealed→live + watermark 同时生效;并落盘 manifest
        if let Err(err) = self.wal.lock().unwrap().checkpoint(up_to_lsn) {
            // checkpoint 只是恢复加速器；写失败不影响 WAL/manifest 正确性，下次启动回退全量校验。
            olog::log(
                olog::Level::Warn,
                "wal_checkpoint_save_failed",
                &[("error", &err.to_string())],
            );
        }

        // 提交后按 gate 回收 MemTable 被吸收前缀。gate 必须取「所有活跃读者下界的最小值」，
        // 绝不能直接用 up_to_lsn —— 否则就是 flush-evict 漏行 bug。仍有旧读者时此值更小、不删其行。
        let gate = WalLsn::new(self.current.min_retained_watermark());
        self.memtable.lock().unwrap().evict_up_to(gate);
        self.persist_read_model_sidecars();
        Ok(())
    }

    /// 删除提交：给某段换一个新的 deletion 块（deletion_seq+1），绝不原地改旧块。
    pub fn commit_delete(&self, seg: SegmentId, row: u32) {
        self.try_commit_delete(seg, row)
            .expect("yiTrace delete commit failed");
    }

    pub fn try_commit_delete(&self, seg: SegmentId, row: u32) -> std::io::Result<()> {
        let _process = self.try_acquire_process_lock("write")?;
        let _w = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        let chunk_id = self.alloc_chunk_id();
        let mut draft = self.current.cow_next();
        if let Some(entry) = draft.segments.get_mut(&seg.get()) {
            let new_dv = entry.deletion_vec.with_deleted(row, chunk_id);
            entry.deletion_vec = Arc::new(new_dv);
            entry.deletion_seq += 1;
        }
        self.commit_and_persist(draft)?;
        self.session_idx.lock().unwrap().dirty = true; // 删除改了段，边车下次读重建
        self.rebuild_trace_rollup_current();
        self.rebuild_filter_attrs_current();
        self.rebuild_bm25_current();
        *self.segment_scan_indexes_stale.lock().unwrap() = false;
        self.persist_read_model_sidecars();
        Ok(())
    }

    /// 属性补写（upgrade）提交：给某段 (trace_id, span_id) 补写**非身份属性**，与 delete 完全对称——
    /// 写时复制出新 upgrade 块（upgrade_seq+1），绝不原地改旧块（旧版本读者读旧块）。
    /// 身份字段冻结（M.7），由上层 schema 保证不进 `fields`。
    pub fn commit_upgrade(
        &self,
        seg: SegmentId,
        trace_id: u64,
        span_id: u64,
        fields: yt_core::fold::SpanFields,
    ) {
        self.try_commit_upgrade(seg, trace_id, span_id, fields)
            .expect("yiTrace upgrade commit failed");
    }

    pub fn try_commit_upgrade(
        &self,
        seg: SegmentId,
        trace_id: u64,
        span_id: u64,
        mut fields: yt_core::fold::SpanFields,
    ) -> std::io::Result<()> {
        let _process = self.try_acquire_process_lock("write")?;
        let _w = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        // 旧 upgrade 接缝只含 trace/span，不能把同段里不同 tenant 的同名 span 一起改掉。
        let tenants: std::collections::HashSet<Option<u64>> = self
            .segments
            .scan_records(seg)
            .iter()
            .filter(|r| r.trace_id == trace_id && r.span_id == span_id)
            .map(|r| r.fields.tenant_id)
            .collect();
        if tenants.len() > 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "ambiguous tenant for trace/span upgrade",
            ));
        }
        if let Some(&tenant) = tenants.iter().next() {
            if fields.tenant_id.is_some() && fields.tenant_id != tenant {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "upgrade cannot change tenant identity",
                ));
            }
            fields.tenant_id = tenant;
        }
        let chunk_id = self.alloc_chunk_id();
        let mut draft = self.current.cow_next();
        if let Some(entry) = draft.segments.get_mut(&seg.get()) {
            let base = entry
                .upgrade_ref
                .as_deref()
                .cloned()
                .unwrap_or_else(UpgradeColChunk::empty);
            let new_chunk = base.with_patch(trace_id, span_id, fields, chunk_id);
            entry.upgrade_ref = Some(Arc::new(new_chunk));
            entry.upgrade_seq += 1;
        }
        self.commit_and_persist(draft)?;
        self.session_idx.lock().unwrap().dirty = true; // 补写改了段，边车下次读重建
        self.rebuild_trace_rollup_current();
        self.rebuild_filter_attrs_current();
        self.rebuild_bm25_current();
        *self.segment_scan_indexes_stale.lock().unwrap() = false;
        self.persist_read_model_sidecars();
        Ok(())
    }

    /// compaction 第 1 步：选段，记录选段瞬间各输入段的 (deletion_seq, upgrade_seq)。
    /// 返回的 plan 交给调用方在**锁外**做昂贵的段重建，再用 `compaction_finish` 提交。
    pub fn compaction_begin(&self, inputs: &[SegmentId]) -> CompactionPlan {
        self.try_compaction_begin(inputs)
            .expect("yiTrace compaction selection failed")
    }

    pub fn try_compaction_begin(&self, inputs: &[SegmentId]) -> std::io::Result<CompactionPlan> {
        let _process = self.try_acquire_process_lock("write")?;
        let _w = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        let m = self.current.manifest();
        let seqs_at_select = inputs
            .iter()
            .filter_map(|s| {
                m.segments
                    .get(&s.get())
                    .map(|e| (s.get(), (e.deletion_seq, e.upgrade_seq)))
            })
            .collect();
        Ok(CompactionPlan {
            inputs: inputs.to_vec(),
            seqs_at_select,
        })
    }

    /// compaction 第 3 步：提交（草案 1 §D1.3 / OPEN-3）。
    /// 在 write_lock 下**重读输入段当前状态**重建新段 —— 这样选段后、提交前并发打到输入段的
    /// 删除/补写**不会丢**：当前 deletion_vec 把后到的删除也滤掉，当前 upgrade 块也并进新段。
    /// 返回是否发生了重读合并（输入段 seq 变了），便于观测/测试。
    pub fn compaction_finish(&self, plan: &CompactionPlan) -> bool {
        self.try_compaction_finish(plan)
            .expect("yiTrace compaction commit failed")
    }

    pub fn try_compaction_finish(&self, plan: &CompactionPlan) -> std::io::Result<bool> {
        let _process = self.try_acquire_process_lock("write")?;
        let _w = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        let m = self.current.manifest();

        let mut reconciled = false;
        let mut merged: Vec<WalRecord> = Vec::new();
        let mut merged_upgrade = UpgradeColChunk::empty();
        let up_chunk_id = self.alloc_chunk_id();

        for &seg in &plan.inputs {
            let Some(entry) = m.segments.get(&seg.get()) else {
                continue;
            };
            // 选段以来 seq 涨了 = 期间有并发删除/补写打到这个输入段 → 触发重读合并
            if plan.seqs_at_select.get(&seg.get()) != Some(&(entry.deletion_seq, entry.upgrade_seq))
            {
                reconciled = true;
            }
            // 用「当前」deletion_vec 过滤（含选段后新增的删除）→ 删除不丢
            for (row, rec) in self.segments.scan_records(seg).into_iter().enumerate() {
                if !entry.deletion_vec.is_deleted(row as u32) {
                    merged.push(rec);
                }
            }
            // 把「当前」upgrade 块并进新段（按 (trace,span) 键，行号变了也不影响）→ 补写不丢
            if let Some(up) = &entry.upgrade_ref {
                for (&(t, s), fields) in up.iter() {
                    merged_upgrade = merged_upgrade.with_patch(t, s, fields.clone(), up_chunk_id);
                }
            }
        }

        let new_seg = self.alloc_segment_id();
        self.segments.try_flush_to_segment(new_seg, &merged)?;
        let bloom = KeyBloom::build(merged.iter().map(|r| (r.trace_id, r.span_id)), merged.len());
        self.seg_key_bloom
            .lock()
            .unwrap()
            .insert(new_seg.get(), Arc::new(bloom));
        let (min_ts, max_ts) = ts_range(&merged);
        let has_upgrade = merged_upgrade.iter().next().is_some();

        let mut draft = self.current.cow_next();
        let v_dead = draft.version.get();
        for s in &plan.inputs {
            draft.segments.remove(&s.get());
        }
        draft.segments.insert(
            new_seg.get(),
            SegmentEntry {
                segment_id: new_seg,
                level: 1,
                state: SegState::Live,
                min_ts,
                max_ts,
                deletion_vec: Arc::new(DeletionVec::empty()), // 删除已物化进 merged，新段从干净开始
                deletion_seq: 0,
                upgrade_ref: has_upgrade.then(|| Arc::new(merged_upgrade)),
                upgrade_seq: 0,
            },
        );
        self.commit_and_persist(draft)?;

        let mut dead = self.dead_set.lock().unwrap();
        for s in &plan.inputs {
            dead.push(DeadResource { seg: *s, v_dead });
        }
        drop(dead);
        self.rebuild_trace_rollup_current();
        self.rebuild_filter_attrs_current();
        self.persist_read_model_sidecars();
        Ok(reconciled)
    }

    /// 便捷：无并发窗口的一次性 compaction（begin + finish 连续）。
    pub fn commit_compaction(&self, inputs: &[SegmentId]) {
        self.try_commit_compaction(inputs)
            .expect("yiTrace compaction failed");
    }

    pub fn try_commit_compaction(&self, inputs: &[SegmentId]) -> std::io::Result<()> {
        let n_in = inputs.len();
        let plan = self.try_compaction_begin(inputs)?;
        self.try_compaction_finish(&plan)?;
        olog::log(
            olog::Level::Info,
            "compaction",
            &[("inputs", &n_in), ("version", &self.current.version())],
        );
        Ok(())
    }

    /// 取 / 放一个段文件的 buffer pin（读路径扫段字节时持有，用完释放）。
    pub fn pin_buffer(&self, seg: SegmentId) {
        self.buffer_pins.pin(seg);
    }
    pub fn unpin_buffer(&self, seg: SegmentId) {
        self.buffer_pins.unpin(seg);
    }

    /// 段回收线程的一轮（草案 1 §D1.4）。对 dead_set 里每个资源，三条同真才物理删除：
    ///   (1) v_dead ≤ safe_version   (没有读者还 pin 在它 dead 之前的版本)
    ///   (2) ∧ 无未释放的 buffer pin  (字节级最后保险)
    ///   (3) ∧ 不被当前 manifest 引用 (防崩溃竞态)
    /// 返回这一轮回收了多少个段。真实实现是后台线程 + IO 限速。
    ///
    /// **崩溃安全**：持久模式（gc.log 存在）下，每个可删段走 MARK→fsync→unlink→DONE→fsync。
    /// 崩溃在 unlink 前（只写了 MARK）：重启据 gc.log 补删（文件还在 → 删）；崩溃在 unlink 后 DONE 前
    /// （文件已没）：重启据 gc.log 判定文件可能已删，幂等补删（不存在跳过）。两边都不留"删一半 +
    /// manifest 没更新"的不一致。
    ///
    /// **非持久模式**（gc.log 不存在）：reclaim 走旧的"直接删"路径——仅靠"段 id 永不复用 + compaction
    /// 只产新段"这两个不变量兜底，没有崩溃恢复。这是纯内存 / 测试场景可接受的退化。
    pub fn reclaim(&self) -> usize {
        self.try_reclaim().expect("yiTrace reclaim refresh failed")
    }

    pub fn try_reclaim(&self) -> std::io::Result<usize> {
        let _process = self.try_acquire_process_lock("write")?;
        let _w = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        let safe = self.current.safe_version();
        let has_process_readers = self
            .process_lock
            .as_ref()
            .map(|mgr| mgr.has_active_readers())
            .unwrap_or(false);
        let mut freed = 0;
        let mut dead = self.dead_set.lock().unwrap();
        let mut gc = self.gc_log.lock().unwrap();
        dead.retain(|r| {
            let ok = r.v_dead <= safe
                && !has_process_readers
                && !self.buffer_pins.is_pinned(r.seg)
                && !self.current.contains_segment(r.seg);
            if !ok {
                return true; // 留着，下一轮再看
            }
            // 崩溃安全路径：MARK → fsync → unlink → DONE → fsync。
            if let Some(log) = gc.as_mut() {
                // MARK 写失败 = 意图没落盘，不能进 unlink（否则崩溃后无法恢复）。保守不删，留下轮。
                if log.mark(r.seg.get()).is_err() {
                    return true;
                }
            }
            self.segments.unlink_segment(r.seg);
            self.seg_fold_cache.lock().unwrap().remove(r.seg.get()); // 段没了，缓存失效
            self.seg_key_bloom.lock().unwrap().remove(&r.seg.get()); // bloom 同失效
            if let Some(log) = gc.as_mut() {
                // DONE 写失败：文件已删但完成标记没落盘。重启时会当成"MARK 没 DONE"补删——
                // 文件不存在了，unlink 幂等（store 实现容忍），正确。所以这里不回滚、继续。
                let _ = log.done(r.seg.get());
            }
            freed += 1;
            false // 出 dead_set
        });
        if freed > 0 {
            olog::log(
                olog::Level::Info,
                "reclaim",
                &[("freed", &freed), ("remaining_dead", &dead.len())],
            );
        }
        Ok(freed)
    }

    /// 待回收 dead 资源数（可观测 / 测试用）。
    pub fn dead_count(&self) -> usize {
        self.dead_set.lock().unwrap().len()
    }

    /// **在线快照备份**（§3.3 数据安全底线）。
    ///
    /// 走 pin 协议拿一致快照（持有的版本不会被 GC），把所有持久文件拷到目标目录，
    /// 得到一个可独立 `open_durable` 恢复的一致快照。复制期间串行化 writer，读者仍可读取已有快照。
    ///
    /// 拷的文件：`segments/`（目录）+ `wal.log` + `manifest.dat` + `vecindex/`（或 `vectors.dat`）+ `gc.log`。
    /// 段文件是不可变的、manifest 是当前版本快照——拷的是那一刻的一致态。
    /// WAL 可能比 manifest 新（有未 flush 的事务），recover 时重放水位之后的尾巴,幂等（确定性 event_id）。
    pub fn backup_snapshot(&self, dest: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        let dest = dest.as_ref();
        let src = self.dir.as_ref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "backup 需要 open_durable 的数据目录",
            )
        })?;
        // 文件集与 manifest/metadata 必须来自同一时刻；writer 锁同时阻止外部 GC。
        let _process = self.try_acquire_process_lock("write")?;
        let _local = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        let _snap = self.current.pin_snapshot();
        let version = self.current.version();
        olog::log(
            olog::Level::Info,
            "backup_start",
            &[
                ("dest", &dest.to_string_lossy().to_string()),
                ("version", &version),
            ],
        );

        std::fs::create_dir_all(dest)?;
        // 拷贝数据文件/目录。segments/ 和 vecindex/ 是目录,其余是文件。
        for name in ["segments", "vecindex"] {
            let s = src.join(name);
            if s.exists() {
                copy_dir_recursive(&s, &dest.join(name))?;
            }
        }
        for name in [
            "wal.log",
            "wal.state",
            "manifest.dat",
            "metadata.dat",
            "vectors.dat",
            "vectors_scoped.dat",
            "gc.log",
        ] {
            let s = src.join(name);
            if s.exists() {
                std::fs::copy(&s, dest.join(name))?;
            }
        }
        olog::log(
            olog::Level::Info,
            "backup_done",
            &[
                ("dest", &dest.to_string_lossy().to_string()),
                ("version", &version),
            ],
        );
        Ok(())
    }

    /// 生产可观测（§3.1）：聚合所有关键运行态，供 /metrics 端点输出。
    /// 返回的字符串是 Prometheus 文本格式（每行一个 metric + 注释），零依赖、好排查。
    /// 返回 owned String，调用者直接写进 HTTP body。
    pub fn metrics(&self) -> String {
        let mut out = String::with_capacity(2048);
        let version = self.current.version();
        let segments = self.current.manifest().segments.len();
        let memtable_rows = self.memtable_len();
        let dead = self.dead_count();
        let active_readers = self.current.active_reader_count();
        let committed_tail = self.current.committed_tail();
        let flush_threshold = self.flush_threshold.load(Ordering::Relaxed);
        let read_model_state = *self.read_model_load_state.lock().unwrap();
        let search_read_model_ready = !*self.segment_scan_indexes_stale.lock().unwrap();
        let filter_attrs_guard = self.filter_attrs.lock().unwrap();
        let filter_attrs = filter_attrs_guard.len();
        let filter_attr_postings = filter_attrs_guard.posting_count();
        let filter_attr_disabled_postings = filter_attrs_guard.disabled_posting_count();
        drop(filter_attrs_guard);
        let fold_cache_entries = self.seg_fold_cache.lock().unwrap().map.len();
        let bloom_count = self.seg_key_bloom.lock().unwrap().len();
        let datasets = self.datasets.lock().unwrap().len();
        let process_lock_metrics = self.process_lock_metrics();

        // 确定性 manifest 版本（每次 commit +1）。
        out.push_str("# HELP yt_manifest_version Manifest 版本号（每次 commit +1）。\n");
        out.push_str("# TYPE yt_manifest_version gauge\n");
        out.push_str(&format!("yt_manifest_version {version}\n\n"));

        out.push_str(
            "# HELP yt_format_version 数据格式版本（persist::FORMAT_VER，升级迁移用）。\n",
        );
        out.push_str("# TYPE yt_format_version gauge\n");
        out.push_str(&format!("yt_format_version {}\n\n", persist::FORMAT_VER));

        out.push_str("# HELP yt_segments_live 活跃段数（含 sealed/live/compacting）。\n");
        out.push_str("# TYPE yt_segments_live gauge\n");
        out.push_str(&format!("yt_segments_live {segments}\n\n"));

        out.push_str("# HELP yt_memtable_rows 活内存表行数。\n");
        out.push_str("# TYPE yt_memtable_rows gauge\n");
        out.push_str(&format!("yt_memtable_rows {memtable_rows}\n\n"));

        out.push_str(
            "# HELP yt_segments_dead 待回收 dead 段数（compaction 摘下、等水位满足删）。\n",
        );
        out.push_str("# TYPE yt_segments_dead gauge\n");
        out.push_str(&format!("yt_segments_dead {dead}\n\n"));

        out.push_str("# HELP yt_readers_active 活跃快照读者数（pin 了某版本的）。\n");
        out.push_str("# TYPE yt_readers_active gauge\n");
        out.push_str(&format!("yt_readers_active {active_readers}\n\n"));

        out.push_str("# HELP yt_wal_committed_tail 已确认的最大 WAL LSN。\n");
        out.push_str("# TYPE yt_wal_committed_tail counter\n");
        out.push_str(&format!("yt_wal_committed_tail {committed_tail}\n\n"));

        out.push_str("# HELP yt_flush_threshold 内存表自动刷盘阈值（行数）。\n");
        out.push_str("# TYPE yt_flush_threshold gauge\n");
        out.push_str(&format!("yt_flush_threshold {flush_threshold}\n\n"));

        out.push_str("# HELP yt_read_model_rollup_ready rollup 是否已加载到当前进程。\n");
        out.push_str("# TYPE yt_read_model_rollup_ready gauge\n");
        out.push_str(&format!(
            "yt_read_model_rollup_ready {}\n\n",
            u8::from(read_model_state.rollup_ready)
        ));
        out.push_str("# HELP yt_read_model_filter_ready attrs 过滤索引是否已加载到当前进程。\n");
        out.push_str("# TYPE yt_read_model_filter_ready gauge\n");
        out.push_str(&format!(
            "yt_read_model_filter_ready {}\n\n",
            u8::from(read_model_state.filter_attrs_ready)
        ));
        out.push_str("# HELP yt_read_model_search_ready BM25 和 bloom 是否已加载到当前进程。\n");
        out.push_str("# TYPE yt_read_model_search_ready gauge\n");
        out.push_str(&format!(
            "yt_read_model_search_ready {}\n\n",
            u8::from(search_read_model_ready)
        ));

        out.push_str("# HELP yt_filter_attrs 检索过滤属性边车条目数。\n");
        out.push_str("# TYPE yt_filter_attrs gauge\n");
        out.push_str(&format!("yt_filter_attrs {filter_attrs}\n\n"));
        out.push_str("# HELP yt_filter_attr_postings 检索过滤属性 postings 条目数。\n");
        out.push_str("# TYPE yt_filter_attr_postings gauge\n");
        out.push_str(&format!(
            "yt_filter_attr_postings {filter_attr_postings}\n\n"
        ));
        out.push_str(
            "# HELP yt_filter_attr_disabled_postings 被预算禁用的过滤属性 postings 数。\n",
        );
        out.push_str("# TYPE yt_filter_attr_disabled_postings gauge\n");
        out.push_str(&format!(
            "yt_filter_attr_disabled_postings {filter_attr_disabled_postings}\n\n"
        ));

        out.push_str("# HELP yt_fold_cache_entries 段折叠缓存条目数（解码后的段）。\n");
        out.push_str("# TYPE yt_fold_cache_entries gauge\n");
        out.push_str(&format!("yt_fold_cache_entries {fold_cache_entries}\n\n"));

        out.push_str("# HELP yt_seg_bloom_count 段级 key Bloom 条目数。\n");
        out.push_str("# TYPE yt_seg_bloom_count gauge\n");
        out.push_str(&format!("yt_seg_bloom_count {bloom_count}\n\n"));

        if let Some(lock) = process_lock_metrics {
            out.push_str("# HELP yt_process_lock_acquire_total embedded 进程锁 acquire 次数。\n");
            out.push_str("# TYPE yt_process_lock_acquire_total counter\n");
            out.push_str(&format!(
                "yt_process_lock_acquire_total {}\n\n",
                lock.acquire_count
            ));

            out.push_str(
                "# HELP yt_process_lock_try_acquire_total embedded 进程锁 try_acquire 次数。\n",
            );
            out.push_str("# TYPE yt_process_lock_try_acquire_total counter\n");
            out.push_str(&format!(
                "yt_process_lock_try_acquire_total {}\n\n",
                lock.try_acquire_count
            ));

            out.push_str("# HELP yt_process_lock_wait_total embedded 进程锁发生等待的次数。\n");
            out.push_str("# TYPE yt_process_lock_wait_total counter\n");
            out.push_str(&format!(
                "yt_process_lock_wait_total {}\n\n",
                lock.wait_count
            ));

            out.push_str(
                "# HELP yt_process_lock_active_waiters 当前正在等 embedded 进程锁的线程数。\n",
            );
            out.push_str("# TYPE yt_process_lock_active_waiters gauge\n");
            out.push_str(&format!(
                "yt_process_lock_active_waiters {}\n\n",
                lock.active_wait_count
            ));

            out.push_str(
                "# HELP yt_process_lock_wait_seconds_total embedded 进程锁累计等待秒数。\n",
            );
            out.push_str("# TYPE yt_process_lock_wait_seconds_total counter\n");
            out.push_str(&format!(
                "yt_process_lock_wait_seconds_total {}\n\n",
                (lock.wait_ns as f64) / 1_000_000_000.0
            ));

            out.push_str("# HELP yt_process_lock_timeout_total embedded 进程锁等待超时次数。\n");
            out.push_str("# TYPE yt_process_lock_timeout_total counter\n");
            out.push_str(&format!(
                "yt_process_lock_timeout_total {}\n\n",
                lock.timeout_count
            ));

            out.push_str("# HELP yt_process_lock_try_busy_total try_acquire 发现锁正忙的次数。\n");
            out.push_str("# TYPE yt_process_lock_try_busy_total counter\n");
            out.push_str(&format!(
                "yt_process_lock_try_busy_total {}\n\n",
                lock.try_busy_count
            ));

            out.push_str("# HELP yt_process_lock_stale_cleared_total 清掉 stale 进程锁的次数。\n");
            out.push_str("# TYPE yt_process_lock_stale_cleared_total counter\n");
            out.push_str(&format!(
                "yt_process_lock_stale_cleared_total {}\n\n",
                lock.stale_lock_cleared_count
            ));

            out.push_str("# HELP yt_process_reader_pin_total 跨进程 reader pin 创建次数。\n");
            out.push_str("# TYPE yt_process_reader_pin_total counter\n");
            out.push_str(&format!(
                "yt_process_reader_pin_total {}\n\n",
                lock.reader_pin_count
            ));

            out.push_str(
                "# HELP yt_process_reader_stale_cleared_total 清掉 stale reader pin 的次数。\n",
            );
            out.push_str("# TYPE yt_process_reader_stale_cleared_total counter\n");
            out.push_str(&format!(
                "yt_process_reader_stale_cleared_total {}\n\n",
                lock.stale_reader_cleared_count
            ));
        }

        out.push_str("# HELP yt_datasets 评测数据集数。\n");
        out.push_str("# TYPE yt_datasets gauge\n");
        out.push_str(&format!("yt_datasets {datasets}\n"));

        out
    }

    /// 当前引擎支持的数据格式版本（persist::FORMAT_VER）。
    pub fn format_version() -> u32 {
        persist::FORMAT_VER
    }

    /// 检查数据目录的 manifest 版本：返回 (磁盘上的版本, 引擎支持的版本)。
    /// 两者相等 = 兼容；磁盘 < 引擎 = 需迁移；磁盘 > 引擎 = 需新引擎。
    /// 无 manifest = 新目录（返回 (0, FORMAT_VER)）。
    pub fn check_format(dir: impl AsRef<std::path::Path>) -> (u32, u32) {
        let manifest_path = dir.as_ref().join("manifest.dat");
        match std::fs::read(&manifest_path) {
            Ok(bytes) => {
                // 文件布局：[crc32 u32][MAGIC u32][FORMAT_VER u32]...
                // 跳过 4 字节 crc 前缀读 magic + version。
                if bytes.len() < 12 {
                    return (0, persist::FORMAT_VER);
                }
                let magic = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
                if magic != 0x5654_4D46 {
                    return (0, persist::FORMAT_VER); // 损坏或非本格式
                }
                let disk_ver = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
                (disk_ver, persist::FORMAT_VER)
            }
            Err(_) => (0, persist::FORMAT_VER), // 无文件 = 新目录
        }
    }

    /// **迁移骨架**（§3.4）：把数据目录从 `from_ver` 升级到当前引擎版本。
    ///
    /// 当前 FORMAT_VER=1，无历史老版本数据，所以 from_ver 只可能是 1（无操作）或损坏（报错）。
    /// 真实迁移工具的逻辑（版本 1→2、2→3…）会在引入格式变更时逐版本实现，沿这个签名扩展。
    pub fn migrate(dir: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        let (disk, current) = Self::check_format(&dir);
        match disk.cmp(&current) {
            std::cmp::Ordering::Equal => {
                olog::log(
                    olog::Level::Info,
                    "migrate",
                    &[("status", &"already current"), ("ver", &disk)],
                );
                Ok(())
            }
            std::cmp::Ordering::Less => {
                olog::log(
                    olog::Level::Error,
                    "migrate",
                    &[
                        ("status", &"old version not yet supported"),
                        ("disk", &disk),
                        ("engine", &current),
                    ],
                );
                Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    format!(
                        "从格式版本 {} 迁移到 {} 尚未实现（当前引擎无历史老版本数据）",
                        disk, current
                    ),
                ))
            }
            std::cmp::Ordering::Greater => {
                olog::log(
                    olog::Level::Error,
                    "migrate",
                    &[
                        ("status", &"data newer than engine"),
                        ("disk", &disk),
                        ("engine", &current),
                    ],
                );
                Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    format!(
                        "数据格式版本 {} 比引擎支持的 {} 新，需升级引擎",
                        disk, current
                    ),
                ))
            }
        }
    }
}
