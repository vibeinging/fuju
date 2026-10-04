impl WriteCoordinator {
    /// 折叠核心。`keys=Some(集合)` 时**只折叠命中这些 (trace,span) 的行**（检索用：先由索引拿到命中 key,
    /// 只折叠它们,不折叠全库）；`None` = 折叠全部（普通读）。`proj` 声明要读哪些可折叠值列——列式段据此
    /// 跳过不读的列（尤其大文本列），行式/内存源忽略它（无列 I/O 可省）。文件段有 key 目录时直接点查
    /// 物理记录；旧段或其他存储不支持时再回退整段扫描。
    fn fold_query(
        &self,
        snap: &Snapshot,
        q: &TraceQuery,
        keys: Option<&std::collections::HashSet<ScopedSpanKey>>,
        proj: Projection,
    ) -> (Vec<FoldedSpan>, FoldQueryStats) {
        // 租户隔离时，强制把 tenant_id 列纳入投影（否则列式段窄投影读不到 tenant，过滤会误删全部）。
        let proj = Projection::of(proj.bits() | Projection::TENANT_ID);
        let mut inputs: Vec<FoldInput> = Vec::new();
        let mut stats = FoldQueryStats::default();
        let in_keys = |tenant: Option<u64>, t: u64, s: u64| {
            keys.map_or(true, |ks| ks.contains(&(tenant, t, s)))
        };
        let pair_keys = keys.map(|ks| ks.iter().map(|&(_, t, s)| (t, s)).collect::<HashSet<_>>());
        if keys.is_some() {
            stats = self.ensure_seg_key_bloom_for_manifest(&snap.manifest);
        }

        // 段源：先用段 zone-map(min_ts/max_ts) 做时间窗剪枝 —— 不重叠的段整段跳过、不扫。
        let mut upgrades: std::collections::BTreeMap<ScopedSpanKey, SpanFields> =
            std::collections::BTreeMap::new();
        for entry in snap.manifest.segments.values() {
            if entry.max_ts < q.time_from || entry.min_ts > q.time_to {
                continue; // 时间窗外，整段剪掉
            }
            let input_start = inputs.len();
            match keys {
                // ★ 检索快路：已知候选 key → 段级 bloom 跳段，只解码命中 key 的行。
                Some(ks) => {
                    // 这个段肯定没有任何候选 key → 整段跳过折叠定位（upgrade 仍在下面照常处理）。
                    let bloom_skip = self
                        .seg_key_bloom
                        .lock()
                        .unwrap()
                        .get(&entry.segment_id.get())
                        .map_or(false, |b| {
                            !ks.iter().any(|&(_, t, s)| b.maybe_contains((t, s)))
                        });
                    if !bloom_skip {
                        stats.scanned_segments += 1;
                        if let Some(scan) = self.segments.scan_fold_inputs_for_keys(
                            entry.segment_id,
                            pair_keys.as_ref().unwrap(),
                        ) {
                            stats.decoded_segment_rows += scan.decoded_rows;
                            stats.index_bytes_read =
                                stats.index_bytes_read.saturating_add(scan.index_bytes_read);
                            stats.data_bytes_read =
                                stats.data_bytes_read.saturating_add(scan.data_bytes_read);
                            stats.indexes_validated += scan.indexes_validated;
                            stats.indexes_rebuilt += scan.indexes_rebuilt;
                            if scan.used_point_index {
                                stats.point_lookup_segments += 1;
                            }
                            for (row, fi) in scan.rows {
                                if entry.deletion_vec.is_deleted(row) {
                                    continue; // 删除位图按行号照查
                                }
                                inputs.push(fi);
                            }
                        } else {
                            // 内存/测试段的兼容回退：仍使用段折叠缓存，但默认文件段不会走这里。
                            let sf = self.seg_fold(entry.segment_id);
                            stats.decoded_segment_rows += sf.rows.len();
                            for &(_tenant, t, s) in ks {
                                if q.trace_id.map_or(false, |tid| t != tid) {
                                    continue;
                                }
                                let Some(rowlist) = sf.by_key.get(&(t, s)) else {
                                    continue;
                                };
                                for &row in rowlist {
                                    if entry.deletion_vec.is_deleted(row) {
                                        continue;
                                    }
                                    inputs.push(sf.rows[row as usize].clone());
                                }
                            }
                        }
                    }
                }
                // 普通读/聚合：三条扫描路（投影 `proj` 贯穿——列式段据此只解码命中列）：
                //   ① 段无删除 + 有真实时间窗 → 时间下推 + 投影（丢行号，段无删除用不到）。
                //   ② 否则纯投影下推：只裁列、不丢行 → 行号完整，删除位图照行号生效。
                //   ③ 都不支持 → 回退 `scan_fold_inputs` 读全列。
                None => {
                    stats.scanned_segments += 1;
                    let time_pushed = if entry.deletion_seq == 0
                        && (q.time_from != i64::MIN || q.time_to != i64::MAX)
                    {
                        self.segments.scan_fold_inputs_in_time(
                            entry.segment_id,
                            q.time_from,
                            q.time_to,
                            proj,
                        )
                    } else {
                        None
                    };
                    match time_pushed {
                        Some(folds) => {
                            stats.decoded_segment_rows += folds.len();
                            for fi in folds {
                                if q.trace_id.map_or(false, |tid| fi.trace_id != tid) {
                                    continue;
                                }
                                inputs.push(fi);
                            }
                        }
                        None => {
                            let rows = match self
                                .segments
                                .scan_fold_inputs_projected(entry.segment_id, proj)
                            {
                                Some(rows) => rows,
                                None => {
                                    let scan =
                                        self.segments.scan_records_with_stats(entry.segment_id);
                                    stats.data_bytes_read =
                                        stats.data_bytes_read.saturating_add(scan.data_bytes_read);
                                    scan.rows
                                        .into_iter()
                                        .enumerate()
                                        .map(|(row, record)| {
                                            (
                                                row as u32,
                                                FoldInput {
                                                    trace_id: record.trace_id,
                                                    span_id: record.span_id,
                                                    identity: record.identity,
                                                    fields: record.fields,
                                                },
                                            )
                                        })
                                        .collect()
                                }
                            };
                            stats.decoded_segment_rows += rows.len();
                            for (row, fi) in rows {
                                if entry.deletion_vec.is_deleted(row) {
                                    continue;
                                }
                                if let Some(tid) = q.trace_id {
                                    if fi.trace_id != tid {
                                        continue;
                                    }
                                }
                                inputs.push(fi);
                            }
                        }
                    }
                }
            }
            if let Some(up) = &entry.upgrade_ref {
                for (&(t, s), patch) in up.iter() {
                    if q.trace_id.map_or(false, |tid| t != tid) {
                        continue;
                    }
                    if !keys
                        .is_none_or(|ks| ks.iter().any(|&(_, trace, span)| trace == t && span == s))
                    {
                        continue;
                    }
                    // 同一 span 跨段的多份 upgrade 也按 last-non-null + logs 并集合一起。
                    let tenant = if let Some(tenant) = patch.tenant_id {
                        Some(Some(tenant))
                    } else {
                        // 旧补写不带tenant；只有当前段的身份唯一时才恢复其归属。
                        let tenants = inputs[input_start..]
                            .iter()
                            .filter(|input| input.trace_id == t && input.span_id == s)
                            .map(|input| input.fields.tenant_id)
                            .collect::<HashSet<_>>();
                        if tenants.len() == 1 {
                            tenants.into_iter().next()
                        } else {
                            None
                        }
                    };
                    if let Some(tenant) = tenant {
                        let mut scoped_patch = patch.clone();
                        scoped_patch.tenant_id = tenant;
                        upgrades
                            .entry((tenant, t, s))
                            .or_default()
                            .merge_from(&scoped_patch);
                    }
                }
            }
        }

        // MemTable 源：半开区间 (retained_watermark, live_lsn]，再按时间窗 + trace_id 行级过滤。
        {
            let mt = self.memtable.lock().unwrap();
            let mut collect = |r: &MemRow| {
                if r.ts < q.time_from || r.ts > q.time_to {
                    return;
                }
                if let Some(tid) = q.trace_id {
                    if r.trace_id != tid {
                        return;
                    }
                }
                if !in_keys(r.fields.tenant_id, r.trace_id, r.span_id) {
                    return;
                }
                inputs.push(r.to_fold_input());
            };
            if keys.is_some() {
                let rows = mt.read_keys_range(
                    pair_keys.as_ref().unwrap(),
                    snap.retained_watermark,
                    snap.live_lsn,
                );
                stats.decoded_memtable_rows += rows.len();
                for r in rows {
                    collect(r);
                }
            } else {
                for r in mt.read_range(snap.retained_watermark, snap.live_lsn) {
                    stats.decoded_memtable_rows += 1;
                    collect(r);
                }
            }
        }

        // 四源 k 路归并折叠：event_id 去重、last-non-null-wins、logs union。
        inputs.retain(|input| {
            in_keys(input.fields.tenant_id, input.trace_id, input.span_id)
                && q.tenant_id
                    .is_none_or(|tenant| input.fields.tenant_id == Some(tenant))
        });
        let mut spans = fold_events(inputs);

        // upgrade 校正：晚到属性补写盖到对应 span 上（只覆盖非身份属性，非空才覆盖）。
        for sp in &mut spans {
            if let Some(patch) = upgrades.get(&(sp.tenant_id, sp.trace_id, sp.span_id)) {
                if patch.tenant_id == sp.tenant_id {
                    sp.apply_patch(patch);
                }
            }
        }
        // 租户隔离：只留本租户的 span（列表/读路径与检索路径一致地强制过滤）。
        if let Some(t) = q.tenant_id {
            spans.retain(|sp| sp.tenant_id == Some(t));
        }
        (spans, stats)
    }
}

impl WriteCoordinator {
    pub(crate) fn snapshot_is_current(&self, snap: &Snapshot) -> bool {
        let current = self.current.manifest();
        current.version == snap.manifest.version
            && self.current.committed_tail() == snap.live_lsn.get()
    }
    fn records_for_snapshot(
        &self,
        snap: &Snapshot,
    ) -> (Vec<WalRecord>, Vec<(ScopedSpanKey, SpanFields)>) {
        let (mut records, patches) = self.collect_segment_rollup_parts(&snap.manifest);
        let mt = self.memtable.lock().unwrap();
        records.extend(
            mt.read_range(snap.retained_watermark, snap.live_lsn)
                .into_iter()
                .map(|r| WalRecord {
                    trace_id: r.trace_id,
                    span_id: r.span_id,
                    ts: r.ts,
                    identity: r.identity.clone(),
                    fields: r.fields.clone(),
                }),
        );
        (records, patches)
    }
    pub(crate) fn filter_candidate_span_keys_for_snapshot(
        &self,
        snap: &Snapshot,
        filter: &SearchFilter,
    ) -> HashSet<ScopedSpanKey> {
        self.ensure_filter_attrs_current();
        {
            let _guard = self.write_lock.lock().unwrap();
            if self.snapshot_is_current(snap) {
                return self
                    .filter_attrs
                    .lock()
                    .unwrap()
                    .candidate_span_keys(filter);
            }
        }
        let (records, patches) = self.records_for_snapshot(snap);
        FilterAttrsIndex::from_records(records, patches).candidate_span_keys(filter)
    }
    pub(crate) fn search_text_snapshot_scoped(
        &self,
        snap: &Snapshot,
        query: &str,
        k: usize,
        filter: &dyn Fn(Option<u64>, u64, u64) -> bool,
    ) -> Vec<(Option<u64>, u64, u64, f32)> {
        // 旧快照必须独立取候选；当前索引先截top-k再回填会漏掉旧可见行。
        let index = self.bm25.empty_like().unwrap_or_else(|| {
            Arc::new(Bm25TextIndex::with_tokenizer(Box::new(
                ChineseTokenizer::default(),
            )))
        });
        let (records, patches) = self.records_for_snapshot(snap);
        if patches.is_empty() {
            for record in records {
                let fields = &record.fields;
                let mut parts = Vec::<&str>::new();
                for field in [
                    &fields.input_text,
                    &fields.output_text,
                    &fields.span_name,
                    &fields.agent_name,
                    &fields.tool_name,
                    &fields.model,
                ] {
                    if let Some(text) = field.as_deref() {
                        parts.push(text);
                    }
                }
                parts.extend(fields.logs.iter().map(String::as_str));
                index.index_event_scoped(
                    fields.tenant_id,
                    record.identity.event_id().0,
                    record.trace_id,
                    record.span_id,
                    &parts.join(" "),
                );
            }
        } else {
            // upgrade 是明确覆盖值；与维护操作后的派生文本重建同口径。
            for span in self
                .fold_query(snap, &TraceQuery::all(), None, Projection::ALL)
                .0
            {
                let mut parts = Vec::<&str>::new();
                for field in [
                    &span.input_text,
                    &span.output_text,
                    &span.span_name,
                    &span.agent_name,
                    &span.tool_name,
                    &span.model,
                ] {
                    if let Some(text) = field.as_deref() {
                        parts.push(text);
                    }
                }
                parts.extend(span.logs.iter().map(String::as_str));
                index.index_text_scoped(
                    span.tenant_id,
                    span.trace_id,
                    span.span_id,
                    &parts.join(" "),
                );
            }
        }
        index.search_scoped(query, k, filter)
    }
}
