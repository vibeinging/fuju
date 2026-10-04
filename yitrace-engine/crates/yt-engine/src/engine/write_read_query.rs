impl WriteCoordinator {
    /// 读路径：在固定快照上跨四源折叠出可见的所有 span（草案 2 §D2.2 端到端，全开窗）。
    pub fn read_spans(&self, snap: &Snapshot) -> Vec<FoldedSpan> {
        self.read_spans_query(snap, &TraceQuery::all()).0
    }

    /// 取某段的折叠缓存（不可变段，首次解码全列 + 建 (trace,span)→行号 索引，之后命中直接用）。
    fn seg_fold(&self, seg: SegmentId) -> Arc<SegFold> {
        {
            let mut c = self.seg_fold_cache.lock().unwrap();
            c.tick += 1;
            let t = c.tick;
            if let Some(e) = c.map.get_mut(&seg.get()) {
                e.1 = t;
                return e.0.clone();
            }
        }
        // 未命中：在锁外解码整段一次（之后所有查询命中缓存）。
        let raw = self.segments.scan_fold_inputs(seg);
        let mut rows = Vec::with_capacity(raw.len());
        let mut by_key: HashMap<(u64, u64), Vec<u32>> = HashMap::new();
        for (row, fi) in raw {
            by_key
                .entry((fi.trace_id, fi.span_id))
                .or_default()
                .push(row);
            rows.push(fi);
        }
        let n = rows.len();
        let sf = Arc::new(SegFold { rows, by_key });
        let mut c = self.seg_fold_cache.lock().unwrap();
        c.tick += 1;
        let tk = c.tick;
        if let Some((old, _)) = c.map.insert(seg.get(), (sf.clone(), tk)) {
            c.cur_rows -= old.rows.len();
        }
        c.cur_rows += n;
        if c.cur_rows > c.cap_rows {
            c.evict();
        }
        sf
    }

    /// 带剪枝的读路径。按时间窗（段 zone-map）+ trace_id 剪枝，减少触及的段数（活 trace 读扇出上界）。
    /// 返回 (折叠出的 span, 实际扫描的段数)。所有判定只用快照里钉死的版本。
    pub fn read_spans_query(&self, snap: &Snapshot, q: &TraceQuery) -> (Vec<FoldedSpan>, usize) {
        // 普通读 / trace 详情要原文,读全列。
        // trace_id 是点查，不应退化成逐段解码。filter sidecar 已经保存了该 trace 的 span key，
        // 交给候选 key 快路后，段级 bloom 会把无关 segment 直接跳过。
        if let Some(trace_id) = q.trace_id {
            let filter = SearchFilter {
                trace_id: Some(trace_id),
                tenant_id: q.tenant_id,
                ..Default::default()
            };
            let keys = self.filter_candidate_span_keys_for_snapshot(snap, &filter);
            if !keys.is_empty() {
                let (spans, stats) = self.fold_query(snap, q, Some(&keys), Projection::ALL);
                return (spans, stats.scanned_segments);
            }
        }
        let (spans, stats) = self.fold_query(snap, q, None, Projection::ALL);
        (spans, stats.scanned_segments)
    }

    /// 结构化读的索引入口：能用派生过滤索引时只折叠候选 span，不能用时回退到普通扫描。
    ///
    /// 这里不改变查询语义：调用方仍需在返回 span 上做最终过滤。索引只负责缩小候选集合，
    /// 尤其服务 traceSearch/aggregate/loop/task 这类“先过滤再聚合”的读模型。
    pub fn read_spans_query_indexed(
        &self,
        snap: &Snapshot,
        q: &TraceQuery,
        filter: &SearchFilter,
        proj: Projection,
    ) -> (Vec<FoldedSpan>, ReadPlanStats) {
        let unsupported_attr_keys = filter
            .attrs
            .keys()
            .filter(|key| !is_filter_attr_key(key))
            .cloned()
            .collect::<Vec<_>>();
        let mut candidate_filter = filter.clone();
        candidate_filter
            .attrs
            .retain(|key, _| is_filter_attr_key(key));

        let mut stats = ReadPlanStats {
            unsupported_attr_keys,
            ..ReadPlanStats::default()
        };

        if candidate_filter.needs_attrs() {
            let keys = self.filter_candidate_span_keys_for_snapshot(snap, &candidate_filter);
            stats.used_filter_index = true;
            stats.candidate_span_keys = Some(keys.len());
            if keys.is_empty() {
                return (Vec::new(), stats);
            }
            let (spans, scan) = self.fold_query(snap, q, Some(&keys), proj);
            stats.scanned_segments = scan.scanned_segments;
            stats.point_lookup_segments = scan.point_lookup_segments;
            stats.decoded_segment_rows = scan.decoded_segment_rows;
            stats.decoded_memtable_rows = scan.decoded_memtable_rows;
            stats.index_bytes_read = scan.index_bytes_read;
            stats.data_bytes_read = scan.data_bytes_read;
            stats.indexes_validated = scan.indexes_validated;
            stats.indexes_rebuilt = scan.indexes_rebuilt;
            stats.matched_spans = spans.len();
            return (spans, stats);
        }

        stats.fallback_reason = Some(if stats.unsupported_attr_keys.is_empty() {
            "no_indexed_filter".to_string()
        } else {
            "unsupported_attr_keys_only".to_string()
        });
        let (spans, scan) = self.fold_query(snap, q, None, proj);
        stats.scanned_segments = scan.scanned_segments;
        stats.point_lookup_segments = scan.point_lookup_segments;
        stats.decoded_segment_rows = scan.decoded_segment_rows;
        stats.decoded_memtable_rows = scan.decoded_memtable_rows;
        stats.index_bytes_read = scan.index_bytes_read;
        stats.data_bytes_read = scan.data_bytes_read;
        stats.indexes_validated = scan.indexes_validated;
        stats.indexes_rebuilt = scan.indexes_rebuilt;
        stats.matched_spans = spans.len();
        (spans, stats)
    }
}
