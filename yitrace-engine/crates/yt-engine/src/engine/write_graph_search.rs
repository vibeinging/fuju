/// 查询期间允许临时物化的过滤 key 上限。按每个 HashSet key 约 48 字节估算，超过预算
/// 就回到磁盘逐条校验，避免低选择性属性把进程内存顶满。
const DEFAULT_BM25_FILTER_SET_BUDGET_BYTES: usize = 64 * 1024 * 1024;
const BM25_FILTER_KEY_ESTIMATED_BYTES: usize = 48;

fn bm25_filter_set_budget_bytes() -> usize {
    std::env::var("YT_BM25_FILTER_SET_BUDGET_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_BM25_FILTER_SET_BUDGET_BYTES)
}

impl WriteCoordinator {
    /// 装一条 trace 的父子树（树+瀑布视图用）：读出该 trace 的 span，按 parent_span_id 连成树。
    /// 父不在本 trace 内的 span 当根（容错：丢了 root 事件也能渲染）。
    pub fn load_trace_tree(&self, snap: &Snapshot, trace_id: u64) -> TraceTree {
        let (spans, _) =
            self.read_spans_query(snap, &TraceQuery::trace(trace_id, i64::MIN, i64::MAX));
        let mut nodes: BTreeMap<u64, TraceNode> = BTreeMap::new();
        for s in spans {
            nodes.insert(
                s.span_id,
                TraceNode {
                    span: s,
                    children: Vec::new(),
                },
            );
        }
        let mut roots = Vec::new();
        let ids: Vec<u64> = nodes.keys().copied().collect();
        for id in ids {
            let parent = nodes[&id].span.parent_span_id;
            match parent {
                Some(p) if nodes.contains_key(&p) => nodes.get_mut(&p).unwrap().children.push(id),
                _ => roots.push(id),
            }
        }
        for n in nodes.values_mut() {
            n.children.sort_unstable(); // 确定序
        }
        roots.sort_unstable();
        TraceTree {
            trace_id,
            roots,
            nodes,
        }
    }

    /// 一条 trace 的 **agent 执行图（DAG）**：把 span 父子树按 agent/工具维度收拢成"谁调用了谁"。
    /// 角色判定:有 tool_name → Tool;否则有 agent_name → Agent;都没有 → `span:<id>`(Other)。
    /// 边 = 父 span 的角色 → 子 span 的角色(同角色自环剔除,只留跨角色调用/移交),按出现次数聚合。
    /// 节点带聚合统计(span 数、token)。节点/边都确定排序,可复算。
    pub fn agent_graph(&self, snap: &Snapshot, trace_id: u64) -> AgentGraph {
        // 执行图按 agent/工具/父子连边 + 聚合 token —— 只读这些维度,不读原文。
        let proj = Projection::of(
            Projection::AGENT_NAME
                | Projection::TOOL_NAME
                | Projection::PARENT_SPAN_ID
                | Projection::INPUT_TOKENS
                | Projection::OUTPUT_TOKENS
                | Projection::CACHE_READ_TOKENS
                | Projection::CACHE_WRITE_TOKENS,
        );
        let (spans, _) = self.fold_query(
            snap,
            &TraceQuery::trace(trace_id, i64::MIN, i64::MAX),
            None,
            proj,
        );

        // 角色判定（返回 (名字, 类型)）。
        let actor_of = |s: &FoldedSpan| -> (String, ActorKind) {
            if let Some(t) = &s.tool_name {
                (t.clone(), ActorKind::Tool)
            } else if let Some(a) = &s.agent_name {
                (a.clone(), ActorKind::Agent)
            } else {
                (format!("span:{}", s.span_id), ActorKind::Other)
            }
        };

        // span_id → 角色名，供连边时查父角色。
        let mut span_actor: HashMap<u64, String> = HashMap::new();
        // 节点聚合：actor → (kind, span_count, in_tok, out_tok)。
        let mut nodes: BTreeMap<String, (ActorKind, usize, u64, u64, u64, u64, usize, usize)> =
            BTreeMap::new();
        for s in &spans {
            let (name, kind) = actor_of(s);
            span_actor.insert(s.span_id, name.clone());
            let e = nodes.entry(name).or_insert((kind, 0, 0, 0, 0, 0, 0, 0));
            e.1 += 1;
            e.2 += s.input_tokens.unwrap_or(0);
            e.3 += s.output_tokens.unwrap_or(0);
            if let Some(tokens) = s.cache_read_tokens {
                e.4 += tokens;
                e.6 += 1;
            }
            if let Some(tokens) = s.cache_write_tokens {
                e.5 += tokens;
                e.7 += 1;
            }
        }

        // 边聚合：父角色 → 子角色（跳过父不在本 trace 内 / 同角色自环）。
        let mut edges: BTreeMap<(String, String), usize> = BTreeMap::new();
        for s in &spans {
            let Some(parent_id) = s.parent_span_id else {
                continue;
            };
            let Some(from) = span_actor.get(&parent_id) else {
                continue;
            };
            let to = &span_actor[&s.span_id];
            if from == to {
                continue; // 同角色多步,不算一次调用/移交
            }
            *edges.entry((from.clone(), to.clone())).or_insert(0) += 1;
        }

        AgentGraph {
            trace_id,
            nodes: nodes
                .into_iter()
                .map(
                    |(
                        actor,
                        (
                            kind,
                            span_count,
                            input_tokens,
                            output_tokens,
                            read,
                            write,
                            read_n,
                            write_n,
                        ),
                    )| AgentGraphNode {
                        actor,
                        kind,
                        span_count,
                        input_tokens,
                        output_tokens,
                        cache_read_tokens: (read_n > 0).then_some(read),
                        cache_write_tokens: (write_n > 0).then_some(write),
                    },
                )
                .collect(),
            edges: edges
                .into_iter()
                .map(|((from, to), count)| AgentGraphEdge { from, to, count })
                .collect(),
        }
    }

    /// 旧图缺tenant记录时，仅在首次向量操作依据真实span解析；多租户同pair拒绝猜测。
    pub fn try_prepare_vector_search(&self) -> std::io::Result<()> {
        if !self.graph.needs_legacy_tenant_migration() {
            return Ok(());
        }
        let _process = self.try_acquire_process_lock("write")?;
        let _local = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        self.migrate_legacy_vector_tenants_locked()
    }
    fn migrate_legacy_vector_tenants_locked(&self) -> std::io::Result<()> {
        if !self.graph.needs_legacy_tenant_migration() {
            return Ok(());
        }
        let keys = self
            .graph
            .legacy_embedding_keys()?
            .into_iter()
            .collect::<HashSet<_>>();
        let snap = self.current.pin_snapshot();
        let mut tenants = HashMap::new();
        for span in self
            .fold_query(&snap, &TraceQuery::all(), None, Projection::ALL)
            .0
        {
            let key = (span.trace_id, span.span_id);
            if !keys.contains(&key) {
                continue;
            }
            if let Some(old) = tenants.insert(key, span.tenant_id) {
                if old != span.tenant_id {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData,format!("ambiguous legacy vector tenant for trace {} span {}; reindex vectors with explicit tenants",key.0,key.1)));
                }
            }
        }
        self.graph.migrate_legacy_embeddings(&tenants)
    }

    /// 无租户旧入口保持原身份；有租户调用方须显式使用 index_embedding_for_tenant。
    pub fn index_embedding(&self, trace_id: u64, span_id: u64, embedding: Vec<f32>) {
        self.index_embedding_for_tenant(None, trace_id, span_id, embedding)
            .expect("vector write failed");
    }
    pub fn index_embedding_for_tenant(
        &self,
        tenant_id: Option<u64>,
        trace_id: u64,
        span_id: u64,
        embedding: Vec<f32>,
    ) -> std::io::Result<()> {
        if embedding.is_empty() || embedding.iter().any(|v| !v.is_finite()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "embedding must contain finite values",
            ));
        }
        if tenant_id.is_some() && !self.graph.supports_tenant_scope() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "graph adapter does not support tenant-scoped embeddings",
            ));
        }
        let _process = self.try_acquire_process_lock("write")?;
        let _local = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        self.migrate_legacy_vector_tenants_locked()?;
        if let Some(path) = &self.vector_path {
            if tenant_id.is_none() {
                vecstore::append(path, trace_id, span_id, &embedding)?;
            } else {
                vecstore::append_scoped(
                    path.with_file_name("vectors_scoped.dat"),
                    tenant_id,
                    trace_id,
                    span_id,
                    &embedding,
                )?;
            }
        }
        self.graph
            .index_embedding_scoped(tenant_id, trace_id, span_id, embedding)?;
        self.graph.flush_checked()
    }

    pub fn search_text(&self, snap: &Snapshot, query: &str, k: usize) -> Vec<(FoldedSpan, f32)> {
        let candidates = self.bm25_all_for_snapshot(snap, query, k);
        self.join_folded(snap, candidates)
    }
    pub fn search_text_filtered(
        &self,
        snap: &Snapshot,
        query: &str,
        k: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(FoldedSpan, f32)> {
        let pred = |_: Option<u64>, t, s| filter(t, s);
        let candidates = self.bm25_for_snapshot(snap, query, k, &pred);
        self.join_folded(snap, candidates)
    }
    fn bm25_all_for_snapshot(
        &self,
        snap: &Snapshot,
        query: &str,
        k: usize,
    ) -> Vec<(Option<u64>, u64, u64, f32)> {
        self.ensure_segment_scan_indexes_current();
        {
            let _guard = self.write_lock.lock().unwrap();
            if self.snapshot_is_current(snap) {
                return self.bm25.search_all_scoped(query, k);
            }
        }
        self.search_text_snapshot_scoped(snap, query, k, &|_, _, _| true)
    }
    fn bm25_for_snapshot(
        &self,
        snap: &Snapshot,
        query: &str,
        k: usize,
        filter: &dyn Fn(Option<u64>, u64, u64) -> bool,
    ) -> Vec<(Option<u64>, u64, u64, f32)> {
        self.ensure_segment_scan_indexes_current();
        {
            // 派生索引和尾水位必须作为一次读观察，避免写入尚未提交时进入候选。
            let _guard = self.write_lock.lock().unwrap();
            if self.snapshot_is_current(snap) {
                return self.bm25.search_scoped(query, k, filter);
            }
        }
        self.search_text_snapshot_scoped(snap, query, k, filter)
    }
    fn vectors_for_snapshot(
        &self,
        snap: &Snapshot,
        query: &[f32],
        k: usize,
        filter: &dyn Fn(Option<u64>, u64, u64) -> bool,
    ) -> Vec<(Option<u64>, u64, u64, f32)> {
        {
            let _guard = self.write_lock.lock().unwrap();
            if self.snapshot_is_current(snap) {
                return self.graph.search_scoped(query, k, filter);
            }
        }
        let visible = self
            .read_spans(snap)
            .into_iter()
            .map(|s| (s.tenant_id, s.trace_id, s.span_id))
            .collect::<HashSet<_>>();
        self.graph.search_scoped(query, k, &|tenant, t, s| {
            filter(tenant, t, s) && visible.contains(&(tenant, t, s))
        })
    }
    pub fn search_similar(
        &self,
        snap: &Snapshot,
        query: &[f32],
        k: usize,
    ) -> Vec<(FoldedSpan, f32)> {
        self.search_similar_filtered(snap, query, k, &|_, _| true)
    }
    pub fn search_similar_filtered(
        &self,
        snap: &Snapshot,
        query: &[f32],
        k: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(FoldedSpan, f32)> {
        if let Err(error) = self.try_prepare_vector_search() {
            eprintln!("vector search failed: {error}");
            return Vec::new();
        }
        let predicate = |_: Option<u64>, t, s| filter(t, s);
        let candidates = self.vectors_for_snapshot(snap, query, k, &predicate);
        self.join_folded(snap, candidates)
    }
    pub fn search_hybrid(
        &self,
        snap: &Snapshot,
        text: &str,
        query_vec: &[f32],
        k: usize,
    ) -> Vec<(FoldedSpan, f32)> {
        self.search_hybrid_filtered(snap, text, query_vec, k, &|_, _| true)
    }
    pub fn search_hybrid_filtered(
        &self,
        snap: &Snapshot,
        text: &str,
        query_vec: &[f32],
        k: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(FoldedSpan, f32)> {
        if let Err(error) = self.try_prepare_vector_search() {
            eprintln!("vector search failed: {error}");
            return Vec::new();
        }
        let predicate = |_: Option<u64>, t, s| filter(t, s);
        let pool = k.max(10);
        let bm = self.bm25_for_snapshot(snap, text, pool, &predicate);
        let vectors = self.vectors_for_snapshot(snap, query_vec, pool, &predicate);
        self.join_folded(snap, fuse_scoped(bm, vectors, k))
    }

    fn vector_attr_candidates(
        &self,
        snap: &Snapshot,
        query: &[f32],
        k: usize,
        filter: &SearchFilter,
    ) -> Vec<(Option<u64>, u64, u64, f32)> {
        if !filter.needs_attrs() {
            return self.vectors_for_snapshot(snap, query, k, &|_, t, _| {
                filter.trace_id.map_or(true, |expected| expected == t)
            });
        }
        let keys = self.filter_candidate_span_keys_for_snapshot(snap, filter);
        self.graph
            .search_scoped(query, k, &|tenant, t, s| keys.contains(&(tenant, t, s)))
    }
    pub fn search_similar_attr(
        &self,
        snap: &Snapshot,
        query: &[f32],
        k: usize,
        filter: &SearchFilter,
    ) -> Vec<(FoldedSpan, f32)> {
        if let Err(error) = self.try_prepare_vector_search() {
            eprintln!("vector search failed: {error}");
            return Vec::new();
        }
        let candidates = self.vector_attr_candidates(snap, query, k, filter);
        self.join_folded(snap, candidates)
    }
    pub fn search_text_attr(
        &self,
        snap: &Snapshot,
        query: &str,
        k: usize,
        filter: &SearchFilter,
    ) -> Vec<(FoldedSpan, f32)> {
        let candidates = self.search_text_attr_candidates(snap, query, k, filter);
        self.join_folded(snap, candidates)
    }
    pub fn search_text_attr_with_read_plan(
        &self,
        snap: &Snapshot,
        query: &str,
        k: usize,
        filter: &SearchFilter,
    ) -> (Vec<(FoldedSpan, f32)>, ReadPlanStats) {
        let candidates = self.search_text_attr_candidates(snap, query, k, filter);
        let candidate_span_keys = candidates
            .iter()
            .map(|&(tenant, t, s, _)| (tenant, t, s))
            .collect::<HashSet<_>>()
            .len();
        let (hits, scan) = self.join_folded_with_stats(snap, candidates);
        let plan = ReadPlanStats {
            used_filter_index: filter.needs_indexed_filter(),
            candidate_span_keys: Some(candidate_span_keys),
            scanned_segments: scan.scanned_segments,
            point_lookup_segments: scan.point_lookup_segments,
            decoded_segment_rows: scan.decoded_segment_rows,
            decoded_memtable_rows: scan.decoded_memtable_rows,
            index_bytes_read: scan.index_bytes_read,
            data_bytes_read: scan.data_bytes_read,
            indexes_validated: scan.indexes_validated,
            indexes_rebuilt: scan.indexes_rebuilt,
            matched_spans: hits.len(),
            fallback_reason: scan.fallback_reason,
            ..ReadPlanStats::default()
        };
        (hits, plan)
    }
    fn search_text_attr_candidates(
        &self,
        snap: &Snapshot,
        query: &str,
        k: usize,
        filter: &SearchFilter,
    ) -> Vec<(Option<u64>, u64, u64, f32)> {
        self.ensure_segment_scan_indexes_current();
        self.ensure_filter_attrs_current();
        {
            let _guard = self.write_lock.lock().unwrap();
            if self.snapshot_is_current(snap) {
                if !filter.needs_attrs() {
                    if filter.trace_id.is_none() {
                        return self.bm25.search_all_scoped(query, k);
                    }
                    return self.bm25.search_scoped(query, k, &|_, t, _| {
                        filter.trace_id.map_or(true, |expected| expected == t)
                    });
                }
                let mut index = self.filter_attrs.lock().unwrap();
                if index.filter_matches_all(filter) {
                    return self.bm25.search_all_scoped(query, k);
                }
                if index
                    .candidate_materialization_key_hint(filter)
                    .is_some_and(|n| {
                        n.saturating_mul(BM25_FILTER_KEY_ESTIMATED_BYTES)
                            > bm25_filter_set_budget_bytes()
                    })
                {
                    // 候选太多时逐项检查，持写锁让attrs和BM25看同一个已提交版本。
                    let index = std::cell::RefCell::new(index);
                    return self.bm25.search_scoped(query, k, &|tenant, t, s| {
                        index.borrow_mut().span_matches((tenant, t, s), filter)
                    });
                }
                let keys = index.candidate_span_keys(filter);
                return self
                    .bm25
                    .search_scoped(query, k, &|tenant, t, s| keys.contains(&(tenant, t, s)));
            }
        }
        let keys = self.filter_candidate_span_keys_for_snapshot(snap, filter);
        self.search_text_snapshot_scoped(snap, query, k, &|tenant, t, s| {
            keys.contains(&(tenant, t, s))
        })
    }
    pub fn search_hybrid_attr(
        &self,
        snap: &Snapshot,
        text: &str,
        query_vec: &[f32],
        k: usize,
        filter: &SearchFilter,
    ) -> Vec<(FoldedSpan, f32)> {
        if let Err(error) = self.try_prepare_vector_search() {
            eprintln!("vector search failed: {error}");
            return Vec::new();
        }
        let pool = k.max(10);
        let bm = self.search_text_attr_candidates(snap, text, pool, filter);
        let vectors = self.vector_attr_candidates(snap, query_vec, pool, filter);
        self.join_folded(snap, fuse_scoped(bm, vectors, k))
    }
    fn join_folded(
        &self,
        snap: &Snapshot,
        candidates: Vec<(Option<u64>, u64, u64, f32)>,
    ) -> Vec<(FoldedSpan, f32)> {
        self.join_folded_with_stats(snap, candidates).0
    }
    fn join_folded_with_stats(
        &self,
        snap: &Snapshot,
        candidates: Vec<(Option<u64>, u64, u64, f32)>,
    ) -> (Vec<(FoldedSpan, f32)>, FoldQueryStats) {
        let mut seen = HashSet::new();
        let candidates: Vec<_> = candidates
            .into_iter()
            .filter(|&(tenant, t, s, _)| seen.insert((tenant, t, s)))
            .collect();
        let keys = candidates
            .iter()
            .map(|&(tenant, t, s, _)| (tenant, t, s))
            .collect();
        let (hits, stats) = self.fold_query(snap, &TraceQuery::all(), Some(&keys), Projection::ALL);
        let map: HashMap<_, _> = hits
            .into_iter()
            .map(|span| ((span.tenant_id, span.trace_id, span.span_id), span))
            .collect();
        let hits = candidates
            .into_iter()
            .filter_map(|(tenant, t, s, score)| {
                map.get(&(tenant, t, s)).cloned().map(|span| (span, score))
            })
            .collect();
        (hits, stats)
    }
}

fn fuse_scoped(
    bm: Vec<(Option<u64>, u64, u64, f32)>,
    vectors: Vec<(Option<u64>, u64, u64, f32)>,
    k: usize,
) -> Vec<(Option<u64>, u64, u64, f32)> {
    let mut scores = BTreeMap::new();
    for ranking in [bm, vectors] {
        for (rank, (tenant, t, s, _)) in ranking.into_iter().enumerate() {
            *scores.entry((tenant, t, s)).or_insert(0.0f32) += 1.0 / (60.0 + rank as f32 + 1.0);
        }
    }
    let mut fused: Vec<_> = scores.into_iter().collect();
    fused.sort_by(|a, b| b.1.total_cmp(&a.1));
    fused
        .into_iter()
        .take(k)
        .map(|((tenant, t, s), score)| (tenant, t, s, score))
        .collect()
}
