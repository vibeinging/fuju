#[test]
fn search_text_and_vector_find_and_fold_spans() {
    // 产品噱头：按中文内容搜 trace、按向量找相似 trace。
    let store = Arc::new(CapturingStore::default());
    let wc = WriteCoordinator::new(store.clone());

    let e1 = ev(1, 10, 1, Some(0), Some(100), &["用户登录 风控通过"]);
    let e2 = ev(2, 20, 1, Some(0), Some(200), &["疑似盗刷 已拦截"]);
    let e3 = ev(3, 30, 1, Some(0), Some(300), &["转账成功"]);
    let all = vec![e1.clone(), e2.clone(), e3.clone()];
    wc.ingest(all.clone());
    wc.commit_flush(&all, WalLsn::new(3));

    // 给三个 span 各加一个二维向量
    wc.index_embedding(1, 10, vec![0.0, 0.0]);
    wc.index_embedding(2, 20, vec![1.0, 0.0]);
    wc.index_embedding(3, 30, vec![5.0, 5.0]);

    let snap = wc.pin_snapshot();

    // 中文检索「盗刷」：只命中 span(2,20)，且折叠出完整 span（带 duration）
    let hits = wc.search_text(&snap, "盗刷", 10);
    assert_eq!(hits.len(), 1);
    assert_eq!((hits[0].0.trace_id, hits[0].0.span_id), (2, 20));
    assert_eq!(
        hits[0].0.duration_ns,
        Some(200),
        "返回的是折叠出的完整 span，不只是命中行"
    );

    // 向量找相似：查 [0.9,0.0] 最近的是 span(2,20) 的 [1,0]，其次 span(1,10) 的 [0,0]
    let sim = wc.search_similar(&snap, &[0.9, 0.0], 2);
    assert_eq!(sim.len(), 2);
    assert_eq!((sim[0].0.trace_id, sim[0].0.span_id), (2, 20));
    assert_eq!((sim[1].0.trace_id, sim[1].0.span_id), (1, 10));
}

#[test]
fn builder_injects_custom_tokenizer_end_to_end() {
    // 注入口验证：用 CoordinatorBuilder 换分词器后起引擎，自定义分词一路贯穿到 search_text。
    // 这条就是「外部分词或自定义分词只换分词层」在引擎层的契约。
    struct WordTokenizer; // 按空白切，整段中文当一个词（不拆 bigram）
    impl Tokenizer for WordTokenizer {
        fn tokenize(&self, text: &str) -> Vec<String> {
            text.split_whitespace().map(|w| w.to_lowercase()).collect()
        }
    }

    let store = Arc::new(CapturingStore::default());
    let wc = CoordinatorBuilder::new()
        .with_tokenizer(Box::new(WordTokenizer))
        .build(store);

    // (1,10) 文本里 "风控" 是独立词；(2,20) 没有空格分隔的 "风控" 词。
    let e1 = ev(1, 10, 1, Some(0), Some(100), &["盗刷 风控 已拦截"]);
    let e2 = ev(2, 20, 1, Some(0), Some(200), &["盗刷风控合并成一个词"]);
    let all = vec![e1.clone(), e2.clone()];
    wc.ingest(all.clone());
    wc.commit_flush(&all, WalLsn::new(2));
    let snap = wc.pin_snapshot();

    // 注入的分词器决定切分：查 "风控" 只命中 (1,10)（默认 bigram 会把两条都命中）。
    let hits = wc.search_text(&snap, "风控", 10);
    assert_eq!(hits.len(), 1, "注入的分词器一路生效到检索");
    assert_eq!((hits[0].0.trace_id, hits[0].0.span_id), (1, 10));
}

#[test]
fn segment_key_bloom_skips_unrelated_segments_keeps_results() {
    // 段级 bloom：候选 key 只在段 A，段 B 的 bloom 拒绝它 → B 被跳过，结果仍正确。
    let store = Arc::new(CapturingStore::default());
    let wc = WriteCoordinator::new(store);
    // 段 A：trace 1 含"盗刷"
    let a = ev(1, 10, 1, Some(0), Some(100), &["疑似盗刷 已拦截"]);
    wc.ingest(vec![a.clone()]);
    wc.flush_memtable(); // → 段 A，建 bloom（含 (1,10)）
                         // 段 B：trace 2 不含"盗刷"，且 key 不同
    let b = ev(2, 20, 1, Some(0), Some(200), &["正常转账"]);
    wc.ingest(vec![b.clone()]);
    wc.flush_memtable(); // → 段 B，建 bloom（含 (2,20)，不含 (1,10)）

    // 两段都有 bloom
    assert_eq!(wc.seg_key_bloom.lock().unwrap().len(), 2);
    let snap = wc.pin_snapshot();
    // 查"盗刷"：候选 (1,10) 只在段 A；段 B 的 bloom 拒绝它 → 只回 trace 1，结果正确。
    let hits = wc.search_text(&snap, "盗刷", 10);
    assert_eq!(hits.len(), 1);
    assert_eq!((hits[0].0.trace_id, hits[0].0.span_id), (1, 10));
    assert_eq!(
        hits[0].0.duration_ns,
        Some(100),
        "折叠出完整 span（跨段定位正确）"
    );
    // 直接验证 bloom 语义：段 B 的 bloom 对 (1,10) 说"肯定没有"。
    let blooms = wc.seg_key_bloom.lock().unwrap();
    let seg_ids: Vec<u64> = snap.manifest.segments.keys().copied().collect();
    let any_rejects_a = seg_ids.iter().any(|sid| {
        blooms
            .get(sid)
            .map_or(false, |bl| !bl.maybe_contains((1, 10)))
    });
    assert!(any_rejects_a, "应有段的 bloom 对 (1,10) 判定肯定没有");
}

#[test]
fn tenant_filter_isolates_list_and_read_paths() {
    // 列表/读路径的租户隔离：read_spans_query / list_traces 带 tenant → 只见本租户。
    let store = Arc::new(CapturingStore::default());
    let wc = WriteCoordinator::new(store);
    let mut a = ev(1, 10, 1, Some(0), Some(100), &["t1"]);
    a.fields.tenant_id = Some(1);
    let mut b = ev(2, 20, 1, Some(0), Some(200), &["t2"]);
    b.fields.tenant_id = Some(2);
    let all = vec![a, b];
    wc.ingest(all.clone());
    wc.commit_flush(&all, WalLsn::new(2));
    let snap = wc.pin_snapshot();

    // 不带 tenant：两条都见。
    assert_eq!(wc.read_spans_query(&snap, &TraceQuery::all()).0.len(), 2);
    // 带 tenant 1：只见 trace 1。
    let (s1, _) = wc.read_spans_query(&snap, &TraceQuery::all().for_tenant(1));
    assert_eq!(s1.len(), 1);
    assert_eq!(s1[0].trace_id, 1);
    // 列表也隔离。
    let l1 = wc.list_traces(&snap, &TraceQuery::all().for_tenant(1));
    assert!(
        l1.iter().all(|t| t.trace_id == 1) && !l1.is_empty(),
        "列表只见租户1"
    );
    let l2 = wc.list_traces(&snap, &TraceQuery::all().for_tenant(2));
    assert!(
        l2.iter().all(|t| t.trace_id == 2) && !l2.is_empty(),
        "列表只见租户2"
    );
}

#[test]
fn tenant_filter_isolates_search_across_tenants() {
    // 逻辑隔离：共享一套索引，查询强制带 tenant 过滤 → 只见本租户的 span（BM25 文本 + 向量找相似都隔离）。
    let store = Arc::new(CapturingStore::default());
    let wc = WriteCoordinator::new(store);

    // 两个租户各一条"盗刷"相关 span，文本相同、向量相近 —— 不隔离的话会互相召回。
    let mut a = ev(1, 10, 1, Some(0), Some(100), &["疑似盗刷 已拦截"]);
    a.fields.tenant_id = Some(1);
    let mut b = ev(2, 20, 1, Some(0), Some(200), &["疑似盗刷 已拦截"]);
    b.fields.tenant_id = Some(2);
    let all = vec![a.clone(), b.clone()];
    wc.ingest(all.clone());
    wc.commit_flush(&all, WalLsn::new(2));
    wc.index_embedding_for_tenant(Some(1), 1, 10, vec![0.0, 0.0])
        .unwrap();
    wc.index_embedding_for_tenant(Some(2), 2, 20, vec![0.01, 0.0])
        .unwrap(); // 和租户1的几乎重合
    let snap = wc.pin_snapshot();

    let t1 = SearchFilter {
        tenant_id: Some(1),
        ..Default::default()
    };
    let t2 = SearchFilter {
        tenant_id: Some(2),
        ..Default::default()
    };

    // BM25 文本检索：查"盗刷"，scope 租户1 → 只回 (1,10)，不漏租户2。
    let txt1 = wc.search_text_attr(&snap, "盗刷", 10, &t1);
    assert!(
        txt1.iter().all(|(s, _)| s.trace_id == 1),
        "租户1 文本检索不漏租户2"
    );
    assert!(txt1.iter().any(|(s, _)| s.span_id == 10));
    let txt2 = wc.search_text_attr(&snap, "盗刷", 10, &t2);
    assert!(
        txt2.iter().all(|(s, _)| s.trace_id == 2),
        "租户2 文本检索不漏租户1"
    );

    // 向量找相似：scope 租户1 → 即便租户2的向量更近也不返回（进图过滤隔离）。
    let sim1 = wc.search_similar_attr(&snap, &[0.0, 0.0], 10, &t1);
    assert!(!sim1.is_empty());
    assert!(
        sim1.iter().all(|(s, _)| s.trace_id == 1),
        "租户1 找相似不漏租户2（向量更近也挡）"
    );
    let sim2 = wc.search_similar_attr(&snap, &[0.0, 0.0], 10, &t2);
    assert!(
        sim2.iter().all(|(s, _)| s.trace_id == 2),
        "租户2 找相似不漏租户1"
    );
}

#[test]
fn text_filter_expands_beyond_initial_bm25_window() {
    let store = Arc::new(CapturingStore::default());
    let wc = WriteCoordinator::new(store);
    let mut rows = Vec::new();
    for trace_id in 1..=120 {
        let mut row = ev(trace_id, 1, 1, Some(0), Some(100), &["任务执行 固定文本"]);
        row.fields.tenant_id = Some(1);
        if trace_id == 120 {
            row.fields
                .attrs
                .insert("project_id".to_string(), "\"target-project\"".to_string());
        }
        rows.push(row);
    }
    wc.ingest(rows.clone());
    wc.commit_flush(&rows, WalLsn::new(rows.len() as u64));
    let snap = wc.pin_snapshot();
    let mut filter = SearchFilter {
        tenant_id: Some(1),
        ..Default::default()
    };
    filter
        .attrs
        .insert("project_id".to_string(), "\"target-project\"".to_string());

    let hits = wc.search_text_attr(&snap, "任务执行", 1, &filter);
    assert_eq!(hits.len(), 1, "匹配项在初始 Top 50 外也不能漏召回");
    assert_eq!(hits[0].0.trace_id, 120);
}

#[test]
fn builder_injects_custom_graph_index_end_to_end() {
    // 注入口验证：用 CoordinatorBuilder 换 GraphIndex 后，search_similar 走的是注入的实现，不是默认 ANN。
    // 这条是「外部或自定义 GraphIndex 只换向量索引层」在引擎层的契约（与分词那条对称）。
    struct StubGraph; // 无视查询向量，永远只返回 (7,99) —— 默认 L2 ANN 不会这么选
    impl GraphIndex for StubGraph {
        fn index_embedding(&self, _t: u64, _s: u64, _e: Vec<f32>) {}
        fn search(
            &self,
            _q: &[f32],
            _k: usize,
            _f: &dyn Fn(u64, u64) -> bool,
        ) -> Vec<(u64, u64, f32)> {
            vec![(7, 99, 0.0)]
        }
    }

    let store = Arc::new(CapturingStore::default());
    let wc = CoordinatorBuilder::new()
        .with_graph(Arc::new(StubGraph))
        .build(store);

    // 两个 span 都摄入（才能被折叠出来）；查询向量明显更靠近 (1,10)。
    let e1 = ev(1, 10, 1, Some(0), Some(100), &["a"]);
    let e2 = ev(7, 99, 1, Some(0), Some(700), &["b"]);
    let all = vec![e1.clone(), e2.clone()];
    wc.ingest(all.clone());
    wc.commit_flush(&all, WalLsn::new(2));
    let snap = wc.pin_snapshot();

    let sim = wc.search_similar(&snap, &[0.0, 0.0], 5);
    assert_eq!(sim.len(), 1, "注入的图索引决定返回什么");
    assert_eq!(
        (sim[0].0.trace_id, sim[0].0.span_id),
        (7, 99),
        "走的是 StubGraph，不是默认 L2"
    );
    assert_eq!(sim[0].0.duration_ns, Some(700), "返回折叠出的完整 span");
}

#[test]
fn hybrid_fusion_beats_single_signal() {
    let store = Arc::new(CapturingStore::default());
    let wc = WriteCoordinator::new(store.clone());
    let e1 = ev(1, 10, 1, Some(0), Some(100), &["用户登录 风控通过"]);
    let e2 = ev(2, 20, 1, Some(0), Some(200), &["疑似盗刷 已拦截"]);
    let e3 = ev(3, 30, 1, Some(0), Some(300), &["转账成功"]);
    let all = vec![e1.clone(), e2.clone(), e3.clone()];
    wc.ingest(all.clone());
    wc.commit_flush(&all, WalLsn::new(3));
    wc.index_embedding(1, 10, vec![0.0, 0.0]);
    wc.index_embedding(2, 20, vec![1.0, 0.0]);
    wc.index_embedding(3, 30, vec![5.0, 5.0]);
    let snap = wc.pin_snapshot();

    // 向量查 [0.1,0.0]：单走向量,最近的是 span(1,10)
    assert_eq!((wc.search_similar(&snap, &[0.1, 0.0], 3)[0].0.trace_id), 1);

    // 混合「盗刷」+ 同一个向量：span(2,20) 被关键词和语义双命中 → 融合后反超到第一,
    // 这是单走向量给不出的排序。
    let hy = wc.search_hybrid(&snap, "盗刷", &[0.1, 0.0], 3);
    assert_eq!(
        (hy[0].0.trace_id, hy[0].0.span_id),
        (2, 20),
        "双命中的 span 经 RRF 融合居首"
    );
    assert_eq!(hy[1].0.trace_id, 1, "向量单命中的次之");
}

#[test]
fn search_folds_only_hit_rows_across_sources() {
    // 只折叠命中行:命中 span 的 start 在段、end 在内存,检索仍跨源折叠正确;无关 span 不进结果。
    let store = Arc::new(CapturingStore::default());
    let wc = WriteCoordinator::new(store);
    let start = ev(2, 20, 1, Some(0), None, &["疑似盗刷"]); // 段
    wc.ingest(vec![start.clone()]);
    wc.commit_flush(&[start], WalLsn::new(1));
    wc.ingest(vec![ev(2, 20, 2, None, Some(500), &["已拦截"])]); // 内存
                                                                 // 噪声 span(别的 trace),不该被折进检索结果。
    wc.ingest(vec![
        ev(1, 10, 1, Some(0), Some(9), &["登录"]),
        ev(3, 30, 1, Some(0), Some(9), &["转账"]),
    ]);

    let snap = wc.pin_snapshot();
    let hits = wc.search_text(&snap, "盗刷", 10);
    assert_eq!(hits.len(), 1, "只命中 span(2,20),噪声不进结果");
    let s = &hits[0].0;
    assert_eq!((s.trace_id, s.span_id), (2, 20));
    assert_eq!(s.status, Some(0), "来自段的 start");
    assert_eq!(s.duration_ns, Some(500), "来自内存的 end");
    assert_eq!(s.logs, vec!["疑似盗刷", "已拦截"], "命中行跨源折叠正确");
    assert_eq!(s.event_count, 2);
}

#[test]
fn filtered_similar_search_pushes_predicate_into_graph() {
    // 进图过滤接到引擎层:谓词下推进 graph.search,即便被排除 trace 里有更近的点,也不返回。
    let store = Arc::new(CapturingStore::default());
    let wc = WriteCoordinator::new(store);
    let rows = vec![
        ev(1, 10, 1, Some(0), Some(100), &["a"]),
        ev(1, 11, 1, Some(0), Some(100), &["b"]),
        ev(2, 20, 1, Some(0), Some(100), &["c"]),
    ];
    wc.ingest(rows);
    wc.index_embedding(1, 10, vec![0.0, 1.0]);
    wc.index_embedding(1, 11, vec![0.0, 2.0]);
    wc.index_embedding(2, 20, vec![0.0, 0.0]); // 离 query[0,0] 最近,但属 trace2

    let snap = wc.pin_snapshot();
    // 不过滤:最近的是 trace2 的 span20
    let all = wc.search_similar(&snap, &[0.0, 0.0], 3);
    assert_eq!((all[0].0.trace_id, all[0].0.span_id), (2, 20));

    // 只搜 trace1:谓词下推进图,trace2 的最近点被排除,仍能召回 trace1 里最近的 span10。
    let only1 = wc.search_similar_filtered(&snap, &[0.0, 0.0], 3, &|t, _| t == 1);
    assert!(
        only1.iter().all(|(s, _)| s.trace_id == 1),
        "过滤后只剩 trace1"
    );
    assert!(
        !only1.iter().any(|(s, _)| s.span_id == 20),
        "trace2 的最近点被进图过滤排除"
    );
    assert_eq!(
        (only1[0].0.trace_id, only1[0].0.span_id),
        (1, 10),
        "trace1 里离 query 最近的是 span10"
    );
}

#[test]
fn attr_filtered_search_filters_by_agent_status_and_time() {
    // 带过滤 ANN 在真实查询维度上:按 agent / 状态 / 时间过滤,不只 (trace,span) id。
    let store = Arc::new(CapturingStore::default());
    let wc = WriteCoordinator::new(store);
    let mut e1 = ev_at(1, 10, 1, 100, Some(0), Some(100), &["a"]); // 风控, 正常, 最近
    e1.fields.agent_name = Some("风控".into());
    let mut e2 = ev_at(1, 11, 1, 200, Some(1), Some(100), &["b"]); // 规划, 报错
    e2.fields.agent_name = Some("规划".into());
    let mut e3 = ev_at(2, 20, 1, 300, Some(1), Some(100), &["c"]); // 风控, 报错, 较远
    e3.fields.agent_name = Some("风控".into());
    wc.ingest(vec![e1, e2, e3]);
    wc.index_embedding(1, 10, vec![0.0, 0.0]); // 离 query[0,0] 最近
    wc.index_embedding(1, 11, vec![0.0, 1.0]);
    wc.index_embedding(2, 20, vec![0.0, 2.0]);

    let snap = wc.pin_snapshot();
    // 找 agent=风控 且 报错(status=1) 的相似:最近的 span10 是风控但正常 → 排除;命中 span20。
    let f = SearchFilter {
        agent_name: Some("风控".into()),
        status: Some(1),
        ..Default::default()
    };
    let hits = wc.search_similar_attr(&snap, &[0.0, 0.0], 5, &f);
    assert!(!hits.is_empty(), "应召回风控+报错的 span");
    assert!(hits
        .iter()
        .all(|(s, _)| s.agent_name.as_deref() == Some("风控") && s.status == Some(1)));
    assert!(
        hits.iter().any(|(s, _)| s.span_id == 20),
        "命中风控+报错的 span20"
    );
    assert!(
        !hits.iter().any(|(s, _)| s.span_id == 10),
        "最近但 status=0 被排除"
    );
    assert!(
        !hits.iter().any(|(s, _)| s.span_id == 11),
        "agent 不符被排除"
    );

    // 时间窗:只要 ts ≤ 150 → 只 span10(ts=100)。
    let ft = SearchFilter {
        time_to: Some(150),
        ..Default::default()
    };
    let timed = wc.search_similar_attr(&snap, &[0.0, 0.0], 5, &ft);
    assert!(
        !timed.is_empty() && timed.iter().all(|(s, _)| s.span_id == 10),
        "只剩时间窗内的 span10"
    );
}
