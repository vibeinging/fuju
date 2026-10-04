use std::{
    path::PathBuf,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use yt_core::{
    event::{EventIdentity, EventType},
    fold::SpanFields,
};
use yt_engine::{
    DiskGraphConfig, DiskGraphIndex, GraphIndex, Projection, SearchFilter, TraceQuery,
    WriteCoordinator,
};
use yt_wal::WalRecord;
fn fresh(label: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "yt_vector_regression_{label}_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}
fn rec(n: u64) -> WalRecord {
    WalRecord {
        trace_id: n,
        span_id: n,
        ts: 1,
        identity: EventIdentity {
            ext_span_id: format!("vector-{n}"),
            seq: 1,
            event_type: EventType::SpanEnd,
        },
        fields: SpanFields {
            logs: vec![format!("vector {n}")],
            attrs: std::collections::BTreeMap::from([(
                "project_id".into(),
                "vector-project".into(),
            )]),
            ..Default::default()
        },
    }
}
#[test]
fn independent_process_vector_updates_do_not_overwrite_slots() {
    if let Ok(dir) = std::env::var("YT_VECTOR_REFRESH_CHILD") {
        let p = PathBuf::from(dir);
        let c = WriteCoordinator::open_durable(&p).unwrap();
        c.recover();
        std::fs::write(p.join("ready"), b"ok").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !p.join("go").exists() {
            assert!(
                Instant::now() < deadline,
                "parent did not release vector worker"
            );
            thread::sleep(Duration::from_millis(1));
        }
        c.index_embedding(3, 3, vec![30., 0.]);
        c.flush_memtable();
        return;
    }
    let p = fresh("process");
    {
        let c = WriteCoordinator::open_durable(&p).unwrap();
        c.ingest(vec![rec(1), rec(2), rec(3)]);
        c.index_embedding(1, 1, vec![10., 0.]);
        c.flush_memtable();
    }
    let a = WriteCoordinator::open_durable(&p).unwrap();
    a.recover();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "independent_process_vector_updates_do_not_overwrite_slots",
            "--nocapture",
        ])
        .env("YT_VECTOR_REFRESH_CHILD", &p)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !p.join("ready").exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("vector worker exited before ready: {status}");
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("vector worker did not become ready in 10 seconds");
        }
        thread::sleep(Duration::from_millis(1));
    }
    a.index_embedding(2, 2, vec![20., 0.]);
    a.flush_memtable();
    std::fs::write(p.join("go"), b"ok").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "vector worker failed: {status}");
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("vector worker did not finish in 10 seconds");
        }
        thread::sleep(Duration::from_millis(1));
    }
    let disk = DiskGraphIndex::open(p.join("vecindex"), 0, DiskGraphConfig::default()).unwrap();
    assert_eq!(
        disk.store().len(),
        3,
        "another process must not reuse an allocated vector slot"
    );
    let snap = a.pin_snapshot();
    let ids = a
        .search_similar(&snap, &[30., 0.], 3)
        .into_iter()
        .map(|(s, _)| s.trace_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids, std::collections::BTreeSet::from([1, 2, 3]));
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn repeated_embeddings_do_not_occupy_multiple_top_k_positions() {
    let p = fresh("duplicate");
    {
        let i = DiskGraphIndex::open(&p, 2, DiskGraphConfig::default()).unwrap();
        for _ in 0..5 {
            i.index_embedding(1, 1, vec![0., 0.]);
        }
        i.index_embedding(2, 2, vec![1., 0.]);
        i.flush();
        assert_eq!(
            i.search(&[0., 0.], 2, &|_, _| true)
                .iter()
                .map(|x| x.0)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }
    let i = DiskGraphIndex::open(&p, 0, DiskGraphConfig::default()).unwrap();
    assert_eq!(
        i.search(&[0., 0.], 2, &|_, _| true)
            .iter()
            .map(|x| x.0)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn updated_embedding_supersedes_old_distance() {
    let p = fresh("update");
    {
        let i = DiskGraphIndex::open(&p, 2, DiskGraphConfig::default()).unwrap();
        i.index_embedding(1, 1, vec![0., 0.]);
        i.index_embedding(2, 2, vec![1., 0.]);
        i.index_embedding(1, 1, vec![100., 0.]);
        i.flush();
        assert_eq!(
            i.search(&[0., 0.], 2, &|_, _| true)
                .iter()
                .map(|x| x.0)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
    }
    let i = DiskGraphIndex::open(&p, 0, DiskGraphConfig::default()).unwrap();
    assert_eq!(
        i.search(&[0., 0.], 2, &|_, _| true)
            .iter()
            .map(|x| x.0)
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    std::fs::remove_dir_all(p).unwrap();
}
#[test]
fn indexed_query_preserves_physical_read_statistics() {
    let p = fresh("stats");
    {
        let c = WriteCoordinator::open_durable(&p).unwrap();
        c.ingest(vec![rec(1)]);
        c.flush_memtable();
    }
    let c = WriteCoordinator::open_durable(&p).unwrap();
    c.recover();
    let snap = c.pin_snapshot();
    let filter = SearchFilter {
        attrs: std::collections::BTreeMap::from([("project_id".into(), "vector-project".into())]),
        ..Default::default()
    };
    let (rows, plan) =
        c.read_spans_query_indexed(&snap, &TraceQuery::all(), &filter, Projection::ALL);
    assert_eq!(rows.len(), 1);
    assert!(
        plan.index_bytes_read > 0,
        "actual index bytes must survive FoldQueryStats projection"
    );
    assert!(plan.data_bytes_read > 0);
    assert!(plan.indexes_validated > 0);
    std::fs::remove_dir_all(p).unwrap();
}

#[test]
fn vector_identity_includes_nullable_tenant_after_reopen() {
    let path = fresh("tenant");
    {
        let index = DiskGraphIndex::open(&path, 2, DiskGraphConfig::default()).unwrap();
        index
            .index_embedding_scoped(None, 7, 7, vec![50., 0.])
            .unwrap();
        index
            .index_embedding_scoped(Some(0), 7, 7, vec![0., 0.])
            .unwrap();
        index
            .index_embedding_scoped(Some(u64::MAX), 7, 7, vec![100., 0.])
            .unwrap();
        index
            .index_embedding_scoped(Some(0), 7, 7, vec![1., 0.])
            .unwrap();
        index.flush();
    }
    let index = DiskGraphIndex::open(&path, 0, DiskGraphConfig::default()).unwrap();
    let hits = index.search_scoped(&[0., 0.], 3, &|_, _, _| true);
    assert_eq!(
        hits.iter().map(|x| x.0).collect::<Vec<_>>(),
        vec![Some(0), None, Some(u64::MAX)]
    );
    assert_eq!(
        hits[0].3, 1.0,
        "only the newest embedding for tenant 0 is eligible"
    );
    assert_eq!(
        index.search(&[0., 0.], 3, &|_, _| true).len(),
        1,
        "the legacy API must not claim scoped vectors are global"
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn vector_identity_reads_existing_nodes_without_tenant_sidecar() {
    let path = fresh("legacy-tenant");
    {
        let index = DiskGraphIndex::open(&path, 2, DiskGraphConfig::default()).unwrap();
        index.index_embedding(7, 7, vec![0., 0.]);
        index.flush();
    }
    std::fs::remove_file(path.join("tenants")).unwrap();
    std::fs::remove_file(path.join("tenant_format")).unwrap();
    let index = DiskGraphIndex::open(&path, 0, DiskGraphConfig::default()).unwrap();
    assert_eq!(
        index
            .search_scoped(&[0., 0.], 1, &|tenant, _, _| tenant.is_none())
            .len(),
        1
    );
    index
        .index_embedding_scoped(Some(1), 7, 7, vec![1., 0.])
        .unwrap();
    assert_eq!(index.search_scoped(&[0., 0.], 2, &|_, _, _| true).len(), 2);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn unfiltered_scan_preserves_physical_read_statistics() {
    let path = fresh("scan-stats");
    {
        let c = WriteCoordinator::open_durable(&path).unwrap();
        c.ingest(vec![rec(1)]);
        c.flush_memtable();
    }
    let c = WriteCoordinator::open_durable(&path).unwrap();
    c.recover();
    let snap = c.pin_snapshot();
    let (rows, plan) = c.read_spans_query_indexed(
        &snap,
        &TraceQuery::all(),
        &SearchFilter::default(),
        Projection::ALL,
    );
    assert_eq!(rows.len(), 1);
    assert!(
        plan.data_bytes_read > 0,
        "scan I/O cannot be reported as zero"
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn existing_duplicate_nodes_are_filtered_before_top_k() {
    let path = fresh("existing-duplicates");
    {
        let index = DiskGraphIndex::open(&path, 2, DiskGraphConfig::default()).unwrap();
        index.index_embedding(1, 1, vec![0., 0.]);
        let duplicate = index.store().add_node(1, 1, &[0., 0.], 0).unwrap().unwrap();
        let distinct = index.store().add_node(2, 2, &[1., 0.], 0).unwrap().unwrap();
        index
            .store()
            .set_neighbors(0, &[duplicate, distinct])
            .unwrap();
        index
            .store()
            .set_neighbors(duplicate, &[0, distinct])
            .unwrap();
        index
            .store()
            .set_neighbors(distinct, &[0, duplicate])
            .unwrap();
        index.flush();
    }
    let index = DiskGraphIndex::open(&path, 0, DiskGraphConfig::default()).unwrap();
    let hits = index.search(&[0., 0.], 2, &|_, _| true);
    assert_eq!(hits.iter().map(|x| x.0).collect::<Vec<_>>(), vec![1, 2]);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn legacy_vectors_resolve_unique_source_tenant_on_first_vector_query() {
    let path = fresh("legacy-unique-source");
    {
        let c = WriteCoordinator::open_durable(&path).unwrap();
        let mut row = rec(1);
        row.fields.tenant_id = Some(1);
        c.ingest(vec![row]);
        c.index_embedding(1, 1, vec![0., 0.]);
        c.flush_memtable();
    }
    std::fs::remove_file(path.join("vecindex/tenants")).unwrap();
    let _ = std::fs::remove_file(path.join("vecindex/tenant_format"));
    let c = WriteCoordinator::open_durable(&path).unwrap();
    c.recover();
    let api = yt_engine::EngineJsonApi::new(c.clone());
    let (status, body) =
        api.route_with_tenant("POST", "/v1/search", r#"{"vector":[0,0],"k":1}"#, Some(1));
    assert_eq!(status, 200, "{body}");
    assert!(
        body.contains("vector 1"),
        "legacy vector must retain unique source tenant: {body}"
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn legacy_vectors_reject_ambiguous_source_without_blocking_text_or_ingest() {
    let path = fresh("legacy-ambiguous-source");
    {
        let c = WriteCoordinator::open_durable(&path).unwrap();
        let mut first = rec(1);
        first.fields.tenant_id = Some(1);
        let mut second = rec(1);
        second.fields.tenant_id = Some(2);
        c.ingest(vec![first, second]);
        c.index_embedding(1, 1, vec![0., 0.]);
        c.flush_memtable();
    }
    std::fs::remove_file(path.join("vecindex/tenants")).unwrap();
    let _ = std::fs::remove_file(path.join("vecindex/tenant_format"));
    let c = WriteCoordinator::open_durable(&path).unwrap();
    c.recover();
    let api = yt_engine::EngineJsonApi::new(c.clone());
    let (status, body) =
        api.route_with_tenant("POST", "/v1/search", r#"{"vector":[0,0],"k":1}"#, Some(1));
    assert_eq!(status, 500, "{body}");
    assert!(body.contains("ambiguous"), "{body}");
    assert_eq!(
        api.route_with_tenant("POST", "/v1/search", r#"{"text":"vector","k":1}"#, Some(1))
            .0,
        200
    );
    assert_eq!(api.route_with_tenant("POST","/v1/ingest",r#"[{"trace_id":2,"span_id":2,"ts":2,"seq":1,"event_type":2,"ext_span_id":"new","logs":["new"]}]"#,Some(1)).0,200);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn modern_tenant_sidecar_loss_is_rejected_without_guessing() {
    let path = fresh("modern-tenant-loss");
    {
        let index = DiskGraphIndex::open(&path, 2, DiskGraphConfig::default()).unwrap();
        index
            .index_embedding_scoped(Some(1), 1, 1, vec![0., 0.])
            .unwrap();
        index.flush();
    }
    std::fs::remove_file(path.join("tenants")).unwrap();
    assert!(DiskGraphIndex::open(&path, 0, DiskGraphConfig::default()).is_err());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn modern_none_embedding_is_not_inferred_from_tenant_source() {
    let path = fresh("modern-none");
    let c = WriteCoordinator::open_durable(&path).unwrap();
    let mut row = rec(1);
    row.fields.tenant_id = Some(1);
    c.ingest(vec![row]);
    c.index_embedding(1, 1, vec![0., 0.]);
    let api = yt_engine::EngineJsonApi::new(c.clone());
    let (status, body) =
        api.route_with_tenant("POST", "/v1/search", r#"{"vector":[0,0],"k":1}"#, Some(1));
    assert_eq!(status, 200);
    assert_eq!(body, "[]", "modern None identity must stay None");
    std::fs::remove_dir_all(path).unwrap();
}
