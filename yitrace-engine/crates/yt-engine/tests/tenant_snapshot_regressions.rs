//! 使用真实持久目录守住租户身份、重传和快照查询的一致性。
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use yt_core::event::{EventIdentity, EventType};
use yt_core::fold::SpanFields;
use yt_engine::{EngineJsonApi, SearchFilter, TraceQuery, WriteCoordinator};
use yt_wal::WalRecord;
struct Temp(PathBuf);
impl Temp {
    fn new(label: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        Self(std::env::temp_dir().join(format!(
            "yt_{label}_{}_{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn event(
    tenant: u64,
    trace: u64,
    span: u64,
    seq: u64,
    ext: &str,
    status: u8,
    text: &str,
) -> WalRecord {
    WalRecord {
        trace_id: trace,
        span_id: span,
        ts: seq as i64,
        identity: EventIdentity {
            ext_span_id: ext.into(),
            seq,
            event_type: EventType::Log,
        },
        fields: SpanFields {
            tenant_id: Some(tenant),
            session_id: Some(1),
            status: Some(status),
            logs: vec![text.into()],
            ..Default::default()
        },
    }
}
fn filter(tenant: u64, status: Option<u8>) -> SearchFilter {
    SearchFilter {
        tenant_id: Some(tenant),
        status,
        ..Default::default()
    }
}
#[test]
fn tenant_same_ids_and_event_identity_remain_independent_after_reopen() {
    let dir = Temp::new("tenant_identity");
    {
        let c = WriteCoordinator::open_durable(&dir.0).unwrap();
        c.recover();
        c.ingest(vec![
            event(1, 1, 1, 1, "same", 0, "alphasecret"),
            event(2, 1, 1, 1, "same", 1, "betaprivate"),
            event(2, 1, 1, 2, "bob", 1, "betalate"),
        ]);
        c.flush_memtable();
    }
    let c = WriteCoordinator::open_durable(&dir.0).unwrap();
    c.recover();
    let s = c.pin_snapshot();
    for (tenant, own, other) in [
        (1, "alphasecret", "betaprivate"),
        (2, "betaprivate", "alphasecret"),
    ] {
        let spans = c
            .read_spans_query(&s, &TraceQuery::all().for_tenant(tenant))
            .0;
        assert_eq!(spans.len(), 1, "每个租户都必须保留独立span");
        assert!(spans[0].logs.iter().any(|x| x == own));
        assert!(
            !spans[0].logs.iter().any(|x| x == other),
            "不能合并其他租户原文"
        );
        assert_eq!(
            c.search_text_attr(&s, own, 10, &filter(tenant, None)).len(),
            1,
            "相同eventid不能吞掉第二租户全文索引"
        );
        assert!(c
            .search_text_attr(&s, other, 10, &filter(tenant, None))
            .is_empty());
        assert_eq!(
            c.search_text_attr(&s, own, 10, &filter(tenant, Some((tenant - 1) as u8)))
                .len(),
            1
        );
        let (rollup, _) = c
            .trace_aggregate_rollup_spans(
                &TraceQuery::all().for_tenant(tenant),
                &filter(tenant, None),
            )
            .unwrap();
        assert_eq!(rollup.len(), 1);
        assert_eq!(rollup[0].status, Some((tenant - 1) as u8));
        assert_eq!(rollup[0].event_count, tenant as usize);
        let api = EngineJsonApi::new(c.clone());
        let (code, body) = api.route_with_tenant("GET", "/v1/sessions", "", Some(tenant));
        assert_eq!(code, 200, "{body}");
        assert!(
            body.contains(&format!(
                "\"status\":\"{}\"",
                if tenant == 2 { "error" } else { "ok" }
            )),
            "{body}"
        );
    }
    assert_eq!(
        c.read_spans(&s)
            .iter()
            .map(|x| x.tenant_id)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([Some(1), Some(2)])
    );
}
#[test]
fn retention_apply_does_not_delete_other_tenant_same_trace() {
    let dir = Temp::new("tenant_retention");
    let c = WriteCoordinator::open_durable(&dir.0).unwrap();
    c.recover();
    c.ingest(vec![
        event(1, 9, 1, 1, "retention-a", 0, "alice"),
        event(2, 9, 2, 1, "retention-b", 0, "bob"),
    ]);
    c.flush_memtable();
    let api = EngineJsonApi::new(c.clone());
    let (code, body) = api.route_with_tenant(
        "POST",
        "/v1/retention/apply",
        r#"{"deleteBeforeTs":100}"#,
        Some(1),
    );
    assert_eq!(code, 200, "{body}");
    assert!(c
        .read_spans_query(&c.pin_snapshot(), &TraceQuery::all().for_tenant(1))
        .0
        .is_empty());
    assert_eq!(
        c.read_spans_query(&c.pin_snapshot(), &TraceQuery::all().for_tenant(2))
            .0
            .len(),
        1,
        "retention按tenant+trace删行"
    );
    drop(api);
    drop(c);
    let c = WriteCoordinator::open_durable(&dir.0).unwrap();
    c.recover();
    assert_eq!(
        c.read_spans_query(&c.pin_snapshot(), &TraceQuery::all().for_tenant(2))
            .0
            .len(),
        1
    );
}
#[test]
fn retried_old_event_does_not_revert_persisted_filter_or_rollup() {
    let dir = Temp::new("event_retry");
    let old = event(1, 2, 2, 1, "retry", 0, "risk");
    {
        let c = WriteCoordinator::open_durable(&dir.0).unwrap();
        c.recover();
        c.ingest(vec![old.clone(), event(1, 2, 2, 2, "retry", 1, "risk")]);
        c.flush_memtable();
    }
    {
        let c = WriteCoordinator::open_durable(&dir.0).unwrap();
        c.recover();
        c.ingest(vec![old]);
        let s = c.pin_snapshot();
        assert_eq!(
            c.search_text_attr(&s, "risk", 10, &filter(1, Some(1)))
                .len(),
            1,
            "重传旧seq不能倒退过滤字段"
        );
        assert!(c
            .search_text_attr(&s, "risk", 10, &filter(1, Some(0)))
            .is_empty());
        let (rows, _) = c
            .trace_aggregate_rollup_spans(&TraceQuery::all().for_tenant(1), &filter(1, None))
            .unwrap();
        assert_eq!(rows[0].status, Some(1));
        assert_eq!(rows[0].event_count, 2, "重传不得增加rollup事件数");
        c.flush_memtable();
    }
    let c = WriteCoordinator::open_durable(&dir.0).unwrap();
    c.recover();
    assert_eq!(
        c.search_text_attr(&c.pin_snapshot(), "risk", 10, &filter(1, Some(1)))
            .len(),
        1
    );
}
#[test]
fn old_snapshot_filters_use_the_same_visible_events_as_plain_read() {
    let dir = Temp::new("snapshot_filter");
    let c = WriteCoordinator::open_durable(&dir.0).unwrap();
    c.recover();
    c.ingest(vec![event(1, 3, 3, 1, "snapshot", 0, "risk")]);
    let s = c.pin_snapshot();
    c.ingest(vec![
        event(1, 3, 3, 2, "snapshot", 1, "risk"),
        event(1, 4, 4, 1, "later", 0, "risk"),
    ]);
    c.flush_memtable();
    assert_eq!(
        c.read_spans_query(&s, &TraceQuery::all().for_tenant(1)).0[0].status,
        Some(0)
    );
    let hits = c.search_text_attr(&s, "risk", 10, &filter(1, Some(0)));
    assert_eq!(hits.len(), 1, "旧快照不能被最新attrs删掉候选");
    assert_eq!(hits[0].0.trace_id, 3);
    assert!(
        c.search_text_attr(&s, "risk", 10, &filter(1, Some(1)))
            .is_empty(),
        "旧快照不能按新status命中"
    );
}

#[test]
fn session_rebuild_does_not_lose_retry_ordering() {
    let dir = Temp::new("session_retry");
    let old = event(1, 5, 5, 1, "session-retry", 0, "risk");
    {
        let c = WriteCoordinator::open_durable(&dir.0).unwrap();
        c.recover();
        c.ingest(vec![
            old.clone(),
            event(1, 5, 5, 2, "session-retry", 1, "risk"),
        ]);
        c.flush_memtable();
    }
    let c = WriteCoordinator::open_durable(&dir.0).unwrap();
    c.recover();
    // 首次写入把段索引补齐后，再走普通session的折叠重建路径。
    c.ingest(vec![event(1, 6, 6, 1, "another-session", 0, "other")]);
    let api = EngineJsonApi::new(c.clone());
    let (code, before) = api.route_with_tenant("GET", "/v1/sessions", "", None);
    assert_eq!(code, 200);
    assert!(before.contains("\"status\":\"error\""), "{before}");
    c.ingest(vec![old]);
    let (code, after) = api.route_with_tenant("GET", "/v1/sessions", "", None);
    assert_eq!(code, 200);
    assert!(after.contains("\"status\":\"error\""), "{after}");
}

#[test]
fn upgrades_in_different_segments_keep_full_tenant_identity() {
    let dir = Temp::new("tenant_upgrade_segments");
    {
        let c = WriteCoordinator::open_durable(&dir.0).unwrap();
        c.recover();
        for (tenant, text) in [(1, "upalpha"), (2, "upbeta")] {
            c.ingest(vec![event(tenant, 7, 7, 1, "upgrade-same", 0, "initial")]);
            c.flush_memtable();
            let seg = c
                .pin_snapshot()
                .manifest
                .segments
                .values()
                .last()
                .unwrap()
                .segment_id;
            c.try_commit_upgrade(
                seg,
                7,
                7,
                SpanFields {
                    input_text: Some(text.into()),
                    status: Some((tenant - 1) as u8),
                    ..Default::default()
                },
            )
            .unwrap();
        }
        for (tenant, text) in [(1, "upalpha"), (2, "upbeta")] {
            let spans = c
                .read_spans_query(&c.pin_snapshot(), &TraceQuery::all().for_tenant(tenant))
                .0;
            assert_eq!(spans.len(), 1);
            assert_eq!(spans[0].input_text.as_deref(), Some(text));
        }
    }
    let c = WriteCoordinator::open_durable(&dir.0).unwrap();
    c.recover();
    for (tenant, text) in [(1, "upalpha"), (2, "upbeta")] {
        let s = c.pin_snapshot();
        let hits = c.search_text_attr(&s, text, 10, &filter(tenant, Some((tenant - 1) as u8)));
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0.input_text.as_deref(), Some(text));
        let (rows, _) = c
            .trace_aggregate_rollup_spans(
                &TraceQuery::all().for_tenant(tenant),
                &filter(tenant, None),
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, Some((tenant - 1) as u8));
    }
}
