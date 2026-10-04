use std::{fs, path::PathBuf, sync::Arc};
use yt_core::{
    event::{EventIdentity, EventType},
    fold::SpanFields,
    ids::SegmentId,
};
use yt_engine::{NewTraceAnnotation, TraceAnnotationFilter, WriteCoordinator};
use yt_wal::WalRecord;

struct TempDir(PathBuf);
impl TempDir {
    fn new(label: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "yt-durability-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&p).unwrap();
        Self(p)
    }
    fn open(&self) -> Arc<WriteCoordinator> {
        let e = WriteCoordinator::open_durable(&self.0).unwrap();
        e.recover();
        e
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn record(n: u64) -> WalRecord {
    WalRecord {
        trace_id: n,
        span_id: n,
        ts: n as i64,
        identity: EventIdentity {
            ext_span_id: format!("durability-{n}"),
            seq: 1,
            event_type: EventType::SpanStart,
        },
        fields: SpanFields {
            span_name: Some(format!("record-{n}")),
            ..Default::default()
        },
    }
}
fn count(e: &WriteCoordinator) -> usize {
    e.read_spans(&e.pin_snapshot()).len()
}

#[test]
fn durability_regression_failed_segment_preserves_acknowledged_rows() {
    let d = TempDir::new("segment-error");
    let e = d.open();
    e.ingest(vec![record(1)]);
    fs::create_dir(d.0.join("segments/seg-1.tmp")).unwrap();
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| e.flush_memtable()));
    assert!(failed.is_err(), "a failed segment write must be reported");
    assert_eq!(
        count(&e),
        1,
        "a failure must neither evict data nor poison writer locks"
    );
    assert_eq!(e.memtable_len(), 1);
    drop(e);
    assert_eq!(count(&d.open()), 1);
}

#[test]
fn durability_regression_failed_manifest_does_not_publish_delete() {
    let d = TempDir::new("manifest-error");
    let e = d.open();
    e.ingest(vec![record(1)]);
    e.flush_memtable();
    fs::create_dir(d.0.join("manifest.tmp")).unwrap();
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        e.commit_delete(SegmentId::new(1), 0)
    }));
    assert!(failed.is_err(), "a failed manifest commit must be reported");
    assert_eq!(
        count(&e),
        1,
        "failed commit must keep previous manifest and usable locks"
    );
    fs::remove_dir(d.0.join("manifest.tmp")).unwrap();
    e.commit_delete(SegmentId::new(1), 0);
    assert_eq!(count(&e), 0);
    drop(e);
    assert_eq!(count(&d.open()), 0);
}

#[test]
fn durability_regression_corrupt_manifest_is_rejected() {
    let d = TempDir::new("manifest-corrupt");
    let e = d.open();
    e.ingest(vec![record(1)]);
    e.flush_memtable();
    e.commit_delete(SegmentId::new(1), 0);
    drop(e);
    let p = d.0.join("manifest.dat");
    let mut bytes = fs::read(&p).unwrap();
    bytes[0] ^= 1; // manifest CRC, not an optional index or checkpoint
    fs::write(p, bytes).unwrap();
    assert!(
        WriteCoordinator::open_durable(&d.0).is_err(),
        "corruption must not silently resurrect deleted WAL records"
    );
}

#[test]
fn durability_regression_refresh_keeps_existing_snapshot_memtable_rows() {
    let d = TempDir::new("snapshot-refresh");
    let a = d.open();
    let b = d.open();
    a.ingest(vec![record(1)]);
    let old = a.pin_snapshot();
    assert_eq!(a.read_spans(&old).len(), 1);
    b.ingest(vec![record(2)]);
    b.flush_memtable();
    let fresh = a.pin_snapshot();
    assert_eq!(a.read_spans(&fresh).len(), 2);
    assert_eq!(
        a.read_spans(&old).len(),
        1,
        "refresh must retain rows required by a pinned older version"
    );
    drop(old);
    drop(fresh);
    a.try_flush_memtable().unwrap();
    assert_eq!(
        a.memtable_len(),
        0,
        "released snapshot rows must be reclaimed without duplicating a segment"
    );
}

#[test]
fn durability_regression_busy_writer_pin_refreshes_before_reading_reclaimed_segments() {
    let d = TempDir::new("pin-gc");
    let writer = d.open();
    writer.ingest(vec![record(1)]);
    writer.flush_memtable();
    writer.ingest(vec![record(2)]);
    writer.flush_memtable();
    let stale = d.open();
    writer.commit_compaction(&[SegmentId::new(1), SegmentId::new(2)]);
    writer.reclaim();
    assert!(!d.0.join("segments/seg-1.dat").exists());
    // 保持正常 writer 锁占用窗口，所有文件仅在本测试的临时目录。
    let lock = d.0.join(".yitrace.write.lock.d");
    fs::create_dir(&lock).unwrap();
    fs::write(
        lock.join("owner.json"),
        format!("{{\"pid\":{}}}", std::process::id()),
    )
    .unwrap();
    let release = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(100));
        fs::remove_dir_all(lock).unwrap();
    });
    let snapshot = stale.pin_snapshot();
    release.join().unwrap();
    assert_eq!(
        stale.read_spans(&snapshot).len(),
        2,
        "pin must wait for writer and select a protected current manifest"
    );
}

#[test]
fn durability_regression_backup_retains_metadata() {
    let d = TempDir::new("backup-source");
    let dest = TempDir::new("backup-dest");
    let e = d.open();
    e.ingest(vec![record(1)]);
    e.flush_memtable();
    e.add_annotation(
        NewTraceAnnotation {
            trace_id: 1,
            label: "retain-me".into(),
            ..Default::default()
        },
        None,
    );
    e.backup_snapshot(&dest.0).unwrap();
    let restored = dest.open();
    assert_eq!(count(&restored), 1);
    assert_eq!(
        restored
            .annotations(&TraceAnnotationFilter::default())
            .len(),
        1
    );
}

#[test]
fn durability_regression_failed_metadata_write_is_reported_and_retryable() {
    let d = TempDir::new("metadata-error");
    let e = d.open();
    fs::create_dir(d.0.join("metadata.tmp")).unwrap();
    let api = yt_engine::EngineJsonApi::new(Arc::clone(&e));
    let (status, body) = api.route(
        "POST",
        "/v1/annotations",
        r#"{"trace_id":1,"label":"retryable"}"#,
    );
    assert_eq!(status, 500, "API must return save failure: {body}");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        e.add_annotation(
            NewTraceAnnotation {
                trace_id: 1,
                label: "retryable".into(),
                ..Default::default()
            },
            None,
        )
    }));
    assert!(failed.is_err(), "failed metadata save must be reported");
    assert!(e.annotations(&TraceAnnotationFilter::default()).is_empty());
    fs::remove_dir(d.0.join("metadata.tmp")).unwrap();
    e.add_annotation(
        NewTraceAnnotation {
            trace_id: 1,
            label: "retryable".into(),
            ..Default::default()
        },
        None,
    );
    drop(e);
    assert_eq!(
        d.open()
            .annotations(&TraceAnnotationFilter::default())
            .len(),
        1
    );
}

#[test]
fn durability_regression_upgrade_rejects_ambiguous_or_changed_tenant() {
    let d = TempDir::new("upgrade-tenant");
    let e = d.open();
    let mut first = record(1);
    first.fields.tenant_id = Some(10);
    let mut second = first.clone();
    second.fields.tenant_id = Some(20);
    e.try_ingest(vec![first, second]).unwrap();
    e.try_flush_memtable().unwrap();
    let err = e
        .try_commit_upgrade(
            SegmentId::new(1),
            1,
            1,
            SpanFields {
                status: Some(1),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
    assert_eq!(count(&e), 2);

    let mut unique = record(2);
    unique.fields.tenant_id = Some(10);
    e.try_ingest(vec![unique]).unwrap();
    e.try_flush_memtable().unwrap();
    let err = e
        .try_commit_upgrade(
            SegmentId::new(2),
            2,
            2,
            SpanFields {
                tenant_id: Some(20),
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    e.try_commit_upgrade(
        SegmentId::new(2),
        2,
        2,
        SpanFields {
            status: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    let spans = e.read_spans(&e.pin_snapshot());
    let updated = spans.iter().find(|s| s.trace_id == 2).unwrap();
    assert_eq!(updated.tenant_id, Some(10));
    assert_eq!(updated.status, Some(1));
}
