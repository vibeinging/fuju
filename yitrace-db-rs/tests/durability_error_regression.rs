use yitrace_db::{SpanEventBuilder, YiTraceDb};

#[test]
fn embedded_flush_and_close_return_io_errors_without_closing_handle() {
    let dir = std::env::temp_dir().join(format!(
        "yt-rs-io-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut db = YiTraceDb::open(&dir).unwrap();
    let mut events = SpanEventBuilder::new("durable-run");
    events.start_span("durable-span", "preserve acknowledged row");
    db.ingest_builder(&events).unwrap();
    for id in [1, 2] {
        std::fs::create_dir(dir.join(format!("segments/seg-{id}.tmp"))).unwrap();
    }
    assert!(db.flush().is_err());
    assert!(db.close().is_err());
    assert!(db
        .trace("durable-run")
        .unwrap()
        .contains("preserve acknowledged row"));
    for id in [1, 2] {
        std::fs::remove_dir(dir.join(format!("segments/seg-{id}.tmp"))).unwrap();
    }
    db.close().unwrap();
    let mut reopened = YiTraceDb::open(&dir).unwrap();
    assert!(reopened
        .trace("durable-run")
        .unwrap()
        .contains("preserve acknowledged row"));
    reopened.close().unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
