use std::time::{SystemTime, UNIX_EPOCH};
use yitrace_db::{OpenOptions, YiTraceDb};

#[test]
fn embedded_vectors_keep_connection_tenant() {
    let path = std::env::temp_dir().join(format!(
        "yt_rs_vector_tenant_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    for tenant in [7, 8] {
        let mut db =
            YiTraceDb::open_with_options(OpenOptions::new(&path).tenant_id(tenant)).unwrap();
        db.ingest_json(&format!(r#"[{{"trace_id":1,"span_id":1,"ts":1,"seq":1,"event_type":2,"ext_span_id":"1-1","logs":["tenant-{tenant}"]}}]"#)).unwrap();
        db.index_embedding(1, 1, vec![tenant as f32, 0.]).unwrap();
        db.close().unwrap();
    }
    for tenant in [7, 8] {
        let mut db =
            YiTraceDb::open_with_options(OpenOptions::new(&path).tenant_id(tenant)).unwrap();
        let result = db.search_json(r#"{"vector":[0,0],"k":1}"#).unwrap();
        assert!(result.contains(&format!("tenant-{tenant}")), "{result}");
        assert!(
            !result.contains(&format!("tenant-{}", if tenant == 7 { 8 } else { 7 })),
            "{result}"
        );
        db.close().unwrap();
    }
    std::fs::remove_dir_all(path).unwrap();
}
