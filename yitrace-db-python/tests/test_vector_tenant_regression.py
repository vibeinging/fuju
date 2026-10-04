from __future__ import annotations

from yitrace_db import YiTraceDB


def test_vectors_keep_nullable_connection_tenant_after_reopen(tmp_path):
    for tenant in [None, 0, 7]:
        with YiTraceDB.open(tmp_path, tenant_id=tenant) as db:
            db.ingest([{"trace_id": "shared-trace", "span_id": "shared-span", "ts": 1, "seq": 1, "event_type": 2, "ext_span_id": "shared-span", "logs": [f"tenant-{tenant}"]}])
            db.index_embedding("shared-trace", "shared-span", [float(tenant or 0), 0.0])
    for tenant in [0, 7]:
        with YiTraceDB.open(tmp_path, tenant_id=tenant) as db:
            hits = db.search(vector=[0.0, 0.0], k=1)
            assert len(hits) == 1
            assert hits[0]["logs"] == [f"tenant-{tenant}"]
    with YiTraceDB.open(tmp_path) as db:
        assert len(db.search(vector=[0.0, 0.0], k=3)) == 3
