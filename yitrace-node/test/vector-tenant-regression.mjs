import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { YiTraceDB } from "../index.js";

const dir = await mkdtemp(join(tmpdir(), "yitrace-vector-tenant-"));
try {
  // 相同事件、trace、span来自不同租户，身份仍须隔离，重开后也成立。
  for (const tenantId of [0, 7]) {
    const db = await YiTraceDB.open({ dataDir: dir, tenantId });
    try {
      await db.ingest([{ trace_id: "shared-trace", span_id: "shared-span", ts: 1, seq: 1, event_type: 2, ext_span_id: "shared-span", logs: [`tenant-${tenantId}`] }]);
      await db.indexEmbedding({ traceId: "shared-trace", spanId: "shared-span", vector: [tenantId, 0] });
    } finally { await db.close(); }
  }
  for (const tenantId of [0, 7]) {
    const db = await YiTraceDB.open({ dataDir: dir, tenantId });
    try {
      const hits = await db.search({ vector: [0, 0], k: 1 });
      assert.equal(hits.length, 1);
      assert.deepEqual(hits[0].logs, [`tenant-${tenantId}`]);
    } finally { await db.close(); }
  }
} finally { await rm(dir, { recursive: true, force: true }); }
