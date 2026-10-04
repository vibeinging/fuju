import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { YiTraceDB } from "../index.js";

const dir = await mkdtemp(join(tmpdir(), "yt-node-durability-"));
let db;
try {
  db = await YiTraceDB.open({ dataDir: dir });
  await db.ingest([{ trace_id: "durable-run", span_id: "durable-span", ts: 1, seq: 1, event_type: 1, ext_span_id: "durable-span", span_name: "preserve acknowledged row" }]);
  for (const id of [1, 2]) await mkdir(join(dir, "segments", `seg-${id}.tmp`));
  await assert.rejects(() => db.flush(), /flush yiTrace failed/);
  await assert.rejects(() => db.close(), /flush yiTrace failed/);
  assert.match(JSON.stringify(await db.trace("durable-run")), /preserve acknowledged row/);
  for (const id of [1, 2]) await rm(join(dir, "segments", `seg-${id}.tmp`), { recursive: true });
  await db.close();
  db = await YiTraceDB.open({ dataDir: dir });
  assert.match(JSON.stringify(await db.trace("durable-run")), /preserve acknowledged row/);
} finally {
  if (db) await db.close();
  await rm(dir, { recursive: true, force: true });
}
console.log("Node durability errors: ok");
