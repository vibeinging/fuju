# @yitrace/db

Embedded yiTrace database for Node.js and Electron.

## Install

```bash
npm install @yitrace/db
```

`@yitrace/db` is the JavaScript entry package. Native binaries are published as
optional per-platform packages such as `@yitrace/db-darwin-arm64` and
`@yitrace/db-linux-x64-gnu`, so users only download the binary for their
platform.

Supported targets:

- macOS x64 / arm64
- Linux x64 / arm64 with glibc
- Windows x64 with MSVC

For an internal consumer that needs a stable file-based install source, build
immutable tarballs and lock those exact files in the consumer repo:

```bash
npm ci
npm run build
npm run release:artifacts
npm run release:prepublish
npm run pack:verify  # runs pack:local first
```

This writes `@yitrace/db` plus the current platform package to
`yitrace-node/dist/*.tgz`. Set `YITRACE_PACK_ALL_PLATFORMS=1` only after a
release job has deliberately collected fresh binaries for every platform.
`pack:local` appends a commit label such as
`g1a2b3c4d5e6f` to each tarball name and writes `dist/pack-manifest.json` with
the exact files. Set `YITRACE_PACK_LABEL=<release-id>` when the consuming repo
needs a human-chosen immutable label. Put those tarballs in the consuming repo
or an internal package registry, then depend on exact `file:` tarballs:

```json
{
  "dependencies": {
    "@yitrace/db": "file:vendor/yitrace-db-0.1.4-g1a2b3c4d5e6f.tgz",
    "@yitrace/db-darwin-x64": "file:vendor/yitrace-db-darwin-x64-0.1.4-g1a2b3c4d5e6f.tgz"
  }
}
```

Do not keep overwriting a shared `yitrace-db-0.0.1.tgz`. If the payload changes,
the filename or registry version must change too, so lockfiles and rollbacks can
identify the exact native binary.

For a registry publish, publish platform packages first and the root package
last. Do not publish a root package that contains only the current machine's
native binary.

Current platform policy:

- AgenticData server builds stay on one native architecture at a time. The
  current server baseline is x64: Node, DuckDB, yiTrace native, and sqlite native
  must all be x64.
- Do not mix an arm64 yiTrace native package with x64 DuckDB/sqlite, or the
  inverse. If AgenticData switches the server to arm64, first move DuckDB and
  sqlite to arm64 too, or adopt the same per-platform optional package strategy
  for those dependencies.
- AgenticData local development may still use macOS arm64 artifacts
  (`@yitrace/db` root tarball + `@yitrace/db-darwin-arm64` tarball), but that is
  not the server architecture decision.
- Local tarballs ignore stale binaries for other platforms by default.
- Public npm / CI release target set remains macOS x64/arm64, Linux x64/arm64
  glibc, and Windows x64 MSVC. Those platform packages must be produced by CI
  or matching build machines before publishing the root package.

## Usage

ESM:

```ts
import { YiTraceDB, createSpanEventBuilder } from "@yitrace/db";

const db = await YiTraceDB.open({ dataDir: "./data", tenantId: 1 });

const builder = createSpanEventBuilder({
  traceId: "run-uuid",
  sessionId: "session-uuid",
  agentName: "risk-agent",
  attrs: {
    project_id: "agentic-data",
    skill: "review",
    mode: "auto",
    call_site: "worker.ts:10",
  },
});

builder.startSpan({
  spanId: "span-uuid",
  name: "risk.review",
  displayName: "风险审核",
  toolName: "card-risk",
  model: "gpt-5",
  inputText: "疑似盗刷订单",
});
builder.log({ spanId: "span-uuid", message: "疑似盗刷" });
builder.endSpan({
  spanId: "span-uuid",
  status: 0,
  durationNs: 12_000_000,
  outputText: "建议人工复核",
});

await builder.ingest(db);

const hits = await db.search({
  text: "盗刷",
  k: 10,
  filter: {
    attrs: {
      project_id: "agentic-data",
      skill: "review",
      mode: "auto",
      call_site: "worker.ts:10",
    },
  },
});
const trace = await db.trace("run-uuid");
const span = await db.span("run-uuid", "span-uuid");
const logMessages = span?.logEvents?.flatMap((event) => event.messages) ?? [];
const traces = await db.traces();

const page = await db.traceSearch({
  filter: { projectId: "agentic-data", skill: "review", toolName: "card-risk" },
  limit: 10,
});
console.log(page.readPlan?.source, page.readPlan?.candidateSpanKeys);

await db.close();
```

普通用户只需要给 `startSpan` 传 `name`。它会自动写成内部 `span_name`；只有技术名不适合直接展示时才额外传 `displayName`。`agentName` 建议放在 builder 默认上下文中，所有事件都会继承。`displayName` 只写在 SpanStart，只负责展示，不参与检索、过滤或节点合并；全空格按未设置处理。

Semantic / hybrid search is opt-in. Pass an embedder when opening the DB, then
explicitly choose `mode: "semantic"` or `mode: "hybrid"` for query-time
embedding. Plain `search({ text })` stays BM25-only and does not call the model.

```ts
const db = await YiTraceDB.open({
  dataDir: "./data",
  embedder: {
    model: "your-embedding-model",
    dimensions: 1536,
    embedQuery: async (text) => {
      // Call OpenAI, a local model, or your own embedding service here.
      return vector;
    },
    embedDocuments: async (texts) => {
      return vectors;
    },
  },
});

await db.ingest(events);
await db.indexEmbeddings([
  { traceId: "run-uuid", spanId: "span-uuid", text: "疑似盗刷订单 建议人工复核" },
]);

const semantic = await db.search({ text: "相似的风控失败", mode: "semantic", k: 10 });
const hybrid = await db.search({ text: "盗刷", mode: "hybrid", k: 10 });
```

Use cases:

- Use `search({ text })` for cheap keyword/BM25 search. It never calls the embedder.
- Use `search({ text, mode: "semantic" })` for vector-only search from query text.
- Use `search({ text, mode: "hybrid" })` or `vector: "auto"` for BM25 + vector fusion.
- Use `search({ vector })` when the caller already has a query vector.
- Use `indexEmbedding({ traceId, spanId, vector })` when the caller already has a span vector.
- Use `indexEmbeddings([{ traceId, spanId, text }])` to batch document embedding through the configured embedder.
- Use `ingest(events, { indexEmbeddings: true })` only when the ingest path is allowed to wait for embedding calls. The default is off so trace ingestion is not blocked by model latency.

Embedding writes use the connection's `tenantId`, or the `tenantId` in the method options, just like ingest and search. The vector identity includes tenant, trace and span; repeating the same vector is idempotent, and a new vector for the same identity replaces the old search result. Numeric tenant `0` is distinct from a connection without a tenant.

Do not mix different embedding models or dimensions in the same data dir. The
wrapper validates vector dimensions in the current process, and the disk graph
also rejects wrong dimensions, but same-dimension different-model vectors are a
caller-level contract. Index finalized span text once where possible; repeated
indexing of the same span is accepted, but it adds extra graph nodes.

`traceSearch` / `traceAggregate` / `storageStats` / `traceTrajectories` / `trajectoryGroups` /
`loops` / `loop` / `taskTraces` return `readPlan`. `source: "filter_index"` means the query
first used the attrs sidecar postings to narrow span keys. Postings are memory-budgeted:
very wide values or total-entry pressure disable only the affected postings, then queries
fall back to the sidecar rows and still return correct results. Persistent data dirs write a
disposable `filter_attrs.dat` segment cache and reopen recovery loads it before replaying the
WAL tail. `source: "scan"` means it fell back to a full folded scan, for example when the
filter only contains `text` or unknown attrs.
`traceAggregate` can also return `source: "aggregate_rollup"` for no-text aggregate queries;
persistent data dirs write a `trace_rollup.dat` segment cache, and reopen recovery loads it before
replaying the WAL tail. The cache is disposable: deletes, retention apply, segment upgrades, stale
versions, or corrupt cache contents rebuild it from the current snapshot.
Persistent data dirs also write `bm25.dat` and `segment_bloom.dat`. When all four caches
(`trace_rollup.dat`, `filter_attrs.dat`, `bm25.dat`, `segment_bloom.dat`) match the manifest,
reopen recovery does not scan historical segments. A point lookup loads the bloom cache lazily,
then validates and reads only segments that may contain the requested span; it does not load BM25.
The v2 bloom cache has a full-file CRC. A v1, missing, or corrupt bloom cache is rebuilt once from
the real segments and written back atomically. That migration I/O is included in `readPlan` with
`fallbackReason: "segment_bloom_migrated"`. A missing BM25 cache is rebuilt by the first text or
hybrid search.
Trajectory, loop, and task helpers can return `source: "trajectory_rollup"` for no-text path
summaries and reuse the same `trace_rollup.dat` cache after reopen. When those helpers expand
complete traces after finding candidates, `readPlan.traceFetchSource` shows whether that second
step also used the rollup by trace id. Text filters still use the normal folded read path.

Direct event ingest is still supported when you already have wire events:

```ts
await db.ingest([
  {
    trace_id: "run-uuid",
    span_id: "span-uuid",
    ts: 1,
    seq: 1,
    event_type: 2,
    ext_span_id: "span-uuid",
    logs: ["疑似盗刷"],
    attrs: {
      external_run_id: "run-uuid",
      project_id: "agentic-data",
      skill: "review",
      mode: "auto",
      call_site: "worker.ts:10",
    },
  },
]);
```

CommonJS:

```js
const { YiTraceDB, createSpanEventBuilder } = require("@yitrace/db");
```

This package does not read yiTrace files directly. It embeds the Rust engine in
the Node process through Node-API, so WAL recovery, manifest snapshots, folding,
tenant filtering, BM25, and vector search still run through the database engine.
Internally it calls yiTrace's `EngineJsonApi` in-process; it does not start an
HTTP server, bind a port, or send traffic through a TCP socket.

For Electron, the main process is still the cleanest owner for UI calls, but
multiple local app processes may open the same `dataDir`. For Node servers,
`cluster` or PM2 workers on the same machine may also open the same local data
directory. yiTrace serializes open/write paths with internal data-dir locks and
uses reader pins so `reclaim()` does not delete segment files while another
process is reading a snapshot.

Do not share one `dataDir` across machines or unreliable network filesystems.
For multi-host deployments, run one yiTrace server process and send trace data
over HTTP.

IDs passed to `db.ingest()` are treated as business IDs, whether they are JSON
strings or numbers. yiTrace keeps internal numeric IDs for indexing and stores
the original values in `external_trace_id`, `external_span_id`,
`external_parent_span_id`, and `external_session_id`. For public queries,
`filter.traceId: "business-run-id"` and `filter.traceId: 123456` are both
treated as external trace IDs and use the filter sidecar fast path. `attrs` is
persisted through the engine and returned as JSON on search, trace, and span
detail responses.

Trace and span detail responses also return raw `logEvents`, so applications do
not need to mirror log lines into `attrs.event_logs`. Each event keeps its
`ts`, `seq`, `eventType`, stable `eventId`, `messages`, and event-level
`attrs`:

```ts
const span = await db.span("run-uuid", "tool-call-1");
for (const event of span?.logEvents ?? []) {
  console.log(event.seq, event.messages);
}
```

The event builder hides `seq`, `event_type`, start/end event pairing, and
`ext_span_id`. It emits the same wire format as direct `db.ingest()`, so callers
can inspect `builder.events()` before sending or call `builder.ingest(db)` to
flush the batch.

`SpanEvent` explicitly supports the commonly consumed fields:
`duration_ns`, `tool_name`, `model`, `input_text`, and `output_text`, plus token
counts, status, logs, tenant, external IDs, and `attrs`.

Search supports exact attrs filters for the high-cardinality dimensions used by
AgenticData trace pages:

```ts
await db.search({
  text: "盗刷",
  filter: {
    project_id: "agentic-data",
    skill: "review",
    mode: "auto",
    call_site: "worker.ts:10",
  },
});
```

The equivalent nested form is `filter.attrs.{project_id,skill,mode,call_site}`.
Values are exact matches after JSON normalization. Strings remain strings,
numbers remain JSON numbers, booleans remain booleans, `null` remains null, and
arrays/objects round-trip as JSON arrays/objects in `attrs` on search, trace,
and span detail responses. Filtering is guaranteed for the high-frequency read
model fields: `project_id`, `skill`, `mode`, `call_site`, `task_fingerprint`,
`loop_id`, `harness_version`, `schema_fingerprint`, `intent_signature`,
`validation_status`, `review_status`, `eval_status`, `path_memory_id`,
`stop_reason`, `phase`, and `validator`. Other attrs are stored and returned,
but do not have a dedicated index contract yet.

Session listing also accepts the same attrs filter shape:

```ts
await db.sessions({
  attrs: { project_id: "agentic-data", skill: "review", mode: "auto" },
});
```

The session filter returns a session when at least one span in that session
matches all supplied attrs, then returns the complete session aggregate.

Single-node read models are exposed as thin wrappers over the same in-process
engine API:

```ts
const trajectories = await db.traceTrajectories({
  filter: { projectId: "agentic-data", taskFingerprint: "refund-v1" },
});
const groups = await db.trajectoryGroups({
  filter: { projectId: "agentic-data", taskFingerprint: "refund-v1" },
});
const diff = await db.traceDiff("run-a", "run-b");
const loops = await db.loops({ projectId: "agentic-data", taskFingerprint: "refund-v1" });
const loop = await db.loop("loop-refund");
const taskRuns = await db.taskTraces("refund-v1", { validationStatus: "pass" });
```

These APIs use the in-memory filter index for the common filtering step. No-text
path summaries reuse the span rollup cache; materialized disk indexes dedicated
to trajectory, loop, and task views are a later performance upgrade.

Annotation and dataset association are stored in the embedded metadata ledger.
They do not copy trace payloads and do not change the trace/WAL/segment format:

```ts
const annotation = await db.annotate({
  traceId: "run-a",
  spanId: "span-a",
  label: "best_path",
  score: 950,
  source: "human",
  attrs: { project_id: "agentic-data", skill: "review" },
});

await db.updateAnnotation(annotation.annotationId, {
  status: "resolved",
  reviewer: "qa",
});

await db.linkDatasetItem({
  datasetId: "agentic-regression",
  itemId: "case-1",
  traceId: "run-a",
  spanId: "span-a",
  split: "eval",
  label: "pass",
});

const annotations = await db.annotations({ projectId: "agentic-data", includeDeleted: true });
const datasetLinks = await db.datasetAssociations({ datasetId: "agentic-regression" });
```

Retention uses the same metadata ledger for audit and policy records. It is
explicit: call `retentionPlan()` first, then `applyRetention()` when the plan is
acceptable. Hot traces still in the WAL tail are skipped. Audit and policy
queries use the same in-memory metadata postings as annotations.

```ts
const plan = await db.retentionPlan({
  filter: { projectId: "agentic-data" },
  deleteBeforeTs: 100000,
  protect: { annotations: true, datasetAssociations: true },
});

const result = await db.applyRetention({
  filter: { projectId: "agentic-data" },
  deleteBeforeTs: 100000,
  requestedBy: "nightly-retention",
});

const audits = await db.retentionAudits({ source: "nightly-retention" });
```

`OpenOptions.readOnly` is intentionally not exposed yet. Passing `readOnly`
throws at runtime so applications do not accidentally assume a true read-only
open while the engine still uses the writable durable path.

## Electron Packaging

Package the native `.node` binary outside Electron's `asar` archive. For
electron-builder, keep the platform package and unpack native files:

```json
{
  "build": {
    "asarUnpack": [
      "**/*.node",
      "node_modules/@yitrace/db*/**/*"
    ]
  }
}
```

Do not tree-shake or prune `@yitrace/db-*` optional platform packages from the
main-process bundle. The JavaScript loader resolves the matching optional
package at runtime. If your bundler copies native files to a custom location,
set `NAPI_RS_NATIVE_LIBRARY_PATH` to the unpacked `.node` file before importing
`@yitrace/db`:

```js
process.env.NAPI_RS_NATIVE_LIBRARY_PATH = require("path").join(
  process.resourcesPath,
  "native",
  "yitrace-db.darwin-arm64.node",
);
```

Open `YiTraceDB` only from Electron's main process and expose specific IPC
methods such as `search`, `trace`, and `span` to renderers.

## Local Development

```bash
npm install
npm run build
npm test
```

`npm run build` produces a local `yitrace-db.<platform>.node` file for the
current Node platform. That file is intentionally ignored by git. Set
`NAPI_TARGET=aarch64-apple-darwin` or another Rust target triple when you need
to override local target detection.

From the repository root, also run the package-mode eval when changing the Node
package or cross-language package contracts:

```bash
./scripts/package_mode_eval.sh
```

That eval builds and tests `@yitrace/db` together with the Python, Rust, and
TypeScript package surfaces, so package drift is caught in one place. For native
packaging itself, run `npm run pack:verify`; it installs the root tarball and
the current platform optional package in a clean consumer, then checks ESM,
CommonJS, `NAPI_RS_NATIVE_LIBRARY_PATH` loading, and root/native package version
agreement.

## Publishing

Do not publish a root package that only contains the local machine's `.node`
file. Public npm releases must use the optional platform-package layout.

Release flow:

```bash
npm ci
npm run npm:dirs

# Run once per target in CI. Example:
npm run build:release -- --target x86_64-unknown-linux-gnu

# After all .node artifacts have been collected under ./artifacts:
npm run release:artifacts
npm run release:prepublish  # metadata only; this script skips automatic optional package publish
npm run pack:check
npm run pack:verify         # verify ESM/CJS/native-path loading in a clean consumer
```

Then publish each platform package first, followed by the root package:

```bash
npm publish npm/darwin-x64 --access public
npm publish npm/darwin-arm64 --access public
npm publish npm/linux-x64-gnu --access public
npm publish npm/linux-arm64-gnu --access public
npm publish npm/win32-x64-msvc --access public
npm publish --access public
```

The root `@yitrace/db` package declares those platform packages as
`optionalDependencies`. npm skips incompatible OS/CPU packages during install,
and `native.js` loads the matching binary at runtime.
