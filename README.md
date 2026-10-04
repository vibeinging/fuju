# yiTrace

## Agent evaluation or trace diagnostics?

- [Fuju RSI](https://github.com/vibeinging/fuju-rsi): test and improve existing agents with a coding-agent Skill, starting with text-to-SQL. Try the offline example or connect a real program for a baseline report.
- [Fuju Trace](https://github.com/vibeinging/fuju-trace): record, search, and diagnose agent execution.

This repository contains the original yiTrace engine and compatibility packages. RSI is a separate development tool; applications can run without it.


**A local TraceDB for agentic engineering.**

Agentic engineering needs more than prompts, logs, and chat memory. You need to
see the real path an agent took: prompts, tool calls, spans, errors, tokens,
model outputs, and the step that worked. yiTrace stores that path in a local
TraceDB so you can search it, replay it, turn it into eval data, and feed useful
execution history back into Agent Memory.

[中文](README.zh-CN.md) · English

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.80%2B-orange?logo=rust)](https://www.rust-lang.org/)
[![npm](https://img.shields.io/npm/v/%40yitrace%2Fdb?logo=npm)](https://www.npmjs.com/package/@yitrace/db)
[![PyPI](https://img.shields.io/pypi/v/yitrace-db?logo=pypi)](https://pypi.org/project/yitrace-db/)

![yiTrace console](docs/images/console-overview.png)

## Why

Building agents is not just prompt engineering. It is engineering a system that
can call tools, recover from failures, compare runs, and improve over time.

The missing layer is execution memory: not just what the user said, but what
the agent actually did. yiTrace gives you that layer locally:

- replay multi-turn, tool-heavy agent runs
- search prompts, logs, tool calls, errors, and model outputs
- filter by tenant, agent, status, time, and custom attrs
- track token and cost by trace, session, and agent
- collect failed or useful spans into eval datasets
- reuse successful run paths as evidence for Agent Memory

## Quick Start

Choose the install path that matches your app.

### Python

Use this when your Python or FastAPI app wants local trace search without
running a separate yiTrace service.

```bash
python -m pip install yitrace yitrace-db
```

```python
from yitrace import DbExporter, Tracer, connect

with connect(path="./yitrace-data", tenant_id=1) as db:
    tracer = Tracer(exporter=DbExporter(db, tenant_id=1), node_id=1)

    with tracer.trace("risk review", tenant_id=1) as trace:
        with trace.span("LLM check") as span:
            span.log("疑似盗刷")
            span.set_tokens(input_tokens=900, output_tokens=120)

    tracer.close()

    hits = db.search({"text": "盗刷", "k": 10})
    print(hits)
```

The `with connect(...)` form above is for scripts and small examples. A
long-running FastAPI, ARQ, or Celery service should initialize yiTrace once per
process, reuse the process runtime, and shut it down once when that process
exits. Do not open the DB for every request, task, or query. See the
[Python service integration guide (Chinese)](docs/design/2026-07-14_python-service-integration.md)
for lifecycle hooks and the buffered/spool/HTTP choice.

If you only want to send traces to a running yiTrace server, install just the
SDK:

```bash
python -m pip install yitrace
```

```python
from yitrace import HttpExporter, Tracer

tracer = Tracer(
    exporter=HttpExporter("http://127.0.0.1:7878/v1/ingest", tenant_id=1),
    node_id=1,
)

with tracer.trace("risk review", tenant_id=1) as trace:
    with trace.span("tool call") as span:
        span.log("query risk database")

tracer.close()
```

### Node / Electron

Use `@yitrace/db` when a Node backend or Electron main process needs local
trace storage and search. It embeds the Rust engine through Node-API; it does
not parse database files in JavaScript.

```bash
npm install @yitrace/db
```

```ts
import { YiTraceDB, createSpanEventBuilder } from "@yitrace/db";

const db = await YiTraceDB.open({ dataDir: "./yitrace-data", tenantId: 1 });

const events = createSpanEventBuilder({
  traceId: "run-uuid",
  sessionId: "session-uuid",
  attrs: {
    project_id: "agentic-data",
    skill: "review",
    mode: "auto",
    call_site: "worker.ts:10",
  },
});

events.startSpan({ spanId: "span-uuid", name: "risk review", agentName: "risk-agent" });
events.log({ spanId: "span-uuid", message: "possible card fraud" });
events.endSpan({ spanId: "span-uuid", status: 0, durationNs: 12_000_000 });

await events.ingest(db);

const hits = await db.search({
  text: "fraud",
  k: 10,
  filter: { attrs: { project_id: "agentic-data", skill: "review" } },
});

console.log(hits);
await db.close();
```

If your TypeScript app only emits traces to a server, use the lighter SDK:

```bash
npm install @yitrace/trace-sdk
```

```ts
import { HttpExporter, Tracer } from "@yitrace/trace-sdk";

const tracer = new Tracer(
  new HttpExporter({ url: "http://127.0.0.1:7878/v1/ingest", tenantId: 1 }),
  1,
);

tracer.trace("risk review", (trace) => {
  trace.span("tool call", (span) => {
    span.log("query risk database");
  });
}, undefined, 1);

await tracer.close();
```

### Run a local yiTrace server

Use server mode for multiple machines, containers on different hosts, or cases
where one machine cannot own the data directory. Multiple worker processes on
the same machine may use embedded mode with the same local data directory.

```bash
python -m pip install "yitrace-db[server]"
yitrace-db serve --data-dir ./yitrace-data --bind 127.0.0.1:7878
```

Then search over HTTP:

```bash
curl -XPOST http://127.0.0.1:7878/v1/search \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-Id: 1' \
  -d '{"text":"盗刷","k":10}'
```

The full HTTP contract is in [API Reference](docs/API_REFERENCE.md).

## Which Package Should I Install?

| App shape | Install | What it does |
|---|---|---|
| Python app writes and searches local traces | `pip install yitrace yitrace-db` | Embedded DB in the Python process |
| Python app only sends traces to a server | `pip install yitrace` | Lightweight tracing SDK |
| FastAPI app wants a yiTrace route/server | `pip install "yitrace-db[server]"` | Embedded DB plus optional FastAPI/Uvicorn server |
| Node backend or Electron app writes and searches local traces | `npm install @yitrace/db` | Embedded DB through Node-API |
| TypeScript app only sends traces to a server | `npm install @yitrace/trace-sdk` | Lightweight tracing SDK |
| Existing OTel/OpenInference app | Send `POST /v1/traces` | OTLP/OpenInference compatible ingest |
| Rust app | Source dependency | Rust SDK and embedded wrapper live in this repo |

Published packages:

- npm: [`@yitrace/db`](https://www.npmjs.com/package/@yitrace/db), [`@yitrace/trace-sdk`](https://www.npmjs.com/package/@yitrace/trace-sdk)
- PyPI: [`yitrace`](https://pypi.org/project/yitrace/), [`yitrace-db`](https://pypi.org/project/yitrace-db/)

## Embedded Mode vs Server Mode

**Embedded mode** opens a local data directory inside your app process:

```python
db = connect(path="./yitrace-data", tenant_id=1)
```

```ts
const db = await YiTraceDB.open({ dataDir: "./yitrace-data", tenantId: 1 });
```

Use it when one machine owns the data directory. Multiple local worker
processes on the same machine are supported; yiTrace serializes open/write
paths with data-dir locks and uses reader pins for snapshot cleanup.

**Server mode** runs one yiTrace process and lets clients write over HTTP:

```bash
yitrace-db serve --data-dir ./yitrace-data --bind 0.0.0.0:7878
```

Use it for multiple machines, containers on different hosts, or any unreliable
network filesystem. Do not share one embedded data directory across machines.

## Console

yiTrace includes a replay console. If you install from packages, run the Python
server and open:

```text
http://127.0.0.1:7878/
```

From source, build the React console once and copy it into the engine crate:

```bash
cd yitrace-console
npm install
VITE_API=http npm run build
rm -rf ../yitrace-engine/crates/yt-engine/console_dist
cp -r dist ../yitrace-engine/crates/yt-engine/console_dist
```

The console uses the same `/v1/*` JSON API as any custom UI.

## Build From Source

Use this path when you are changing the engine, console, or package wrappers.

```bash
git clone https://github.com/vibeinging/fuju.git
cd fuju

cd yitrace-engine
cargo test --offline
cargo run -p yt-engine --example server
```

The Rust engine core is intentionally std-only. Heavy integrations live in
separate crates or package directories.

Package-level checks:

```bash
# Python SDK
cd yitrace-sdk/python
python -m pytest

# TypeScript SDK
cd yitrace-sdk/typescript
npm install
npm test
npm run build

# Node embedded DB
cd yitrace-node
npm install
npm run build
npm test

# Python embedded DB
cd yitrace-db-python
python -m pytest
```

Release artifact build:

```bash
python scripts/check_release_versions.py
./scripts/package_release_artifacts.sh
./tests/upgrade_012_to_current.sh
./tests/sidecar_rebuild_kill9.sh
```

The GitHub Actions package workflow runs only on `v*` tag pushes. Use
`vX.Y.Z-only-python-sdk` style tags to test one package target; use `vX.Y.Z`
tags for the full package matrix. Every native job installs its final wheel or
tarballs in a clean consumer before uploading the artifact.

## How It Works

```text
SDKs / OTLP / embedded package
    |
    v
EngineJsonApi or HTTP ingest
    |
    v
WAL + memtable --flush--> immutable segments
    |                         |
    v                         v
BM25 / vector / attrs indexes read-time fold
    |                         |
    +---------- search / replay / cost / eval
```

The key idea is simple: yiTrace stores events, not mutable spans. A span is
written as start, log, end, and late attribute events. Reads fold those events
into a complete span. Event identity is deterministic:

```text
event_id = hash(ext_span_id, seq, event_type)
```

That makes retries and crash replay safe: the same event is counted once.

## Available Today

yiTrace is built for teams doing agentic engineering: debugging real runs,
building eval loops, inspecting tool behavior, and keeping execution history
private and searchable.

| Area | What you get |
|---|---|
| Local TraceDB | WAL, restart recovery, snapshot reads |
| Python | SDK and embedded DB on PyPI |
| Node / Electron | Embedded DB on npm |
| TypeScript | Lightweight tracing SDK on npm |
| Rust | SDK and embedded wrapper in this repo |
| Search | Chinese BM25, vector recall, hybrid search |
| Filtering | Tenant, agent, status, time, attrs |
| Replay | Session, trace, span, and log event views |
| Eval data | Annotation and dataset hooks |
| Agent Memory | Searchable execution history for what worked and what failed |
| Console | Local replay UI |

For implementation boundaries, see [Current State](docs/CURRENT_STATE.md).

## Repository Layout

```text
yitrace-engine/              # Rust engine workspace
yitrace-console/             # React replay console
yitrace-sdk/python/          # Python tracing SDK: yitrace
yitrace-sdk/typescript/      # TypeScript tracing SDK: @yitrace/trace-sdk
yitrace-sdk/rust/            # Rust tracing SDK
yitrace-node/                # Node/Electron embedded DB: @yitrace/db
yitrace-db-python/           # Python embedded DB: yitrace-db
yitrace-db-rs/               # Rust embedded DB wrapper
yitrace-segstore-vortex/     # optional Vortex segment-store adapter
yitrace-tokenizer-jieba/     # optional tokenizer adapter
yitrace-vecindex-graph/      # optional graph-index adapter
docs/                        # public API reference, current-state index, screenshot
```

## License

MIT
