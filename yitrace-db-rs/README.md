# yitrace-db

Embedded yiTrace DB for Rust agents.

This crate is the Rust equivalent of `@yitrace/db` and `yitrace-db` for
Python. It opens a yiTrace data directory in-process, calls the Rust engine
through `EngineJsonApi`, and does not start an HTTP server or parse storage
files in application code.

If a Rust app only needs to emit traces to a running yiTrace service, use the
pure SDK crate under `yitrace-sdk/rust` instead:

```toml
[dependencies]
yitrace = { path = "../yitrace-sdk/rust" }
```

Use this `yitrace-db` crate only when the Rust app needs local searchable
storage.

It is an embedded crate, not a remote deployment server. Multiple processes on
the same machine may open the same local data directory; the engine serializes
open/write paths with internal data-dir locks and protects cross-process reader
snapshots from physical segment reclaim. Do not share one data directory across
machines or unreliable network filesystems. For multi-host deployments, run a
yiTrace server process and send requests to it over HTTP.

## Install

Local development from this repository:

```toml
[dependencies]
yitrace-db = { path = "../yitrace-db-rs" }
```

The crate is not published yet. A real release should publish this crate as
`yitrace-db` and keep the internal `yt-engine` API behind this wrapper.

## Usage

`db.index_embedding(trace_id, span_id, vector)` accepts numeric internal IDs and uses the connection tenant. Repeating a vector is idempotent; updating it replaces the old vector for the same tenant and span.

```rust
use yitrace_db::{OpenOptions, SearchQuery, SpanEndOptions, SpanEventBuilder, YiTraceDb};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = YiTraceDb::open_with_options(OpenOptions::new("./data").tenant_id(1))?;

    let mut events = SpanEventBuilder::new("run-uuid");
    events
        .session_id("session-uuid")
        .attr("project_id", "agentic-data")
        .attr("skill", "review")
        .start_span("span-uuid", "风控研判")
        .log("span-uuid", "疑似盗刷")
        .end_span_with(
            "span-uuid",
            SpanEndOptions::ok()
                .duration_ns(12_000_000)
                .input_tokens(900)
                .output_tokens(120),
        );

    db.ingest_builder(&events)?;

    let _rows = db.search(
        &SearchQuery::text("盗刷")
            .k(10)
            .attr("project_id", "agentic-data"),
    )?;

    let _trajectories = db.trace_trajectories_json(
        r#"{"filter":{"projectId":"agentic-data","taskFingerprint":"refund-v1"}}"#,
    )?;
    let _groups = db.trajectory_groups_json(
        r#"{"filter":{"projectId":"agentic-data","taskFingerprint":"refund-v1"}}"#,
    )?;
    let _diff = db.trace_diff_json(
        r#"{"baseTraceId":"run-a","candidateTraceId":"run-b"}"#,
    )?;
    let _loops = db.loops()?;
    let _task_runs = db.route_json(
        "GET",
        "/v1/tasks/refund-v1/traces?validationStatus=pass",
        "",
    )?;
    let annotation = db.annotate_json(
        r#"{"traceId":"run-uuid","spanId":"span-uuid","label":"best_path","score":950,"attrs":{"project_id":"agentic-data","skill":"review"}}"#,
    )?;
    let _ = annotation;
    db.update_annotation_json(1, r#"{"status":"resolved","reviewer":"qa"}"#)?;
    db.link_dataset_item_json(
        r#"{"datasetId":"agentic-regression","itemId":"case-1","traceId":"run-uuid","spanId":"span-uuid","split":"eval","label":"pass"}"#,
    )?;
    let _annotations = db.annotations_query("projectId=agentic-data&includeDeleted=true")?;
    let _dataset_links = db.dataset_associations_query("datasetId=agentic-regression")?;
    let _plan = db.retention_plan_json(
        r#"{"filter":{"projectId":"agentic-data"},"deleteBeforeTs":100000,"protect":{"annotations":true,"datasetAssociations":true}}"#,
    )?;
    let _result = db.apply_retention_json(
        r#"{"filter":{"projectId":"agentic-data"},"deleteBeforeTs":100000,"requestedBy":"nightly-retention"}"#,
    )?;
    let _audits = db.retention_audits_query("source=nightly-retention")?;

    Ok(())
}
```

`SpanEventBuilder` hides `seq`, `event_type`, `ext_span_id`, and start/end event
pairs. String trace/span/session IDs are accepted; the engine hashes them into
internal IDs and keeps the original external IDs in query output.

Annotation and dataset association are kept in a small metadata ledger beside
the trace store. They are useful for human review and regression-set links, and
do not copy large trace payloads.

Retention audit and policy records use the same ledger. Retention is explicit:
plan first, apply second, and trigger saved policies from your own scheduler.
Audit and policy queries use the same in-memory metadata postings as annotations.

Read-model JSON results include `readPlan`. Common filters such as
`project_id`, `skill`, `task_fingerprint`, `loop_id`, `validation_status`,
`tool_name`, and `model` use the attrs sidecar postings first. Postings are
memory-budgeted: very wide values or total-entry pressure disable only the
affected postings, then queries fall back to the sidecar rows and still return
correct results. Persistent data dirs write a disposable `filter_attrs.dat`
segment cache; reopen loads it before replaying the WAL tail, and stale or
corrupt cache contents are rebuilt from the current snapshot. No-text
`trace_aggregate_json()` can use the in-memory aggregate rollup
(`readPlan.source == "aggregate_rollup"`). Persistent data dirs also write a
disposable `trace_rollup.dat` segment cache; reopen loads it before replaying
the WAL tail, and stale or corrupt cache contents are rebuilt from the current
snapshot. Deletes, retention apply, and segment upgrades rebuild the cache as
well.
Trajectory, loop, and task helpers can return
`readPlan.source == "trajectory_rollup"` for no-text path summaries and reuse
the same `trace_rollup.dat` cache after reopen. When those helpers expand
complete traces after finding candidates, `readPlan.traceFetchSource` shows
whether that second step also used the rollup by trace id. Text filters still
use the normal folded read path. Disk sidecars and dedicated
trajectory-loop-task indexes can be added later without changing the public
methods.

For endpoints that do not have a typed helper yet, use the raw in-process JSON
boundary. Only call endpoints that the current engine exposes:

```rust
let json = db.route_json("GET", "/v1/traces", "")?;
```

## Test

```bash
cd yitrace-db-rs
cargo test --offline
```

From the repository root, run the package-mode eval when changing the Rust
embedded API or any shared embedded contract:

```bash
./scripts/package_mode_eval.sh
```
