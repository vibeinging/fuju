"""Embedded yiTrace DB for Python.

This package embeds the Rust yiTrace engine in the current Python process.
It does not parse WAL, manifest, or segment files in Python and it does not
start a local HTTP server. All operations go through the same EngineJsonApi
boundary used by the Node package.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, urlencode

from ._native import NativeYiTraceDB

TenantId = str | int
JsonDict = dict[str, Any]

_ATTR_ALIASES = {
    "projectId": "project_id",
    "callSite": "call_site",
    "taskFingerprint": "task_fingerprint",
    "loopId": "loop_id",
    "harnessVersion": "harness_version",
    "validationStatus": "validation_status",
    "stopReason": "stop_reason",
    "externalRunId": "external_run_id",
    "connectionIds": "connection_ids",
    "dataSourceIds": "data_source_ids",
    "schemaFingerprint": "schema_fingerprint",
    "intentSignature": "intent_signature",
    "reviewStatus": "review_status",
    "evalStatus": "eval_status",
    "pathMemoryId": "path_memory_id",
}

_ATTR_KEYS = {
    "project_id",
    "external_run_id",
    "skill",
    "mode",
    "call_site",
    "task_fingerprint",
    "loop_id",
    "harness_version",
    "validation_status",
    "stop_reason",
    "phase",
    "validator",
    "connection_ids",
    "data_source_ids",
    "schema_fingerprint",
    "intent_signature",
    "review_status",
    "eval_status",
    "path_memory_id",
}

_FIELD_ALIASES = {
    "traceId": "trace_id",
    "spanId": "span_id",
    "parentSpanId": "parent_span_id",
    "sessionId": "session_id",
    "tenantId": "tenant_id",
    "extSpanId": "ext_span_id",
    "externalTraceId": "external_trace_id",
    "externalSpanId": "external_span_id",
    "externalParentSpanId": "external_parent_span_id",
    "externalSessionId": "external_session_id",
    "eventType": "event_type",
    "agentName": "agent_name",
    "toolName": "tool_name",
    "inputText": "input_text",
    "outputText": "output_text",
    "durationNs": "duration_ns",
    "inputTokens": "input_tokens",
    "outputTokens": "output_tokens",
    "cacheReadTokens": "cache_read_tokens",
    "cacheWriteTokens": "cache_write_tokens",
    "datasetId": "dataset_id",
    "itemId": "item_id",
    "datasetItemId": "dataset_item_id",
    "evalRunId": "eval_run_id",
    "snapshotId": "snapshot_id",
    "snapshotHash": "snapshot_hash",
}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_loads(value: str) -> Any:
    return json.loads(value) if value else None


def _tenant_id(value: TenantId | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _option(source: Mapping[str, Any], snake: str, camel: str | None = None) -> Any:
    if snake in source:
        return source[snake]
    if camel and camel in source:
        return source[camel]
    return None


def _set_if_defined(target: JsonDict, key: str, value: Any) -> None:
    if value is not None:
        target[key] = value


def _normalize_attrs(options: Mapping[str, Any] | None) -> JsonDict | None:
    if not options:
        return None
    out: JsonDict = {}
    attrs = options.get("attrs")
    if isinstance(attrs, Mapping):
        out.update({str(k): v for k, v in attrs.items() if v is not None})
    for key in _ATTR_KEYS:
        if key in options and options[key] is not None:
            out[key] = options[key]
    for camel, snake in _ATTR_ALIASES.items():
        if camel in options and options[camel] is not None:
            out[snake] = options[camel]
    return out or None


def _normalize_metadata_body(body: Mapping[str, Any] | None) -> JsonDict:
    out: JsonDict = dict(body or {})
    attrs: JsonDict = {}
    raw_attrs = out.get("attrs")
    if isinstance(raw_attrs, Mapping):
        attrs.update({str(k): v for k, v in raw_attrs.items() if v is not None})
    normalized = _normalize_attrs(out)
    if normalized:
        attrs.update(normalized)
    if attrs:
        out["attrs"] = attrs
    return out


def _query_string(params: Mapping[str, Any] | None = None) -> str:
    if not params:
        return ""
    out: dict[str, str] = {}
    for key, value in params.items():
        if value is None or key == "tenant_id" or key == "tenantId":
            continue
        if key == "attrs" and isinstance(value, Mapping):
            out[key] = _json_dumps(value)
        else:
            out[_FIELD_ALIASES.get(key, _ATTR_ALIASES.get(key, key))] = str(value)
    return urlencode(out)


def _append_query(path: str, query: str | None) -> str:
    if not query:
        return path
    return f"{path}{'&' if '?' in path else '?'}{query}"


class SpanEventBuilder:
    """Build yiTrace wire events without exposing seq/event_type details."""

    def __init__(self, defaults: Mapping[str, Any] | None = None) -> None:
        self._defaults = dict(defaults or {})
        self._events: list[JsonDict] = []
        self._seq_by_span: dict[str, int] = {}

    def start_span(self, **options: Any) -> JsonDict:
        event = self._base_event(options, 1)
        self._copy_common_fields(event, options)
        logs = [str(x) for x in options.get("logs", [])] if isinstance(options.get("logs"), list) else []
        name = _option(options, "name")
        if name is not None:
            logs.insert(0, str(name))
        if logs:
            event["logs"] = logs
        self._events.append(event)
        return dict(event)

    def log(self, message: str | None = None, **options: Any) -> JsonDict:
        event = self._base_event(options, 4)
        self._copy_common_fields(event, options)
        logs: list[str] = []
        if message is not None:
            logs.append(str(message))
        for key in ("message", "log"):
            if key in options and options[key] is not None:
                logs.append(str(options[key]))
        if isinstance(options.get("logs"), list):
            logs.extend(str(x) for x in options["logs"])
        if logs:
            event["logs"] = logs
        self._events.append(event)
        return dict(event)

    def end_span(self, **options: Any) -> JsonDict:
        event = self._base_event(options, 2)
        self._copy_common_fields(event, options)
        event["status"] = options.get("status", 0)
        _set_if_defined(event, "duration_ns", _option(options, "duration_ns", "durationNs"))
        _set_if_defined(event, "output_text", _option(options, "output_text", "outputText"))
        self._events.append(event)
        return dict(event)

    def events(self) -> list[JsonDict]:
        return [dict(event) for event in self._events]

    def clear(self) -> None:
        self._events.clear()
        self._seq_by_span.clear()

    def ingest(self, db: "YiTraceDB", **options: Any) -> Any:
        return db.ingest(self.events(), **options)

    def _base_event(self, options: Mapping[str, Any], event_type: int) -> JsonDict:
        trace_id = _option(options, "trace_id", "traceId")
        if trace_id is None:
            trace_id = _option(self._defaults, "trace_id", "traceId")
        span_id = _option(options, "span_id", "spanId")
        if trace_id is None:
            raise ValueError("SpanEventBuilder requires trace_id or traceId")
        if span_id is None:
            raise ValueError("SpanEventBuilder requires span_id or spanId")
        ext_span_id = _option(options, "ext_span_id", "extSpanId") or str(span_id)
        key = f"{trace_id}\0{ext_span_id}"
        seq = options.get("seq")
        if seq is None:
            seq = self._next_seq(key)
        event: JsonDict = {
            "trace_id": trace_id,
            "span_id": span_id,
            "ts": options.get("ts") or time.time_ns(),
            "seq": seq,
            "event_type": event_type,
            "ext_span_id": ext_span_id,
        }
        for snake, camel in [
            ("parent_span_id", "parentSpanId"),
            ("session_id", "sessionId"),
            ("tenant_id", "tenantId"),
            ("external_trace_id", "externalTraceId"),
            ("external_span_id", "externalSpanId"),
            ("external_parent_span_id", "externalParentSpanId"),
            ("external_session_id", "externalSessionId"),
        ]:
            value = _option(options, snake, camel)
            if value is None and snake in {"session_id", "tenant_id"}:
                value = _option(self._defaults, snake, camel)
            _set_if_defined(event, snake, value)
        attrs: JsonDict = {}
        for source in (_normalize_attrs(self._defaults), _normalize_attrs(options)):
            if source:
                attrs.update(source)
        if attrs:
            event["attrs"] = attrs
        return event

    def _copy_common_fields(self, event: JsonDict, options: Mapping[str, Any]) -> None:
        for snake, camel in [
            ("agent_name", "agentName"),
            ("tool_name", "toolName"),
            ("model", None),
            ("input_text", "inputText"),
            ("output_text", "outputText"),
            ("input_tokens", "inputTokens"),
            ("output_tokens", "outputTokens"),
            ("cache_read_tokens", "cacheReadTokens"),
            ("cache_write_tokens", "cacheWriteTokens"),
        ]:
            _set_if_defined(event, snake, _option(options, snake, camel))

    def _next_seq(self, key: str) -> int:
        value = self._seq_by_span.get(key, 0) + 1
        self._seq_by_span[key] = value
        return value

    startSpan = start_span
    endSpan = end_span


def create_span_event_builder(defaults: Mapping[str, Any] | None = None) -> SpanEventBuilder:
    return SpanEventBuilder(defaults)


createSpanEventBuilder = create_span_event_builder


class YiTraceDB:
    """Embedded yiTrace database handle."""

    def __init__(self, native: NativeYiTraceDB, *, tenant_id: TenantId | None = None) -> None:
        self._native = native
        self._tenant_id = _tenant_id(tenant_id)
        self._closed = False

    @classmethod
    def open(
        cls,
        data_dir: str | Path | Mapping[str, Any],
        *,
        tenant_id: TenantId | None = None,
        **options: Any,
    ) -> "YiTraceDB":
        if isinstance(data_dir, Mapping):
            options = {**data_dir, **options}
            data_dir = options.get("data_dir") or options.get("dataDir")
            tenant_id = options.get("tenant_id", options.get("tenantId", tenant_id))
        if not data_dir:
            raise ValueError("YiTraceDB.open requires a data_dir")
        if "readOnly" in options or "read_only" in options:
            raise ValueError("OpenOptions.readOnly is not supported yet")
        return cls(NativeYiTraceDB(str(data_dir)), tenant_id=tenant_id)

    def __enter__(self) -> "YiTraceDB":
        self._ensure_open()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def route_json(
        self,
        method: str,
        path: str,
        body: Any = "",
        *,
        tenant_id: TenantId | None = None,
    ) -> Any:
        self._ensure_open()
        if body is None:
            body_text = ""
        elif isinstance(body, str):
            body_text = body
        else:
            body_text = _json_dumps(body)
        response = self._native.route_json(method, path, body_text, _tenant_id(tenant_id) or self._tenant_id)
        return _json_loads(response)

    def ingest(self, events: list[Mapping[str, Any]], *, tenant_id: TenantId | None = None) -> Any:
        return self.route_json("POST", "/v1/ingest", events, tenant_id=tenant_id)

    def index_embedding(self, trace_id: int | str, span_id: int | str, embedding: list[float], *, tenant_id: TenantId | None = None) -> None:
        self._ensure_open()
        self._native.index_embedding(str(trace_id), str(span_id), embedding, _tenant_id(tenant_id) or self._tenant_id)

    def ingest_otlp(self, body: Mapping[str, Any] | str, *, tenant_id: TenantId | None = None) -> Any:
        return self.route_json("POST", "/v1/traces", body, tenant_id=tenant_id)

    def search(self, query: Mapping[str, Any] | None = None, *, tenant_id: TenantId | None = None, **kwargs: Any) -> Any:
        body = dict(query or {})
        body.update(kwargs)
        return self.route_json("POST", "/v1/search", body, tenant_id=tenant_id)

    def trace_search(self, query: Mapping[str, Any] | None = None, *, tenant_id: TenantId | None = None, **kwargs: Any) -> Any:
        body = dict(query or {})
        body.update(kwargs)
        return self.route_json("POST", "/v1/trace-search", body, tenant_id=tenant_id)

    def trace_aggregate(self, query: Mapping[str, Any] | None = None, *, tenant_id: TenantId | None = None, **kwargs: Any) -> Any:
        body = dict(query or {})
        body.update(kwargs)
        return self.route_json("POST", "/v1/trace-aggregate", body, tenant_id=tenant_id)

    def storage_stats(self, query: Mapping[str, Any] | None = None, *, tenant_id: TenantId | None = None, **kwargs: Any) -> Any:
        body = dict(query or {})
        body.update(kwargs)
        return self.route_json("POST", "/v1/storage-stats", body, tenant_id=tenant_id)

    def retention_plan(self, query: Mapping[str, Any] | None = None, *, tenant_id: TenantId | None = None, **kwargs: Any) -> Any:
        body = dict(query or {})
        body.update(kwargs)
        return self.route_json("POST", "/v1/retention-plan", body, tenant_id=tenant_id)

    def apply_retention(self, query: Mapping[str, Any] | None = None, *, tenant_id: TenantId | None = None, **kwargs: Any) -> Any:
        body = dict(query or {})
        body.update(kwargs)
        return self.route_json("POST", "/v1/retention/apply", body, tenant_id=tenant_id)

    def retention_audits(
        self,
        cursor: int = 0,
        limit: int = 50,
        *,
        tenant_id: TenantId | None = None,
        **options: Any,
    ) -> Any:
        query = _query_string({"cursor": cursor, "limit": limit, **options})
        return self.route_json("GET", _append_query("/v1/retention-audits", query), tenant_id=tenant_id)

    def create_retention_policy(self, policy: Mapping[str, Any] | None = None, *, tenant_id: TenantId | None = None, **kwargs: Any) -> Any:
        body = {**dict(policy or {}), **kwargs}
        return self.route_json("POST", "/v1/retention-policies", body, tenant_id=tenant_id)

    def retention_policies(
        self,
        cursor: int = 0,
        limit: int = 50,
        *,
        tenant_id: TenantId | None = None,
        **options: Any,
    ) -> Any:
        query = _query_string({"cursor": cursor, "limit": limit, **options})
        return self.route_json("GET", _append_query("/v1/retention-policies", query), tenant_id=tenant_id)

    def run_retention_policies(self, query: Mapping[str, Any] | None = None, *, tenant_id: TenantId | None = None, **kwargs: Any) -> Any:
        body = dict(query or {})
        body.update(kwargs)
        return self.route_json("POST", "/v1/retention-policies/run-due", body, tenant_id=tenant_id)

    def trace_trajectories(self, query: Mapping[str, Any] | None = None, *, tenant_id: TenantId | None = None, **kwargs: Any) -> Any:
        body = dict(query or {})
        body.update(kwargs)
        return self.route_json("POST", "/v1/trace-trajectories", body, tenant_id=tenant_id)

    def trajectory_groups(self, query: Mapping[str, Any] | None = None, *, tenant_id: TenantId | None = None, **kwargs: Any) -> Any:
        body = dict(query or {})
        body.update(kwargs)
        return self.route_json("POST", "/v1/trajectory-groups", body, tenant_id=tenant_id)

    def trace_diff(
        self,
        left_trace_id: TenantId | Mapping[str, Any],
        right_trace_id: TenantId | None = None,
        *,
        tenant_id: TenantId | None = None,
        **kwargs: Any,
    ) -> Any:
        if isinstance(left_trace_id, Mapping):
            body = dict(left_trace_id)
        else:
            body = {"baseTraceId": left_trace_id, "candidateTraceId": right_trace_id}
        body.update(kwargs)
        return self.route_json("POST", "/v1/traces/diff", body, tenant_id=tenant_id)

    traceSearch = trace_search
    traceAggregate = trace_aggregate
    storageStats = storage_stats
    retentionPlan = retention_plan
    applyRetention = apply_retention
    retentionAudits = retention_audits
    createRetentionPolicy = create_retention_policy
    retentionPolicies = retention_policies
    runRetentionPolicies = run_retention_policies
    traceTrajectories = trace_trajectories
    trajectoryGroups = trajectory_groups
    traceDiff = trace_diff

    def annotate(
        self,
        annotation: Mapping[str, Any] | None = None,
        *,
        tenant_id: TenantId | None = None,
        **kwargs: Any,
    ) -> Any:
        body = _normalize_metadata_body({**dict(annotation or {}), **kwargs})
        return self.route_json("POST", "/v1/annotations", body, tenant_id=tenant_id)

    def annotations(
        self,
        cursor: int = 0,
        limit: int = 50,
        *,
        tenant_id: TenantId | None = None,
        **options: Any,
    ) -> Any:
        attrs = _normalize_attrs(options)
        query = _query_string({"cursor": cursor, "limit": limit, "attrs": attrs, **options})
        return self.route_json("GET", _append_query("/v1/annotations", query), tenant_id=tenant_id)

    def update_annotation(
        self,
        annotation_id: TenantId,
        update: Mapping[str, Any] | None = None,
        *,
        tenant_id: TenantId | None = None,
        **kwargs: Any,
    ) -> Any:
        body = _normalize_metadata_body({**dict(update or {}), **kwargs})
        path = f"/v1/annotations/{quote(str(annotation_id), safe='')}"
        return self.route_json("PATCH", path, body, tenant_id=tenant_id)

    def delete_annotation(
        self,
        annotation_id: TenantId,
        delete_info: Mapping[str, Any] | None = None,
        *,
        tenant_id: TenantId | None = None,
        **kwargs: Any,
    ) -> Any:
        body = {**dict(delete_info or {}), **kwargs}
        path = f"/v1/annotations/{quote(str(annotation_id), safe='')}"
        return self.route_json("DELETE", path, body, tenant_id=tenant_id)

    def link_dataset_item(
        self,
        association: Mapping[str, Any] | None = None,
        *,
        tenant_id: TenantId | None = None,
        **kwargs: Any,
    ) -> Any:
        body = _normalize_metadata_body({**dict(association or {}), **kwargs})
        return self.route_json("POST", "/v1/dataset-associations", body, tenant_id=tenant_id)

    def dataset_associations(
        self,
        cursor: int = 0,
        limit: int = 50,
        *,
        tenant_id: TenantId | None = None,
        **options: Any,
    ) -> Any:
        attrs = _normalize_attrs(options)
        query = _query_string({"cursor": cursor, "limit": limit, "attrs": attrs, **options})
        return self.route_json("GET", _append_query("/v1/dataset-associations", query), tenant_id=tenant_id)

    updateAnnotation = update_annotation
    deleteAnnotation = delete_annotation
    linkDatasetItem = link_dataset_item
    datasetAssociations = dataset_associations

    def traces(self, *, tenant_id: TenantId | None = None) -> Any:
        return self.route_json("GET", "/v1/traces", tenant_id=tenant_id)

    def sessions(self, cursor: int = 0, limit: int = 50, *, tenant_id: TenantId | None = None, **options: Any) -> Any:
        attrs = _normalize_attrs(options)
        query = _query_string({"cursor": cursor, "limit": limit, "attrs": attrs, **options})
        return self.route_json("GET", _append_query("/v1/sessions", query), tenant_id=tenant_id)

    def loops(self, cursor: int = 0, limit: int = 50, *, tenant_id: TenantId | None = None, **options: Any) -> Any:
        attrs = _normalize_attrs(options)
        query = _query_string({"cursor": cursor, "limit": limit, "attrs": attrs, **options})
        return self.route_json("GET", _append_query("/v1/loops", query), tenant_id=tenant_id)

    def loop(self, loop_id: TenantId, *, tenant_id: TenantId | None = None) -> Any:
        return self.route_json("GET", f"/v1/loops/{quote(str(loop_id), safe='')}", tenant_id=tenant_id)

    def task_traces(self, fingerprint: str, cursor: int = 0, limit: int = 50, *, tenant_id: TenantId | None = None, **options: Any) -> Any:
        attrs = _normalize_attrs(options)
        query = _query_string({"cursor": cursor, "limit": limit, "attrs": attrs, **options})
        path = f"/v1/tasks/{quote(str(fingerprint), safe='')}/traces"
        return self.route_json("GET", _append_query(path, query), tenant_id=tenant_id)

    taskTraces = task_traces

    def trace(self, trace_id: TenantId, *, tenant_id: TenantId | None = None) -> Any:
        return self.route_json("GET", f"/v1/traces/{quote(str(trace_id), safe='')}", tenant_id=tenant_id)

    def span(self, trace_id: TenantId, span_id: TenantId, *, tenant_id: TenantId | None = None) -> Any:
        trace_part = quote(str(trace_id), safe="")
        span_part = quote(str(span_id), safe="")
        return self.route_json("GET", f"/v1/traces/{trace_part}/spans/{span_part}", tenant_id=tenant_id)

    def flush(self) -> None:
        self._ensure_open()
        self._native.flush()

    def lock_metrics(self) -> Any:
        self._ensure_open()
        return _json_loads(self._native.lock_metrics_json())

    lockMetrics = lock_metrics

    def close(self) -> None:
        if self._closed:
            return
        self._native.close()
        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("YiTraceDB is closed")


__all__ = [
    "YiTraceDB",
    "SpanEventBuilder",
    "create_span_event_builder",
    "createSpanEventBuilder",
]
