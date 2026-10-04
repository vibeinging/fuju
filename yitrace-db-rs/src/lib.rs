use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use yt_engine::{EngineJsonApi, WriteCoordinator};

pub type Result<T> = std::result::Result<T, YiTraceError>;

#[derive(Debug)]
pub enum YiTraceError {
    Io(std::io::Error),
    Closed,
    InvalidInput(String),
    RequestFailed { status: u16, body: String },
}

impl fmt::Display for YiTraceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            YiTraceError::Io(e) => write!(f, "{e}"),
            YiTraceError::Closed => write!(f, "YiTraceDb is closed"),
            YiTraceError::InvalidInput(message) => write!(f, "{message}"),
            YiTraceError::RequestFailed { status, body } => {
                write!(f, "yiTrace request failed: status={status} body={body}")
            }
        }
    }
}

impl std::error::Error for YiTraceError {}

impl From<std::io::Error> for YiTraceError {
    fn from(value: std::io::Error) -> Self {
        YiTraceError::Io(value)
    }
}

#[derive(Debug, Clone)]
pub struct OpenOptions {
    pub data_dir: PathBuf,
    pub tenant_id: Option<u64>,
}

impl OpenOptions {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            tenant_id: None,
        }
    }

    pub fn tenant_id(mut self, tenant_id: u64) -> Self {
        self.tenant_id = Some(tenant_id);
        self
    }
}

pub struct YiTraceDb {
    coord: Arc<WriteCoordinator>,
    api: EngineJsonApi,
    tenant_id: Option<u64>,
    closed: bool,
}

impl YiTraceDb {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(OpenOptions::new(data_dir.as_ref().to_path_buf()))
    }

    pub fn open_with_options(options: OpenOptions) -> Result<Self> {
        std::fs::create_dir_all(&options.data_dir)?;
        let coord = WriteCoordinator::open_durable(&options.data_dir)?;
        coord.try_recover()?;
        let api = EngineJsonApi::new(Arc::clone(&coord));
        Ok(Self {
            coord,
            api,
            tenant_id: options.tenant_id,
            closed: false,
        })
    }

    pub fn route_json(&self, method: &str, path: &str, body: &str) -> Result<String> {
        self.route_json_with_tenant(method, path, body, None)
    }

    pub fn route_json_with_tenant(
        &self,
        method: &str,
        path: &str,
        body: &str,
        tenant_id: Option<u64>,
    ) -> Result<String> {
        self.ensure_open()?;
        let tenant = tenant_id.or(self.tenant_id);
        let (status, response) = self.api.route_with_tenant(method, path, body, tenant);
        if (200..300).contains(&status) {
            Ok(response)
        } else {
            Err(YiTraceError::RequestFailed {
                status,
                body: response,
            })
        }
    }

    pub fn ingest(&self, events: &[SpanEvent]) -> Result<String> {
        self.ingest_json(&events_to_json(events))
    }

    pub fn ingest_builder(&self, builder: &SpanEventBuilder) -> Result<String> {
        self.ingest(builder.events())
    }

    pub fn ingest_json(&self, events_json: &str) -> Result<String> {
        self.route_json("POST", "/v1/ingest", events_json)
    }

    pub fn ingest_otlp_json(&self, otlp_json: &str) -> Result<String> {
        self.route_json("POST", "/v1/traces", otlp_json)
    }

    /// 旁路向量沿用连接的租户身份；旧无租户连接仍使用 None。
    pub fn index_embedding(&self, trace_id: u64, span_id: u64, embedding: Vec<f32>) -> Result<()> {
        self.ensure_open()?;
        self.coord
            .index_embedding_for_tenant(self.tenant_id, trace_id, span_id, embedding)?;
        Ok(())
    }

    pub fn search(&self, query: &SearchQuery) -> Result<String> {
        self.search_json(&query.to_json())
    }

    pub fn search_json(&self, query_json: &str) -> Result<String> {
        self.route_json("POST", "/v1/search", query_json)
    }

    pub fn trace_search_json(&self, query_json: &str) -> Result<String> {
        self.route_json("POST", "/v1/trace-search", query_json)
    }

    pub fn trace_aggregate_json(&self, query_json: &str) -> Result<String> {
        self.route_json("POST", "/v1/trace-aggregate", query_json)
    }

    pub fn storage_stats_json(&self, query_json: &str) -> Result<String> {
        self.route_json("POST", "/v1/storage-stats", query_json)
    }

    pub fn retention_plan_json(&self, query_json: &str) -> Result<String> {
        self.route_json("POST", "/v1/retention-plan", query_json)
    }

    pub fn apply_retention_json(&self, query_json: &str) -> Result<String> {
        self.route_json("POST", "/v1/retention/apply", query_json)
    }

    pub fn retention_audits(&self) -> Result<String> {
        self.retention_audits_query("")
    }

    pub fn retention_audits_query(&self, query: &str) -> Result<String> {
        let path = path_with_query("/v1/retention-audits", query);
        self.route_json("GET", &path, "")
    }

    pub fn create_retention_policy_json(&self, policy_json: &str) -> Result<String> {
        self.route_json("POST", "/v1/retention-policies", policy_json)
    }

    pub fn retention_policies(&self) -> Result<String> {
        self.retention_policies_query("")
    }

    pub fn retention_policies_query(&self, query: &str) -> Result<String> {
        let path = path_with_query("/v1/retention-policies", query);
        self.route_json("GET", &path, "")
    }

    pub fn run_retention_policies_json(&self, query_json: &str) -> Result<String> {
        self.route_json("POST", "/v1/retention-policies/run-due", query_json)
    }

    pub fn trace_trajectories_json(&self, query_json: &str) -> Result<String> {
        self.route_json("POST", "/v1/trace-trajectories", query_json)
    }

    pub fn trajectory_groups_json(&self, query_json: &str) -> Result<String> {
        self.route_json("POST", "/v1/trajectory-groups", query_json)
    }

    pub fn trace_diff_json(&self, query_json: &str) -> Result<String> {
        self.route_json("POST", "/v1/traces/diff", query_json)
    }

    pub fn annotate_json(&self, annotation_json: &str) -> Result<String> {
        self.route_json("POST", "/v1/annotations", annotation_json)
    }

    pub fn annotations(&self) -> Result<String> {
        self.annotations_query("")
    }

    pub fn annotations_query(&self, query: &str) -> Result<String> {
        let path = path_with_query("/v1/annotations", query);
        self.route_json("GET", &path, "")
    }

    pub fn update_annotation_json(
        &self,
        annotation_id: impl fmt::Display,
        update_json: &str,
    ) -> Result<String> {
        self.route_json(
            "PATCH",
            &format!("/v1/annotations/{annotation_id}"),
            update_json,
        )
    }

    pub fn delete_annotation_json(
        &self,
        annotation_id: impl fmt::Display,
        delete_json: &str,
    ) -> Result<String> {
        self.route_json(
            "DELETE",
            &format!("/v1/annotations/{annotation_id}"),
            delete_json,
        )
    }

    pub fn link_dataset_item_json(&self, association_json: &str) -> Result<String> {
        self.route_json("POST", "/v1/dataset-associations", association_json)
    }

    pub fn dataset_associations(&self) -> Result<String> {
        self.dataset_associations_query("")
    }

    pub fn dataset_associations_query(&self, query: &str) -> Result<String> {
        let path = path_with_query("/v1/dataset-associations", query);
        self.route_json("GET", &path, "")
    }

    pub fn traces(&self) -> Result<String> {
        self.route_json("GET", "/v1/traces", "")
    }

    pub fn sessions(&self) -> Result<String> {
        self.route_json("GET", "/v1/sessions", "")
    }

    pub fn loops(&self) -> Result<String> {
        self.route_json("GET", "/v1/loops", "")
    }

    pub fn loop_detail(&self, loop_id: impl fmt::Display) -> Result<String> {
        self.route_json("GET", &format!("/v1/loops/{loop_id}"), "")
    }

    pub fn task_traces(&self, fingerprint: impl fmt::Display) -> Result<String> {
        self.route_json("GET", &format!("/v1/tasks/{fingerprint}/traces"), "")
    }

    pub fn trace(&self, trace_id: impl fmt::Display) -> Result<String> {
        self.route_json("GET", &format!("/v1/traces/{trace_id}"), "")
    }

    pub fn span(&self, trace_id: impl fmt::Display, span_id: impl fmt::Display) -> Result<String> {
        self.route_json("GET", &format!("/v1/traces/{trace_id}/spans/{span_id}"), "")
    }

    pub fn flush(&self) -> Result<()> {
        self.ensure_open()?;
        self.coord.try_flush_memtable()?;
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.coord.try_flush_memtable()?;
        self.closed = true;
        Ok(())
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            Err(YiTraceError::Closed)
        } else {
            Ok(())
        }
    }
}

impl Drop for YiTraceDb {
    fn drop(&mut self) {
        if !self.closed {
            if let Err(err) = self.coord.try_flush_memtable() {
                eprintln!("yiTrace close flush failed: {err}");
            }
            self.closed = true;
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
}

impl JsonValue {
    pub fn number_literal(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(YiTraceError::InvalidInput(
                "JSON number literal cannot be empty".to_string(),
            ));
        }
        Ok(JsonValue::Number(value))
    }

    pub fn to_json(&self) -> String {
        match self {
            JsonValue::Null => "null".to_string(),
            JsonValue::Bool(value) => value.to_string(),
            JsonValue::Number(value) => value.clone(),
            JsonValue::String(value) => json_string(value),
            JsonValue::Array(items) => format!(
                "[{}]",
                items
                    .iter()
                    .map(JsonValue::to_json)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            JsonValue::Object(items) => format!(
                "{{{}}}",
                items
                    .iter()
                    .map(|(key, value)| format!("{}:{}", json_string(key), value.to_json()))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        }
    }
}

impl From<&str> for JsonValue {
    fn from(value: &str) -> Self {
        JsonValue::String(value.to_string())
    }
}

impl From<String> for JsonValue {
    fn from(value: String) -> Self {
        JsonValue::String(value)
    }
}

impl From<bool> for JsonValue {
    fn from(value: bool) -> Self {
        JsonValue::Bool(value)
    }
}

impl From<u64> for JsonValue {
    fn from(value: u64) -> Self {
        JsonValue::Number(value.to_string())
    }
}

impl From<u32> for JsonValue {
    fn from(value: u32) -> Self {
        JsonValue::Number(value.to_string())
    }
}

impl From<usize> for JsonValue {
    fn from(value: usize) -> Self {
        JsonValue::Number(value.to_string())
    }
}

impl From<i64> for JsonValue {
    fn from(value: i64) -> Self {
        JsonValue::Number(value.to_string())
    }
}

impl From<i32> for JsonValue {
    fn from(value: i32) -> Self {
        JsonValue::Number(value.to_string())
    }
}

#[derive(Clone, Debug)]
pub struct SpanEvent {
    trace_id: String,
    span_id: String,
    ts: i64,
    seq: u64,
    event_type: u8,
    ext_span_id: String,
    parent_span_id: Option<String>,
    session_id: Option<String>,
    tenant_id: Option<u64>,
    status: Option<u8>,
    duration_ns: Option<u64>,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cache_read_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
    agent_name: Option<String>,
    tool_name: Option<String>,
    model: Option<String>,
    input_text: Option<String>,
    output_text: Option<String>,
    logs: Vec<String>,
    attrs: Vec<(String, JsonValue)>,
}

impl SpanEvent {
    pub fn to_json(&self) -> String {
        let mut fields = vec![
            format!("\"trace_id\":{}", json_string(&self.trace_id)),
            format!("\"span_id\":{}", json_string(&self.span_id)),
            format!("\"ts\":{}", self.ts),
            format!("\"seq\":{}", self.seq),
            format!("\"event_type\":{}", self.event_type),
            format!("\"ext_span_id\":{}", json_string(&self.ext_span_id)),
        ];
        push_opt_str(
            &mut fields,
            "parent_span_id",
            self.parent_span_id.as_deref(),
        );
        push_opt_str(&mut fields, "session_id", self.session_id.as_deref());
        push_opt_num(&mut fields, "tenant_id", self.tenant_id);
        push_opt_num(&mut fields, "status", self.status);
        push_opt_num(&mut fields, "duration_ns", self.duration_ns);
        push_opt_num(&mut fields, "input_tokens", self.input_tokens);
        push_opt_num(&mut fields, "output_tokens", self.output_tokens);
        push_opt_num(&mut fields, "cache_read_tokens", self.cache_read_tokens);
        push_opt_num(&mut fields, "cache_write_tokens", self.cache_write_tokens);
        push_opt_str(&mut fields, "agent_name", self.agent_name.as_deref());
        push_opt_str(&mut fields, "tool_name", self.tool_name.as_deref());
        push_opt_str(&mut fields, "model", self.model.as_deref());
        push_opt_str(&mut fields, "input_text", self.input_text.as_deref());
        push_opt_str(&mut fields, "output_text", self.output_text.as_deref());
        if !self.logs.is_empty() {
            fields.push(format!(
                "\"logs\":[{}]",
                self.logs
                    .iter()
                    .map(|value| json_string(value))
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        if !self.attrs.is_empty() {
            fields.push(format!(
                "\"attrs\":{{{}}}",
                self.attrs
                    .iter()
                    .map(|(key, value)| format!("{}:{}", json_string(key), value.to_json()))
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        format!("{{{}}}", fields.join(","))
    }
}

pub fn events_to_json(events: &[SpanEvent]) -> String {
    format!(
        "[{}]",
        events
            .iter()
            .map(SpanEvent::to_json)
            .collect::<Vec<_>>()
            .join(",")
    )
}

#[derive(Clone, Debug)]
pub struct SpanEventBuilder {
    trace_id: String,
    session_id: Option<String>,
    tenant_id: Option<u64>,
    attrs: Vec<(String, JsonValue)>,
    events: Vec<SpanEvent>,
    seq_by_span: BTreeMap<String, u64>,
}

impl SpanEventBuilder {
    pub fn new(trace_id: impl Into<String>) -> Self {
        Self {
            trace_id: trace_id.into(),
            session_id: None,
            tenant_id: None,
            attrs: Vec::new(),
            events: Vec::new(),
            seq_by_span: BTreeMap::new(),
        }
    }

    pub fn session_id(&mut self, session_id: impl Into<String>) -> &mut Self {
        self.session_id = Some(session_id.into());
        self
    }

    pub fn tenant_id(&mut self, tenant_id: u64) -> &mut Self {
        self.tenant_id = Some(tenant_id);
        self
    }

    pub fn attr(&mut self, key: impl Into<String>, value: impl Into<JsonValue>) -> &mut Self {
        self.attrs.push((key.into(), value.into()));
        self
    }

    pub fn start_span(&mut self, span_id: impl Into<String>, name: impl Into<String>) -> &mut Self {
        self.start_span_with(span_id, SpanStartOptions::new(name))
    }

    pub fn start_span_with(
        &mut self,
        span_id: impl Into<String>,
        options: SpanStartOptions,
    ) -> &mut Self {
        let span_id = span_id.into();
        let mut event = self.base_event(&span_id, 1);
        event.parent_span_id = options.parent_span_id;
        event.agent_name = options.agent_name;
        event.tool_name = options.tool_name;
        event.model = options.model;
        event.input_text = options.input_text;
        event.logs.push(options.name);
        event.logs.extend(options.logs);
        event.attrs.extend(options.attrs);
        self.events.push(event);
        self
    }

    pub fn log(&mut self, span_id: impl Into<String>, message: impl Into<String>) -> &mut Self {
        self.log_with(span_id, SpanLogOptions::new(message))
    }

    pub fn log_with(&mut self, span_id: impl Into<String>, options: SpanLogOptions) -> &mut Self {
        let span_id = span_id.into();
        let mut event = self.base_event(&span_id, 4);
        event.logs.push(options.message);
        event.attrs.extend(options.attrs);
        self.events.push(event);
        self
    }

    pub fn end_span(&mut self, span_id: impl Into<String>, status: u8) -> &mut Self {
        self.end_span_with(span_id, SpanEndOptions::new(status))
    }

    pub fn end_span_with(
        &mut self,
        span_id: impl Into<String>,
        options: SpanEndOptions,
    ) -> &mut Self {
        let span_id = span_id.into();
        let mut event = self.base_event(&span_id, 2);
        event.status = Some(options.status);
        event.duration_ns = options.duration_ns;
        event.input_tokens = options.input_tokens;
        event.output_tokens = options.output_tokens;
        event.cache_read_tokens = options.cache_read_tokens;
        event.cache_write_tokens = options.cache_write_tokens;
        event.output_text = options.output_text;
        event.attrs.extend(options.attrs);
        self.events.push(event);
        self
    }

    pub fn events(&self) -> &[SpanEvent] {
        &self.events
    }

    pub fn into_events(self) -> Vec<SpanEvent> {
        self.events
    }

    pub fn to_json(&self) -> String {
        events_to_json(&self.events)
    }

    pub fn clear(&mut self) {
        self.events.clear();
        self.seq_by_span.clear();
    }

    fn base_event(&mut self, span_id: &str, event_type: u8) -> SpanEvent {
        let ext_span_id = span_id.to_string();
        let seq = self.next_seq(&ext_span_id);
        SpanEvent {
            trace_id: self.trace_id.clone(),
            span_id: span_id.to_string(),
            ts: now_ns(),
            seq,
            event_type,
            ext_span_id,
            parent_span_id: None,
            session_id: self.session_id.clone(),
            tenant_id: self.tenant_id,
            status: None,
            duration_ns: None,
            input_tokens: None,
            output_tokens: None,
            cache_read_tokens: None,
            cache_write_tokens: None,
            agent_name: None,
            tool_name: None,
            model: None,
            input_text: None,
            output_text: None,
            logs: Vec::new(),
            attrs: self.attrs.clone(),
        }
    }

    fn next_seq(&mut self, ext_span_id: &str) -> u64 {
        let key = format!("{}\0{}", self.trace_id, ext_span_id);
        let next = self.seq_by_span.get(&key).copied().unwrap_or(0) + 1;
        self.seq_by_span.insert(key, next);
        next
    }
}

#[derive(Clone, Debug)]
pub struct SpanStartOptions {
    name: String,
    parent_span_id: Option<String>,
    agent_name: Option<String>,
    tool_name: Option<String>,
    model: Option<String>,
    input_text: Option<String>,
    logs: Vec<String>,
    attrs: Vec<(String, JsonValue)>,
}

impl SpanStartOptions {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            parent_span_id: None,
            agent_name: None,
            tool_name: None,
            model: None,
            input_text: None,
            logs: Vec::new(),
            attrs: Vec::new(),
        }
    }

    pub fn parent_span_id(mut self, value: impl Into<String>) -> Self {
        self.parent_span_id = Some(value.into());
        self
    }

    pub fn agent_name(mut self, value: impl Into<String>) -> Self {
        self.agent_name = Some(value.into());
        self
    }

    pub fn tool_name(mut self, value: impl Into<String>) -> Self {
        self.tool_name = Some(value.into());
        self
    }

    pub fn model(mut self, value: impl Into<String>) -> Self {
        self.model = Some(value.into());
        self
    }

    pub fn input_text(mut self, value: impl Into<String>) -> Self {
        self.input_text = Some(value.into());
        self
    }

    pub fn log(mut self, value: impl Into<String>) -> Self {
        self.logs.push(value.into());
        self
    }

    pub fn attr(mut self, key: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.attrs.push((key.into(), value.into()));
        self
    }
}

#[derive(Clone, Debug)]
pub struct SpanLogOptions {
    message: String,
    attrs: Vec<(String, JsonValue)>,
}

impl SpanLogOptions {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            attrs: Vec::new(),
        }
    }

    pub fn attr(mut self, key: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.attrs.push((key.into(), value.into()));
        self
    }
}

#[derive(Clone, Debug)]
pub struct SpanEndOptions {
    status: u8,
    duration_ns: Option<u64>,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cache_read_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
    output_text: Option<String>,
    attrs: Vec<(String, JsonValue)>,
}

impl SpanEndOptions {
    pub fn new(status: u8) -> Self {
        Self {
            status,
            duration_ns: None,
            input_tokens: None,
            output_tokens: None,
            cache_read_tokens: None,
            cache_write_tokens: None,
            output_text: None,
            attrs: Vec::new(),
        }
    }

    pub fn ok() -> Self {
        Self::new(0)
    }

    pub fn error() -> Self {
        Self::new(1)
    }

    pub fn duration_ns(mut self, value: u64) -> Self {
        self.duration_ns = Some(value);
        self
    }

    pub fn input_tokens(mut self, value: u64) -> Self {
        self.input_tokens = Some(value);
        self
    }

    pub fn output_tokens(mut self, value: u64) -> Self {
        self.output_tokens = Some(value);
        self
    }

    pub fn cache_read_tokens(mut self, value: u64) -> Self {
        self.cache_read_tokens = Some(value);
        self
    }

    pub fn cache_write_tokens(mut self, value: u64) -> Self {
        self.cache_write_tokens = Some(value);
        self
    }

    pub fn output_text(mut self, value: impl Into<String>) -> Self {
        self.output_text = Some(value.into());
        self
    }

    pub fn attr(mut self, key: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.attrs.push((key.into(), value.into()));
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct SearchQuery {
    text: Option<String>,
    vector: Vec<f32>,
    k: Option<usize>,
    filter: SearchFilter,
}

impl SearchQuery {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: Some(text.into()),
            ..Self::default()
        }
    }

    pub fn k(mut self, value: usize) -> Self {
        self.k = Some(value);
        self
    }

    pub fn vector(mut self, vector: impl Into<Vec<f32>>) -> Self {
        self.vector = vector.into();
        self
    }

    pub fn agent_name(mut self, value: impl Into<String>) -> Self {
        self.filter.agent_name = Some(value.into());
        self
    }

    pub fn status(mut self, value: u8) -> Self {
        self.filter.status = Some(value);
        self
    }

    pub fn attr(mut self, key: impl Into<String>, value: impl Into<JsonValue>) -> Self {
        self.filter.attrs.push((key.into(), value.into()));
        self
    }

    pub fn to_json(&self) -> String {
        let mut fields = Vec::new();
        push_opt_str(&mut fields, "text", self.text.as_deref());
        if !self.vector.is_empty() {
            fields.push(format!(
                "\"vector\":[{}]",
                self.vector
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        push_opt_num(&mut fields, "k", self.k);
        if !self.filter.is_empty() {
            fields.push(format!("\"filter\":{}", self.filter.to_json()));
        }
        format!("{{{}}}", fields.join(","))
    }
}

#[derive(Clone, Debug, Default)]
struct SearchFilter {
    agent_name: Option<String>,
    status: Option<u8>,
    attrs: Vec<(String, JsonValue)>,
}

impl SearchFilter {
    fn is_empty(&self) -> bool {
        self.agent_name.is_none() && self.status.is_none() && self.attrs.is_empty()
    }

    fn to_json(&self) -> String {
        let mut fields = Vec::new();
        push_opt_str(&mut fields, "agent_name", self.agent_name.as_deref());
        push_opt_num(&mut fields, "status", self.status);
        if !self.attrs.is_empty() {
            fields.push(format!(
                "\"attrs\":{{{}}}",
                self.attrs
                    .iter()
                    .map(|(key, value)| format!("{}:{}", json_string(key), value.to_json()))
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        format!("{{{}}}", fields.join(","))
    }
}

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

fn push_opt_str(fields: &mut Vec<String>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        fields.push(format!("\"{key}\":{}", json_string(value)));
    }
}

fn push_opt_num<T: fmt::Display>(fields: &mut Vec<String>, key: &str, value: Option<T>) {
    if let Some(value) = value {
        fields.push(format!("\"{key}\":{value}"));
    }
}

fn path_with_query(base: &str, query: &str) -> String {
    let query = query.trim();
    if query.is_empty() {
        base.to_string()
    } else if query.starts_with('?') {
        format!("{base}{query}")
    } else {
        format!("{base}?{query}")
    }
}

fn json_string(value: &str) -> String {
    format!("\"{}\"", json_escape(value))
}

fn json_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}
