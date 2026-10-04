use std::path::PathBuf;
use std::sync::Arc;

use napi::{Error, Result, Status};
use napi_derive::napi;
use yt_engine::{EngineJsonApi, WriteCoordinator};

fn napi_err(message: impl Into<String>) -> Error {
    Error::new(Status::GenericFailure, message.into())
}

fn parse_tenant_id(tenant_id: Option<String>) -> Result<Option<u64>> {
    match tenant_id {
        None => Ok(None),
        Some(s) if s.trim().is_empty() => Ok(None),
        Some(s) => s
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|_| napi_err(format!("bad tenantId: {s}"))),
    }
}

fn status_error(status: u16, body: String) -> Error {
    napi_err(format!(
        "yiTrace request failed: status={status} body={body}"
    ))
}

#[napi(js_name = "NativeYiTraceDB")]
pub struct NativeYiTraceDb {
    coord: Arc<WriteCoordinator>,
    api: EngineJsonApi,
    closed: bool,
}

#[napi]
impl NativeYiTraceDb {
    #[napi(constructor)]
    pub fn new(data_dir: String) -> Result<Self> {
        let dir = PathBuf::from(data_dir);
        std::fs::create_dir_all(&dir)
            .map_err(|e| napi_err(format!("create data dir failed: {e}")))?;
        let coord = WriteCoordinator::open_durable(&dir)
            .map_err(|e| napi_err(format!("open yiTrace data dir failed: {e}")))?;
        coord
            .try_recover()
            .map_err(|e| napi_err(format!("recover yiTrace failed: {e}")))?;
        let api = EngineJsonApi::new(Arc::clone(&coord));
        Ok(Self {
            coord,
            api,
            closed: false,
        })
    }

    #[napi(js_name = "ingestJson")]
    pub fn ingest_json(&self, events_json: String, tenant_id: Option<String>) -> Result<String> {
        self.route("POST", "/v1/ingest", &events_json, tenant_id)
    }

    #[napi(js_name = "ingestOtlpJson")]
    pub fn ingest_otlp_json(&self, otlp_json: String, tenant_id: Option<String>) -> Result<String> {
        self.route("POST", "/v1/traces", &otlp_json, tenant_id)
    }

    #[napi(js_name = "searchJson")]
    pub fn search_json(&self, query_json: String, tenant_id: Option<String>) -> Result<String> {
        self.route("POST", "/v1/search", &query_json, tenant_id)
    }

    #[napi(js_name = "indexEmbedding")]
    pub fn index_embedding(
        &self,
        trace_id: String,
        span_id: String,
        embedding: Vec<f64>,
        tenant_id: Option<String>,
    ) -> Result<()> {
        self.ensure_open()?;
        if embedding.is_empty() {
            return Err(napi_err("embedding must not be empty"));
        }
        let trace_id = parse_id_or_hash(&trace_id)?;
        let span_id = parse_id_or_hash(&span_id)?;
        let mut vector = Vec::with_capacity(embedding.len());
        for (i, value) in embedding.into_iter().enumerate() {
            if !value.is_finite() {
                return Err(napi_err(format!("embedding[{i}] must be a finite number")));
            }
            vector.push(value as f32);
        }
        self.coord
            .index_embedding_for_tenant(parse_tenant_id(tenant_id)?, trace_id, span_id, vector)
            .map_err(|e| napi_err(e.to_string()))
    }

    #[napi(js_name = "traceSearchJson")]
    pub fn trace_search_json(
        &self,
        query_json: String,
        tenant_id: Option<String>,
    ) -> Result<String> {
        self.route("POST", "/v1/trace-search", &query_json, tenant_id)
    }

    #[napi(js_name = "traceAggregateJson")]
    pub fn trace_aggregate_json(
        &self,
        query_json: String,
        tenant_id: Option<String>,
    ) -> Result<String> {
        self.route("POST", "/v1/trace-aggregate", &query_json, tenant_id)
    }

    #[napi(js_name = "storageStatsJson")]
    pub fn storage_stats_json(
        &self,
        query_json: String,
        tenant_id: Option<String>,
    ) -> Result<String> {
        self.route("POST", "/v1/storage-stats", &query_json, tenant_id)
    }

    #[napi(js_name = "retentionPlanJson")]
    pub fn retention_plan_json(
        &self,
        query_json: String,
        tenant_id: Option<String>,
    ) -> Result<String> {
        self.route("POST", "/v1/retention-plan", &query_json, tenant_id)
    }

    #[napi(js_name = "applyRetentionJson")]
    pub fn apply_retention_json(
        &self,
        query_json: String,
        tenant_id: Option<String>,
    ) -> Result<String> {
        self.route("POST", "/v1/retention/apply", &query_json, tenant_id)
    }

    #[napi(js_name = "retentionAuditsJson")]
    pub fn retention_audits_json(
        &self,
        query_string: Option<String>,
        tenant_id: Option<String>,
    ) -> Result<String> {
        let mut path = "/v1/retention-audits".to_string();
        if let Some(query) = query_string {
            if !query.trim().is_empty() {
                path.push('?');
                path.push_str(query.trim_start_matches('?'));
            }
        }
        self.route("GET", &path, "", tenant_id)
    }

    #[napi(js_name = "createRetentionPolicyJson")]
    pub fn create_retention_policy_json(
        &self,
        policy_json: String,
        tenant_id: Option<String>,
    ) -> Result<String> {
        self.route("POST", "/v1/retention-policies", &policy_json, tenant_id)
    }

    #[napi(js_name = "retentionPoliciesJson")]
    pub fn retention_policies_json(
        &self,
        query_string: Option<String>,
        tenant_id: Option<String>,
    ) -> Result<String> {
        let mut path = "/v1/retention-policies".to_string();
        if let Some(query) = query_string {
            if !query.trim().is_empty() {
                path.push('?');
                path.push_str(query.trim_start_matches('?'));
            }
        }
        self.route("GET", &path, "", tenant_id)
    }

    #[napi(js_name = "runRetentionPoliciesJson")]
    pub fn run_retention_policies_json(
        &self,
        query_json: String,
        tenant_id: Option<String>,
    ) -> Result<String> {
        self.route(
            "POST",
            "/v1/retention-policies/run-due",
            &query_json,
            tenant_id,
        )
    }

    #[napi(js_name = "traceTrajectoriesJson")]
    pub fn trace_trajectories_json(
        &self,
        query_json: String,
        tenant_id: Option<String>,
    ) -> Result<String> {
        self.route("POST", "/v1/trace-trajectories", &query_json, tenant_id)
    }

    #[napi(js_name = "trajectoryGroupsJson")]
    pub fn trajectory_groups_json(
        &self,
        query_json: String,
        tenant_id: Option<String>,
    ) -> Result<String> {
        self.route("POST", "/v1/trajectory-groups", &query_json, tenant_id)
    }

    #[napi(js_name = "traceDiffJson")]
    pub fn trace_diff_json(&self, query_json: String, tenant_id: Option<String>) -> Result<String> {
        self.route("POST", "/v1/traces/diff", &query_json, tenant_id)
    }

    #[napi(js_name = "annotateJson")]
    pub fn annotate_json(
        &self,
        annotation_json: String,
        tenant_id: Option<String>,
    ) -> Result<String> {
        self.route("POST", "/v1/annotations", &annotation_json, tenant_id)
    }

    #[napi(js_name = "annotationsJson")]
    pub fn annotations_json(
        &self,
        query_string: Option<String>,
        tenant_id: Option<String>,
    ) -> Result<String> {
        let mut path = "/v1/annotations".to_string();
        if let Some(query) = query_string {
            if !query.trim().is_empty() {
                path.push('?');
                path.push_str(query.trim_start_matches('?'));
            }
        }
        self.route("GET", &path, "", tenant_id)
    }

    #[napi(js_name = "updateAnnotationJson")]
    pub fn update_annotation_json(
        &self,
        annotation_id: String,
        update_json: String,
        tenant_id: Option<String>,
    ) -> Result<String> {
        let path = format!(
            "/v1/annotations/{}",
            url_encode_component(annotation_id.trim())
        );
        self.route("PATCH", &path, &update_json, tenant_id)
    }

    #[napi(js_name = "deleteAnnotationJson")]
    pub fn delete_annotation_json(
        &self,
        annotation_id: String,
        delete_json: Option<String>,
        tenant_id: Option<String>,
    ) -> Result<String> {
        let path = format!(
            "/v1/annotations/{}",
            url_encode_component(annotation_id.trim())
        );
        self.route(
            "DELETE",
            &path,
            delete_json.as_deref().unwrap_or(""),
            tenant_id,
        )
    }

    #[napi(js_name = "linkDatasetItemJson")]
    pub fn link_dataset_item_json(
        &self,
        association_json: String,
        tenant_id: Option<String>,
    ) -> Result<String> {
        self.route(
            "POST",
            "/v1/dataset-associations",
            &association_json,
            tenant_id,
        )
    }

    #[napi(js_name = "datasetAssociationsJson")]
    pub fn dataset_associations_json(
        &self,
        query_string: Option<String>,
        tenant_id: Option<String>,
    ) -> Result<String> {
        let mut path = "/v1/dataset-associations".to_string();
        if let Some(query) = query_string {
            if !query.trim().is_empty() {
                path.push('?');
                path.push_str(query.trim_start_matches('?'));
            }
        }
        self.route("GET", &path, "", tenant_id)
    }

    #[napi(js_name = "tracesJson")]
    pub fn traces_json(&self, tenant_id: Option<String>) -> Result<String> {
        self.route("GET", "/v1/traces", "", tenant_id)
    }

    #[napi(js_name = "sessionsJson")]
    pub fn sessions_json(
        &self,
        cursor: Option<u32>,
        limit: Option<u32>,
        filter: Option<String>,
        attrs_json: Option<String>,
        tenant_id: Option<String>,
    ) -> Result<String> {
        let mut path = format!(
            "/v1/sessions?cursor={}&limit={}",
            cursor.unwrap_or(0),
            limit.unwrap_or(50)
        );
        if let Some(f) = filter {
            if !f.is_empty() {
                path.push_str("&filter=");
                path.push_str(&url_encode_component(&f));
            }
        }
        if let Some(attrs) = attrs_json {
            if !attrs.is_empty() {
                path.push_str("&attrs=");
                path.push_str(&url_encode_component(&attrs));
            }
        }
        self.route("GET", &path, "", tenant_id)
    }

    #[napi(js_name = "loopsJson")]
    pub fn loops_json(
        &self,
        cursor: Option<u32>,
        limit: Option<u32>,
        attrs_json: Option<String>,
        tenant_id: Option<String>,
    ) -> Result<String> {
        let mut path = format!(
            "/v1/loops?cursor={}&limit={}",
            cursor.unwrap_or(0),
            limit.unwrap_or(50)
        );
        if let Some(attrs) = attrs_json {
            if !attrs.is_empty() {
                path.push_str("&attrs=");
                path.push_str(&url_encode_component(&attrs));
            }
        }
        self.route("GET", &path, "", tenant_id)
    }

    #[napi(js_name = "loopJson")]
    pub fn loop_json(&self, loop_id: String, tenant_id: Option<String>) -> Result<String> {
        let path = format!("/v1/loops/{}", url_encode_component(loop_id.trim()));
        self.route("GET", &path, "", tenant_id)
    }

    #[napi(js_name = "taskTracesJson")]
    pub fn task_traces_json(
        &self,
        fingerprint: String,
        cursor: Option<u32>,
        limit: Option<u32>,
        attrs_json: Option<String>,
        tenant_id: Option<String>,
    ) -> Result<String> {
        let mut path = format!(
            "/v1/tasks/{}/traces?cursor={}&limit={}",
            url_encode_component(fingerprint.trim()),
            cursor.unwrap_or(0),
            limit.unwrap_or(50)
        );
        if let Some(attrs) = attrs_json {
            if !attrs.is_empty() {
                path.push_str("&attrs=");
                path.push_str(&url_encode_component(&attrs));
            }
        }
        self.route("GET", &path, "", tenant_id)
    }

    #[napi(js_name = "traceJson")]
    pub fn trace_json(&self, trace_id: String, tenant_id: Option<String>) -> Result<String> {
        let path = format!("/v1/traces/{}", trace_id.trim());
        self.route("GET", &path, "", tenant_id)
    }

    #[napi(js_name = "spanJson")]
    pub fn span_json(
        &self,
        trace_id: String,
        span_id: String,
        tenant_id: Option<String>,
    ) -> Result<String> {
        let path = format!("/v1/traces/{}/spans/{}", trace_id.trim(), span_id.trim());
        self.route("GET", &path, "", tenant_id)
    }

    #[napi]
    pub fn flush(&self) -> Result<()> {
        self.ensure_open()?;
        self.coord
            .try_flush_memtable()
            .map_err(|e| napi_err(format!("flush yiTrace failed: {e}")))?;
        Ok(())
    }

    #[napi]
    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.coord
            .try_flush_memtable()
            .map_err(|e| napi_err(format!("flush yiTrace failed: {e}")))?;
        self.closed = true;
        Ok(())
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            Err(napi_err("YiTraceDB is closed"))
        } else {
            Ok(())
        }
    }

    fn route(
        &self,
        method: &str,
        path: &str,
        body: &str,
        tenant_id: Option<String>,
    ) -> Result<String> {
        self.ensure_open()?;
        let tenant = parse_tenant_id(tenant_id)?;
        let (status, response) = self.api.route_with_tenant(method, path, body, tenant);
        if (200..300).contains(&status) {
            Ok(response)
        } else {
            Err(status_error(status, response))
        }
    }
}

impl Drop for NativeYiTraceDb {
    fn drop(&mut self) {
        if !self.closed {
            if let Err(err) = self.coord.try_flush_memtable() {
                eprintln!("yiTrace close flush failed: {err}");
            }
            self.closed = true;
        }
    }
}

fn url_encode_component(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn parse_id_or_hash(value: &str) -> Result<u64> {
    let value = value.trim();
    if value.is_empty() {
        return Err(napi_err("traceId/spanId must not be empty"));
    }
    Ok(value
        .parse::<u64>()
        .unwrap_or_else(|_| fnv1a64(value.as_bytes())))
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    h
}
