//! 极小 HTTP/1.1 摄入+查询服务（只用 std::net，零依赖、离线可编译）。
//!
//! 路由：
//!   POST /v1/ingest  —— body 是 SDK 线格式 JSON 批 → `parse_wire_batch` → `ingest_wire`
//!   POST /v1/traces  —— OTLP/HTTP 标准端点：OTLP/OpenInference trace → `ingest_otlp`（生态入口）
//!   GET  /v1/traces  —— 返回 trace 列表（JSON）
//!   POST /v1/search  —— 中文检索 + 可选属性过滤(agent/状态/时间) → `search_text_attr`（产品差异化出口）
//!
//! 这是 SDK→引擎跨进程的最后一层。真要上量/上 TLS，换 axum/hyper 即可，路由逻辑（`route`）不变。
//! OTLP 走「OTLP→WireRecord 适配器」（`otlp.rs`）接到同一个 `ingest_wire` 边界。
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use crate::{
    parse_wire_batch, AnnotationStatus, AnnotationTarget, DatasetAssociation,
    DatasetAssociationFilter, NewDatasetAssociation, NewTraceAnnotation, Projection, ReadPlanStats,
    SearchFilter, TraceAnnotation, TraceAnnotationFilter, TraceQuery, UpdateTraceAnnotation,
    WriteCoordinator,
};
use yt_core::fold::FoldedSpan;

/// 进程内 JSON API 边界。
///
/// 它复用 HTTP 的 path/body JSON 契约，但不负责 socket、鉴权头解析、body limit、gzip
/// 或静态资源。HTTP server 和 Node/Electron N-API 都走这里，从而共享同一套路由语义。
#[derive(Clone)]
pub struct EngineJsonApi {
    coord: Arc<WriteCoordinator>,
}

pub struct HttpIngestServer {
    api: EngineJsonApi,
    /// 鉴权 token。None = 不鉴权（仅限本机开发）。Some = 要求 `Authorization: Bearer <token>`。
    auth_token: Option<String>,
    /// 请求体上限（字节）。超了直接 413，**绝不按 Content-Length 预分配** —— 堵 OOM 拒绝服务。
    max_body: usize,
}

impl HttpIngestServer {
    pub fn new(coord: Arc<WriteCoordinator>) -> Self {
        Self {
            api: EngineJsonApi::new(coord),
            auth_token: None,
            max_body: 16 << 20,
        } // 默认 16 MiB
    }

    /// 要求 Bearer token 鉴权（金融政企私有化最低门槛）。
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    pub fn with_max_body(mut self, bytes: usize) -> Self {
        self.max_body = bytes;
        self
    }

    /// 鉴权判定：未配 token 则放行；配了则要求 `Authorization: Bearer <token>` 精确匹配。
    fn authorized(&self, auth_header: Option<&str>) -> bool {
        match &self.auth_token {
            None => true,
            Some(tok) => auth_header
                .and_then(|h| h.trim().strip_prefix("Bearer "))
                .map_or(false, |got| got.trim() == tok),
        }
    }

    /// 永久 accept 循环（给二进制用）。
    pub fn serve(&self, listener: &TcpListener) {
        for stream in listener.incoming().flatten() {
            self.handle(stream);
        }
    }

    /// 只处理 n 个连接后返回（给测试用，可 join）。
    pub fn serve_n(&self, listener: &TcpListener, n: usize) {
        for _ in 0..n {
            if let Ok((stream, _)) = listener.accept() {
                self.handle(stream);
            }
        }
    }

    /// 固定大小线程池 accept（生产用）：`workers` 个工作线程从 channel 取连接处理，
    /// accept 循环在调用线程。线程数有界 → 不会被高并发连接打爆（无界 spawn 本身是 DoS 面）。
    pub fn serve_pool(self: Arc<Self>, listener: TcpListener, workers: usize) {
        let (tx, rx) = mpsc::channel::<TcpStream>();
        let rx = Arc::new(Mutex::new(rx));
        for _ in 0..workers.max(1) {
            let rx = Arc::clone(&rx);
            let me = Arc::clone(&self);
            thread::spawn(move || loop {
                let next = rx.lock().unwrap().recv();
                match next {
                    Ok(stream) => me.handle(stream),
                    Err(_) => break, // 发送端关闭
                }
            });
        }
        for stream in listener.incoming().flatten() {
            if tx.send(stream).is_err() {
                break;
            }
        }
    }

    fn handle(&self, mut stream: TcpStream) {
        let Ok(clone) = stream.try_clone() else {
            return;
        };
        let mut reader = BufReader::new(clone);

        let mut line = String::new();
        if reader.read_line(&mut line).is_err() {
            return;
        }
        let mut parts = line.split_whitespace();
        let method = parts.next().unwrap_or("").to_string();
        let path = parts.next().unwrap_or("").to_string();

        let mut content_length = 0usize;
        let mut auth: Option<String> = None;
        let mut encoding: Option<String> = None;
        // 租户来自**鉴权上下文**（X-Tenant-Id 头），不信任请求体——客户端不能自选租户。
        let mut tenant: Option<u64> = None;
        loop {
            let mut h = String::new();
            if reader.read_line(&mut h).unwrap_or(0) == 0 {
                break;
            }
            if h == "\r\n" || h == "\n" {
                break;
            }
            let hl = h.to_ascii_lowercase();
            if let Some(v) = hl.strip_prefix("content-length:") {
                content_length = v.trim().parse().unwrap_or(0);
            } else if hl.starts_with("authorization:") {
                // 取原始大小写的值（token 大小写敏感）
                auth = h.splitn(2, ':').nth(1).map(|s| s.trim().to_string());
            } else if let Some(v) = hl.strip_prefix("content-encoding:") {
                encoding = Some(v.trim().to_string());
            } else if let Some(v) = hl.strip_prefix("x-tenant-id:") {
                tenant = v.trim().parse().ok();
            }
        }

        // ① 请求体上限：超了直接 413，**绝不按 Content-Length 预分配** → 堵 OOM 拒绝服务。
        if content_length > self.max_body {
            self.respond(&mut stream, 413, r#"{"error":"body too large"}"#);
            self.audit(&method, &path, 413, content_length);
            return;
        }
        // ② 静态资源（内嵌的控制台前端）：页面本身匿名可取，API 仍按下面 Bearer 鉴权。
        // 浏览器访问 `/` 不能带 Authorization 头；把鉴权留给 /v1/* 数据请求。
        if method == "GET" && !path.starts_with("/v1") {
            let p = path.split('?').next().unwrap_or("/");
            if self.serve_static(&mut stream, p) {
                self.audit(&method, &path, 200, 0);
                return;
            }
        }

        // ③ 鉴权：未带/错 token → 401，且不读 body。
        if !self.authorized(auth.as_deref()) {
            self.respond(&mut stream, 401, r#"{"error":"unauthorized"}"#);
            self.audit(&method, &path, 401, content_length);
            return;
        }

        let mut body_buf = vec![0u8; content_length];
        if content_length > 0 && reader.read_exact(&mut body_buf).is_err() {
            return;
        }
        // gzip 解压（带防炸弹上限）。未开 gzip feature 且 body 是 gzip → 415。
        let body_bytes = match self.decode_body(encoding.as_deref(), body_buf) {
            Ok(b) => b,
            Err(code) => {
                self.respond(
                    &mut stream,
                    code,
                    r#"{"error":"bad or unsupported body encoding"}"#,
                );
                self.audit(&method, &path, code, content_length);
                return;
            }
        };
        let body = String::from_utf8_lossy(&body_bytes).into_owned();

        let (status, resp_body) = self.api.route_with_tenant(&method, &path, &body, tenant);
        self.respond(&mut stream, status, &resp_body);
        self.audit(&method, &path, status, content_length);
    }

    /// 从内嵌资源服务静态文件。`/` → index.html；未知无扩展名路径 → 回退 index.html（SPA 前端路由）。
    /// 返回是否命中（命中已写响应）。console_dist 未构建时 ASSETS 为空 → 一律 miss。
    fn serve_static(&self, stream: &mut TcpStream, path: &str) -> bool {
        let want = if path == "/" { "/index.html" } else { path };
        for (url, ct, bytes) in crate::assets::ASSETS {
            if *url == want {
                self.respond_bytes(stream, ct, bytes);
                return true;
            }
        }
        // SPA 回退：无扩展名的路径当前端路由，回 index.html。
        if !path.contains('.') {
            for (url, ct, bytes) in crate::assets::ASSETS {
                if *url == "/index.html" {
                    self.respond_bytes(stream, ct, bytes);
                    return true;
                }
            }
        }
        false
    }

    fn respond_bytes(&self, stream: &mut TcpStream, content_type: &str, body: &[u8]) {
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(head.as_bytes());
        let _ = stream.write_all(body);
        let _ = stream.flush();
    }

    fn respond(&self, stream: &mut TcpStream, status: u16, body: &str) {
        let reason = match status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            404 => "Not Found",
            413 => "Payload Too Large",
            415 => "Unsupported Media Type",
            _ => "Error",
        };
        let resp = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(resp.as_bytes());
        let _ = stream.flush();
    }

    /// gzip 解压（feature = gzip）。带防炸弹上限：解压后超 max_body → 413。
    #[cfg(feature = "gzip")]
    fn decode_body(&self, encoding: Option<&str>, raw: Vec<u8>) -> Result<Vec<u8>, u16> {
        if encoding.map_or(false, |e| e.eq_ignore_ascii_case("gzip")) {
            use std::io::Read;
            let mut out = Vec::new();
            // take(max_body+1)：限制解压输出,防小包炸大（gzip bomb）。
            let mut dec = flate2::read::GzDecoder::new(&raw[..]).take(self.max_body as u64 + 1);
            if dec.read_to_end(&mut out).is_err() {
                return Err(400);
            }
            if out.len() > self.max_body {
                return Err(413);
            }
            return Ok(out);
        }
        Ok(raw)
    }

    /// 未编译 gzip feature：gzip body 直接 415（不静默当原文，避免误判）。
    #[cfg(not(feature = "gzip"))]
    fn decode_body(&self, encoding: Option<&str>, raw: Vec<u8>) -> Result<Vec<u8>, u16> {
        if encoding.map_or(false, |e| e.eq_ignore_ascii_case("gzip")) {
            return Err(415);
        }
        Ok(raw)
    }

    /// ③ 审计留痕（等保三级硬要求）。骨架打到 stderr；真实实现落持久、防篡改的审计日志
    /// （含主体身份/源 IP/时间戳/操作/结果），并接入 SIEM。
    fn audit(&self, method: &str, path: &str, status: u16, body_len: usize) {
        eprintln!("[AUDIT] {method} {path} -> {status} ({body_len}B)");
    }

    /// 兼容旧测试/调用方的路由入口。真实进程内 API 请优先使用 `EngineJsonApi`。
    pub fn route(&self, method: &str, path: &str, body: &str) -> (u16, String) {
        self.api.route(method, path, body)
    }

    /// 兼容旧测试/调用方的带租户路由入口。真实进程内 API 请优先使用 `EngineJsonApi`。
    pub fn route_with_tenant(
        &self,
        method: &str,
        path: &str,
        body: &str,
        tenant: Option<u64>,
    ) -> (u16, String) {
        self.api.route_with_tenant(method, path, body, tenant)
    }
}

impl EngineJsonApi {
    pub fn new(coord: Arc<WriteCoordinator>) -> Self {
        Self { coord }
    }

    /// 纯路由（无 socket，便于单测和嵌入式调用）。返回 (status, json_body)。
    pub fn route(&self, method: &str, path: &str, body: &str) -> (u16, String) {
        self.route_with_tenant(method, path, body, None)
    }

    /// 带租户上下文的进程内路由：检索/列表端点据此强制隔离。
    pub fn route_with_tenant(
        &self,
        method: &str,
        path: &str,
        body: &str,
        tenant: Option<u64>,
    ) -> (u16, String) {
        if let Err(err) = self.coord.try_refresh_from_disk_for_api() {
            return internal_error(err);
        }
        // 切掉查询串：精确路由按 base 匹配，查询参数（分页 cursor/limit）单独解析。
        let (base, query) = path.split_once('?').unwrap_or((path, ""));
        match (method, base) {
            ("POST", "/v1/ingest") => match parse_wire_batch(body) {
                Ok(recs) => {
                    let n = recs.len();
                    match self.coord.try_ingest_wire_for_tenant(recs, tenant) {
                        Ok(_) => (200, format!(r#"{{"ingested":{n}}}"#)),
                        Err(err) => internal_error(err),
                    }
                }
                Err(e) => (400, format!(r#"{{"error":"{}"}}"#, e.replace('"', "'"))),
            },
            // OTLP/HTTP 标准 trace 端点（生态入口）：OpenTelemetry / OpenInference 埋点直接 POST 到这里。
            ("POST", "/v1/traces") => match crate::otlp::parse_otlp_traces(body) {
                Ok(records) => match self.coord.try_ingest_wire_for_tenant(records, tenant) {
                    Ok(_) => (200, r#"{"partialSuccess":{}}"#.to_string()),
                    Err(err) => internal_error(err),
                },
                Err(e) => (400, format!(r#"{{"error":"{}"}}"#, e.replace('"', "'"))),
            },
            ("GET", "/v1/traces") => (200, self.traces_json(tenant)),
            // 检索端点（产品差异化的出口）：中文 BM25 + 可选属性过滤(agent/状态/时间/trace) + 租户隔离。
            ("POST", "/v1/search") => self.search_json(body, tenant),
            // 单机读模型：先保证 trace 级过滤、聚合和空间统计可用；高性能索引后续单独迁移。
            ("POST", "/v1/trace-search") => self.trace_search_json(body, tenant),
            ("POST", "/v1/trace-aggregate") => self.trace_aggregate_json(body, tenant),
            ("POST", "/v1/storage-stats") => self.storage_stats_json(body, tenant),
            ("POST", "/v1/retention-plan") | ("POST", "/v1/retention/plan") => {
                self.retention_plan_json(body, tenant, false)
            }
            ("POST", "/v1/retention/apply") => self.retention_plan_json(body, tenant, true),
            ("GET", "/v1/retention-audits") | ("GET", "/v1/retention/audits") => {
                self.retention_audits_query_json(query, tenant)
            }
            ("POST", "/v1/retention-audits") | ("POST", "/v1/retention/audits") => {
                self.retention_audits_body_json(body, tenant)
            }
            ("POST", "/v1/retention-policies") | ("POST", "/v1/retention/policies") => {
                self.create_retention_policy_json(body, tenant)
            }
            ("GET", "/v1/retention-policies") | ("GET", "/v1/retention/policies") => {
                self.retention_policies_query_json(query, tenant)
            }
            ("POST", "/v1/retention-policies/run-due")
            | ("POST", "/v1/retention/policies/run-due")
            | ("POST", "/v1/retention/run-due") => {
                self.run_due_retention_policies_json(body, tenant)
            }
            ("POST", "/v1/trace-trajectories") => self.trace_trajectories_json(body, tenant),
            ("POST", "/v1/trajectory-groups") => self.trajectory_groups_json(body, tenant),
            ("POST", "/v1/traces/diff") => self.trace_diff_json(body, tenant),
            ("POST", "/v1/annotations") => self.create_annotation_json(body, tenant),
            ("GET", "/v1/annotations") => self.annotations_json(query, tenant),
            ("POST", "/v1/dataset-associations") | ("POST", "/v1/dataset-links") => {
                self.create_dataset_association_json(body, tenant)
            }
            ("GET", "/v1/dataset-associations") | ("GET", "/v1/dataset-links") => {
                self.dataset_associations_json(query, tenant)
            }
            // 容器/编排系统探针：只表明进程和路由可用，不做深度数据一致性检查。
            ("GET", "/v1/healthz") | ("GET", "/v1/readyz") => (200, r#"{"ok":true}"#.to_string()),
            // 生产可观测（§3.1）：Prometheus 文本格式，无需租户隔离（全局指标）。
            ("GET", "/v1/metrics") => (200, self.coord.metrics()),
            // 控制台数据端点（前端 yitrace-console 对接）：会话游标分页 / 轮次 / trace span / span 详情。
            ("GET", "/v1/sessions") => (200, self.sessions_page_json(query, tenant)),
            ("GET", "/v1/loops") => (200, self.loops_page_json(query, tenant)),
            _ => self.route_console(method, base, query, body, tenant),
        }
    }

    /// 带路径参数的控制台路由（/v1/sessions/:id/turns 等）。
    fn route_console(
        &self,
        method: &str,
        base: &str,
        query: &str,
        body: &str,
        tenant: Option<u64>,
    ) -> (u16, String) {
        let segs: Vec<&str> = base
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();
        match (method, segs.as_slice()) {
            ("GET", ["v1", "sessions", id, "turns"]) => self.turns_json(id, tenant),
            ("GET", ["v1", "traces", id]) => self.trace_json(id, tenant),
            ("GET", ["v1", "traces", id, "steps"]) => self.steps_json(id, tenant),
            ("GET", ["v1", "traces", id, "spans", sid]) => self.span_detail_json(id, sid, tenant),
            ("GET", ["v1", "loops", loop_id]) => self.loop_detail_json(loop_id, query, tenant),
            ("PATCH", ["v1", "annotations", id]) => self.update_annotation_json(id, body, tenant),
            ("POST", ["v1", "annotations", id, "status"]) => {
                self.update_annotation_json(id, body, tenant)
            }
            ("DELETE", ["v1", "annotations", id]) => self.delete_annotation_json(id, body, tenant),
            ("GET", ["v1", "tasks", fingerprint, "traces"]) => {
                (200, self.task_traces_json(fingerprint, query, tenant))
            }
            _ => (404, r#"{"error":"not found"}"#.to_string()),
        }
    }

    /// 处理 `POST /v1/search`：body = `{"text":"盗刷","vector":[..],"k":10,"filter":{"agent_name":"风控"}}`。
    /// 按给了什么自动选检索路:只 text→中文检索;只 vector→找相似;两个都给→混合(RRF)。都按 filter 过滤。
    fn search_json(&self, body: &str, tenant: Option<u64>) -> (u16, String) {
        use crate::wire::{field, parse, Json};
        let v = match parse(body) {
            Ok(v) => v,
            Err(e) => return (400, format!(r#"{{"error":"{}"}}"#, e.replace('"', "'"))),
        };
        let text = field(&v, "text").and_then(Json::as_str).unwrap_or("");
        let k = field(&v, "k").and_then(Json::as_u64).unwrap_or(10) as usize;
        let vector: Vec<f32> = field(&v, "vector")
            .map(|j| j.as_array().iter().filter_map(Json::as_f32).collect())
            .unwrap_or_default();
        let mut filter = crate::SearchFilter::default();
        if let Some(f) = field(&v, "filter") {
            let trace_id_value = json_field_alias(f, &["trace_id", "traceId"]);
            filter.external_trace_id =
                json_field_alias(f, &["external_trace_id", "externalTraceId"])
                    .and_then(Json::as_str)
                    .map(str::to_string)
                    .or_else(|| trace_id_value.and_then(json_id_text));
            filter.agent_name = field(f, "agent_name")
                .and_then(Json::as_str)
                .map(|s| s.to_string());
            filter.tool_name = json_field_alias(f, &["tool_name", "toolName"])
                .and_then(Json::as_str)
                .map(|s| s.to_string());
            filter.model = field(f, "model").and_then(Json::as_str).map(str::to_string);
            filter.status = field(f, "status").and_then(Json::as_u64).map(|x| x as u8);
            filter.time_from = field(f, "time_from").and_then(Json::as_i64);
            filter.time_to = field(f, "time_to").and_then(Json::as_i64);
            collect_attr_filters(f, &mut filter);
        }
        // 租户来自鉴权头（X-Tenant-Id），覆盖请求体——客户端不能越权查别的租户。
        filter.tenant_id = tenant;

        if !vector.is_empty() {
            if let Err(error) = self.coord.try_prepare_vector_search() {
                return internal_error(error);
            }
        }
        let snap = self.coord.pin_snapshot();
        let hits = match (!text.is_empty(), !vector.is_empty()) {
            (true, true) => self
                .coord
                .search_hybrid_attr(&snap, text, &vector, k, &filter), // 混合
            (false, true) => self.coord.search_similar_attr(&snap, &vector, k, &filter), // 找相似
            _ => self.coord.search_text_attr(&snap, text, k, &filter),                   // 中文检索
        };
        let items: Vec<String> = hits
            .iter()
            .map(|(s, score)| {
                let logs: Vec<String> = s.logs.iter().map(|l| format!("\"{}\"", json_escape(l))).collect();
                format!(
                    r#"{{"trace_id":{},"span_id":{},"external_trace_id":{},"external_span_id":{},"score":{:.4},"status":{},"duration_ns":{},"agent_name":{},"logs":[{}],"attrs":{}}}"#,
                    s.trace_id,
                    s.span_id,
                    json_opt_str(s.external_trace_id.as_deref()),
                    json_opt_str(s.external_span_id.as_deref()),
                    score,
                    s.status.map_or("null".to_string(), |x| x.to_string()),
                    s.duration_ns.map_or("null".to_string(), |x| x.to_string()),
                    s.agent_name.as_ref().map_or("null".to_string(), |a| format!("\"{}\"", json_escape(a))),
                    logs.join(","),
                    json_attrs(&s.attrs),
                )
            })
            .collect();
        (200, format!("[{}]", items.join(",")))
    }

    fn traces_json(&self, tenant: Option<u64>) -> String {
        let snap = self.coord.pin_snapshot();
        let mut q = TraceQuery::all();
        q.tenant_id = tenant; // 租户隔离：只列本租户的 trace
        let traces = self.coord.list_traces(&snap, &q);
        let items: Vec<String> = traces
            .iter()
            .map(|t| {
                format!(
                    r#"{{"trace_id":{},"external_trace_id":{},"span_count":{},"total_duration_ns":{},"max_duration_ns":{},"error_count":{},"total_input_tokens":{},"total_output_tokens":{},"total_cache_read_tokens":{},"total_cache_write_tokens":{},"cache_read_reported_spans":{},"cache_write_reported_spans":{},"total_llm_spans":{}}}"#,
                    t.trace_id,
                    json_opt_str(t.external_trace_id.as_deref()),
                    t.span_count,
                    t.total_duration_ns,
                    t.max_duration_ns,
                    t.error_count,
                    t.total_input_tokens,
                    t.total_output_tokens,
                    t.total_cache_read_tokens.map_or("null".to_string(), |v| v.to_string()),
                    t.total_cache_write_tokens.map_or("null".to_string(), |v| v.to_string()),
                    t.cache_read_reported_spans,
                    t.cache_write_reported_spans,
                    t.total_llm_spans,
                )
            })
            .collect();
        format!("[{}]", items.join(","))
    }

    // ───────────────────── 控制台数据端点（游标分页 / 轮次 / span / 详情） ─────────────────────
}

include!("http/read_model_api.rs");
include!("http/metadata_api.rs");
include!("http/path_api.rs");
include!("http/console_api.rs");

include!("http/json_helpers.rs");
include!("http/trace_filter_helpers.rs");
include!("http/metadata_helpers.rs");
include!("http/read_model_helpers.rs");
include!("http/trajectory_helpers.rs");
include!("http/json_escape.rs");
include!("http/retention_helpers.rs");
include!("http/retention_api.rs");

#[cfg(test)]
mod tests;

fn internal_error(err: impl std::fmt::Display) -> (u16, String) {
    (
        500,
        format!(r#"{{"error":"{}"}}"#, json_escape(&err.to_string())),
    )
}
