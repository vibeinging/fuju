/// 一个 span 的**可过滤元数据**（带过滤 ANN 的 payload）。摄入时按 last-non-null 累积、ts 取范围。
/// 让向量检索能按真实查询维度（agent / 状态 / 时间）过滤，而不只按 (trace,span) id。
#[derive(Clone, Debug, Default)]
struct FilterAttrs {
    event_order: SpanEventOrder,
    external_trace_id: Option<String>,
    status: Option<u8>,
    agent_name: Option<String>,
    tool_name: Option<String>,
    model: Option<String>,
    attrs: BTreeMap<String, String>,
    min_ts: i64,
    max_ts: i64,
    /// 租户隔离维度（last-non-null）。
    tenant_id: Option<u64>,
}

/// 检索过滤条件（产品维度）。下推进图搜索 / 后置过滤关键词候选。全 None = 不过滤。
/// 例："找 agent『风控研判』报错(status≠0)的相似 span" → trace_id=None, agent_name=Some(风控研判), status...
#[derive(Default, Clone)]
pub struct SearchFilter {
    pub trace_id: Option<u64>,
    pub external_trace_id: Option<String>,
    pub agent_name: Option<String>,
    pub tool_name: Option<String>,
    pub model: Option<String>,
    pub status: Option<u8>,
    pub time_from: Option<i64>,
    pub time_to: Option<i64>,
    /// attrs 精确过滤。高频字段见 `is_filter_attr_key`，未知字段可做最终校验但不保证走索引。
    pub attrs: BTreeMap<String, String>,
    /// **租户隔离**：设了它，只返回该租户的 span。服务层须按鉴权身份对每个查询注入它。
    pub tenant_id: Option<u64>,
}

impl SearchFilter {
    pub(crate) fn needs_indexed_filter(&self) -> bool {
        self.needs_attrs()
    }

    /// 是否带"要查属性边车"的约束。仅 trace_id 约束不算（trace_id 在 key 里直接判）。
    fn needs_attrs(&self) -> bool {
        self.external_trace_id.is_some()
            || self.agent_name.is_some()
            || self.tool_name.is_some()
            || self.model.is_some()
            || self.status.is_some()
            || self.time_from.is_some()
            || self.time_to.is_some()
            || !self.attrs.is_empty()
            || self.tenant_id.is_some()
    }

    pub(crate) fn has_non_tenant_constraints(&self) -> bool {
        self.trace_id.is_some()
            || self.external_trace_id.is_some()
            || self.agent_name.is_some()
            || self.tool_name.is_some()
            || self.model.is_some()
            || self.status.is_some()
            || self.time_from.is_some()
            || self.time_to.is_some()
            || !self.attrs.is_empty()
    }

    /// 属性是否匹配（不含 trace_id，那个在 key 上单独判）。
    fn attrs_match(&self, a: &FilterAttrs) -> bool {
        // 租户隔离：tenant 不符直接出局（最先判，隔离优先）。
        if let Some(t) = self.tenant_id {
            if a.tenant_id != Some(t) {
                return false;
            }
        }
        if let Some(external_trace_id) = &self.external_trace_id {
            if a.external_trace_id.as_deref() != Some(external_trace_id.as_str()) {
                return false;
            }
        }
        if let Some(ag) = &self.agent_name {
            if a.agent_name.as_deref() != Some(ag.as_str()) {
                return false;
            }
        }
        if let Some(tool) = &self.tool_name {
            if a.tool_name.as_deref() != Some(tool.as_str()) {
                return false;
            }
        }
        if let Some(model) = &self.model {
            if a.model.as_deref() != Some(model.as_str()) {
                return false;
            }
        }
        if let Some(st) = self.status {
            if a.status != Some(st) {
                return false;
            }
        }
        // 时间窗：span 的 [min_ts,max_ts] 与 [time_from,time_to] 有重叠才算命中。
        if let Some(from) = self.time_from {
            if a.max_ts < from {
                return false;
            }
        }
        if let Some(to) = self.time_to {
            if a.min_ts > to {
                return false;
            }
        }
        for (k, v) in &self.attrs {
            if a.attrs.get(k) != Some(v) {
                return false;
            }
        }
        true
    }
}

fn is_filter_attr_key(k: &str) -> bool {
    matches!(
        k,
        "project_id"
            | "skill"
            | "mode"
            | "call_site"
            | "task_fingerprint"
            | "loop_id"
            | "harness_version"
            | "schema_fingerprint"
            | "intent_signature"
            | "validation_status"
            | "review_status"
            | "eval_status"
            | "path_memory_id"
            | "stop_reason"
            | "phase"
            | "validator"
    )
}
