use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use crate::{CacheTokenCoverage, ConsoleSession, ReadPlanStats, SearchFilter, TraceQuery};
use yt_core::event::EventType;
use yt_core::fold::{FoldedSpan, SpanFields};
use yt_wal::WalRecord;

mod disk;
use disk::DiskTraceRollup;

pub(crate) struct TraceAggregateRollupIndex {
    rows: HashMap<crate::ScopedSpanKey, TraceAggregateRollupRow>,
    by_trace: BTreeMap<u64, Vec<u64>>,
    disk: Option<DiskTraceRollup>,
    dirty: bool,
    session_cache: std::sync::Mutex<
        HashMap<(Option<u64>, i64, i64, Vec<(String, String)>), Vec<ConsoleSession>>,
    >,
}

impl Default for TraceAggregateRollupIndex {
    fn default() -> Self {
        Self {
            rows: HashMap::new(),
            by_trace: BTreeMap::new(),
            disk: None,
            dirty: false,
            session_cache: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

impl TraceAggregateRollupIndex {
    pub(crate) fn apply_record(&mut self, record: &WalRecord) {
        self.session_cache.get_mut().unwrap().clear();
        if self.dirty {
            return;
        }
        let key = (record.fields.tenant_id, record.trace_id, record.span_id);
        if !self.rows.contains_key(&key) {
            if let Some(existing) = self.disk.as_mut().and_then(|disk| {
                disk.find_row(record.fields.tenant_id, record.trace_id, record.span_id)
            }) {
                self.rows.insert(key, existing);
            }
        }
        if !self.rows.contains_key(&key) {
            self.by_trace
                .entry(record.trace_id)
                .or_default()
                .push(record.span_id);
        }
        let row = self
            .rows
            .entry(key)
            .or_insert_with(|| TraceAggregateRollupRow::new(record.trace_id, record.span_id));
        row.apply_record(record);
    }

    pub(crate) fn mark_dirty(&mut self) {
        self.dirty = true;
        self.session_cache.get_mut().unwrap().clear();
    }

    pub(crate) fn from_records(
        records: impl IntoIterator<Item = WalRecord>,
        patches: impl IntoIterator<Item = (crate::ScopedSpanKey, SpanFields)>,
    ) -> Self {
        let mut next = Self::default();
        for record in records {
            next.apply_record(&record);
        }
        for (key, fields) in patches {
            if let Some(row) = next.rows.get_mut(&key) {
                row.apply_fields(&fields);
            }
        }
        next
    }

    pub(crate) fn rebuild(
        &mut self,
        records: impl IntoIterator<Item = WalRecord>,
        patches: impl IntoIterator<Item = (crate::ScopedSpanKey, SpanFields)>,
    ) {
        *self = Self::from_records(records, patches);
    }

    pub(crate) fn len(&self) -> usize {
        self.rows.len() + self.disk.as_ref().map_or(0, DiskTraceRollup::row_count)
    }

    pub(crate) fn save_cache(
        &mut self,
        path: &Path,
        manifest_version: u64,
        memtable_watermark: u64,
    ) -> std::io::Result<()> {
        let mut rows = match self.disk.as_mut() {
            Some(disk) => disk.all_rows().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "trace rollup page is corrupt",
                )
            })?,
            None => Vec::new(),
        };
        rows.retain(|row| {
            !self
                .rows
                .contains_key(&(row.tenant_id, row.trace_id, row.span_id))
        });
        rows.extend(self.rows.values().cloned());
        disk::write_atomic(path, manifest_version, memtable_watermark, &mut rows)
    }

    pub(crate) fn load_cache(
        path: &Path,
        manifest_version: u64,
        memtable_watermark: u64,
    ) -> Option<Self> {
        let disk = DiskTraceRollup::open(path, manifest_version, memtable_watermark)?;
        if !disk.has_event_order() {
            return None;
        }
        Some(Self {
            disk: Some(disk),
            ..Self::default()
        })
    }

    pub(crate) fn query(
        &mut self,
        query: &TraceQuery,
        filter: &SearchFilter,
    ) -> Option<(Vec<FoldedSpan>, ReadPlanStats)> {
        if self.dirty {
            let stats = ReadPlanStats {
                source: Some("scan".to_string()),
                fallback_reason: Some("rollup_dirty".to_string()),
                ..ReadPlanStats::default()
            };
            return Some((Vec::new(), stats));
        }
        let rows = self.matching_rows(query, filter)?;
        let (pages_read, pages_total) = self
            .disk
            .as_ref()
            .map_or((0, 0), DiskTraceRollup::last_read_stats);
        let mut spans: Vec<_> = rows
            .iter()
            .map(TraceAggregateRollupRow::to_folded_span)
            .collect();
        spans.sort_by_key(|span| (span.trace_id, span.span_id));
        let stats = ReadPlanStats {
            source: Some("aggregate_rollup".to_string()),
            used_filter_index: filter.needs_indexed_filter(),
            candidate_span_keys: Some(spans.len()),
            scanned_segments: 0,
            matched_spans: spans.len(),
            rollup_pages_read: Some(pages_read),
            rollup_pages_total: Some(pages_total),
            ..ReadPlanStats::default()
        };
        Some((spans, stats))
    }

    pub(crate) fn query_trace_ids(
        &mut self,
        trace_ids: &[u64],
        tenant: Option<u64>,
    ) -> Option<(BTreeMap<u64, Vec<FoldedSpan>>, ReadPlanStats)> {
        if self.dirty {
            let stats = ReadPlanStats {
                source: Some("scan".to_string()),
                fallback_reason: Some("rollup_dirty".to_string()),
                ..ReadPlanStats::default()
            };
            return Some((BTreeMap::new(), stats));
        }
        let trace_ids: std::collections::BTreeSet<_> = trace_ids.iter().copied().collect();
        let mut rows = match self.disk.as_mut() {
            Some(disk) => disk.rows_for_trace_ids(&trace_ids, tenant)?,
            None => Vec::new(),
        };
        rows.retain(|row| {
            !self
                .rows
                .contains_key(&(row.tenant_id, row.trace_id, row.span_id))
        });
        rows.extend(
            self.rows
                .values()
                .filter(|row| {
                    trace_ids.contains(&row.trace_id)
                        && tenant.is_none_or(|tenant| row.tenant_id == Some(tenant))
                })
                .cloned(),
        );
        let matched = rows.len();
        let (pages_read, pages_total) = self
            .disk
            .as_ref()
            .map_or((0, 0), DiskTraceRollup::last_read_stats);
        let mut out: BTreeMap<u64, Vec<FoldedSpan>> = BTreeMap::new();
        for row in rows {
            out.entry(row.trace_id)
                .or_default()
                .push(row.to_folded_span());
        }
        for spans in out.values_mut() {
            spans.sort_by_key(|span| span.span_id);
        }
        let stats = ReadPlanStats {
            source: Some("trajectory_rollup".to_string()),
            candidate_span_keys: Some(matched),
            scanned_segments: 0,
            matched_spans: matched,
            rollup_pages_read: Some(pages_read),
            rollup_pages_total: Some(pages_total),
            ..ReadPlanStats::default()
        };
        Some((out, stats))
    }

    /// 从小字段 rollup 直接聚合会话列表，避免 tenant 视图为每个请求重新扫描原始 segment。
    pub(crate) fn query_sessions(
        &mut self,
        query: &TraceQuery,
        filter: &SearchFilter,
    ) -> Option<Vec<ConsoleSession>> {
        if self.dirty {
            return None;
        }
        let cache_key = (
            filter.tenant_id,
            query.time_from,
            query.time_to,
            filter
                .attrs
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>(),
        );
        if let Some(rows) = self.session_cache.lock().unwrap().get(&cache_key) {
            return Some(rows.clone());
        }
        #[derive(Default)]
        struct Acc {
            traces: HashSet<u64>,
            external_session: Option<String>,
            input_tokens: u64,
            output_tokens: u64,
            coverage: CacheTokenCoverage,
            error_count: usize,
            running_count: usize,
            title: String,
            first_trace: u64,
        }

        let matching_sessions = if filter.attrs.is_empty() {
            None
        } else {
            let mut ids = HashSet::new();
            for row in self.matching_rows(query, filter)? {
                if let Some(session_id) = row.session_id {
                    ids.insert(session_id);
                }
            }
            Some(ids)
        };
        let mut aggregate_filter = filter.clone();
        aggregate_filter.attrs.clear();
        let mut sessions: BTreeMap<u64, Acc> = BTreeMap::new();
        for row in self.matching_rows(query, &aggregate_filter)? {
            let Some(session_id) = row.session_id else {
                continue;
            };
            if matching_sessions
                .as_ref()
                .is_some_and(|ids| !ids.contains(&session_id))
            {
                continue;
            }
            let acc = sessions.entry(session_id).or_default();
            acc.traces.insert(row.trace_id);
            acc.input_tokens += row.input_tokens.unwrap_or(0);
            acc.output_tokens += row.output_tokens.unwrap_or(0);
            acc.coverage.observe_values(
                row.model.is_some(),
                row.input_tokens,
                row.output_tokens,
                row.cache_read_tokens,
                row.cache_write_tokens,
            );
            acc.error_count += usize::from(row.status.unwrap_or(0) != 0);
            acc.running_count += usize::from(row.has_start && !row.has_end);
            if acc.external_session.is_none() {
                acc.external_session = row.external_session_id.clone();
            }
            if acc.title.is_empty() {
                if let Some(agent) = &row.agent_name {
                    acc.title = agent.clone();
                }
            }
            if acc.first_trace == 0 || row.trace_id < acc.first_trace {
                acc.first_trace = row.trace_id;
            }
        }

        let mut rows: Vec<ConsoleSession> = sessions
            .into_iter()
            .map(|(session_id, acc)| ConsoleSession {
                session_id,
                external_session_id: acc.external_session,
                title: if acc.title.is_empty() {
                    format!("会话 {session_id}")
                } else {
                    acc.title
                },
                turn_count: acc.traces.len(),
                input_tokens: acc.input_tokens,
                output_tokens: acc.output_tokens,
                cache_read_tokens: acc.coverage.read_total(),
                cache_write_tokens: acc.coverage.write_total(),
                cache_read_reported_spans: acc.coverage.read_reported_spans,
                cache_write_reported_spans: acc.coverage.write_reported_spans,
                total_llm_spans: acc.coverage.total_llm_spans,
                has_error: acc.error_count > 0,
                has_running: acc.running_count > 0,
                first_trace_id: acc.first_trace,
            })
            .collect();
        rows.sort_by(|a, b| b.session_id.cmp(&a.session_id));
        self.session_cache
            .lock()
            .unwrap()
            .insert(cache_key, rows.clone());
        Some(rows)
    }

    fn matching_rows(
        &mut self,
        query: &TraceQuery,
        filter: &SearchFilter,
    ) -> Option<Vec<TraceAggregateRollupRow>> {
        let mut rows = match self.disk.as_mut() {
            Some(disk) => disk.matching_rows(query, filter)?,
            None => Vec::new(),
        };
        rows.retain(|row| {
            !self
                .rows
                .contains_key(&(row.tenant_id, row.trace_id, row.span_id))
        });
        rows.extend(
            self.rows
                .values()
                .filter(|row| row.matches_query(query) && row.matches_filter(filter))
                .cloned(),
        );
        Some(rows)
    }
}

#[derive(Clone, Debug)]
struct TraceAggregateRollupRow {
    event_order: crate::SpanEventOrder,
    trace_id: u64,
    span_id: u64,
    has_start: bool,
    has_end: bool,
    parent_span_id: Option<u64>,
    session_id: Option<u64>,
    tenant_id: Option<u64>,
    external_trace_id: Option<String>,
    external_span_id: Option<String>,
    external_parent_span_id: Option<String>,
    external_session_id: Option<String>,
    span_name: Option<String>,
    display_name: Option<String>,
    status: Option<u8>,
    duration_ns: Option<u64>,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cache_read_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
    agent_name: Option<String>,
    tool_name: Option<String>,
    model: Option<String>,
    attrs: BTreeMap<String, String>,
    min_ts: i64,
    max_ts: i64,
    event_count: usize,
}

impl TraceAggregateRollupRow {
    #[cfg(test)]
    fn empty_for_test() -> Self {
        Self::new(0, 0)
    }

    fn new(trace_id: u64, span_id: u64) -> Self {
        Self {
            event_order: crate::SpanEventOrder::default(),
            trace_id,
            span_id,
            has_start: false,
            has_end: false,
            parent_span_id: None,
            session_id: None,
            tenant_id: None,
            external_trace_id: None,
            external_span_id: None,
            external_parent_span_id: None,
            external_session_id: None,
            span_name: None,
            display_name: None,
            status: None,
            duration_ns: None,
            input_tokens: None,
            output_tokens: None,
            cache_read_tokens: None,
            cache_write_tokens: None,
            agent_name: None,
            tool_name: None,
            model: None,
            attrs: BTreeMap::new(),
            min_ts: i64::MAX,
            max_ts: i64::MIN,
            event_count: 0,
        }
    }

    fn apply_record(&mut self, record: &WalRecord) {
        if !self.event_order.accept(record.identity.event_id().0) {
            return;
        }
        self.min_ts = self.min_ts.min(record.ts);
        self.max_ts = self.max_ts.max(record.ts);
        self.event_count = self.event_count.saturating_add(1);
        match record.identity.event_type {
            EventType::SpanStart => self.has_start = true,
            EventType::SpanEnd => self.has_end = true,
            _ => {}
        }
        self.apply_fields_at(&record.fields, record.identity.seq);
    }

    fn apply_fields(&mut self, fields: &SpanFields) {
        self.apply_fields_at(fields, u64::MAX);
    }
    fn apply_fields_at(&mut self, fields: &SpanFields, seq: u64) {
        if fields.session_id.is_some() && self.event_order.field("session_id", seq) {
            self.session_id = fields.session_id;
        }
        if fields.tenant_id.is_some() && self.event_order.field("tenant_id", seq) {
            self.tenant_id = fields.tenant_id;
        }
        if fields.external_trace_id.is_some() && self.event_order.field("external_trace_id", seq) {
            self.external_trace_id = fields.external_trace_id.clone();
        }
        if fields.external_span_id.is_some() && self.event_order.field("external_span_id", seq) {
            self.external_span_id = fields.external_span_id.clone();
        }
        if fields.external_parent_span_id.is_some()
            && self.event_order.field("external_parent_span_id", seq)
        {
            self.external_parent_span_id = fields.external_parent_span_id.clone();
        }
        if fields.external_session_id.is_some()
            && self.event_order.field("external_session_id", seq)
        {
            self.external_session_id = fields.external_session_id.clone();
        }
        if fields.span_name.is_some() && self.event_order.field("span_name", seq) {
            self.span_name = fields.span_name.clone();
        }
        if fields.display_name.is_some() && self.event_order.field("display_name", seq) {
            self.display_name = fields.display_name.clone();
        }
        if fields.status.is_some() && self.event_order.field("status", seq) {
            self.status = fields.status;
        }
        if fields.parent_span_id.is_some() && self.event_order.field("parent_span_id", seq) {
            self.parent_span_id = fields.parent_span_id;
        }
        if fields.duration_ns.is_some() && self.event_order.field("duration_ns", seq) {
            self.duration_ns = fields.duration_ns;
        }
        if fields.input_tokens.is_some() && self.event_order.field("input_tokens", seq) {
            self.input_tokens = fields.input_tokens;
        }
        if fields.output_tokens.is_some() && self.event_order.field("output_tokens", seq) {
            self.output_tokens = fields.output_tokens;
        }
        if fields.cache_read_tokens.is_some() && self.event_order.field("cache_read_tokens", seq) {
            self.cache_read_tokens = fields.cache_read_tokens;
        }
        if fields.cache_write_tokens.is_some() && self.event_order.field("cache_write_tokens", seq)
        {
            self.cache_write_tokens = fields.cache_write_tokens;
        }
        if fields.agent_name.is_some() && self.event_order.field("agent_name", seq) {
            self.agent_name = fields.agent_name.clone();
        }
        if fields.tool_name.is_some() && self.event_order.field("tool_name", seq) {
            self.tool_name = fields.tool_name.clone();
        }
        if fields.model.is_some() && self.event_order.field("model", seq) {
            self.model = fields.model.clone();
        }
        for (key, value) in &fields.attrs {
            if self.event_order.field(&format!("attrs:{key}"), seq) {
                self.attrs.insert(key.clone(), value.clone());
            }
        }
    }

    fn matches_query(&self, query: &TraceQuery) -> bool {
        if query
            .trace_id
            .map_or(false, |trace_id| trace_id != self.trace_id)
        {
            return false;
        }
        if let Some(tenant_id) = query.tenant_id {
            if self.tenant_id != Some(tenant_id) {
                return false;
            }
        }
        self.max_ts >= query.time_from && self.min_ts <= query.time_to
    }

    fn matches_filter(&self, filter: &SearchFilter) -> bool {
        if filter
            .trace_id
            .map_or(false, |trace_id| trace_id != self.trace_id)
        {
            return false;
        }
        if let Some(tenant_id) = filter.tenant_id {
            if self.tenant_id != Some(tenant_id) {
                return false;
            }
        }
        if let Some(external_trace_id) = &filter.external_trace_id {
            if self.external_trace_id.as_deref() != Some(external_trace_id.as_str()) {
                return false;
            }
        }
        if let Some(agent_name) = &filter.agent_name {
            if self.agent_name.as_deref() != Some(agent_name.as_str()) {
                return false;
            }
        }
        if let Some(tool_name) = &filter.tool_name {
            if self.tool_name.as_deref() != Some(tool_name.as_str()) {
                return false;
            }
        }
        if let Some(model) = &filter.model {
            if self.model.as_deref() != Some(model.as_str()) {
                return false;
            }
        }
        if let Some(status) = filter.status {
            if self.status != Some(status) {
                return false;
            }
        }
        if let Some(from) = filter.time_from {
            if self.max_ts < from {
                return false;
            }
        }
        if let Some(to) = filter.time_to {
            if self.min_ts > to {
                return false;
            }
        }
        for (key, expected) in &filter.attrs {
            if self.attrs.get(key) != Some(expected) {
                return false;
            }
        }
        true
    }

    fn to_folded_span(&self) -> FoldedSpan {
        FoldedSpan {
            trace_id: self.trace_id,
            span_id: self.span_id,
            has_start: self.has_start,
            has_end: self.has_end,
            parent_span_id: self.parent_span_id,
            status: self.status,
            duration_ns: self.duration_ns,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_read_tokens: self.cache_read_tokens,
            cache_write_tokens: self.cache_write_tokens,
            session_id: self.session_id,
            tenant_id: self.tenant_id,
            external_trace_id: self.external_trace_id.clone(),
            external_span_id: self.external_span_id.clone(),
            external_parent_span_id: self.external_parent_span_id.clone(),
            external_session_id: self.external_session_id.clone(),
            span_name: self.span_name.clone(),
            display_name: self.display_name.clone(),
            agent_name: self.agent_name.clone(),
            tool_name: self.tool_name.clone(),
            model: self.model.clone(),
            input_text: None,
            output_text: None,
            eval_score: None,
            eval_label: None,
            logs: Vec::new(),
            attrs: self.attrs.clone(),
            event_count: self.event_count,
        }
    }

    fn encode(&self, out: &mut Vec<u8>, version: u32) {
        put_u64(out, self.trace_id);
        put_u64(out, self.span_id);
        put_opt_u64(out, self.parent_span_id);
        put_opt_u64(out, self.session_id);
        put_opt_u64(out, self.tenant_id);
        put_opt_string(out, self.external_trace_id.as_deref());
        put_opt_string(out, self.external_span_id.as_deref());
        put_opt_string(out, self.external_parent_span_id.as_deref());
        put_opt_string(out, self.external_session_id.as_deref());
        put_opt_u8(out, self.status);
        put_opt_u64(out, self.duration_ns);
        put_opt_u64(out, self.input_tokens);
        put_opt_u64(out, self.output_tokens);
        put_opt_string(out, self.agent_name.as_deref());
        put_opt_string(out, self.tool_name.as_deref());
        put_opt_string(out, self.model.as_deref());
        put_i64(out, self.min_ts);
        put_i64(out, self.max_ts);
        put_u64(out, self.event_count as u64);
        put_u64(out, self.attrs.len() as u64);
        for (key, value) in &self.attrs {
            put_string(out, key);
            put_string(out, value);
        }
        if version >= 3 {
            // v3 只在行尾追加字段，v2 页仍可用原布局读取。
            put_opt_string(out, self.span_name.as_deref());
            put_opt_string(out, self.display_name.as_deref());
        }
        if version >= 4 {
            put_opt_u64(out, self.cache_read_tokens);
            put_opt_u64(out, self.cache_write_tokens);
        }
        if version >= 6 {
            let order = self.event_order.encode();
            put_u64(out, order.len() as u64);
            out.extend_from_slice(&order);
        }
        if version >= 5 {
            out.push(u8::from(self.has_start));
            out.push(u8::from(self.has_end));
        }
    }

    fn decode(cur: &mut CacheCursor<'_>, version: u32) -> Option<Self> {
        let trace_id = cur.u64()?;
        let span_id = cur.u64()?;
        let parent_span_id = cur.opt_u64()?;
        let session_id = cur.opt_u64()?;
        let tenant_id = cur.opt_u64()?;
        let external_trace_id = cur.opt_string()?;
        let external_span_id = cur.opt_string()?;
        let external_parent_span_id = cur.opt_string()?;
        let external_session_id = cur.opt_string()?;
        let status = cur.opt_u8()?;
        let duration_ns = cur.opt_u64()?;
        let input_tokens = cur.opt_u64()?;
        let output_tokens = cur.opt_u64()?;
        let agent_name = cur.opt_string()?;
        let tool_name = cur.opt_string()?;
        let model = cur.opt_string()?;
        let min_ts = cur.i64()?;
        let max_ts = cur.i64()?;
        let event_count = cur.u64()? as usize;
        let attr_count = cur.u64()? as usize;
        let mut attrs = BTreeMap::new();
        for _ in 0..attr_count {
            attrs.insert(cur.string()?, cur.string()?);
        }
        let (span_name, display_name) = if version >= 3 {
            (cur.opt_string()?, cur.opt_string()?)
        } else {
            (None, None)
        };
        let (cache_read_tokens, cache_write_tokens) = if version >= 4 {
            (cur.opt_u64()?, cur.opt_u64()?)
        } else {
            (None, None)
        };
        let event_order = if version >= 6 {
            let len = usize::try_from(cur.u64()?).ok()?;
            crate::SpanEventOrder::decode(cur.take(len)?)?
        } else {
            crate::SpanEventOrder::default()
        };
        let (has_start, has_end) = if version >= 5 {
            (cur.u8()? != 0, cur.u8()? != 0)
        } else {
            // 旧 rollup 没有保存事件类型。旧记录有耗时时按已结束兼容；
            // 否则保守当作未结束，避免继续误报成 0ms 成功。
            (true, duration_ns.is_some())
        };
        Some(Self {
            event_order,
            trace_id,
            span_id,
            has_start,
            has_end,
            parent_span_id,
            session_id,
            tenant_id,
            external_trace_id,
            external_span_id,
            external_parent_span_id,
            external_session_id,
            span_name,
            display_name,
            status,
            duration_ns,
            input_tokens,
            output_tokens,
            cache_read_tokens,
            cache_write_tokens,
            agent_name,
            tool_name,
            model,
            attrs,
            min_ts,
            max_ts,
            event_count,
        })
    }
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_opt_u8(out: &mut Vec<u8>, value: Option<u8>) {
    match value {
        Some(value) => {
            out.push(1);
            out.push(value);
        }
        None => out.push(0),
    }
}

fn put_opt_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            out.push(1);
            put_u64(out, value);
        }
        None => out.push(0),
    }
}

fn put_string(out: &mut Vec<u8>, value: &str) {
    put_u64(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

fn put_opt_string(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            out.push(1);
            put_string(out, value);
        }
        None => out.push(0),
    }
}

struct CacheCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> CacheCursor<'a> {
    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(len)?;
        let bytes = self.bytes.get(self.pos..end)?;
        self.pos = end;
        Some(bytes)
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn i64(&mut self) -> Option<i64> {
        Some(i64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn u8(&mut self) -> Option<u8> {
        let value = *self.bytes.get(self.pos)?;
        self.pos += 1;
        Some(value)
    }

    fn opt_u8(&mut self) -> Option<Option<u8>> {
        match self.u8()? {
            0 => Some(None),
            1 => Some(Some(self.u8()?)),
            _ => None,
        }
    }

    fn opt_u64(&mut self) -> Option<Option<u64>> {
        match self.u8()? {
            0 => Some(None),
            1 => Some(Some(self.u64()?)),
            _ => None,
        }
    }

    fn string(&mut self) -> Option<String> {
        let len = self.u64()? as usize;
        String::from_utf8(self.take(len)?.to_vec()).ok()
    }

    fn opt_string(&mut self) -> Option<Option<String>> {
        match self.u8()? {
            0 => Some(None),
            1 => Some(Some(self.string()?)),
            _ => None,
        }
    }
}
