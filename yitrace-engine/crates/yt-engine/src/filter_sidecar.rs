use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::Path;

use crate::filter_disk::{self, DiskFilterCache, PostingLookup as DiskPostingLookup};
use crate::filter_external_sort::PostingRunBuilder;
use crate::{is_filter_attr_key, FilterAttrs, SearchFilter};
use yt_core::fold::SpanFields;
use yt_wal::WalRecord;

type SpanKey = crate::ScopedSpanKey;

const DEFAULT_POSTING_ENTRY_BUDGET: usize = 2_000_000;
const DEFAULT_POSTING_SET_BUDGET: usize = 200_000;

pub(crate) struct FilterAttrsIndex {
    rows: HashMap<SpanKey, FilterAttrs>,
    postings: HashMap<PostingKey, HashSet<SpanKey>>,
    disabled_postings: HashSet<PostingKey>,
    posting_entries: usize,
    posting_entry_budget: usize,
    posting_set_budget: usize,
    disk: Option<DiskFilterCache>,
}

impl Default for FilterAttrsIndex {
    fn default() -> Self {
        Self::with_posting_budget(DEFAULT_POSTING_ENTRY_BUDGET, DEFAULT_POSTING_SET_BUDGET)
    }
}

#[derive(Clone, Debug, Eq)]
struct PostingKey {
    field: String,
    value: String,
}

impl PostingKey {
    fn new(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            value: value.into(),
        }
    }
}

impl PartialEq for PostingKey {
    fn eq(&self, other: &Self) -> bool {
        self.field == other.field && self.value == other.value
    }
}

impl Hash for PostingKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.field.hash(state);
        self.value.hash(state);
    }
}

enum PostingLookup<'a> {
    Resident(&'a HashSet<SpanKey>),
    Disabled,
    Missing,
}

impl FilterAttrsIndex {
    pub(crate) fn with_posting_budget(
        posting_entry_budget: usize,
        posting_set_budget: usize,
    ) -> Self {
        Self {
            rows: HashMap::new(),
            postings: HashMap::new(),
            disabled_postings: HashSet::new(),
            posting_entries: 0,
            posting_entry_budget,
            posting_set_budget,
            disk: None,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.disk
            .as_ref()
            .map_or(self.rows.len(), DiskFilterCache::row_count)
    }

    pub(crate) fn posting_count(&self) -> usize {
        self.disk
            .as_ref()
            .map_or(self.posting_entries, DiskFilterCache::posting_count)
    }

    pub(crate) fn disabled_posting_count(&self) -> usize {
        self.disk
            .as_ref()
            .map_or(self.disabled_postings.len(), |disk| {
                disk.disabled_posting_count()
            })
    }

    pub(crate) fn apply_record(&mut self, record: &WalRecord) {
        self.materialize_disk();
        let key = (record.fields.tenant_id, record.trace_id, record.span_id);
        if let Some(old) = self.rows.get(&key).cloned() {
            self.remove_postings(key, &old);
        }
        let mut row = self.rows.remove(&key).unwrap_or_else(|| FilterAttrs {
            min_ts: record.ts,
            max_ts: record.ts,
            ..Default::default()
        });
        row.apply_record(record);
        self.add_postings(key, &row);
        self.rows.insert(key, row);
    }

    pub(crate) fn from_records(
        records: impl IntoIterator<Item = WalRecord>,
        patches: impl IntoIterator<Item = (SpanKey, SpanFields)>,
    ) -> Self {
        let mut next = Self::default();
        for record in records {
            next.apply_record(&record);
        }
        for (key, fields) in patches {
            next.apply_fields(key, &fields);
        }
        next
    }

    pub(crate) fn rebuild(
        &mut self,
        records: impl IntoIterator<Item = WalRecord>,
        patches: impl IntoIterator<Item = (SpanKey, SpanFields)>,
    ) {
        *self = Self::from_records(records, patches);
    }

    pub(crate) fn candidate_span_keys(&mut self, filter: &SearchFilter) -> HashSet<SpanKey> {
        if self.disk.is_some() {
            return self.disk_candidate_span_keys(filter);
        }
        let mut sets: Vec<&HashSet<SpanKey>> = Vec::new();
        if let Some(trace_id) = filter.trace_id {
            match self.posting_lookup(&PostingKey::new("trace_id", trace_id.to_string())) {
                PostingLookup::Resident(set) => sets.push(set),
                PostingLookup::Disabled => {}
                PostingLookup::Missing => return HashSet::new(),
            }
        }
        if let Some(external_trace_id) = &filter.external_trace_id {
            match self.posting_lookup(&PostingKey::new(
                "external_trace_id",
                external_trace_id.as_str(),
            )) {
                PostingLookup::Resident(set) => sets.push(set),
                PostingLookup::Disabled => {}
                PostingLookup::Missing => return HashSet::new(),
            }
        }
        if let Some(tenant_id) = filter.tenant_id {
            match self.posting_lookup(&PostingKey::new("tenant_id", tenant_id.to_string())) {
                PostingLookup::Resident(set) => sets.push(set),
                PostingLookup::Disabled => {}
                PostingLookup::Missing => return HashSet::new(),
            }
        }
        if let Some(status) = filter.status {
            match self.posting_lookup(&PostingKey::new("status", status.to_string())) {
                PostingLookup::Resident(set) => sets.push(set),
                PostingLookup::Disabled => {}
                PostingLookup::Missing => return HashSet::new(),
            }
        }
        for (field, value) in [
            ("agent_name", filter.agent_name.as_deref()),
            ("tool_name", filter.tool_name.as_deref()),
            ("model", filter.model.as_deref()),
        ] {
            if let Some(value) = value {
                match self.posting_lookup(&PostingKey::new(field, value)) {
                    PostingLookup::Resident(set) => sets.push(set),
                    PostingLookup::Disabled => {}
                    PostingLookup::Missing => return HashSet::new(),
                }
            }
        }
        for (key, value) in &filter.attrs {
            match self.posting_lookup(&PostingKey::new(attr_field(key), value)) {
                PostingLookup::Resident(set) => sets.push(set),
                PostingLookup::Disabled => {}
                PostingLookup::Missing => return HashSet::new(),
            }
        }

        let mut out = if let Some((first, rest)) = smallest_first(&sets) {
            let mut out = first.clone();
            for set in rest {
                out.retain(|key| set.contains(key));
            }
            out
        } else {
            self.rows.keys().copied().collect()
        };
        out.retain(|key| {
            if let Some(trace_id) = filter.trace_id {
                if key.1 != trace_id {
                    return false;
                }
            }
            self.rows
                .get(key)
                .map(|attrs| filter.attrs_match(attrs))
                .unwrap_or(false)
        });
        out
    }

    /// BM25 已经给出少量候选时，逐条做最终过滤比复制一个低选择性的百万 key postings 更省。
    /// 磁盘态只读取这一条 attrs row；内存态直接查 HashMap，语义与候选集合路径一致。
    pub(crate) fn span_matches(&mut self, key: SpanKey, filter: &SearchFilter) -> bool {
        if filter.trace_id.is_some_and(|trace_id| trace_id != key.1) {
            return false;
        }
        if let Some(disk) = self.disk.as_mut() {
            let Some(bytes) = disk.load_row(key) else {
                return false;
            };
            let mut cur = CacheCursor {
                bytes: &bytes,
                pos: 0,
            };
            let Some(row) = FilterAttrs::decode(&mut cur) else {
                return false;
            };
            return cur.pos == bytes.len() && filter.attrs_match(&row);
        }
        self.rows
            .get(&key)
            .is_some_and(|row| filter.attrs_match(row))
    }

    pub(crate) fn filter_matches_all(&self, filter: &SearchFilter) -> bool {
        let Some(tenant_id) = filter.tenant_id else {
            return false;
        };
        if filter.has_non_tenant_constraints() {
            return false;
        }
        if let Some(disk) = self.disk.as_ref() {
            return disk.posting_cardinality("tenant_id", &tenant_id.to_string())
                == Some(disk.row_count());
        }
        self.postings
            .get(&PostingKey::new("tenant_id", tenant_id.to_string()))
            .is_some_and(|keys| keys.len() == self.rows.len())
    }

    pub(crate) fn candidate_count_hint(&self, filter: &SearchFilter) -> Option<usize> {
        let postings = requested_postings(filter);
        if postings.is_empty() {
            return None;
        }
        if let Some(disk) = self.disk.as_ref() {
            return postings
                .iter()
                .filter_map(|posting| disk.posting_cardinality(&posting.field, &posting.value))
                .min();
        }
        postings
            .iter()
            .filter_map(|posting| self.postings.get(posting).map(HashSet::len))
            .min()
    }

    /// 估算构造过滤集合需要处理多少个 key。与 `candidate_count_hint` 的“最小结果集”不同，
    /// 这里累加所有非全量 postings，防止小 project + 超大 tenant 低估查询期内存。
    pub(crate) fn candidate_materialization_key_hint(
        &self,
        filter: &SearchFilter,
    ) -> Option<usize> {
        let postings = requested_postings(filter);
        if postings.is_empty() {
            return None;
        }
        if let Some(disk) = self.disk.as_ref() {
            let row_count = disk.row_count();
            let mut total = 0usize;
            for posting in postings {
                let Some(count) = disk.posting_cardinality(&posting.field, &posting.value) else {
                    return Some(usize::MAX);
                };
                if count != row_count {
                    total = total.saturating_add(count);
                }
            }
            return Some(if total == 0 { row_count } else { total });
        }

        let row_count = self.rows.len();
        let mut total = 0usize;
        for posting in postings {
            match self.posting_lookup(&posting) {
                PostingLookup::Resident(keys) if keys.len() == row_count => {}
                PostingLookup::Resident(keys) => total = total.saturating_add(keys.len()),
                PostingLookup::Disabled | PostingLookup::Missing => return Some(usize::MAX),
            }
        }
        Some(if total == 0 { row_count } else { total })
    }

    pub(crate) fn matches_key(&self, trace_id: u64, span_id: u64, filter: &SearchFilter) -> bool {
        self.rows
            .get(&(filter.tenant_id, trace_id, span_id))
            .map(|attrs| filter.attrs_match(attrs))
            .unwrap_or(false)
    }

    pub(crate) fn save_cache(
        &self,
        path: &Path,
        manifest_version: u64,
        memtable_watermark: u64,
    ) -> std::io::Result<()> {
        if let Some(disk) = self.disk.as_ref() {
            return disk.persist_with_metadata(path, manifest_version, memtable_watermark);
        }
        // 内存 postings 受预算保护，宽字段可能被禁用。落盘走固定内存 run + 多路归并，
        // 生成完整 postings，避免百万级 tenant/project 查询退回全行扫描或 flush OOM。
        let mut posting_runs = PostingRunBuilder::new(path);
        for (&key, row) in &self.rows {
            for posting in posting_keys_for_row(key, row) {
                posting_runs.push(posting.field, posting.value, key)?;
            }
        }
        let postings = posting_runs.finish()?;

        let mut rows: Vec<_> = self.rows.iter().collect();
        rows.sort_unstable_by_key(|(&key, _)| key);
        let rows = rows.into_iter().map(|(&key, row)| {
            let mut bytes = Vec::new();
            row.encode(&mut bytes);
            (key, bytes)
        });
        filter_disk::write_atomic(path, manifest_version, memtable_watermark, rows, postings)
    }

    pub(crate) fn load_cache(
        path: &Path,
        manifest_version: u64,
        memtable_watermark: u64,
    ) -> Option<Self> {
        let disk = DiskFilterCache::open(path, manifest_version, memtable_watermark)?;
        Some(Self {
            disk: Some(disk),
            ..Self::default()
        })
    }

    fn apply_fields(&mut self, key: SpanKey, fields: &SpanFields) {
        self.materialize_disk();
        let Some(mut row) = self.rows.remove(&key) else {
            return;
        };
        self.remove_postings(key, &row);
        row.apply_fields(fields);
        self.add_postings(key, &row);
        self.rows.insert(key, row);
    }

    fn add_postings(&mut self, key: SpanKey, row: &FilterAttrs) {
        for posting in posting_keys_for_row(key, row) {
            self.add_posting(posting, key);
        }
    }

    fn remove_postings(&mut self, key: SpanKey, row: &FilterAttrs) {
        for posting in posting_keys_for_row(key, row) {
            self.remove_posting(&posting, key);
        }
    }

    fn add_posting(&mut self, posting: PostingKey, key: SpanKey) {
        if self.disabled_postings.contains(&posting) {
            return;
        }
        let already_present = self
            .postings
            .get(&posting)
            .map_or(false, |keys| keys.contains(&key));
        if already_present {
            return;
        }
        let would_exceed_set = self
            .postings
            .get(&posting)
            .map_or(false, |keys| keys.len() >= self.posting_set_budget);
        if would_exceed_set || self.posting_entries >= self.posting_entry_budget {
            self.disable_posting(posting);
            return;
        }
        self.postings.entry(posting).or_default().insert(key);
        self.posting_entries = self.posting_entries.saturating_add(1);
    }

    fn remove_posting(&mut self, posting: &PostingKey, key: SpanKey) {
        if self.disabled_postings.contains(posting) {
            return;
        }
        let remove_posting = if let Some(keys) = self.postings.get_mut(posting) {
            let removed = keys.remove(&key);
            if removed {
                self.posting_entries = self.posting_entries.saturating_sub(1);
            }
            keys.is_empty()
        } else {
            false
        };
        if remove_posting {
            self.postings.remove(posting);
        }
    }

    fn disable_posting(&mut self, posting: PostingKey) {
        if let Some(keys) = self.postings.remove(&posting) {
            self.posting_entries = self.posting_entries.saturating_sub(keys.len());
        }
        self.disabled_postings.insert(posting);
    }

    fn posting_lookup(&self, posting: &PostingKey) -> PostingLookup<'_> {
        if let Some(keys) = self.postings.get(posting) {
            PostingLookup::Resident(keys)
        } else if self.disabled_postings.contains(posting) {
            PostingLookup::Disabled
        } else {
            PostingLookup::Missing
        }
    }

    fn disk_candidate_span_keys(&mut self, filter: &SearchFilter) -> HashSet<SpanKey> {
        let postings = requested_postings(filter);
        let disk = self.disk.as_mut().expect("disk filter cache");
        let mut sets = Vec::new();
        let mut needs_row_check = filter.time_from.is_some() || filter.time_to.is_some();
        for posting in postings {
            if disk.posting_cardinality(&posting.field, &posting.value) == Some(disk.row_count()) {
                continue;
            }
            match disk.lookup(&posting.field, &posting.value) {
                DiskPostingLookup::Resident(set) => sets.push(set),
                DiskPostingLookup::Disabled => needs_row_check = true,
                DiskPostingLookup::Missing => return HashSet::new(),
            }
        }

        sets.sort_unstable_by_key(|set| set.len());
        let mut out = if let Some(first) = sets.first() {
            let mut out = (**first).clone();
            for set in sets.iter().skip(1) {
                out.retain(|key| set.contains(key));
            }
            out
        } else {
            disk.row_keys().collect()
        };

        if let Some(trace_id) = filter.trace_id {
            out.retain(|key| key.1 == trace_id);
        }
        if needs_row_check {
            out.retain(|&key| {
                let Some(bytes) = disk.load_row(key) else {
                    return false;
                };
                let mut cur = CacheCursor {
                    bytes: &bytes,
                    pos: 0,
                };
                let Some(row) = FilterAttrs::decode(&mut cur) else {
                    return false;
                };
                cur.pos == bytes.len() && filter.attrs_match(&row)
            });
        }
        out
    }

    fn materialize_disk(&mut self) {
        let Some(mut disk) = self.disk.take() else {
            return;
        };
        let keys: Vec<_> = disk.row_keys().collect();
        self.rows.reserve(keys.len());
        for key in keys {
            let bytes = disk
                .load_row(key)
                .expect("filter cache row referenced by directory must be readable");
            let mut cur = CacheCursor {
                bytes: &bytes,
                pos: 0,
            };
            let row = FilterAttrs::decode(&mut cur)
                .expect("filter cache row referenced by directory must decode");
            assert_eq!(cur.pos, bytes.len(), "filter cache row has trailing bytes");
            self.rows.insert(key, row);
        }
        self.rebuild_postings();
    }

    fn rebuild_postings(&mut self) {
        self.postings.clear();
        self.disabled_postings.clear();
        self.posting_entries = 0;
        let rows = std::mem::take(&mut self.rows);
        for (&key, row) in &rows {
            self.add_postings(key, row);
        }
        self.rows = rows;
    }
}

fn requested_postings(filter: &SearchFilter) -> Vec<PostingKey> {
    let mut out = Vec::new();
    if let Some(trace_id) = filter.trace_id {
        out.push(PostingKey::new("trace_id", trace_id.to_string()));
    }
    if let Some(value) = &filter.external_trace_id {
        out.push(PostingKey::new("external_trace_id", value));
    }
    if let Some(tenant_id) = filter.tenant_id {
        out.push(PostingKey::new("tenant_id", tenant_id.to_string()));
    }
    if let Some(status) = filter.status {
        out.push(PostingKey::new("status", status.to_string()));
    }
    for (field, value) in [
        ("agent_name", filter.agent_name.as_deref()),
        ("tool_name", filter.tool_name.as_deref()),
        ("model", filter.model.as_deref()),
    ] {
        if let Some(value) = value {
            out.push(PostingKey::new(field, value));
        }
    }
    out.extend(
        filter
            .attrs
            .iter()
            .map(|(key, value)| PostingKey::new(attr_field(key), value)),
    );
    out
}

fn posting_keys_for_row(key: SpanKey, row: &FilterAttrs) -> Vec<PostingKey> {
    let mut out = vec![PostingKey::new("trace_id", key.1.to_string())];
    if let Some(value) = &row.external_trace_id {
        out.push(PostingKey::new("external_trace_id", value));
    }
    if let Some(value) = row.tenant_id {
        out.push(PostingKey::new("tenant_id", value.to_string()));
    }
    if let Some(value) = row.status {
        out.push(PostingKey::new("status", value.to_string()));
    }
    for (field, value) in [
        ("agent_name", row.agent_name.as_deref()),
        ("tool_name", row.tool_name.as_deref()),
        ("model", row.model.as_deref()),
    ] {
        if let Some(value) = value {
            out.push(PostingKey::new(field, value));
        }
    }
    out.extend(
        row.attrs
            .iter()
            .map(|(key, value)| PostingKey::new(attr_field(key), value)),
    );
    out
}

impl FilterAttrs {
    fn apply_record(&mut self, record: &WalRecord) {
        if !self.event_order.accept(record.identity.event_id().0) {
            return;
        }
        self.min_ts = self.min_ts.min(record.ts);
        self.max_ts = self.max_ts.max(record.ts);
        self.apply_fields_at(&record.fields, record.identity.seq);
    }

    fn apply_fields(&mut self, fields: &SpanFields) {
        self.apply_fields_at(fields, u64::MAX);
    }
    fn apply_fields_at(&mut self, fields: &SpanFields, seq: u64) {
        if fields.external_trace_id.is_some() && self.event_order.field("external_trace_id", seq) {
            self.external_trace_id = fields.external_trace_id.clone();
        }
        if fields.status.is_some() && self.event_order.field("status", seq) {
            self.status = fields.status;
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
        if fields.tenant_id.is_some() && self.event_order.field("tenant_id", seq) {
            self.tenant_id = fields.tenant_id;
        }
        for (key, value) in &fields.attrs {
            if is_filter_attr_key(key) && self.event_order.field(&format!("attrs:{key}"), seq) {
                self.attrs.insert(key.clone(), value.clone());
            }
        }
    }

    fn encode(&self, out: &mut Vec<u8>) {
        let order = self.event_order.encode();
        put_u64(out, order.len() as u64);
        out.extend_from_slice(&order);
        put_opt_string(out, self.external_trace_id.as_deref());
        put_opt_u8(out, self.status);
        put_opt_string(out, self.agent_name.as_deref());
        put_opt_string(out, self.tool_name.as_deref());
        put_opt_string(out, self.model.as_deref());
        put_i64(out, self.min_ts);
        put_i64(out, self.max_ts);
        put_opt_u64(out, self.tenant_id);
        put_u64(out, self.attrs.len() as u64);
        for (key, value) in &self.attrs {
            put_string(out, key);
            put_string(out, value);
        }
    }

    fn decode(cur: &mut CacheCursor<'_>) -> Option<Self> {
        let len = usize::try_from(cur.u64()?).ok()?;
        let event_order = crate::SpanEventOrder::decode(cur.take(len)?)?;
        let external_trace_id = cur.opt_string()?;
        let status = cur.opt_u8()?;
        let agent_name = cur.opt_string()?;
        let tool_name = cur.opt_string()?;
        let model = cur.opt_string()?;
        let min_ts = cur.i64()?;
        let max_ts = cur.i64()?;
        let tenant_id = cur.opt_u64()?;
        let attr_count = cur.u64()? as usize;
        let mut attrs = BTreeMap::new();
        for _ in 0..attr_count {
            attrs.insert(cur.string()?, cur.string()?);
        }
        Some(Self {
            event_order,
            external_trace_id,
            status,
            agent_name,
            tool_name,
            model,
            attrs,
            min_ts,
            max_ts,
            tenant_id,
        })
    }
}

fn smallest_first<'a>(
    sets: &[&'a HashSet<SpanKey>],
) -> Option<(&'a HashSet<SpanKey>, Vec<&'a HashSet<SpanKey>>)> {
    let (idx, first) = sets.iter().enumerate().min_by_key(|(_, set)| set.len())?;
    let rest = sets
        .iter()
        .enumerate()
        .filter_map(|(i, set)| (i != idx).then_some(*set))
        .collect();
    Some((*first, rest))
}

fn attr_field(key: &str) -> String {
    format!("attr:{key}")
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_opt_u8(out: &mut Vec<u8>, value: Option<u8>) {
    out.push(value.is_some() as u8);
    if let Some(value) = value {
        out.push(value);
    }
}

fn put_opt_u64(out: &mut Vec<u8>, value: Option<u64>) {
    out.push(value.is_some() as u8);
    if let Some(value) = value {
        put_u64(out, value);
    }
}

fn put_string(out: &mut Vec<u8>, value: &str) {
    put_u64(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

fn put_opt_string(out: &mut Vec<u8>, value: Option<&str>) {
    out.push(value.is_some() as u8);
    if let Some(value) = value {
        put_string(out, value);
    }
}

struct CacheCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl CacheCursor<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        let end = self.pos.checked_add(n)?;
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

    fn opt_u8(&mut self) -> Option<Option<u8>> {
        match *self.take(1)?.first()? {
            0 => Some(None),
            1 => Some(Some(*self.take(1)?.first()?)),
            _ => None,
        }
    }

    fn opt_u64(&mut self) -> Option<Option<u64>> {
        match *self.take(1)?.first()? {
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
        match *self.take(1)?.first()? {
            0 => Some(None),
            1 => Some(Some(self.string()?)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yt_core::event::{EventIdentity, EventType};

    fn record(trace_id: u64, span_id: u64, project_id: &str, skill: &str) -> WalRecord {
        WalRecord {
            trace_id,
            span_id,
            ts: span_id as i64,
            identity: EventIdentity {
                ext_span_id: format!("{trace_id}-{span_id}"),
                seq: span_id,
                event_type: EventType::Attr,
            },
            fields: SpanFields {
                attrs: BTreeMap::from([
                    ("project_id".to_string(), format!("\"{project_id}\"")),
                    ("skill".to_string(), format!("\"{skill}\"")),
                ]),
                ..Default::default()
            },
        }
    }

    fn attr_filter(pairs: &[(&str, &str)]) -> SearchFilter {
        SearchFilter {
            attrs: pairs
                .iter()
                .map(|(key, value)| ((*key).to_string(), format!("\"{value}\"")))
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn wide_posting_is_disabled_but_results_stay_correct() {
        let mut index = FilterAttrsIndex::with_posting_budget(64, 2);
        index.apply_record(&record(1, 1, "wide", "a"));
        index.apply_record(&record(2, 1, "wide", "b"));
        index.apply_record(&record(3, 1, "wide", "c"));

        assert_eq!(index.len(), 3);
        assert_eq!(index.disabled_posting_count(), 1);

        let keys = index.candidate_span_keys(&attr_filter(&[("project_id", "wide")]));
        assert_eq!(
            keys,
            HashSet::from([(None, 1, 1), (None, 2, 1), (None, 3, 1)])
        );

        let keys =
            index.candidate_span_keys(&attr_filter(&[("project_id", "wide"), ("skill", "b")]));
        assert_eq!(keys, HashSet::from([(None, 2, 1)]));
    }

    #[test]
    fn total_posting_budget_falls_back_to_row_sidecar() {
        let mut index = FilterAttrsIndex::with_posting_budget(1, 64);
        index.apply_record(&record(10, 7, "budgeted", "review"));

        assert_eq!(index.len(), 1);
        assert_eq!(index.posting_count(), 1);
        assert!(index.disabled_posting_count() >= 1);

        let keys = index.candidate_span_keys(&attr_filter(&[("project_id", "budgeted")]));
        assert_eq!(keys, HashSet::from([(None, 10, 7)]));
    }

    #[test]
    fn external_trace_id_posting_supports_fast_positive_and_negative_lookup() {
        let mut index = FilterAttrsIndex::with_posting_budget(64, 64);
        let mut first = record(11, 1, "p", "a");
        first.fields.external_trace_id = Some("run-a".to_string());
        let mut second = record(12, 1, "p", "b");
        second.fields.external_trace_id = Some("run-b".to_string());
        index.apply_record(&first);
        index.apply_record(&second);

        let hit = index.candidate_span_keys(&SearchFilter {
            external_trace_id: Some("run-a".to_string()),
            ..Default::default()
        });
        assert_eq!(hit, HashSet::from([(None, 11, 1)]));

        let miss = index.candidate_span_keys(&SearchFilter {
            external_trace_id: Some("run-missing".to_string()),
            ..Default::default()
        });
        assert!(miss.is_empty(), "不存在的外部 trace id 应直接空候选");
    }

    #[test]
    fn disabled_trace_id_posting_still_filters_by_trace_id() {
        let mut index = FilterAttrsIndex::with_posting_budget(0, 64);
        index.apply_record(&record(11, 1, "p", "a"));
        index.apply_record(&record(12, 1, "p", "b"));

        let keys = index.candidate_span_keys(&SearchFilter {
            trace_id: Some(11),
            ..Default::default()
        });
        assert_eq!(keys, HashSet::from([(None, 11, 1)]));
        assert_eq!(
            index.candidate_materialization_key_hint(&SearchFilter {
                trace_id: Some(11),
                ..Default::default()
            }),
            Some(usize::MAX),
            "disabled postings must force the bounded fallback"
        );
    }

    #[test]
    fn disk_cache_pages_exact_postings_and_materializes_on_first_write() {
        let dir = std::env::temp_dir().join(format!(
            "yt_filter_disk_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("filter_attrs.dat");

        let mut index = FilterAttrsIndex::with_posting_budget(1, 1);
        for mut row in [
            record(1, 10, "alpha", "review"),
            record(2, 20, "alpha", "write"),
            record(3, 30, "beta", "review"),
        ] {
            row.fields.tenant_id = Some(7);
            index.apply_record(&row);
        }
        assert!(index.disabled_posting_count() > 0);
        index.save_cache(&path, 7, 3).unwrap();

        let mut loaded = FilterAttrsIndex::load_cache(&path, 7, 3).unwrap();
        assert!(loaded.rows.is_empty(), "open must not materialize all rows");
        assert!(
            loaded.postings.is_empty(),
            "open must not materialize postings"
        );
        assert_eq!(loaded.len(), 3);
        assert!(loaded.filter_matches_all(&SearchFilter {
            tenant_id: Some(7),
            ..SearchFilter::default()
        }));
        assert!(!loaded.filter_matches_all(&SearchFilter {
            tenant_id: Some(8),
            ..SearchFilter::default()
        }));
        assert_eq!(
            loaded.candidate_count_hint(&SearchFilter {
                tenant_id: Some(7),
                ..attr_filter(&[("project_id", "alpha")])
            }),
            Some(2)
        );
        assert_eq!(
            loaded.candidate_materialization_key_hint(&SearchFilter {
                tenant_id: Some(7),
                ..attr_filter(&[("project_id", "alpha")])
            }),
            Some(2),
            "covering tenant postings should not count against the query budget"
        );
        assert_eq!(
            loaded.candidate_materialization_key_hint(&attr_filter(&[
                ("project_id", "alpha"),
                ("skill", "review")
            ])),
            Some(4),
            "all non-covering postings must count toward the query budget"
        );
        assert_eq!(
            loaded.candidate_span_keys(&attr_filter(&[
                ("project_id", "alpha"),
                ("skill", "review")
            ])),
            HashSet::from([(Some(7), 1, 10)])
        );
        assert!(
            loaded.rows.is_empty(),
            "exact lookup should stay disk-backed"
        );
        assert!(loaded.span_matches(
            (Some(7), 1, 10),
            &attr_filter(&[("project_id", "alpha"), ("skill", "review")])
        ));
        assert!(!loaded.span_matches(
            (Some(7), 2, 20),
            &attr_filter(&[("project_id", "alpha"), ("skill", "review")])
        ));

        let time_filtered = SearchFilter {
            time_from: Some(15),
            time_to: Some(25),
            ..attr_filter(&[("project_id", "alpha")])
        };
        assert_eq!(
            loaded.candidate_span_keys(&time_filtered),
            HashSet::from([(Some(7), 2, 20)])
        );

        // compaction 只推进 manifest 时，不应重建整份侧车，但必须原子更新版本头。
        loaded.save_cache(&path, 8, 3).unwrap();
        assert!(FilterAttrsIndex::load_cache(&path, 8, 3).is_some());

        loaded.apply_record(&record(4, 40, "alpha", "review"));
        assert!(
            loaded.disk.is_none(),
            "first write materializes the immutable base"
        );
        assert_eq!(
            loaded.candidate_span_keys(&attr_filter(&[
                ("project_id", "alpha"),
                ("skill", "review")
            ])),
            HashSet::from([(Some(7), 1, 10), (None, 4, 40)])
        );
        assert!(FilterAttrsIndex::load_cache(&path, 9, 3).is_none());
        let _ = std::fs::remove_dir_all(dir);
    }
}
