//! trace rollup 的分页磁盘格式。
//!
//! 页按 tenant、project_id、trace_id 排序，目录常驻内存，行数据按查询命中的页读取。
//! 这样常见的租户/project 聚合和 trace 下钻不需要解码整份 `trace_rollup.dat`。

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use crate::{SearchFilter, TraceQuery};

use super::{CacheCursor, TraceAggregateRollupRow};

const MAGIC: u32 = 0x5954_524f; // "YTRO"
const VERSION: u32 = 6;
const MIN_READ_VERSION: u32 = 2;
const HEADER_LEN: u64 = 64;
const PAGE_ROWS: usize = 4096;
const DEFAULT_CACHE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
struct PageRef {
    tenant_id: Option<u64>,
    project_id: Option<String>,
    first_trace_id: u64,
    last_trace_id: u64,
    min_ts: i64,
    max_ts: i64,
    offset: u64,
    len: u64,
    row_count: u32,
}

pub(super) struct DiskTraceRollup {
    file: File,
    version: u32,
    row_count: usize,
    pages: Vec<PageRef>,
    cached: HashMap<usize, Arc<Vec<TraceAggregateRollupRow>>>,
    lru: VecDeque<usize>,
    cached_bytes: usize,
    cache_budget: usize,
    last_pages_read: usize,
}

impl DiskTraceRollup {
    pub(super) fn has_event_order(&self) -> bool {
        self.version >= 6
    }
    pub(super) fn open(
        path: &Path,
        manifest_version: u64,
        memtable_watermark: u64,
    ) -> Option<Self> {
        Self::open_with_budget(
            path,
            manifest_version,
            memtable_watermark,
            page_cache_budget(),
        )
    }

    fn open_with_budget(
        path: &Path,
        manifest_version: u64,
        memtable_watermark: u64,
        cache_budget: usize,
    ) -> Option<Self> {
        let mut file = File::open(path).ok()?;
        let file_len = file.metadata().ok()?.len();
        if file_len < HEADER_LEN {
            return None;
        }
        let mut header = [0u8; HEADER_LEN as usize];
        file.read_exact(&mut header).ok()?;
        let mut cur = CacheCursor {
            bytes: &header,
            pos: 0,
        };
        if cur.u32()? != MAGIC {
            return None;
        }
        let version = cur.u32()?;
        if !(MIN_READ_VERSION..=VERSION).contains(&version) {
            return None;
        }
        if cur.u64()? != manifest_version || cur.u64()? != memtable_watermark {
            return None;
        }
        let row_count = usize::try_from(cur.u64()?).ok()?;
        let page_count = usize::try_from(cur.u64()?).ok()?;
        let pages_offset = cur.u64()?;
        let directory_offset = cur.u64()?;
        let directory_len = cur.u64()?;
        if pages_offset != HEADER_LEN
            || directory_offset < pages_offset
            || directory_offset.checked_add(directory_len)? != file_len
        {
            return None;
        }

        let mut directory_bytes = vec![0u8; usize::try_from(directory_len).ok()?];
        file.seek(SeekFrom::Start(directory_offset)).ok()?;
        file.read_exact(&mut directory_bytes).ok()?;
        let mut dir = CacheCursor {
            bytes: &directory_bytes,
            pos: 0,
        };
        let mut pages = Vec::with_capacity(page_count);
        let mut counted_rows = 0usize;
        let mut previous_offset = pages_offset;
        for _ in 0..page_count {
            let tenant_id = decode_opt_u64(&mut dir)?;
            let project_id = dir.opt_string()?;
            let first_trace_id = dir.u64()?;
            let last_trace_id = dir.u64()?;
            let min_ts = dir.i64()?;
            let max_ts = dir.i64()?;
            let offset = dir.u64()?;
            let len = dir.u64()?;
            let row_count = dir.u32()?;
            let end = offset.checked_add(len)?;
            if row_count == 0
                || first_trace_id > last_trace_id
                || min_ts > max_ts
                || offset < previous_offset
                || end > directory_offset
            {
                return None;
            }
            previous_offset = end;
            counted_rows = counted_rows.checked_add(row_count as usize)?;
            pages.push(PageRef {
                tenant_id,
                project_id,
                first_trace_id,
                last_trace_id,
                min_ts,
                max_ts,
                offset,
                len,
                row_count,
            });
        }
        if dir.pos != directory_bytes.len() || counted_rows != row_count {
            return None;
        }

        Some(Self {
            file,
            version,
            row_count,
            pages,
            cached: HashMap::new(),
            lru: VecDeque::new(),
            cached_bytes: 0,
            cache_budget,
            last_pages_read: 0,
        })
    }

    pub(super) fn row_count(&self) -> usize {
        self.row_count
    }

    pub(super) fn matching_rows(
        &mut self,
        query: &TraceQuery,
        filter: &SearchFilter,
    ) -> Option<Vec<TraceAggregateRollupRow>> {
        let page_ids = self.matching_pages(query, filter)?;
        self.last_pages_read = page_ids.len();
        let mut out = Vec::new();
        for page_id in page_ids {
            let rows = self.load_page(page_id)?;
            out.extend(
                rows.iter()
                    .filter(|row| row.matches_query(query) && row.matches_filter(filter))
                    .cloned(),
            );
        }
        Some(out)
    }

    pub(super) fn rows_for_trace_ids(
        &mut self,
        trace_ids: &BTreeSet<u64>,
        tenant_id: Option<u64>,
    ) -> Option<Vec<TraceAggregateRollupRow>> {
        if trace_ids.is_empty() {
            return Some(Vec::new());
        }
        let mut out = Vec::new();
        let mut pages_read = 0usize;
        for page_id in 0..self.pages.len() {
            let page = &self.pages[page_id];
            if tenant_id.is_some() && page.tenant_id != tenant_id {
                continue;
            }
            if !trace_ids
                .range(page.first_trace_id..=page.last_trace_id)
                .next()
                .is_some()
            {
                continue;
            }
            pages_read += 1;
            let rows = self.load_page(page_id)?;
            out.extend(
                rows.iter()
                    .filter(|row| {
                        trace_ids.contains(&row.trace_id)
                            && tenant_id.is_none_or(|tenant| row.tenant_id == Some(tenant))
                    })
                    .cloned(),
            );
        }
        self.last_pages_read = pages_read;
        Some(out)
    }

    pub(super) fn find_row(
        &mut self,
        tenant: Option<u64>,
        trace_id: u64,
        span_id: u64,
    ) -> Option<TraceAggregateRollupRow> {
        for page_id in 0..self.pages.len() {
            let page = &self.pages[page_id];
            if tenant != page.tenant_id
                || trace_id < page.first_trace_id
                || trace_id > page.last_trace_id
            {
                continue;
            }
            let rows = self.load_page(page_id)?;
            if let Some(row) = rows.iter().find(|row| {
                row.tenant_id == tenant && row.trace_id == trace_id && row.span_id == span_id
            }) {
                return Some(row.clone());
            }
        }
        None
    }

    pub(super) fn all_rows(&mut self) -> Option<Vec<TraceAggregateRollupRow>> {
        let mut out = Vec::with_capacity(self.row_count);
        self.last_pages_read = self.pages.len();
        for page_id in 0..self.pages.len() {
            out.extend(self.load_page(page_id)?.iter().cloned());
        }
        Some(out)
    }

    pub(super) fn last_read_stats(&self) -> (usize, usize) {
        (self.last_pages_read, self.pages.len())
    }

    fn matching_pages(&self, query: &TraceQuery, filter: &SearchFilter) -> Option<Vec<usize>> {
        let tenant_id = merge_exact(query.tenant_id, filter.tenant_id)?;
        let trace_id = merge_exact(query.trace_id, filter.trace_id)?;
        let from = query.time_from.max(filter.time_from.unwrap_or(i64::MIN));
        let to = query.time_to.min(filter.time_to.unwrap_or(i64::MAX));
        if from > to {
            return Some(Vec::new());
        }
        let project_id = filter.attrs.get("project_id");
        Some(
            self.pages
                .iter()
                .enumerate()
                .filter(|(_, page)| {
                    tenant_id.is_none_or(|tenant| page.tenant_id == Some(tenant))
                        && project_id.is_none_or(|project| {
                            page.project_id.as_deref() == Some(project.as_str())
                        })
                        && trace_id.is_none_or(|trace| {
                            trace >= page.first_trace_id && trace <= page.last_trace_id
                        })
                        && page.max_ts >= from
                        && page.min_ts <= to
                })
                .map(|(index, _)| index)
                .collect(),
        )
    }

    fn load_page(&mut self, page_id: usize) -> Option<Arc<Vec<TraceAggregateRollupRow>>> {
        if let Some(rows) = self.cached.get(&page_id).cloned() {
            self.touch(page_id);
            return Some(rows);
        }
        let page = self.pages.get(page_id)?.clone();
        let mut bytes = vec![0u8; usize::try_from(page.len).ok()?];
        self.file.seek(SeekFrom::Start(page.offset)).ok()?;
        self.file.read_exact(&mut bytes).ok()?;
        let mut cur = CacheCursor {
            bytes: &bytes,
            pos: 0,
        };
        let mut rows = Vec::with_capacity(page.row_count as usize);
        for _ in 0..page.row_count {
            rows.push(TraceAggregateRollupRow::decode(&mut cur, self.version)?);
        }
        if cur.pos != bytes.len() {
            return None;
        }
        let rows = Arc::new(rows);
        let byte_len = bytes.len();
        if byte_len <= self.cache_budget {
            while self.cached_bytes.saturating_add(byte_len) > self.cache_budget {
                let Some(oldest) = self.lru.pop_front() else {
                    break;
                };
                if let Some(removed) = self.cached.remove(&oldest) {
                    self.cached_bytes = self
                        .cached_bytes
                        .saturating_sub(self.pages[oldest].len as usize);
                    drop(removed);
                }
            }
            self.cached.insert(page_id, Arc::clone(&rows));
            self.lru.push_back(page_id);
            self.cached_bytes = self.cached_bytes.saturating_add(byte_len);
        }
        Some(rows)
    }

    fn touch(&mut self, page_id: usize) {
        self.lru.retain(|cached| *cached != page_id);
        self.lru.push_back(page_id);
    }

    #[cfg(test)]
    pub(super) fn cached_page_count(&self) -> usize {
        self.cached.len()
    }
}

pub(super) fn write_atomic(
    path: &Path,
    manifest_version: u64,
    memtable_watermark: u64,
    rows: &mut [TraceAggregateRollupRow],
) -> std::io::Result<()> {
    write_atomic_version(path, manifest_version, memtable_watermark, rows, VERSION)
}

fn write_atomic_version(
    path: &Path,
    manifest_version: u64,
    memtable_watermark: u64,
    rows: &mut [TraceAggregateRollupRow],
    version: u32,
) -> std::io::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    std::fs::create_dir_all(parent)?;
    rows.sort_by(|a, b| partition_key(a).cmp(&partition_key(b)));

    let tmp = path.with_extension("tmp");
    let mut file = BufWriter::with_capacity(8 * 1024 * 1024, File::create(&tmp)?);
    file.write_all(&[0u8; HEADER_LEN as usize])?;
    let pages_offset = file.stream_position()?;
    let mut pages = Vec::new();
    let mut start = 0usize;
    while start < rows.len() {
        let tenant_id = rows[start].tenant_id;
        let project_id = rows[start].attrs.get("project_id").cloned();
        let mut end = start + 1;
        while end < rows.len()
            && end - start < PAGE_ROWS
            && rows[end].tenant_id == tenant_id
            && rows[end].attrs.get("project_id") == project_id.as_ref()
        {
            end += 1;
        }
        let offset = file.stream_position()?;
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        for row in &rows[start..end] {
            min_ts = min_ts.min(row.min_ts);
            max_ts = max_ts.max(row.max_ts);
            let mut encoded = Vec::new();
            row.encode(&mut encoded, version);
            file.write_all(&encoded)?;
        }
        let page_end = file.stream_position()?;
        pages.push(PageRef {
            tenant_id,
            project_id,
            first_trace_id: rows[start].trace_id,
            last_trace_id: rows[end - 1].trace_id,
            min_ts,
            max_ts,
            offset,
            len: page_end - offset,
            row_count: u32::try_from(end - start).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "rollup page is too large")
            })?,
        });
        start = end;
    }

    let directory_offset = file.stream_position()?;
    for page in &pages {
        encode_opt_u64(&mut file, page.tenant_id)?;
        encode_opt_string(&mut file, page.project_id.as_deref())?;
        file.write_all(&page.first_trace_id.to_le_bytes())?;
        file.write_all(&page.last_trace_id.to_le_bytes())?;
        file.write_all(&page.min_ts.to_le_bytes())?;
        file.write_all(&page.max_ts.to_le_bytes())?;
        file.write_all(&page.offset.to_le_bytes())?;
        file.write_all(&page.len.to_le_bytes())?;
        file.write_all(&page.row_count.to_le_bytes())?;
    }
    let end = file.stream_position()?;

    file.seek(SeekFrom::Start(0))?;
    file.write_all(&MAGIC.to_le_bytes())?;
    file.write_all(&version.to_le_bytes())?;
    file.write_all(&manifest_version.to_le_bytes())?;
    file.write_all(&memtable_watermark.to_le_bytes())?;
    file.write_all(&(rows.len() as u64).to_le_bytes())?;
    file.write_all(&(pages.len() as u64).to_le_bytes())?;
    file.write_all(&pages_offset.to_le_bytes())?;
    file.write_all(&directory_offset.to_le_bytes())?;
    file.write_all(&(end - directory_offset).to_le_bytes())?;
    file.flush()?;
    file.get_ref().sync_all()?;
    drop(file);
    crate::test_failpoints::before_sidecar_rename("trace_rollup", path);
    std::fs::rename(tmp, path)
}

fn partition_key(row: &TraceAggregateRollupRow) -> (Option<u64>, Option<&str>, u64, u64) {
    (
        row.tenant_id,
        row.attrs.get("project_id").map(String::as_str),
        row.trace_id,
        row.span_id,
    )
}

fn merge_exact<T: Copy + Eq>(left: Option<T>, right: Option<T>) -> Option<Option<T>> {
    match (left, right) {
        (Some(left), Some(right)) if left != right => None,
        (Some(value), _) | (_, Some(value)) => Some(Some(value)),
        (None, None) => Some(None),
    }
}

fn page_cache_budget() -> usize {
    std::env::var("YT_ROLLUP_PAGE_CACHE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_CACHE_BYTES)
}

fn encode_opt_u64(file: &mut impl Write, value: Option<u64>) -> std::io::Result<()> {
    match value {
        Some(value) => {
            file.write_all(&[1])?;
            file.write_all(&value.to_le_bytes())
        }
        None => file.write_all(&[0]),
    }
}

fn decode_opt_u64(cur: &mut CacheCursor<'_>) -> Option<Option<u64>> {
    match cur.u8()? {
        0 => Some(None),
        1 => Some(Some(cur.u64()?)),
        _ => None,
    }
}

fn encode_opt_string(file: &mut impl Write, value: Option<&str>) -> std::io::Result<()> {
    match value {
        Some(value) => {
            file.write_all(&[1])?;
            file.write_all(&(value.len() as u64).to_le_bytes())?;
            file.write_all(value.as_bytes())
        }
        None => file.write_all(&[0]),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn row(trace_id: u64, tenant_id: u64, project_id: &str) -> TraceAggregateRollupRow {
        let mut attrs = BTreeMap::new();
        attrs.insert("project_id".to_string(), project_id.to_string());
        TraceAggregateRollupRow {
            trace_id,
            span_id: 1,
            tenant_id: Some(tenant_id),
            attrs,
            min_ts: trace_id as i64,
            max_ts: trace_id as i64,
            event_count: 1,
            ..TraceAggregateRollupRow::empty_for_test()
        }
    }

    #[test]
    fn opens_directory_and_pages_only_matching_project() {
        let path = std::env::temp_dir().join(format!(
            "yt_rollup_page_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut rows = vec![row(1, 7, "a"), row(2, 7, "b"), row(3, 8, "a")];
        write_atomic(&path, 4, 9, &mut rows).unwrap();
        let mut disk = DiskTraceRollup::open_with_budget(&path, 4, 9, 1024).unwrap();
        assert_eq!(disk.row_count(), 3);
        assert_eq!(disk.cached_page_count(), 0);
        let mut filter = SearchFilter {
            tenant_id: Some(7),
            ..SearchFilter::default()
        };
        filter.attrs.insert("project_id".into(), "a".into());
        let matched = disk.matching_rows(&TraceQuery::all(), &filter).unwrap();
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].trace_id, 1);
        assert_eq!(disk.cached_page_count(), 1);
        assert_eq!(disk.last_read_stats(), (1, 3));
        assert!(DiskTraceRollup::open(&path, 5, 9).is_none());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn v3_persists_names_and_v2_remains_readable() {
        let base = std::env::temp_dir().join(format!(
            "yt_rollup_names_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let v3_path = base.with_extension("v3");
        let v2_path = base.with_extension("v2");
        let mut named = row(11, 7, "names");
        named.span_name = Some("risk.review".into());
        named.display_name = Some("风险审核".into());

        write_atomic(&v3_path, 4, 9, &mut [named.clone()]).unwrap();
        let mut v3 = DiskTraceRollup::open(&v3_path, 4, 9).unwrap();
        let v3_rows = v3.all_rows().unwrap();
        assert_eq!(v3_rows[0].span_name.as_deref(), Some("risk.review"));
        assert_eq!(v3_rows[0].display_name.as_deref(), Some("风险审核"));

        write_atomic_version(&v2_path, 4, 9, &mut [named], 2).unwrap();
        let mut v2 = DiskTraceRollup::open(&v2_path, 4, 9).unwrap();
        let v2_rows = v2.all_rows().unwrap();
        assert_eq!(v2_rows[0].span_name, None);
        assert_eq!(v2_rows[0].display_name, None);

        let _ = std::fs::remove_file(v3_path);
        let _ = std::fs::remove_file(v2_path);
    }

    #[test]
    fn v5_persists_span_lifecycle_and_v4_uses_safe_fallback() {
        let base = std::env::temp_dir().join(format!(
            "yt_rollup_lifecycle_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let v5_path = base.with_extension("v5");
        let v4_path = base.with_extension("v4");
        let mut running = row(12, 7, "lifecycle");
        running.has_start = true;
        running.has_end = false;

        write_atomic(&v5_path, 4, 9, &mut [running.clone()]).unwrap();
        let mut v5 = DiskTraceRollup::open(&v5_path, 4, 9).unwrap();
        let v5_rows = v5.all_rows().unwrap();
        assert!(v5_rows[0].has_start);
        assert!(!v5_rows[0].has_end);

        write_atomic_version(&v4_path, 4, 9, &mut [running], 4).unwrap();
        let mut v4 = DiskTraceRollup::open(&v4_path, 4, 9).unwrap();
        let v4_rows = v4.all_rows().unwrap();
        assert!(v4_rows[0].has_start);
        assert!(!v4_rows[0].has_end, "旧记录没耗时时不能猜成已完成");

        let _ = std::fs::remove_file(v5_path);
        let _ = std::fs::remove_file(v4_path);
    }
}
