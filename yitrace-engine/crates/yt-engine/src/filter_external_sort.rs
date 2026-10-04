//! 过滤 postings 的有界外排。
//!
//! flush 时先把 `(field,value,trace,span)` 放进固定字节预算的内存块，块内排序后写临时
//! run；最后多路归并成按 posting 分组的迭代器。数据量增长只增加临时文件，不增加主内存。

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::filter_disk::{PostingWrite, SpanKey};

const DEFAULT_RUN_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Entry {
    field: Arc<str>,
    value: Arc<str>,
    key: SpanKey,
}

pub(crate) struct PostingRunBuilder {
    base: PathBuf,
    run_bytes: usize,
    buffered_bytes: usize,
    entries: Vec<Entry>,
    paths: Vec<PathBuf>,
}

impl PostingRunBuilder {
    pub(crate) fn new(target: &Path) -> Self {
        let run_bytes = std::env::var("YT_FILTER_POSTING_RUN_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_RUN_BYTES)
            .max(1024);
        Self::with_run_bytes(target, run_bytes)
    }

    fn with_run_bytes(target: &Path, run_bytes: usize) -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self {
            base: target.with_extension(format!("postings-{}-{nonce}", std::process::id())),
            run_bytes,
            buffered_bytes: 0,
            entries: Vec::new(),
            paths: Vec::new(),
        }
    }

    pub(crate) fn push(
        &mut self,
        field: String,
        value: String,
        key: SpanKey,
    ) -> std::io::Result<()> {
        self.buffered_bytes = self
            .buffered_bytes
            .saturating_add(field.len())
            .saturating_add(value.len())
            .saturating_add(48);
        self.entries.push(Entry {
            field: Arc::from(field),
            value: Arc::from(value),
            key,
        });
        if self.buffered_bytes >= self.run_bytes {
            self.flush_run()?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> std::io::Result<ExternalPostings> {
        self.flush_run()?;
        let paths = std::mem::take(&mut self.paths);
        ExternalPostings::open(paths)
    }

    fn flush_run(&mut self) -> std::io::Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }
        self.entries.sort_unstable();
        self.entries.dedup();
        let path = PathBuf::from(format!("{}.run-{}", self.base.display(), self.paths.len()));
        let mut file = File::create(&path)?;
        let mut start = 0;
        while start < self.entries.len() {
            let mut end = start + 1;
            while end < self.entries.len()
                && self.entries[end].field == self.entries[start].field
                && self.entries[end].value == self.entries[start].value
            {
                end += 1;
            }
            write_string(&mut file, &self.entries[start].field)?;
            write_string(&mut file, &self.entries[start].value)?;
            file.write_all(&((end - start) as u64).to_le_bytes())?;
            for entry in &self.entries[start..end] {
                file.write_all(&[u8::from(entry.key.0.is_some())])?;
                file.write_all(&entry.key.0.unwrap_or(0).to_le_bytes())?;
                file.write_all(&entry.key.1.to_le_bytes())?;
                file.write_all(&entry.key.2.to_le_bytes())?;
            }
            start = end;
        }
        file.sync_all()?;
        self.paths.push(path);
        self.entries.clear();
        self.buffered_bytes = 0;
        Ok(())
    }
}

impl Drop for PostingRunBuilder {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = std::fs::remove_file(path);
        }
    }
}

struct RunReader {
    file: File,
    field: Option<Arc<str>>,
    value: Option<Arc<str>>,
    remaining: u64,
}

impl RunReader {
    fn next_entry(&mut self) -> std::io::Result<Option<Entry>> {
        if self.remaining == 0 {
            let Some(field) = read_string_or_eof(&mut self.file)? else {
                return Ok(None);
            };
            let value = read_string(&mut self.file)?;
            let mut count = [0u8; 8];
            self.file.read_exact(&mut count)?;
            self.remaining = u64::from_le_bytes(count);
            if self.remaining == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "empty filter posting group",
                ));
            }
            self.field = Some(Arc::from(field));
            self.value = Some(Arc::from(value));
        }
        let mut ids = [0u8; 25];
        self.file.read_exact(&mut ids)?;
        self.remaining -= 1;
        let tenant_id = u64::from_le_bytes(ids[1..9].try_into().unwrap());
        let tenant = match ids[0] {
            0 => None,
            1 => Some(tenant_id),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid tenant",
                ))
            }
        };
        let trace_id = u64::from_le_bytes(ids[9..17].try_into().unwrap());
        let span_id = u64::from_le_bytes(ids[17..25].try_into().unwrap());
        Ok(Some(Entry {
            field: Arc::clone(self.field.as_ref().unwrap()),
            value: Arc::clone(self.value.as_ref().unwrap()),
            key: (tenant, trace_id, span_id),
        }))
    }
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct HeapEntry {
    entry: Entry,
    run: usize,
}

pub(crate) struct ExternalPostings {
    paths: Vec<PathBuf>,
    runs: Vec<RunReader>,
    heap: BinaryHeap<Reverse<HeapEntry>>,
    pending: Option<Entry>,
    failed: bool,
}

impl ExternalPostings {
    fn open(paths: Vec<PathBuf>) -> std::io::Result<Self> {
        let mut runs = Vec::with_capacity(paths.len());
        let mut heap = BinaryHeap::new();
        for (run, path) in paths.iter().enumerate() {
            let mut reader = RunReader {
                file: File::open(path)?,
                field: None,
                value: None,
                remaining: 0,
            };
            if let Some(entry) = reader.next_entry()? {
                heap.push(Reverse(HeapEntry { entry, run }));
            }
            runs.push(reader);
        }
        Ok(Self {
            paths,
            runs,
            heap,
            pending: None,
            failed: false,
        })
    }

    fn pop_entry(&mut self) -> std::io::Result<Option<Entry>> {
        if let Some(entry) = self.pending.take() {
            return Ok(Some(entry));
        }
        let Some(Reverse(item)) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some(next) = self.runs[item.run].next_entry()? {
            self.heap.push(Reverse(HeapEntry {
                entry: next,
                run: item.run,
            }));
        }
        Ok(Some(item.entry))
    }
}

impl Iterator for ExternalPostings {
    type Item = std::io::Result<PostingWrite>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let first = match self.pop_entry() {
            Ok(Some(entry)) => entry,
            Ok(None) => return None,
            Err(error) => {
                self.failed = true;
                return Some(Err(error));
            }
        };
        let field = first.field;
        let value = first.value;
        let mut keys = vec![first.key];
        loop {
            match self.pop_entry() {
                Ok(Some(entry)) if entry.field == field && entry.value == value => {
                    if keys.last().copied() != Some(entry.key) {
                        keys.push(entry.key);
                    }
                }
                Ok(Some(entry)) => {
                    self.pending = Some(entry);
                    break;
                }
                Ok(None) => break,
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error));
                }
            }
        }
        Some(Ok(PostingWrite {
            field: field.to_string(),
            value: value.to_string(),
            disabled: false,
            keys,
        }))
    }
}

impl Drop for ExternalPostings {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn write_string(file: &mut File, value: &str) -> std::io::Result<()> {
    let len = u32::try_from(value.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "filter posting key is too large",
        )
    })?;
    file.write_all(&len.to_le_bytes())?;
    file.write_all(value.as_bytes())
}

fn read_string_or_eof(file: &mut File) -> std::io::Result<Option<String>> {
    let mut len = [0u8; 4];
    let mut read = 0;
    while read < len.len() {
        let count = file.read(&mut len[read..])?;
        if count == 0 {
            if read == 0 {
                return Ok(None);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "truncated filter posting run",
            ));
        }
        read += count;
    }
    let len = usize::try_from(u32::from_le_bytes(len)).unwrap();
    let mut bytes = vec![0; len];
    file.read_exact(&mut bytes)?;
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid UTF-8"))
}

fn read_string(file: &mut File) -> std::io::Result<String> {
    read_string_or_eof(file)?.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "truncated filter posting run",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_runs_merge_and_deduplicate_in_order() {
        let path = std::env::temp_dir().join(format!(
            "yt_filter_sort_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut builder = PostingRunBuilder::with_run_bytes(&path, 128);
        for (field, value, key) in [
            ("tenant", "2", (None, 3, 3)),
            ("project", "a", (None, 2, 2)),
            ("project", "a", (None, 1, 1)),
            ("project", "a", (None, 1, 1)),
            ("tenant", "2", (None, 1, 1)),
        ] {
            builder
                .push(field.to_owned(), value.to_owned(), key)
                .unwrap();
        }
        let groups: Vec<_> = builder.finish().unwrap().map(Result::unwrap).collect();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].field, "project");
        assert_eq!(groups[0].keys, vec![(None, 1, 1), (None, 2, 2)]);
        assert_eq!(groups[1].field, "tenant");
        assert_eq!(groups[1].keys, vec![(None, 1, 1), (None, 3, 3)]);
    }
}
