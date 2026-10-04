//! segstore.rs —— **段落盘**（`FileSegmentStore`）：把不可变段写到磁盘文件，重启不丢。
//!
//! 替掉 `InMemorySegmentStore`（进程没了段就没）。补的是"一 flush 推进水位、那批数据就只活在内存段里、
//! 重启就没"这个真痛点 —— flush 后 WAL 重放只补水位**之后**的尾巴，水位之前的数据必须靠**持久化的段**。
//!
//! 编码复用 WAL 同一套（`yt_wal::encode_records`/`decode_records`），不再各写一份记录序列化。
//! 每个段一个文件 `seg-<id>.dat`，格式 `[crc32 u32][payload]`：
//! - **原子落盘**：先写 `seg-<id>.tmp` + `fsync`，再 `rename` 到正式名（rename 在同目录是原子的）→
//!   不会出现"写一半的段文件"。
//! - **读时校验 crc**：损坏/截断的段当空段（不返回脏数据；上层压测会立刻抓到读空）。
//!
//! 仍缺：列式（现在是行式 WAL 编码，Vortex 列式替换是后续要不要加依赖的单独决定）。manifest 持久化另做
//! —— 段文件在盘上，但"有哪些段、各段的删除/补写"靠 manifest，那块单独一单元。
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use yt_core::fold::FoldInput;
use yt_core::ids::SegmentId;
use yt_wal::{Crc32, WalRecord};

use crate::{FullRecordScan, KeyedRecordScan, KeyedSegmentScan, SegmentStore};

const INDEX_MAGIC: u64 = 0x5954_5345_4749_4458; // "YTSEGIDX"
const INDEX_VERSION: u32 = 1;
const INDEX_HEADER_LEN: u64 = 40;
const INDEX_ENTRY_LEN: u64 = 36;
const INDEX_FOOTER_LEN: u64 = 4;
const INDEX_IO_BUFFER: usize = 1024 * 1024;
const MAX_POINT_LOOKUP_KEYS: usize = 4096;
static INDEX_TMP_NONCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug)]
struct IndexHeader {
    entry_count: u64,
    data_len: u64,
    data_crc: u32,
}

#[derive(Clone, Copy, Debug, Default)]
struct IndexIoStats {
    index_bytes_read: u64,
    data_bytes_read: u64,
    indexes_validated: usize,
    indexes_rebuilt: usize,
}

#[derive(Clone, Copy, Debug)]
struct IndexAccess {
    header: IndexHeader,
    io: IndexIoStats,
}

#[derive(Clone, Copy, Debug)]
struct IndexEntry {
    trace_id: u64,
    span_id: u64,
    row: u32,
    offset: u64,
    len: u32,
    record_crc: u32,
}

impl IndexEntry {
    fn key(self) -> (u64, u64) {
        (self.trace_id, self.span_id)
    }
}

/// 段落盘到一个目录，每段一个文件。
pub struct FileSegmentStore {
    dir: PathBuf,
    validated_indexes: Mutex<HashMap<u64, IndexHeader>>,
}

impl FileSegmentStore {
    /// 打开/创建段目录。
    pub fn open(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            validated_indexes: Mutex::new(HashMap::new()),
        })
    }

    /// 用同一份磁盘 key 目录读取原始记录。折叠点读和日志点读都走这里，避免两套索引逻辑漂移。
    fn point_records(&self, seg: SegmentId, keys: &HashSet<(u64, u64)>) -> Option<KeyedRecordScan> {
        // 大候选集做大量随机 seek 会比顺序扫描更慢；返回 None 让引擎走整段回退。
        if keys.len() > MAX_POINT_LOOKUP_KEYS {
            return None;
        }
        if keys.is_empty() {
            return Some(KeyedRecordScan {
                used_point_index: true,
                ..KeyedRecordScan::default()
            });
        }
        let mut access = self.index_header(seg)?;
        let header = access.header;
        let mut index = File::open(self.index_path(seg)).ok()?;
        let mut entries = Vec::new();
        for &key in keys {
            entries.extend(Self::find_entries(
                &mut index,
                header.entry_count,
                key,
                &mut access.io.index_bytes_read,
            )?);
        }
        entries.sort_unstable_by_key(|entry| entry.row);
        entries.dedup_by_key(|entry| entry.row);

        let mut data = File::open(self.seg_path(seg)).ok()?;
        let mut rows = Vec::with_capacity(entries.len());
        for entry in entries {
            let end = entry.offset.checked_add(entry.len as u64)?;
            if entry.offset < 4 || end > header.data_len {
                return None;
            }
            data.seek(SeekFrom::Start(entry.offset)).ok()?;
            let mut bytes = vec![0u8; entry.len as usize];
            data.read_exact(&mut bytes).ok()?;
            access.io.data_bytes_read = access.io.data_bytes_read.saturating_add(entry.len as u64);
            if yt_wal::crc32(&bytes) != entry.record_crc {
                return None;
            }
            let record = yt_wal::decode_record(&bytes)?;
            if record.trace_id != entry.trace_id || record.span_id != entry.span_id {
                return None;
            }
            rows.push((entry.row, record));
        }
        let decoded_rows = rows.len();
        Some(KeyedRecordScan {
            rows,
            used_point_index: true,
            decoded_rows,
            index_bytes_read: access.io.index_bytes_read,
            data_bytes_read: access.io.data_bytes_read,
            indexes_validated: access.io.indexes_validated,
            indexes_rebuilt: access.io.indexes_rebuilt,
        })
    }

    fn seg_path(&self, seg: SegmentId) -> PathBuf {
        self.dir.join(format!("seg-{}.dat", seg.get()))
    }

    fn full_record_scan(&self, seg: SegmentId) -> FullRecordScan {
        let bytes = fs::read(self.seg_path(seg)).unwrap_or_default();
        let data_bytes_read = bytes.len() as u64;
        if bytes.len() < 4 {
            return FullRecordScan {
                data_bytes_read,
                ..FullRecordScan::default()
            };
        }
        let crc = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let payload = &bytes[4..];
        if crc != yt_wal::crc32(payload) {
            return FullRecordScan {
                data_bytes_read,
                ..FullRecordScan::default()
            };
        }
        FullRecordScan {
            rows: yt_wal::decode_records(payload).unwrap_or_default(),
            data_bytes_read,
        }
    }
    fn tmp_path(&self, seg: SegmentId) -> PathBuf {
        self.dir.join(format!("seg-{}.tmp", seg.get()))
    }

    fn index_path(&self, seg: SegmentId) -> PathBuf {
        self.dir.join(format!("seg-{}.idx", seg.get()))
    }

    fn index_tmp_path(&self, seg: SegmentId) -> PathBuf {
        self.dir.join(format!(
            "seg-{}.{}.{}.idx.tmp",
            seg.get(),
            std::process::id(),
            INDEX_TMP_NONCE.fetch_add(1, Ordering::Relaxed)
        ))
    }

    /// 原子写：写 tmp + fsync + rename。任何写入或同步失败均返回，调用方不得发布该段。
    fn write_atomic(&self, seg: SegmentId, bytes: &[u8]) -> std::io::Result<()> {
        let tmp = self.tmp_path(seg);
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        fs::rename(&tmp, self.seg_path(seg))
    }

    fn write_index_from_segment_bytes(&self, seg: SegmentId, bytes: &[u8]) -> Option<IndexHeader> {
        let crc_bytes = bytes.get(..4)?;
        let data_crc = u32::from_le_bytes(crc_bytes.try_into().ok()?);
        let payload = bytes.get(4..)?;
        if yt_wal::crc32(payload) != data_crc {
            return None;
        }
        let ranges = yt_wal::encoded_record_ranges(payload)?;
        let mut entries = Vec::with_capacity(ranges.len());
        for (row, range) in ranges.into_iter().enumerate() {
            let start = usize::try_from(range.offset).ok()?;
            let end = start.checked_add(range.len as usize)?;
            let record = payload.get(start..end)?;
            entries.push(IndexEntry {
                trace_id: range.trace_id,
                span_id: range.span_id,
                row: u32::try_from(row).ok()?,
                offset: 4 + range.offset,
                len: range.len,
                record_crc: yt_wal::crc32(record),
            });
        }
        entries.sort_unstable_by_key(|entry| (entry.trace_id, entry.span_id, entry.row));

        let header = IndexHeader {
            entry_count: entries.len() as u64,
            data_len: bytes.len() as u64,
            data_crc,
        };
        let tmp = self.index_tmp_path(seg);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .ok()?;
        let mut writer = BufWriter::with_capacity(INDEX_IO_BUFFER, file);
        let mut crc = Crc32::new();
        write_index_part(&mut writer, &mut crc, &INDEX_MAGIC.to_le_bytes()).ok()?;
        write_index_part(&mut writer, &mut crc, &INDEX_VERSION.to_le_bytes()).ok()?;
        write_index_part(
            &mut writer,
            &mut crc,
            &(INDEX_ENTRY_LEN as u32).to_le_bytes(),
        )
        .ok()?;
        write_index_part(&mut writer, &mut crc, &header.entry_count.to_le_bytes()).ok()?;
        write_index_part(&mut writer, &mut crc, &header.data_len.to_le_bytes()).ok()?;
        write_index_part(&mut writer, &mut crc, &header.data_crc.to_le_bytes()).ok()?;
        write_index_part(&mut writer, &mut crc, &0u32.to_le_bytes()).ok()?;
        for entry in entries {
            write_index_part(&mut writer, &mut crc, &entry.trace_id.to_le_bytes()).ok()?;
            write_index_part(&mut writer, &mut crc, &entry.span_id.to_le_bytes()).ok()?;
            write_index_part(&mut writer, &mut crc, &entry.row.to_le_bytes()).ok()?;
            write_index_part(&mut writer, &mut crc, &entry.offset.to_le_bytes()).ok()?;
            write_index_part(&mut writer, &mut crc, &entry.len.to_le_bytes()).ok()?;
            write_index_part(&mut writer, &mut crc, &entry.record_crc.to_le_bytes()).ok()?;
        }
        writer.write_all(&crc.finish().to_le_bytes()).ok()?;
        writer.flush().ok()?;
        writer.get_ref().sync_all().ok()?;
        drop(writer);
        if fs::rename(&tmp, self.index_path(seg)).is_err() {
            let _ = fs::remove_file(&tmp);
            return None;
        }
        Some(header)
    }

    fn validate_index(&self, seg: SegmentId) -> Option<IndexAccess> {
        let index_path = self.index_path(seg);
        let mut file = File::open(index_path).ok()?;
        let mut header_bytes = [0u8; INDEX_HEADER_LEN as usize];
        file.read_exact(&mut header_bytes).ok()?;
        let header = decode_index_header(&header_bytes)?;
        let body_len =
            INDEX_HEADER_LEN.checked_add(header.entry_count.checked_mul(INDEX_ENTRY_LEN)?)?;
        let expected_len = body_len.checked_add(INDEX_FOOTER_LEN)?;
        if file.metadata().ok()?.len() != expected_len {
            return None;
        }

        file.seek(SeekFrom::Start(0)).ok()?;
        let mut reader = BufReader::with_capacity(INDEX_IO_BUFFER, file);
        let mut crc = Crc32::new();
        let mut remaining = body_len;
        let mut chunk = vec![0u8; INDEX_IO_BUFFER];
        while remaining > 0 {
            let take = usize::try_from(remaining.min(chunk.len() as u64)).ok()?;
            reader.read_exact(&mut chunk[..take]).ok()?;
            crc.update(&chunk[..take]);
            remaining -= take as u64;
        }
        let mut footer = [0u8; 4];
        reader.read_exact(&mut footer).ok()?;
        if u32::from_le_bytes(footer) != crc.finish() {
            return None;
        }

        let mut data =
            BufReader::with_capacity(INDEX_IO_BUFFER, File::open(self.seg_path(seg)).ok()?);
        if data.get_ref().metadata().ok()?.len() != header.data_len {
            return None;
        }
        let mut crc_bytes = [0u8; 4];
        data.read_exact(&mut crc_bytes).ok()?;
        if u32::from_le_bytes(crc_bytes) != header.data_crc {
            return None;
        }
        let mut data_crc = Crc32::new();
        let mut remaining = header.data_len.checked_sub(4)?;
        while remaining > 0 {
            let take = usize::try_from(remaining.min(chunk.len() as u64)).ok()?;
            data.read_exact(&mut chunk[..take]).ok()?;
            data_crc.update(&chunk[..take]);
            remaining -= take as u64;
        }
        if data_crc.finish() != header.data_crc {
            return None;
        }
        Some(IndexAccess {
            header,
            io: IndexIoStats {
                index_bytes_read: INDEX_HEADER_LEN.saturating_add(expected_len),
                data_bytes_read: header.data_len,
                indexes_validated: 1,
                indexes_rebuilt: 0,
            },
        })
    }

    fn index_header(&self, seg: SegmentId) -> Option<IndexAccess> {
        if let Some(header) = self.validated_indexes.lock().ok()?.get(&seg.get()).copied() {
            return Some(IndexAccess {
                header,
                io: IndexIoStats::default(),
            });
        }

        let mut guard = self.validated_indexes.lock().ok()?;
        if let Some(header) = guard.get(&seg.get()).copied() {
            return Some(IndexAccess {
                header,
                io: IndexIoStats::default(),
            });
        }
        let access = self.validate_index(seg).or_else(|| {
            let bytes = fs::read(self.seg_path(seg)).ok()?;
            if let Some(header) = self.write_index_from_segment_bytes(seg, &bytes) {
                return Some(IndexAccess {
                    header,
                    io: IndexIoStats {
                        data_bytes_read: bytes.len() as u64,
                        indexes_rebuilt: 1,
                        ..IndexIoStats::default()
                    },
                });
            }
            let mut access = self.validate_index(seg)?;
            access.io.data_bytes_read =
                access.io.data_bytes_read.saturating_add(bytes.len() as u64);
            Some(access)
        })?;
        guard.insert(seg.get(), access.header);
        Some(access)
    }

    fn read_index_entry(file: &mut File, position: u64) -> Option<IndexEntry> {
        let offset = INDEX_HEADER_LEN.checked_add(position.checked_mul(INDEX_ENTRY_LEN)?)?;
        file.seek(SeekFrom::Start(offset)).ok()?;
        let mut bytes = [0u8; INDEX_ENTRY_LEN as usize];
        file.read_exact(&mut bytes).ok()?;
        Some(IndexEntry {
            trace_id: read_u64(&bytes, 0)?,
            span_id: read_u64(&bytes, 8)?,
            row: read_u32(&bytes, 16)?,
            offset: read_u64(&bytes, 20)?,
            len: read_u32(&bytes, 28)?,
            record_crc: read_u32(&bytes, 32)?,
        })
    }

    fn find_entries(
        file: &mut File,
        entry_count: u64,
        key: (u64, u64),
        index_bytes_read: &mut u64,
    ) -> Option<Vec<IndexEntry>> {
        let mut low = 0u64;
        let mut high = entry_count;
        while low < high {
            let mid = low + (high - low) / 2;
            let entry = Self::read_index_entry(file, mid)?;
            *index_bytes_read = index_bytes_read.saturating_add(INDEX_ENTRY_LEN);
            if entry.key() < key {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        let mut out = Vec::new();
        let mut position = low;
        while position < entry_count {
            let entry = Self::read_index_entry(file, position)?;
            *index_bytes_read = index_bytes_read.saturating_add(INDEX_ENTRY_LEN);
            if entry.key() != key {
                break;
            }
            out.push(entry);
            position += 1;
        }
        Some(out)
    }
}

fn decode_index_header(bytes: &[u8; INDEX_HEADER_LEN as usize]) -> Option<IndexHeader> {
    if read_u64(bytes, 0)? != INDEX_MAGIC
        || read_u32(bytes, 8)? != INDEX_VERSION
        || read_u32(bytes, 12)? != INDEX_ENTRY_LEN as u32
    {
        return None;
    }
    Some(IndexHeader {
        entry_count: read_u64(bytes, 16)?,
        data_len: read_u64(bytes, 24)?,
        data_crc: read_u32(bytes, 32)?,
    })
}

fn read_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        bytes.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        bytes.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

fn write_index_part(writer: &mut impl Write, crc: &mut Crc32, bytes: &[u8]) -> std::io::Result<()> {
    writer.write_all(bytes)?;
    crc.update(bytes);
    Ok(())
}

impl SegmentStore for FileSegmentStore {
    fn flush_to_segment(&self, seg: SegmentId, records: &[WalRecord]) {
        self.try_flush_to_segment(seg, records)
            .expect("yiTrace segment write failed");
    }

    fn try_flush_to_segment(&self, seg: SegmentId, records: &[WalRecord]) -> std::io::Result<()> {
        let payload = yt_wal::encode_records(records);
        let mut buf = Vec::with_capacity(payload.len() + 4);
        buf.extend_from_slice(&yt_wal::crc32(&payload).to_le_bytes());
        buf.extend_from_slice(&payload);
        self.write_atomic(seg, &buf)?;
        if let Some(header) = self.write_index_from_segment_bytes(seg, &buf) {
            if let Ok(mut indexes) = self.validated_indexes.lock() {
                indexes.insert(seg.get(), header);
            }
        }
        Ok(())
    }

    fn scan_fold_inputs(&self, seg: SegmentId) -> Vec<(u32, FoldInput)> {
        self.scan_records(seg)
            .iter()
            .enumerate()
            .map(|(i, r)| (i as u32, r.to_fold_input()))
            .collect()
    }

    fn scan_records(&self, seg: SegmentId) -> Vec<WalRecord> {
        self.full_record_scan(seg).rows
    }

    fn scan_records_with_stats(&self, seg: SegmentId) -> FullRecordScan {
        self.full_record_scan(seg)
    }

    fn scan_fold_inputs_for_keys(
        &self,
        seg: SegmentId,
        keys: &HashSet<(u64, u64)>,
    ) -> Option<KeyedSegmentScan> {
        let scan = self.point_records(seg, keys)?;
        Some(KeyedSegmentScan {
            rows: scan
                .rows
                .into_iter()
                .map(|(row, record)| (row, record.to_fold_input()))
                .collect(),
            used_point_index: scan.used_point_index,
            decoded_rows: scan.decoded_rows,
            index_bytes_read: scan.index_bytes_read,
            data_bytes_read: scan.data_bytes_read,
            indexes_validated: scan.indexes_validated,
            indexes_rebuilt: scan.indexes_rebuilt,
        })
    }

    fn scan_records_for_keys(
        &self,
        seg: SegmentId,
        keys: &HashSet<(u64, u64)>,
    ) -> Option<KeyedRecordScan> {
        self.point_records(seg, keys)
    }

    fn unlink_segment(&self, seg: SegmentId) {
        let _ = fs::remove_file(self.seg_path(seg));
        let _ = fs::remove_file(self.index_path(seg));
        if let Ok(mut indexes) = self.validated_indexes.lock() {
            indexes.remove(&seg.get());
        }
    }
}

/// 确保目录 fsync（rename 的目录项也要落盘才真持久；调用方在一批写后调一次即可）。
pub fn fsync_dir(dir: impl AsRef<Path>) {
    if let Ok(f) = File::open(dir.as_ref()) {
        let _ = f.sync_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use yt_core::event::{EventIdentity, EventType};
    use yt_core::fold::SpanFields;

    fn temp_dir() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "yt_segstore_{}_{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&p);
        p
    }

    fn rec(span: &str, seq: u64, log: &str) -> WalRecord {
        WalRecord {
            trace_id: 1,
            span_id: seq,
            ts: seq as i64,
            identity: EventIdentity {
                ext_span_id: span.into(),
                seq,
                event_type: EventType::SpanEnd,
            },
            fields: SpanFields {
                logs: vec![log.into()],
                ..Default::default()
            },
        }
    }

    #[test]
    fn streaming_crc32_matches_wal_crc_across_chunk_boundaries() {
        let bytes = (0..2_057)
            .map(|i| ((i * 131 + i / 7) & 0xff) as u8)
            .collect::<Vec<_>>();
        for len in 0..=bytes.len() {
            let expected = yt_wal::crc32(&bytes[..len]);
            for chunk_size in [1, 3, 7, 8, 9, 31, 128, 1_024] {
                let mut crc = Crc32::new();
                for chunk in bytes[..len].chunks(chunk_size) {
                    crc.update(chunk);
                }
                assert_eq!(crc.finish(), expected, "len={len}, chunk_size={chunk_size}");
            }
        }
    }

    #[test]
    fn segment_survives_reopen_real_disk() {
        // 真落盘:写段 → drop store(模拟进程没了)→ 新 store 开同一目录 → 段还在。
        let dir = temp_dir();
        let seg = SegmentId::new(7);
        {
            let store = FileSegmentStore::open(&dir).unwrap();
            let mut named = rec("反洗钱", 1, "日志1");
            named.fields.span_name = Some("risk.review".into());
            named.fields.display_name = Some("风险审核".into());
            store.flush_to_segment(seg, &[named, rec("盗刷", 2, "日志2")]);
        }
        // 重开（相当于重启进程）
        let store2 = FileSegmentStore::open(&dir).unwrap();
        let recs = store2.scan_records(seg);
        assert_eq!(recs.len(), 2, "段从磁盘读回来");
        assert_eq!(recs[0].identity.ext_span_id, "反洗钱");
        assert_eq!(recs[0].fields.logs, vec!["日志1"]);
        assert_eq!(recs[0].fields.span_name.as_deref(), Some("risk.review"));
        assert_eq!(recs[0].fields.display_name.as_deref(), Some("风险审核"));
        assert_eq!(recs[1].identity.ext_span_id, "盗刷");
        // 行号映射
        let folds = store2.scan_fold_inputs(seg);
        assert_eq!(folds.len(), 2);
        assert_eq!(folds[0].0, 0);
        assert_eq!(folds[1].0, 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn keyed_fold_scan_returns_only_requested_rows() {
        let dir = temp_dir();
        let seg = SegmentId::new(8);
        let store = FileSegmentStore::open(&dir).unwrap();
        store.flush_to_segment(
            seg,
            &[
                rec("a", 1, "first"),
                rec("b", 2, "second"),
                rec("c", 3, "third"),
            ],
        );

        let keys = HashSet::from([(1, 2), (99, 99)]);
        let scan = store.scan_fold_inputs_for_keys(seg, &keys).unwrap();
        assert!(scan.used_point_index);
        assert_eq!(scan.decoded_rows, 1);
        assert_eq!(scan.rows.len(), 1);
        assert_eq!(scan.rows[0].0, 1);
        assert_eq!(scan.rows[0].1.span_id, 2);
        assert!(scan.index_bytes_read > 0);
        assert!(scan.data_bytes_read > 0);
        assert_eq!(scan.indexes_validated, 0, "刚写入的索引已经在本进程校验过");
        assert!(store.index_path(seg).exists());

        let raw = store.scan_records_for_keys(seg, &keys).unwrap();
        assert!(raw.used_point_index);
        assert_eq!(raw.decoded_rows, 1, "原始日志点读也只能解码目标事件");
        assert_eq!(raw.rows.len(), 1);
        assert_eq!(raw.rows[0].0, 1);
        assert_eq!(raw.rows[0].1.span_id, 2);
        assert_eq!(raw.rows[0].1.fields.logs, vec!["second"]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn keyed_scan_rebuilds_missing_index_and_keeps_physical_rows() {
        let dir = temp_dir();
        let seg = SegmentId::new(9);
        let store = FileSegmentStore::open(&dir).unwrap();
        let mut start = rec("same-start", 7, "start");
        start.span_id = 7;
        let mut end = rec("same-end", 8, "end");
        end.span_id = 7;
        store.flush_to_segment(seg, &[rec("other", 1, "x"), start, end]);
        fs::remove_file(store.index_path(seg)).unwrap();
        store.validated_indexes.lock().unwrap().clear();

        let reopened = FileSegmentStore::open(&dir).unwrap();
        let scan = reopened
            .scan_fold_inputs_for_keys(seg, &HashSet::from([(1, 7)]))
            .unwrap();
        assert!(reopened.index_path(seg).exists(), "旧段首次点查补建目录");
        assert_eq!(scan.decoded_rows, 2);
        assert_eq!(scan.indexes_rebuilt, 1);
        assert!(scan.data_bytes_read >= fs::metadata(store.seg_path(seg)).unwrap().len());
        assert_eq!(
            scan.rows.iter().map(|(row, _)| *row).collect::<Vec<_>>(),
            vec![1, 2]
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn keyed_scan_rebuilds_corrupt_index() {
        let dir = temp_dir();
        let seg = SegmentId::new(10);
        let store = FileSegmentStore::open(&dir).unwrap();
        store.flush_to_segment(seg, &[rec("a", 1, "x"), rec("b", 2, "y")]);
        fs::write(store.index_path(seg), b"broken").unwrap();

        let reopened = FileSegmentStore::open(&dir).unwrap();
        let scan = reopened
            .scan_fold_inputs_for_keys(seg, &HashSet::from([(1, 2)]))
            .unwrap();
        assert_eq!(scan.decoded_rows, 1);
        assert_eq!(scan.rows[0].1.span_id, 2);
        assert_eq!(scan.indexes_rebuilt, 1);
        assert!(reopened.validate_index(seg).is_some());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn point_lookup_rejects_segment_when_an_unrelated_record_is_corrupt() {
        let dir = temp_dir();
        let seg = SegmentId::new(12);
        let store = FileSegmentStore::open(&dir).unwrap();
        store.flush_to_segment(seg, &[rec("broken", 1, "x"), rec("wanted", 2, "y")]);

        let path = store.seg_path(seg);
        let mut bytes = fs::read(&path).unwrap();
        let first = yt_wal::encoded_record_ranges(&bytes[4..]).unwrap()[0];
        let corrupt_at = 4 + first.offset as usize + first.len as usize - 1;
        bytes[corrupt_at] ^= 0xff;
        fs::write(&path, bytes).unwrap();

        let reopened = FileSegmentStore::open(&dir).unwrap();
        assert!(reopened.scan_records(seg).is_empty());
        assert!(
            reopened
                .scan_fold_inputs_for_keys(seg, &HashSet::from([(1, 2)]))
                .is_none(),
            "点查必须和整段扫描一样拒绝整体 CRC 已损坏的段"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn multiple_store_instances_can_rebuild_one_missing_index() {
        let dir = temp_dir();
        let seg = SegmentId::new(13);
        let store = FileSegmentStore::open(&dir).unwrap();
        store.flush_to_segment(seg, &[rec("a", 1, "x"), rec("b", 2, "y")]);
        fs::remove_file(store.index_path(seg)).unwrap();

        let workers = 8;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(workers));
        let rebuilt = std::sync::Arc::new(AtomicU64::new(0));
        std::thread::scope(|scope| {
            for _ in 0..workers {
                let barrier = std::sync::Arc::clone(&barrier);
                let rebuilt = std::sync::Arc::clone(&rebuilt);
                let dir = dir.clone();
                scope.spawn(move || {
                    let instance = FileSegmentStore::open(dir).unwrap();
                    barrier.wait();
                    let scan = instance
                        .scan_fold_inputs_for_keys(seg, &HashSet::from([(1, 2)]))
                        .unwrap();
                    assert_eq!(scan.rows.len(), 1);
                    assert_eq!(scan.rows[0].1.span_id, 2);
                    rebuilt.fetch_add(scan.indexes_rebuilt as u64, Ordering::Relaxed);
                });
            }
        });

        let verifier = FileSegmentStore::open(&dir).unwrap();
        assert!(verifier.validate_index(seg).is_some());
        assert!(rebuilt.load(Ordering::Relaxed) >= 1);
        assert!(
            fs::read_dir(&dir).unwrap().all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".idx.tmp")),
            "并发重建不能留下临时索引"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn large_key_set_uses_sequential_scan_fallback() {
        let dir = temp_dir();
        let seg = SegmentId::new(11);
        let store = FileSegmentStore::open(&dir).unwrap();
        store.flush_to_segment(seg, &[rec("a", 1, "x")]);
        let keys = (0..=MAX_POINT_LOOKUP_KEYS as u64)
            .map(|span_id| (1, span_id))
            .collect::<HashSet<_>>();
        assert!(store.scan_fold_inputs_for_keys(seg, &keys).is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_segment_reads_as_empty_not_garbage() {
        // crc 守门:文件被改坏 → 当空段,绝不返回脏数据。
        let dir = temp_dir();
        let seg = SegmentId::new(3);
        let store = FileSegmentStore::open(&dir).unwrap();
        store.flush_to_segment(seg, &[rec("ok", 1, "x")]);
        assert_eq!(store.scan_records(seg).len(), 1);

        // 翻末尾一个字节（破坏 payload，crc 不符）
        let path = store.seg_path(seg);
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        assert!(
            store.scan_records(seg).is_empty(),
            "损坏段读成空,不返回脏数据"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn unlink_removes_segment_file() {
        let dir = temp_dir();
        let seg = SegmentId::new(5);
        let store = FileSegmentStore::open(&dir).unwrap();
        store.flush_to_segment(seg, &[rec("a", 1, "x")]);
        assert!(store.seg_path(seg).exists());
        store.unlink_segment(seg);
        assert!(!store.seg_path(seg).exists(), "unlink 真删段文件");
        assert!(!store.index_path(seg).exists(), "unlink 同时删点查目录");
        assert!(store.scan_records(seg).is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_segment_scans_empty() {
        let dir = temp_dir();
        let store = FileSegmentStore::open(&dir).unwrap();
        assert!(
            store.scan_records(SegmentId::new(999)).is_empty(),
            "不存在的段读成空,不 panic"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
