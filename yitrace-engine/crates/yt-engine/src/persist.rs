//! persist.rs —— **manifest 持久化**：把不可变 manifest（有哪些段 + 各段的删除位图/补写块 + 水位 + epoch
//! + id 计数器）原子写盘，重启后据它重建段集合，配合 `FileSegmentStore` 与 WAL 重放达成"flush 后重启不丢"。
//!
//! 为什么必须持久化 manifest：段文件在盘上了，但引擎重启后**不知道有哪些段、各段删了哪些行/补了什么**——
//! 那些信息全在 manifest。光有段文件、没有 manifest，recover 找不到它们，flush 过的数据（水位之前、WAL 不再
//! 重放的部分）就丢了。
//!
//! 落盘：`[crc32 u32][payload]`，原子写（tmp + fsync + rename）。SpanFields 复用 `yt_wal::encode_span_fields`，
//! 不另写一份字段序列化。格式带 magic + 版本号，便于演进。
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use crate::olog;
use std::collections::BTreeMap;
use yt_core::chunk::{DeletionVec, UpgradeColChunk};
use yt_core::fold::SpanFields;
use yt_core::ids::{ChunkId, ManifestVersion, SegmentId, WalLsn};
use yt_core::manifest::{Manifest, SegState, SegmentEntry};

const MAGIC: u32 = 0x5654_4D46; // "VTMF"
pub const FORMAT_VER: u32 = 1;

/// 持久化的引擎状态 = manifest + 两个 id 计数器（段 id / chunk id 永不复用，必须随 manifest 一起恢复）。
pub struct PersistedState {
    pub manifest: Manifest,
    pub next_segment_id: u64,
    pub next_chunk_id: u64,
}

// ───────────────────────── 字节读写 ─────────────────────────

fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_le_bytes());
}
fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_le_bytes());
}
fn put_opt_u64(b: &mut Vec<u8>, v: Option<u64>) {
    match v {
        Some(x) => {
            b.push(1);
            put_u64(b, x);
        }
        None => b.push(0),
    }
}
fn put_blob(b: &mut Vec<u8>, blob: &[u8]) {
    put_u64(b, blob.len() as u64);
    b.extend_from_slice(blob);
}

struct Cur<'a> {
    b: &'a [u8],
    i: usize,
}
impl<'a> Cur<'a> {
    fn u32(&mut self) -> Option<u32> {
        let e = self.i + 4;
        let s = self.b.get(self.i..e)?;
        self.i = e;
        Some(u32::from_le_bytes(s.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        let e = self.i + 8;
        let s = self.b.get(self.i..e)?;
        self.i = e;
        Some(u64::from_le_bytes(s.try_into().ok()?))
    }
    fn u8(&mut self) -> Option<u8> {
        let v = *self.b.get(self.i)?;
        self.i += 1;
        Some(v)
    }
    fn opt_u64(&mut self) -> Option<Option<u64>> {
        if self.u8()? == 1 {
            Some(Some(self.u64()?))
        } else {
            Some(None)
        }
    }
    fn blob(&mut self) -> Option<&'a [u8]> {
        let n = self.u64()? as usize;
        let e = self.i + n;
        let s = self.b.get(self.i..e)?;
        self.i = e;
        Some(s)
    }
}

// ───────────────────────── 编码 ─────────────────────────

pub fn encode(state: &PersistedState) -> Vec<u8> {
    let m = &state.manifest;
    let mut b = Vec::new();
    put_u32(&mut b, MAGIC);
    put_u32(&mut b, FORMAT_VER);
    put_u64(&mut b, m.version.get());
    put_u64(&mut b, m.memtable_watermark.get());
    put_u64(&mut b, m.epoch);
    put_u64(&mut b, state.next_segment_id);
    put_u64(&mut b, state.next_chunk_id);
    put_u64(&mut b, m.segments.len() as u64);
    for e in m.segments.values() {
        put_u64(&mut b, e.segment_id.get());
        b.push(e.level);
        b.push(match e.state {
            SegState::Live => 0,
            SegState::Compacting => 1,
        });
        put_u64(&mut b, e.min_ts as u64);
        put_u64(&mut b, e.max_ts as u64);
        // deletion
        put_u64(&mut b, e.deletion_seq);
        put_opt_u64(&mut b, e.deletion_vec.chunk_id.map(|c| c.get()));
        let bits = e.deletion_vec.bits();
        put_u64(&mut b, bits.len() as u64);
        for &w in bits {
            put_u64(&mut b, w);
        }
        // upgrade
        put_u64(&mut b, e.upgrade_seq);
        match &e.upgrade_ref {
            None => b.push(0),
            Some(up) => {
                b.push(1);
                put_opt_u64(&mut b, up.chunk_id.map(|c| c.get()));
                let patches: Vec<(&(u64, u64), &SpanFields)> = up.iter().collect();
                put_u64(&mut b, patches.len() as u64);
                for (&(t, s), f) in patches {
                    put_u64(&mut b, t);
                    put_u64(&mut b, s);
                    put_blob(&mut b, &yt_wal::encode_span_fields(f));
                }
            }
        }
    }
    b
}

pub fn decode(bytes: &[u8]) -> Option<PersistedState> {
    let mut c = Cur { b: bytes, i: 0 };
    let magic = c.u32()?;
    let ver = c.u32()?;
    if magic != MAGIC {
        olog::log(
            olog::Level::Error,
            "manifest_decode",
            &[("reason", &"bad magic")],
        );
        return None;
    }
    if ver > FORMAT_VER {
        // 未来版本：需要新引擎。明确报错而非静默当损坏。
        olog::log(
            olog::Level::Error,
            "manifest_decode",
            &[
                ("reason", &"future version needs newer engine"),
                ("found", &ver),
                ("supported", &FORMAT_VER),
            ],
        );
        return None;
    }
    if ver < FORMAT_VER {
        // 老版本：需迁移。骨架阶段直接 None（无旧版本数据）；真实迁移工具见 migrate_manifest。
        olog::log(
            olog::Level::Warn,
            "manifest_decode",
            &[
                ("reason", &"old version needs migration"),
                ("found", &ver),
                ("current", &FORMAT_VER),
            ],
        );
        return None;
    }
    let version = ManifestVersion::new(c.u64()?);
    let memtable_watermark = WalLsn::new(c.u64()?);
    let epoch = c.u64()?;
    let next_segment_id = c.u64()?;
    let next_chunk_id = c.u64()?;
    let seg_n = c.u64()? as usize;
    let mut segments: BTreeMap<u64, SegmentEntry> = BTreeMap::new();
    for _ in 0..seg_n {
        let segment_id = SegmentId::new(c.u64()?);
        let level = c.u8()?;
        let state = match c.u8()? {
            0 => SegState::Live,
            _ => SegState::Compacting,
        };
        let min_ts = c.u64()? as i64;
        let max_ts = c.u64()? as i64;
        // deletion
        let deletion_seq = c.u64()?;
        let del_chunk = c.opt_u64()?.map(ChunkId::new);
        let bits_n = c.u64()? as usize;
        let mut bits = Vec::with_capacity(bits_n);
        for _ in 0..bits_n {
            bits.push(c.u64()?);
        }
        let deletion_vec = Arc::new(DeletionVec::from_bits(del_chunk, bits));
        // upgrade
        let upgrade_seq = c.u64()?;
        let upgrade_ref = match c.u8()? {
            0 => None,
            _ => {
                let up_chunk = c.opt_u64()?.map(ChunkId::new);
                let patch_n = c.u64()? as usize;
                let mut patches: BTreeMap<(u64, u64), SpanFields> = BTreeMap::new();
                for _ in 0..patch_n {
                    let t = c.u64()?;
                    let s = c.u64()?;
                    let blob = c.blob()?;
                    patches.insert((t, s), yt_wal::decode_span_fields(blob)?);
                }
                Some(Arc::new(UpgradeColChunk::from_patches(up_chunk, patches)))
            }
        };
        segments.insert(
            segment_id.get(),
            SegmentEntry {
                segment_id,
                level,
                state,
                min_ts,
                max_ts,
                deletion_vec,
                deletion_seq,
                upgrade_ref,
                upgrade_seq,
            },
        );
    }
    Some(PersistedState {
        manifest: Manifest {
            version,
            segments,
            memtable_watermark,
            epoch,
        },
        next_segment_id,
        next_chunk_id,
    })
}

// ───────────────────────── 落盘 ─────────────────────────

/// 原子写 manifest 到 `path`：`[crc32][payload]`，tmp + fsync + rename。
pub fn save(path: impl AsRef<Path>, state: &PersistedState) -> std::io::Result<()> {
    use std::io::Write;
    let payload = encode(state);
    let mut buf = Vec::with_capacity(payload.len() + 4);
    buf.extend_from_slice(&yt_wal::crc32(&payload).to_le_bytes());
    buf.extend_from_slice(&payload);

    let path = path.as_ref();
    let tmp = path.with_extension("tmp");
    {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?; // ★ fsync 后才 rename
    }
    std::fs::rename(&tmp, path)
}

/// 兼容旧的可选读取入口；数据库恢复必须使用 `try_load` 区分缺失与损坏。
pub fn load(path: impl AsRef<Path>) -> Option<PersistedState> {
    try_load(path).ok().flatten()
}

/// 只有首次创建时的缺文件允许空库；损坏或读失败必须拒绝恢复，避免删除状态丢失。
pub fn try_load(path: impl AsRef<Path>) -> std::io::Result<Option<PersistedState>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    let invalid = || {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid yiTrace manifest (CRC or format)",
        )
    };
    if bytes.len() < 4 {
        return Err(invalid());
    }
    let crc = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let payload = &bytes[4..];
    if crc != yt_wal::crc32(payload) {
        return Err(invalid());
    }
    decode(payload).map(Some).ok_or_else(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_roundtrip_with_deletion_and_upgrade() {
        // 造一个带删除位图 + upgrade 补写的 manifest,编码→解码逐字段还原。
        let chunk = ChunkId::new(9);
        let dv = DeletionVec::empty()
            .with_deleted(3, chunk)
            .with_deleted(70, ChunkId::new(10));
        let up = UpgradeColChunk::empty().with_patch(
            1,
            10,
            SpanFields {
                eval_score: Some(800),
                model: Some("qwen3".into()),
                ..Default::default()
            },
            ChunkId::new(11),
        );
        let mut segments = BTreeMap::new();
        segments.insert(
            5,
            SegmentEntry {
                segment_id: SegmentId::new(5),
                level: 1,
                state: SegState::Live,
                min_ts: -3,
                max_ts: 999,
                deletion_vec: Arc::new(dv),
                deletion_seq: 2,
                upgrade_ref: Some(Arc::new(up)),
                upgrade_seq: 1,
            },
        );
        let state = PersistedState {
            manifest: Manifest {
                version: ManifestVersion::new(7),
                segments,
                memtable_watermark: WalLsn::new(42),
                epoch: 3,
            },
            next_segment_id: 6,
            next_chunk_id: 12,
        };

        let back = decode(&encode(&state)).unwrap();
        assert_eq!(back.next_segment_id, 6);
        assert_eq!(back.next_chunk_id, 12);
        let m = &back.manifest;
        assert_eq!(m.version.get(), 7);
        assert_eq!(m.memtable_watermark.get(), 42);
        assert_eq!(m.epoch, 3);
        let e = &m.segments[&5];
        assert_eq!(
            (e.level, e.min_ts, e.max_ts, e.deletion_seq, e.upgrade_seq),
            (1, -3, 999, 2, 1)
        );
        // 删除位图还原:行 3、70 仍删,别的没删
        assert!(e.deletion_vec.is_deleted(3) && e.deletion_vec.is_deleted(70));
        assert!(!e.deletion_vec.is_deleted(4));
        // upgrade 补写还原:(1,10) 的 eval_score/model
        let patch = e.upgrade_ref.as_ref().unwrap().patch_for(1, 10).unwrap();
        assert_eq!(patch.eval_score, Some(800));
        assert_eq!(patch.model.as_deref(), Some("qwen3"));
    }

    #[test]
    fn corrupt_or_missing_manifest_loads_none() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "yt_manifest_{}_{}.dat",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        assert!(load(&path).is_none(), "缺文件 → None");

        let state = PersistedState {
            manifest: Manifest::empty(),
            next_segment_id: 1,
            next_chunk_id: 1,
        };
        save(&path, &state).unwrap();
        assert!(load(&path).is_some(), "存好能读回");
        // 改坏一字节 → None
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
        assert!(load(&path).is_none(), "crc 不符 → None");
        let _ = std::fs::remove_file(&path);
    }
}
