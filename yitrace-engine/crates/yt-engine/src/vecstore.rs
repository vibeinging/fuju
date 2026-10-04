//! vecstore.rs —— **向量独立落盘**。embedding 与 trace 存储解耦,自己一套追加文件(贴近真实 graph_index
//! 的独立向量存储)。重启 `recover` 时重载、喂回图索引重建。
//!
//! 为什么向量要单独持久:它**不在 trace 数据里**(外部 embedder 算的、`index_embedding` 旁路进来),
//! 段里推不出来 → BM25/属性边车能从段重建,但向量不能,只能持久化。
//!
//! 文件 = 连续追加记录,每条 `[trace u64][span u64][dim u32][f32×dim][crc32 u32]`(crc 覆盖前面所有字段)。
//! append-only;读时逐条校验 crc,遇撕裂/损坏即停(与 WAL 撕裂尾同样语义,不返回脏向量)。
#![allow(dead_code)]

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// 追加一条向量(append + fsync)。一个 (trace,span) 多次写以**最后一条**为准(load 时后写覆盖先写)。
pub fn append(path: impl AsRef<Path>, trace: u64, span: u64, vec: &[f32]) -> std::io::Result<()> {
    let mut b = Vec::with_capacity(20 + vec.len() * 4 + 4);
    b.extend_from_slice(&trace.to_le_bytes());
    b.extend_from_slice(&span.to_le_bytes());
    b.extend_from_slice(&(vec.len() as u32).to_le_bytes());
    for &x in vec {
        b.extend_from_slice(&x.to_le_bytes());
    }
    let crc = yt_wal::crc32(&b);
    b.extend_from_slice(&crc.to_le_bytes());

    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    f.write_all(&b)?;
    f.sync_data()
}

/// 读全部向量,按写入顺序返回 `((trace,span), vec)`。撕裂/损坏尾被丢弃。
/// 同一 (trace,span) 多条都返回(调用方按顺序喂回索引,后写自然覆盖先写)。
pub fn load(path: impl AsRef<Path>) -> Vec<((u64, u64), Vec<f32>)> {
    let bytes = std::fs::read(path).unwrap_or_default();
    let mut out = Vec::new();
    let mut i = 0usize;
    loop {
        if i + 20 > bytes.len() {
            break; // 不足 trace(8)+span(8)+dim(4)
        }
        let trace = u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        let span = u64::from_le_bytes(bytes[i + 8..i + 16].try_into().unwrap());
        let dim = u32::from_le_bytes(bytes[i + 16..i + 20].try_into().unwrap()) as usize;
        let rec_len = 20 + dim * 4 + 4;
        if i + rec_len > bytes.len() {
            break; // 撕裂尾
        }
        let crc = u32::from_le_bytes(bytes[i + rec_len - 4..i + rec_len].try_into().unwrap());
        if crc != yt_wal::crc32(&bytes[i..i + rec_len - 4]) {
            break; // 损坏 → 停
        }
        let mut vec = Vec::with_capacity(dim);
        for j in 0..dim {
            let o = i + 20 + j * 4;
            vec.push(f32::from_le_bytes(bytes[o..o + 4].try_into().unwrap()));
        }
        out.push(((trace, span), vec));
        i += rec_len;
    }
    out
}

/// 租户向量使用独立版本文件，保留旧 vectors.dat 的逐字节读取兼容。
pub fn append_scoped(
    path: impl AsRef<Path>,
    tenant: Option<u64>,
    trace: u64,
    span: u64,
    vector: &[f32],
) -> std::io::Result<()> {
    let mut bytes = Vec::new();
    bytes.push(tenant.is_some() as u8);
    bytes.extend_from_slice(&tenant.unwrap_or(0).to_le_bytes());
    bytes.extend_from_slice(&trace.to_le_bytes());
    bytes.extend_from_slice(&span.to_le_bytes());
    bytes.extend_from_slice(&(vector.len() as u32).to_le_bytes());
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    let crc = yt_wal::crc32(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(&bytes)?;
    file.sync_data()
}
pub fn load_scoped(path: impl AsRef<Path>) -> Vec<(crate::ScopedSpanKey, Vec<f32>)> {
    let bytes = std::fs::read(path).unwrap_or_default();
    let mut result = Vec::new();
    let mut offset = 0;
    while offset + 33 <= bytes.len() {
        let header = &bytes[offset..];
        let dim = u32::from_le_bytes(header[25..29].try_into().unwrap()) as usize;
        let Some(size) = dim.checked_mul(4).and_then(|n| n.checked_add(33)) else {
            break;
        };
        if offset + size > bytes.len() {
            break;
        }
        let frame = &bytes[offset..offset + size];
        let crc = u32::from_le_bytes(frame[size - 4..].try_into().unwrap());
        if crc != yt_wal::crc32(&frame[..size - 4]) || frame[0] > 1 {
            break;
        }
        let tenant = (frame[0] == 1).then(|| u64::from_le_bytes(frame[1..9].try_into().unwrap()));
        let trace = u64::from_le_bytes(frame[9..17].try_into().unwrap());
        let span = u64::from_le_bytes(frame[17..25].try_into().unwrap());
        let vector = frame[29..size - 4]
            .chunks_exact(4)
            .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
            .collect();
        result.push(((tenant, trace, span), vector));
        offset += size;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "yt_vec_{}_{}.dat",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn append_then_load_roundtrip() {
        let p = temp();
        append(&p, 1, 10, &[0.0, 1.5, -2.0]).unwrap();
        append(&p, 2, 20, &[3.0]).unwrap();
        let v = load(&p);
        assert_eq!(v.len(), 2);
        assert_eq!(v[0], ((1, 10), vec![0.0, 1.5, -2.0]));
        assert_eq!(v[1], ((2, 20), vec![3.0]));
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn scoped_roundtrip_preserves_nullable_tenant_and_torn_tail() {
        let path = temp();
        append_scoped(&path, None, 1, 2, &[1.]).unwrap();
        append_scoped(&path, Some(0), 1, 2, &[2.]).unwrap();
        append_scoped(&path, Some(u64::MAX), 1, 2, &[3.]).unwrap();
        assert_eq!(
            load_scoped(&path),
            vec![
                ((None, 1, 2), vec![1.]),
                ((Some(0), 1, 2), vec![2.]),
                ((Some(u64::MAX), 1, 2), vec![3.])
            ]
        );
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.truncate(bytes.len() - 1);
        std::fs::write(&path, bytes).unwrap();
        assert_eq!(load_scoped(&path).len(), 2);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn torn_tail_dropped() {
        let p = temp();
        append(&p, 1, 10, &[1.0, 2.0]).unwrap();
        append(&p, 2, 20, &[3.0, 4.0]).unwrap();
        // 砍掉最后 3 字节(破坏第二条 crc)
        let mut bytes = std::fs::read(&p).unwrap();
        bytes.truncate(bytes.len() - 3);
        std::fs::write(&p, &bytes).unwrap();
        let v = load(&p);
        assert_eq!(v.len(), 1, "撕裂的第二条被丢,第一条完好");
        assert_eq!(v[0], ((1, 10), vec![1.0, 2.0]));
        let _ = std::fs::remove_file(&p);
    }
}
