use super::*;
use std::sync::atomic::{AtomicU64, Ordering as O};

fn tmpdir() -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "yt_diskgraph_{}_{}",
        std::process::id(),
        N.fetch_add(1, O::Relaxed)
    ))
}

/// 测试用配置：大缓冲（不淘汰），按需指定 m。
fn cfg(m: usize) -> DiskGraphConfig {
    DiskGraphConfig {
        m,
        vector_cache_bytes: 1 << 20,
        ..Default::default()
    }
}

#[test]
fn persists_and_reopens_without_rebuild() {
    let dir = tmpdir();
    {
        let idx = DiskGraphIndex::open(&dir, 3, cfg(16)).unwrap();
        idx.index_embedding(1, 10, vec![0.0, 0.0, 0.0]);
        idx.index_embedding(2, 20, vec![1.0, 0.0, 0.0]);
        idx.index_embedding(3, 30, vec![5.0, 5.0, 5.0]);
        assert_eq!(idx.store().len(), 3);
    } // drop：文件已 fsync

    // 重开：不重放、不 rebuild，直接从盘读回。
    let idx = DiskGraphIndex::open(&dir, 3, cfg(16)).unwrap();
    assert_eq!(idx.store().len(), 3, "节点数从盘恢复");
    assert_eq!(idx.store().read_node(0).unwrap().trace_id, 1);
    assert_eq!(idx.store().read_vector(1).unwrap(), vec![1.0, 0.0, 0.0]);

    // 暴力搜索：查 [0.9,0,0] 最近的是 (2,20)，其次 (1,10)。
    let hits = idx.search(&[0.9, 0.0, 0.0], 2, &|_, _| true);
    assert_eq!((hits[0].0, hits[0].1), (2, 20));
    assert_eq!((hits[1].0, hits[1].1), (1, 10));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn filter_and_soft_delete_respected() {
    let dir = tmpdir();
    let idx = DiskGraphIndex::open(&dir, 2, cfg(16)).unwrap();
    idx.index_embedding(1, 10, vec![0.0, 0.0]);
    idx.index_embedding(1, 11, vec![0.1, 0.0]);
    idx.index_embedding(2, 20, vec![0.0, 0.1]); // 不满足谓词
                                                // 谓词只要 trace==1
    let hits = idx.search(&[0.0, 0.0], 5, &|t, _| t == 1);
    assert!(hits.iter().all(|&(t, _, _)| t == 1));
    assert_eq!(hits.len(), 2);
    // 软删 node 1 (span 11) 后不再出现。
    idx.store().mark_deleted(1).unwrap();
    let hits2 = idx.search(&[0.0, 0.0], 5, &|t, _| t == 1);
    assert_eq!(hits2.len(), 1);
    assert_eq!((hits2[0].0, hits2[0].1), (1, 10));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn vector_cache_serves_repeat_reads_from_memory() {
    let dir = tmpdir();
    let idx = DiskGraphIndex::open(&dir, 2, cfg(16)).unwrap();
    for i in 0..5u64 {
        idx.index_embedding(1, i, vec![i as f32, 0.0]);
    }
    // 反复读 node 4（最后写入、在缓存里）→ 命中累加。
    for _ in 0..3 {
        let _ = idx.store().read_vector(4).unwrap();
    }
    let (hits, _) = idx.store().cache_stats();
    assert!(hits >= 3, "重复读热向量命中缓存（不每次读盘）");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn cache_budget_caps_resident_memory_cold_vectors_go_to_disk() {
    // 用户场景：设个内存预算，索引比预算大 → 只有预算内的热向量常驻，冷的回磁盘。
    let dir = tmpdir();
    let dim = 4usize;
    let vec_bytes = dim * 4; // 每条向量 16 字节
                             // 预算 = 刚好 2 条向量。
    let cfg = DiskGraphConfig {
        m: 6,
        vector_cache_bytes: 2 * vec_bytes,
        ..Default::default()
    };
    let idx = DiskGraphIndex::open(&dir, dim, cfg).unwrap();
    // 灌 40 条（远超预算）。
    for i in 0..40u64 {
        idx.index_embedding(1, i, vec![i as f32; dim]);
    }
    // 常驻字节不超预算（“只用 1G”那种上界）。
    let (resident, budget) = idx.store().cache_mem();
    assert!(resident <= budget, "常驻 {resident} 不超预算 {budget}");
    assert!(resident <= 2 * vec_bytes, "最多 2 条向量常驻");

    // 扫全部 40 条：预算只容 2 条 → 大量回磁盘（冷数据去磁盘找），且值都正确。
    let (_, miss_before) = idx.store().cache_stats();
    for i in 0..40u32 {
        assert_eq!(
            idx.store().read_vector(i).unwrap(),
            vec![i as f32; dim],
            "冷向量从磁盘读回值正确"
        );
    }
    let (_, miss_after) = idx.store().cache_stats();
    assert!(
        miss_after - miss_before >= 38,
        "预算只容 2 条，扫 40 条几乎全回磁盘"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// 确定性伪随机（LCG），不依赖 rand、可复算。
struct Lcg(u64);
impl Lcg {
    fn next_f32(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as f32) / (1u64 << 31) as f32
    }
    fn vec(&mut self, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| self.next_f32()).collect()
    }
}

#[test]
fn graph_search_recall_vs_brute_force() {
    // 图导航的核心：beam search 召回 ≈ 暴力 ground-truth（证明"按需读页的图遍历"找得到近邻）。
    let dir = tmpdir();
    let dim = 8usize;
    let idx = DiskGraphIndex::open(
        &dir,
        dim,
        DiskGraphConfig {
            m: 8,
            ef_construction: 64,
            ef_search: 64,
            vector_cache_bytes: 1 << 20,
            metric: Metric::L2,
        },
    )
    .unwrap();
    let mut rng = Lcg(0x51A6_3D11);
    for i in 0..150u64 {
        idx.index_embedding(1, i, rng.vec(dim));
    }
    // 多个查询点求平均召回@10。
    let k = 10;
    let mut hit_sum = 0usize;
    let mut probes = 0usize;
    let mut q = Lcg(0xBEEF);
    for _ in 0..8 {
        let query = q.vec(dim);
        let truth: std::collections::HashSet<(u64, u64)> = idx
            .brute_force(&query, k, &|_, _| true)
            .into_iter()
            .map(|(t, s, _)| (t, s))
            .collect();
        let got = idx.search(&query, k, &|_, _| true);
        hit_sum += got
            .iter()
            .filter(|(t, s, _)| truth.contains(&(*t, *s)))
            .count();
        probes += 1;
    }
    let recall = hit_sum as f32 / (k * probes) as f32;
    eprintln!("[磁盘图索引] 召回@{k} = {recall:.2}");
    assert!(recall >= 0.85, "beam 召回应接近暴力，实测 {recall:.2}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn builds_multiple_layers_and_persists_them() {
    // 多层 HNSW：灌够多点会建出 level≥1 的层；flush 后重启从上层快照恢复入口/上层图（不靠扫描重建）。
    let dir = tmpdir();
    let dim = 8usize;
    let mut rng = Lcg(0x7A5E);
    let top_level;
    {
        let idx = DiskGraphIndex::open(
            &dir,
            dim,
            DiskGraphConfig {
                m: 8,
                ef_construction: 48,
                ef_search: 48,
                vector_cache_bytes: 1 << 20,
                metric: Metric::L2,
            },
        )
        .unwrap();
        for i in 0..300u64 {
            idx.index_embedding(1, i, rng.vec(dim));
        }
        top_level = idx.entry_level();
        assert!(
            top_level >= 1,
            "300 点应建出多层（入口层级≥1），实测 {top_level}"
        );
        idx.flush(); // 持久上层图 + 入口
    }
    // 重开：upper 快照在 → 入口/上层图从快照恢复（入口层级一致），搜索照常。
    let idx = DiskGraphIndex::open(&dir, dim, cfg(8)).unwrap();
    assert_eq!(idx.entry_level(), top_level, "重启后入口层级从快照恢复一致");
    let probe = idx.store().read_vector(42).unwrap();
    let hits = idx.search(&probe, 5, &|_, _| true);
    assert_eq!(
        (hits[0].0, hits[0].1),
        (1, 42),
        "多层重启后搜索查询点自身排第一"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn restart_then_graph_search_still_works() {
    // 图结构（邻边）落盘，重启后**不 rebuild** 直接图搜索。
    let dir = tmpdir();
    let dim = 6usize;
    let mut rng = Lcg(0x1234);
    let probe;
    {
        let idx = DiskGraphIndex::open(&dir, dim, cfg(8)).unwrap();
        for i in 0..60u64 {
            idx.index_embedding(1, i, rng.vec(dim));
        }
        probe = idx.store().read_vector(7).unwrap(); // 拿 node 7 的向量当查询
    } // drop

    let idx = DiskGraphIndex::open(&dir, dim, cfg(8)).unwrap();
    // 不重放、不 rebuild：查 node 7 自身 → 应排第一（距离 ~0）。
    let hits = idx.search(&probe, 5, &|_, _| true);
    assert_eq!(
        (hits[0].0, hits[0].1),
        (1, 7),
        "重启后图搜索照常，查询点自身排第一"
    );
    assert!(hits[0].2 < 1e-3);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn in_graph_filter_returns_only_matching_sorted() {
    // 进图过滤：导航穿过不满足谓词的点、只收满足的，结果按距离升序。
    let dir = tmpdir();
    let dim = 6usize;
    let idx = DiskGraphIndex::open(&dir, dim, cfg(8)).unwrap();
    let mut rng = Lcg(0xACED);
    for i in 0..120u64 {
        let trace = if i % 5 == 0 { 1 } else { 0 }; // 约 20% 命中
        idx.index_embedding(trace, i, rng.vec(dim));
    }
    let probe = idx.store().read_vector(10).unwrap();
    let hits = idx.search(&probe, 10, &|t, _| t == 1);
    assert!(!hits.is_empty());
    assert!(hits.iter().all(|&(t, _, _)| t == 1), "只返回满足谓词的点");
    assert!(hits.windows(2).all(|w| w[0].2 <= w[1].2), "按距离升序");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn cosine_mode_search_recalls_brute_force() {
    // Cosine 模式：图检索召回应 ≈ 同一索引的暴力精确搜索（与 L2 同款断言，验证归一化路径对齐）。
    let dir = tmpdir();
    let dim = 8usize;
    let cfg = DiskGraphConfig {
        m: 8,
        ef_construction: 64,
        ef_search: 64,
        vector_cache_bytes: 1 << 20,
        metric: Metric::Cosine,
    };
    let idx = DiskGraphIndex::open(&dir, dim, cfg).unwrap();
    let mut rng = Lcg(0xC05E);
    // 每点用不同span，不能把同一span的150个历史向量当150条真值。
    for span_id in 0..150u64 {
        idx.index_embedding(1, span_id, rng.vec(dim));
    }
    let k = 10;
    let mut hit_sum = 0usize;
    let mut probes = 0usize;
    for _ in 0..8 {
        let query = rng.vec(dim);
        let truth: std::collections::HashSet<(u64, u64)> = idx
            .brute_force(&query, k, &|_, _| true)
            .into_iter()
            .map(|(t, s, _)| (t, s))
            .collect();
        let got = idx.search(&query, k, &|_, _| true);
        hit_sum += got
            .iter()
            .filter(|(t, s, _)| truth.contains(&(*t, *s)))
            .count();
        probes += 1;
    }
    let recall = hit_sum as f32 / (k * probes) as f32;
    eprintln!("[磁盘图索引·cosine] 召回@{k} = {recall:.2}");
    assert!(recall >= 0.85, "cosine 召回应接近暴力，实={recall:.2}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn neighbors_round_trip_in_place() {
    // 邻边原地改写 + 读回（图导航阶段的基础）。
    let dir = tmpdir();
    let idx = DiskGraphIndex::open(&dir, 2, cfg(4)).unwrap();
    for i in 0..6u64 {
        idx.index_embedding(1, i, vec![i as f32, 0.0]);
    }
    idx.store().set_neighbors(0, &[1, 2, 3]).unwrap();
    assert_eq!(idx.store().read_node(0).unwrap().neighbors, vec![1, 2, 3]);
    // 改写覆盖。
    idx.store().set_neighbors(0, &[4, 5]).unwrap();
    assert_eq!(idx.store().read_node(0).unwrap().neighbors, vec![4, 5]);
    // 重开后邻边还在。
    drop(idx);
    let idx2 = DiskGraphIndex::open(&dir, 2, cfg(4)).unwrap();
    assert_eq!(idx2.store().read_node(0).unwrap().neighbors, vec![4, 5]);
    let _ = std::fs::remove_dir_all(&dir);
}
