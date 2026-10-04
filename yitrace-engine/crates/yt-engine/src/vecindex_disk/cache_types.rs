// ───────────────────────── 向量缓冲池（按字节预算的 LRU） ─────────────────────────

/// 向量缓冲池：**按内存预算（字节）** 缓存热向量，对齐 graph_index 的 `vector_buffers`。
/// 例：预算 1GiB、索引 10GiB → 只有约 1GiB 的热向量常驻，冷向量淘汰、再访问回磁盘读。
///
/// **O(1) 访问**：每项记一个访问 tick，命中只更 tick（不再每次线性扫整个缓存更 LRU 顺序——那是建图/
/// 检索慢的主因之一）；仅在**超预算时**才 O(n) 批量淘汰最久未用的、一次腾出 ~10% 余量（摊销，命中区无淘汰）。
/// 向量存 `Arc<[f32]>`：命中返回 Arc 克隆（仅加引用计数），不复制 dim 个 f32。
struct VecCache {
    budget_bytes: usize,
    cur_bytes: usize,
    map: FastMap<u64, (Arc<[f32]>, u64)>,
    tick: u64,
    hits: u64,
    misses: u64,
}

impl VecCache {
    fn new(budget_bytes: usize) -> Self {
        Self {
            budget_bytes,
            cur_bytes: 0,
            map: FastMap::default(),
            tick: 0,
            hits: 0,
            misses: 0,
        }
    }

    fn get(&mut self, id: u64) -> Option<Arc<[f32]>> {
        self.tick += 1;
        let t = self.tick;
        if let Some(e) = self.map.get_mut(&id) {
            e.1 = t;
            self.hits += 1;
            Some(e.0.clone())
        } else {
            self.misses += 1;
            None
        }
    }

    fn put(&mut self, id: u64, v: Arc<[f32]>) {
        self.tick += 1;
        let bytes = v.len() * 4;
        match self.map.insert(id, (v, self.tick)) {
            Some((old, _)) => self.cur_bytes = self.cur_bytes + bytes - old.len() * 4,
            None => self.cur_bytes += bytes,
        }
        if self.cur_bytes > self.budget_bytes {
            self.evict();
        }
    }

    /// 超预算时批量淘汰最久未用的，腾到 ~90% 预算（一次腾够、不是每 put 都淘）。
    fn evict(&mut self) {
        let target = (self.budget_bytes * 9 / 10).max(1);
        let mut by_tick: Vec<(u64, u64, usize)> = self
            .map
            .iter()
            .map(|(&id, (v, t))| (*t, id, v.len() * 4))
            .collect();
        by_tick.sort_unstable_by_key(|x| x.0);
        for (_, id, bytes) in by_tick {
            if self.cur_bytes <= target || self.map.len() <= 1 {
                break;
            }
            self.map.remove(&id);
            self.cur_bytes -= bytes;
        }
    }
}

/// 节点记录缓存（图拓扑，对齐 graph_index 把邻边/元数据放 shared_buffers）：消除每次访问的 pread 系统调用。
/// 写穿（write_node 同步更新），O(1) 访问的 tick-LRU，按**条数**封顶（节点记录小，默认上限大）。
struct NodeCache {
    cap: usize,
    map: FastMap<u32, (Arc<NodeRec>, u64)>,
    tick: u64,
}

impl NodeCache {
    fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            map: FastMap::default(),
            tick: 0,
        }
    }
    fn get(&mut self, id: u32) -> Option<Arc<NodeRec>> {
        self.tick += 1;
        let t = self.tick;
        if let Some(e) = self.map.get_mut(&id) {
            e.1 = t;
            Some(e.0.clone()) // Arc 克隆 = 加引用计数，不复制 NodeRec
        } else {
            None
        }
    }
    fn put(&mut self, id: u32, rec: Arc<NodeRec>) {
        self.tick += 1;
        self.map.insert(id, (rec, self.tick));
        if self.map.len() > self.cap {
            let target = self.cap * 9 / 10;
            let mut by_tick: Vec<(u64, u32)> =
                self.map.iter().map(|(&id, (_, t))| (*t, id)).collect();
            by_tick.sort_unstable_by_key(|x| x.0);
            for (_, id) in by_tick {
                if self.map.len() <= target {
                    break;
                }
                self.map.remove(&id);
            }
        }
    }
}

/// 磁盘图索引参数（对齐 graph_index 的可调项）。
#[derive(Clone, Copy, Debug)]
pub struct DiskGraphConfig {
    /// 每点最大邻边数（建图参数，对齐 graph_index 的 `m`）。
    pub m: usize,
    /// **向量缓冲池内存预算（字节）**，对齐 graph_index 的 `vector_buffers`。热向量常驻、冷的回磁盘。
    /// 例：`1 << 30` = 1GiB。
    pub vector_cache_bytes: usize,
    /// 建图时候选列表宽度（对齐 `ef_construction`）。越大召回越好、建图越慢。
    pub ef_construction: usize,
    /// 查询时候选列表宽度（对齐 `hnsw_ef_search`）。越大召回越高、查询越慢；实际取 `max(ef_search, k)`。
    pub ef_search: usize,
    /// 距离度量。L2（默认）；Cosine 在索引/查询时归一化后复用 L2 路径；InnerProduct 走负点积。
    pub metric: Metric,
}

impl Default for DiskGraphConfig {
    fn default() -> Self {
        Self {
            m: 16,
            vector_cache_bytes: 256 << 20,
            ef_construction: 64,
            ef_search: 100,
            metric: Metric::L2,
        }
    }
}

impl DiskGraphConfig {
    pub fn with_cache_bytes(mut self, bytes: usize) -> Self {
        self.vector_cache_bytes = bytes;
        self
    }
    pub fn with_m(mut self, m: usize) -> Self {
        self.m = m;
        self
    }
    pub fn with_ef_construction(mut self, ef: usize) -> Self {
        self.ef_construction = ef;
        self
    }
    pub fn with_ef_search(mut self, ef: usize) -> Self {
        self.ef_search = ef;
        self
    }
    pub fn with_metric(mut self, metric: Metric) -> Self {
        self.metric = metric;
        self
    }
}

/// f32 全序包装（NaN 也定序），好进二叉堆。
#[derive(Clone, Copy, PartialEq)]
struct OrdF32(f32);
impl Eq for OrdF32 {}
impl PartialOrd for OrdF32 {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for OrdF32 {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&o.0)
    }
}

// ───────────────────────── 节点记录（定长槽位） ─────────────────────────

/// 一个节点的内存视图（从盘上定长记录解出）。
#[derive(Clone, Debug, PartialEq)]
pub struct NodeRec {
    pub tenant_id: Option<u64>,
    pub trace_id: u64,
    pub span_id: u64,
    pub deleted: bool,
    /// HNSW 层级（0=只在底层；越高越稀疏，做导航）。复用记录里原 pad 字节，不涨记录大小。
    pub level: u8,
    /// **底层（level 0）邻居** node_id。上层邻居在内存的 upper 映射里（稀疏、小）。
    pub neighbors: Vec<u32>,
}

// ───────────────────────── 磁盘图存储 ─────────────────────────
