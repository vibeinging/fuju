// ───────────────────────── GraphIndex 实现（图导航：NSW 落盘版 + beam search） ─────────────────────────

/// HNSW 最高层级上限（level > 16 在任何现实规模都几乎不可能，封顶防失控）。
const MAX_LEVEL: u8 = 16;

/// splitmix64：把 node_id 散成均匀位，用来确定性地定层级（不依赖 rand / Date，可复算）。
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

/// 磁盘型图向量索引（**多层 HNSW**）。底层(level 0)邻边 + 向量在磁盘、按需读；上层图稀疏、常驻内存、
/// 快照持久（导航骨架）。查询从最高层入口贪心下沉、底层 beam 细搜；底层 beam 的收点谓词驱动停止 +
/// 导航穿过不满足点 ⇒ **进图过滤**（带过滤召回不塌）。append 友好：插入只写不刷，靠 `flush` 批量持久。
pub struct DiskGraphIndex {
    store: DiskGraphStore,
    ef_construction: usize,
    ef_search: usize,
    m: usize,
    max_deg: usize,
    metric: Metric,
    ml: f64, // 层级归一 = 1/ln(m)
    /// 上层（level≥1）邻边：稀疏、小，常驻内存。键 (node_id, level)。
    upper: Mutex<HashMap<(u32, u8), Vec<u32>>>,
    /// 入口点 (node_id, 它的 level)。None = 空图。
    entry: Mutex<Option<(u32, u8)>>,
    upper_path: PathBuf,
    mutation: Mutex<()>,
    identities: Mutex<Option<FastMap<crate::ScopedSpanKey, u32>>>,
}

impl DiskGraphIndex {
    pub fn open(dir: impl AsRef<Path>, dim: usize, cfg: DiskGraphConfig) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let store = DiskGraphStore::open(&dir, dim, cfg)?;
        let m = store.m();
        let max_deg = store.max_deg();
        let upper_path = dir.join("upper");
        let (upper, mut entry) = load_upper(&upper_path);
        // 无上层快照（没 flush 过）但盘上有节点 → 从节点 level 重建入口（退化但可搜，不至于搜啥都空）。
        if entry.is_none() && store.len() > 0 {
            let mut best: Option<(u32, u8)> = None;
            for id in 0..store.len() as u32 {
                if let Ok(r) = store.read_node(id) {
                    if !r.deleted && best.map(|(_, l)| r.level > l).unwrap_or(true) {
                        best = Some((id, r.level));
                    }
                }
            }
            entry = best;
        }
        Ok(Self {
            metric: store.metric(),
            store,
            ef_construction: cfg.ef_construction.max(cfg.m),
            ef_search: cfg.ef_search.max(1),
            m,
            max_deg,
            ml: 1.0 / (m as f64).max(2.0).ln(),
            upper: Mutex::new(upper),
            entry: Mutex::new(entry),
            upper_path,
            mutation: Mutex::new(()),
            identities: Mutex::new(None),
        })
    }

    pub fn store(&self) -> &DiskGraphStore {
        &self.store
    }

    /// 节点向量与查询的距离（按需读向量，走缓冲；Arc 命中不复制）。读失败返回 +inf。
    /// L2 / Cosine：归一化（cosine 在 index/search 入口做）后用 l2_sq，两者单调等价、复用整条建图路径。
    /// InnerProduct：负点积（点积越大 = 越「近」，取负进堆排序）。
    fn dist(&self, query: &[f32], id: u32) -> f32 {
        match self.store.read_vector_arc(id) {
            Ok(v) => {
                if self.metric == Metric::InnerProduct {
                    -simd::dot(query, &v)
                } else {
                    l2_sq(query, &v)
                }
            }
            Err(_) => f32::INFINITY,
        }
    }

    /// 由 node_id 确定性定层级：floor(-ln(u) * ml)，u∈(0,1) 由 id 哈希得到。封顶 MAX_LEVEL。
    fn level_for(&self, node_id: u32) -> u8 {
        let h = splitmix64(node_id as u64 ^ 0xD1B54A32D192ED03);
        let u = (((h >> 11) as f64) / ((1u64 << 53) as f64)).max(1e-12);
        ((-u.ln() * self.ml).floor() as i64).clamp(0, MAX_LEVEL as i64) as u8
    }

    fn neighbors_at(&self, id: u32, level: u8) -> Vec<u32> {
        if level == 0 {
            self.store
                .read_node(id)
                .map(|r| r.neighbors)
                .unwrap_or_default()
        } else {
            self.upper
                .lock()
                .unwrap()
                .get(&(id, level))
                .cloned()
                .unwrap_or_default()
        }
    }

    fn set_neighbors_at(&self, id: u32, level: u8, neighbors: &[u32]) -> std::io::Result<()> {
        if level == 0 {
            self.store.set_neighbors(id, neighbors)
        } else {
            self.upper
                .lock()
                .unwrap()
                .insert((id, level), neighbors.to_vec());
            Ok(())
        }
    }

    /// **邻居选择启发式**（hnswlib heuristic）：从候选里选出与查询点分布更散的 m 个邻居，
    /// 替代朴素的「取最近 m 个」。后者在高维下会让近邻簇聚成一团、图连通性变差、召回掉。
    ///
    /// 规则：候选按到 query 的距离升序排；依次考察 e，仅当 e 比**所有已选入的点**都更靠近 query
    /// （即 dist(query,e) < dist(e, r) 对每个已选 r 成立）才选入 —— e 没被任何已选点「挡住」，
    /// 保证选入的点彼此分散。距离函数由 `dist` 闭包给出（复用 self.dist 的按需读 + 缓冲）。
    ///
    /// `candidates` = (id, dist_to_query) 升序；排除 query 自身（id）。
    fn select_neighbors(
        &self,
        _query: &[f32],
        candidates: &[(u32, f32)],
        m: usize,
        dist: &dyn Fn(&[f32], u32) -> f32,
    ) -> Vec<u32> {
        let mut kept: Vec<(u32, f32)> = Vec::with_capacity(m);
        for &(e, de) in candidates {
            if kept.len() >= m {
                break;
            }
            // e 与所有已选点 r 比：只要有一个 r 挡住 e（dist(e,r) < dist(query,e)），丢 e。
            let dominated = kept.iter().any(|&(r, dr)| {
                // dist(query, e) = de；dist(query, r) = dr；这里算 dist(e, r)。
                let er = dist(
                    &self
                        .store
                        .read_vector_arc(e)
                        .unwrap_or_else(|_| Arc::from(Vec::new())),
                    r,
                );
                er < de.max(dr)
            });
            if !dominated {
                kept.push((e, de));
            }
        }
        kept.into_iter().map(|(id, _)| id).collect()
    }

    /// HNSW search-layer：在某一层从 `entries` 出发 beam 扩展。`admit` 决定收点 + 驱动停止，
    /// 导航穿过所有未访问邻居（含 admit=false 的）⇒ 进图过滤。返回 (id, 距离) 升序。
    fn search_layer(
        &self,
        query: &[f32],
        entries: &[u32],
        ef: usize,
        level: u8,
        admit: &dyn Fn(u32) -> bool,
    ) -> Vec<(u32, f32)> {
        let mut visited: FastSet<u32> = FastSet::default();
        let mut frontier: BinaryHeap<Reverse<(OrdF32, u32)>> = BinaryHeap::new();
        let mut result: BinaryHeap<(OrdF32, u32)> = BinaryHeap::new();

        for &e in entries {
            if visited.insert(e) {
                let d = self.dist(query, e);
                frontier.push(Reverse((OrdF32(d), e)));
                if admit(e) {
                    result.push((OrdF32(d), e));
                }
            }
        }

        while let Some(Reverse((cd, cur))) = frontier.pop() {
            if result.len() >= ef {
                if let Some(&(worst, _)) = result.peek() {
                    if cd > worst {
                        break;
                    }
                }
            }
            // 取 cur 在该层的邻居：level 0（热）借 Arc 不克隆；上层取稀疏小表。
            let arc0 = if level == 0 {
                self.store.node_arc(cur).ok()
            } else {
                None
            };
            let upper_v: Vec<u32>;
            let nbrs: &[u32] = if let Some(n) = &arc0 {
                &n.neighbors
            } else if level == 0 {
                &[]
            } else {
                upper_v = self
                    .upper
                    .lock()
                    .unwrap()
                    .get(&(cur, level))
                    .cloned()
                    .unwrap_or_default();
                &upper_v
            };
            for &nb in nbrs {
                if !visited.insert(nb) {
                    continue;
                }
                let d = self.dist(query, nb);
                frontier.push(Reverse((OrdF32(d), nb)));
                if admit(nb) {
                    result.push((OrdF32(d), nb));
                    if result.len() > ef {
                        result.pop();
                    }
                }
            }
        }

        let mut v: Vec<(u32, f32)> = result.into_iter().map(|(d, i)| (i, d.0)).collect();
        v.sort_by(|a, b| a.1.total_cmp(&b.1));
        v
    }

    /// 多层插入：顶层贪心下沉找入口 → 各层 search_layer 连边 + 反向边度数剪枝；新点层级更高则成为新入口。
    fn insert(
        &self,
        tenant_id: Option<u64>,
        trace_id: u64,
        span_id: u64,
        vector: &[f32],
    ) -> std::io::Result<()> {
        // 先占槽得 id（层级由 id 确定性算），再补写 level。
        let Some(id) = self
            .store
            .add_node_scoped(tenant_id, trace_id, span_id, vector, 0)?
        else {
            return Ok(());
        };
        let level = self.level_for(id);
        if level > 0 {
            self.store.set_level(id, level)?;
        }

        let entry = *self.entry.lock().unwrap();
        let Some((mut ep, top)) = entry else {
            *self.entry.lock().unwrap() = Some((id, level)); // 第一个点 = 入口
            return Ok(());
        };

        let alive = |q: u32| self.store.node_arc(q).map(|a| !a.deleted).unwrap_or(false);

        // 1) 顶层贪心下沉到 level+1，找靠近插入点的入口（ef=1）。
        let mut lc = top;
        while lc > level {
            let r = self.search_layer(vector, &[ep], 1, lc, &alive);
            if let Some(&(c, _)) = r.first() {
                ep = c;
            }
            lc -= 1;
        }

        // 2) 从 min(level,top) 到 0：search_layer(ef_construction) → 启发式选邻居连边 + 反向剪枝。
        let mut entries = vec![ep];
        for lc in (0..=level.min(top)).rev() {
            let cap = if lc == 0 { self.max_deg } else { self.m };
            let cands = self.search_layer(vector, &entries, self.ef_construction, lc, &alive);
            // 启发式选 m 个分散邻居（候选已升序、排除自身 id）。
            let cands_clean: Vec<(u32, f32)> =
                cands.into_iter().filter(|&(c, _)| c != id).collect();
            let dist = |q: &[f32], x: u32| self.dist(q, x);
            let chosen = self.select_neighbors(vector, &cands_clean, self.m, &dist);
            self.set_neighbors_at(id, lc, &chosen)?;

            for &nb in &chosen {
                let mut adj = self.neighbors_at(nb, lc);
                if !adj.contains(&id) {
                    adj.push(id);
                }
                if adj.len() > cap {
                    // 反向边也用启发式：以 nb 为查询点，从它的邻边里选 cap 个分散的。
                    let base = self
                        .store
                        .read_vector_arc(nb)
                        .unwrap_or_else(|_| Arc::from(Vec::new()));
                    let mut scored: Vec<(u32, f32)> =
                        adj.iter().map(|&x| (x, self.dist(&base, x))).collect();
                    scored.sort_by(|a, b| a.1.total_cmp(&b.1));
                    let dist2 = |q: &[f32], x: u32| self.dist(q, x);
                    adj = self.select_neighbors(&base, &scored, cap, &dist2);
                }
                self.set_neighbors_at(nb, lc, &adj)?;
            }
            // 下一层的入口 = 这一层找到的近邻。
            entries = if cands_clean.is_empty() {
                vec![ep]
            } else {
                cands_clean.iter().map(|&(c, _)| c).collect()
            };
        }

        // 3) 新点层级更高 → 成为新入口。
        if level > top {
            *self.entry.lock().unwrap() = Some((id, level));
        }
        Ok(())
    }

    fn ensure_identities(&self) -> std::io::Result<()> {
        let mut identities = self.identities.lock().unwrap();
        if identities.is_some() {
            return Ok(());
        }
        // 只在第一次向量操作读取节点身份，恢复入口不扫描向量或整个节点文件。
        let mut latest = FastMap::default();
        for id in 0..self.store.len() as u32 {
            let node = self.store.read_node(id)?;
            if !node.deleted {
                latest.insert((node.tenant_id, node.trace_id, node.span_id), id);
            }
        }
        *identities = Some(latest);
        Ok(())
    }

    fn upsert(
        &self,
        tenant_id: Option<u64>,
        trace_id: u64,
        span_id: u64,
        vector: &[f32],
    ) -> std::io::Result<()> {
        let _mutation = self.mutation.lock().unwrap();
        if vector.len() != self.store.dim || vector.iter().any(|v| !v.is_finite()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "embedding dimension or value is invalid",
            ));
        }
        self.ensure_identities()?;
        let key = (tenant_id, trace_id, span_id);
        let previous = self
            .identities
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .get(&key)
            .copied();
        if let Some(old) = previous {
            if self.store.read_vector(old)? == vector {
                return Ok(());
            }
        }
        let new_id = self.store.len() as u32;
        self.insert(tenant_id, trace_id, span_id, vector)?;
        if let Some(old) = previous {
            self.store.mark_deleted(old)?;
        }
        self.identities
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .insert(key, new_id);
        // 向量更新是独立提交；其它进程重开时需要同批上层入口和新的身份版本。
        self.publish_generation()?;
        Ok(())
    }

    fn publish_generation(&self) -> std::io::Result<()> {
        let generation_path = self.store.dir.join("generation");
        let generation = std::fs::read(&generation_path)
            .ok()
            .and_then(|b| b.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0);
        let tmp = generation_path.with_extension("tmp");
        std::fs::write(&tmp, generation.wrapping_add(1).to_le_bytes())?;
        std::fs::rename(&tmp, generation_path)
    }

    /// 暴力精确搜索（测试用 ground-truth；带过滤、跳软删）。
    pub fn brute_force(
        &self,
        query: &[f32],
        k: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(u64, u64, f32)> {
        // Cosine：归一化查询（与索引时的归一化对齐）。IP：不归一化、不取 sqrt（距离已是 -dot）。
        let q: Vec<f32> = if self.metric == Metric::Cosine {
            simd::normalize(query).0
        } else {
            query.to_vec()
        };
        let finalize = |d: f32| -> f32 {
            if self.metric == Metric::InnerProduct {
                d
            } else {
                d.max(0.0).sqrt()
            }
        };
        let mut scored: Vec<(f32, u64, u64)> = Vec::new();
        for id in 0..self.store.len() as u32 {
            let Ok(node) = self.store.read_node(id) else {
                continue;
            };
            if node.deleted || !filter(node.trace_id, node.span_id) {
                continue;
            }
            scored.push((finalize(self.dist(&q, id)), node.trace_id, node.span_id));
        }
        scored.sort_by(|a, b| a.0.total_cmp(&b.0));
        scored.truncate(k);
        scored.into_iter().map(|(d, t, s)| (t, s, d)).collect()
    }

    /// 当前入口点层级（测试用：验证确实建了多层）。
    pub fn entry_level(&self) -> u8 {
        self.entry.lock().unwrap().map(|(_, l)| l).unwrap_or(0)
    }
}

impl GraphIndex for DiskGraphIndex {
    fn needs_legacy_tenant_migration(&self) -> bool {
        self.store.legacy_tenant_slots.lock().unwrap().is_some()
    }
    fn legacy_embedding_keys(&self) -> std::io::Result<Vec<(u64, u64)>> {
        let legacy = self.store.legacy_tenant_slots.lock().unwrap();
        let mut keys = std::collections::BTreeSet::new();
        if let Some((start, end)) = *legacy {
            for id in start..end {
                let node = self.store.read_node(id)?;
                if !node.deleted {
                    keys.insert((node.trace_id, node.span_id));
                }
            }
        }
        Ok(keys.into_iter().collect())
    }
    fn migrate_legacy_embeddings(
        &self,
        tenants: &HashMap<(u64, u64), Option<u64>>,
    ) -> std::io::Result<()> {
        {
            let _mutation = self.mutation.lock().unwrap();
            let mut legacy = self.store.legacy_tenant_slots.lock().unwrap();
            let Some((start, end)) = *legacy else {
                return Ok(());
            };
            for id in start..end {
                let node = self.store.read_node(id)?;
                let tenant = tenants.get(&(node.trace_id, node.span_id)).copied();
                // 源span已删除时旧向量只可退役，不能赋给碰巧复用相同ID的租户。
                if tenant.is_none() && !node.deleted {
                    self.store.mark_deleted(id)?;
                }
                self.store.write_tenant(id, tenant.flatten())?;
            }
            self.store.tenants.sync_data()?;
            std::fs::write(self.store.dir.join("tenant_format"), b"YT-TENANTS-1")?;
            self.publish_generation()?;
            self.identities.lock().unwrap().take();
            legacy.take();
        }
        self.flush_checked()
    }
    fn supports_tenant_scope(&self) -> bool {
        true
    }
    fn index_embedding(&self, trace_id: u64, span_id: u64, embedding: Vec<f32>) {
        self.index_embedding_scoped(None, trace_id, span_id, embedding)
            .expect("vector write failed");
    }
    fn index_embedding_scoped(
        &self,
        tenant_id: Option<u64>,
        trace_id: u64,
        span_id: u64,
        embedding: Vec<f32>,
    ) -> std::io::Result<()> {
        let vector = if self.metric == Metric::Cosine {
            simd::normalize(&embedding).0
        } else {
            embedding
        };
        self.upsert(tenant_id, trace_id, span_id, &vector)
    }
    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(u64, u64, f32)> {
        self.search_scoped(query, k, &|tenant, t, s| tenant.is_none() && filter(t, s))
            .into_iter()
            .map(|(_, t, s, d)| (t, s, d))
            .collect()
    }
    fn search_scoped(
        &self,
        query: &[f32],
        k: usize,
        filter: &dyn Fn(Option<u64>, u64, u64) -> bool,
    ) -> Vec<(Option<u64>, u64, u64, f32)> {
        if k == 0 || query.len() != self.store.dim {
            return Vec::new();
        }
        // Cosine：归一化查询（与索引时的归一化对齐）。
        let q: Vec<f32> = if self.metric == Metric::Cosine {
            simd::normalize(query).0
        } else {
            query.to_vec()
        };
        let query: &[f32] = &q;
        let Some((mut ep, top)) = *self.entry.lock().unwrap() else {
            return Vec::new();
        };
        let alive = |q: u32| self.store.node_arc(q).map(|a| !a.deleted).unwrap_or(false);

        // 顶层贪心下沉到 level 1（只导航、ef=1）。
        let mut lc = top;
        while lc >= 1 {
            let r = self.search_layer(query, &[ep], 1, lc, &alive);
            if let Some(&(c, _)) = r.first() {
                ep = c;
            }
            lc -= 1;
        }

        // 底层 ef_search beam + 进图过滤（admit = 未删 + 业务谓词）。node_arc 不克隆。
        if self.ensure_identities().is_err() {
            return Vec::new();
        }
        let identities = self.identities.lock().unwrap();
        let latest = identities.as_ref().unwrap();
        let admit = |q: u32| match self.store.node_arc(q) {
            Ok(a) => {
                !a.deleted
                    && latest.get(&(a.tenant_id, a.trace_id, a.span_id)) == Some(&q)
                    && filter(a.tenant_id, a.trace_id, a.span_id)
            }
            Err(_) => false,
        };
        let ef = self.ef_search.max(k);
        // IP：距离已是 -dot，不取 sqrt；L2/Cosine 取 sqrt 还原真实距离。
        let finalize = |d: f32| -> f32 {
            if self.metric == Metric::InnerProduct {
                d
            } else {
                d.max(0.0).sqrt()
            }
        };
        let mut out: Vec<(Option<u64>, u64, u64, f32)> = self
            .search_layer(query, &[ep], ef, 0, &admit)
            .into_iter()
            .filter_map(|(id, d)| {
                self.store
                    .read_node(id)
                    .ok()
                    .map(|r| (r.tenant_id, r.trace_id, r.span_id, finalize(d)))
            })
            .collect();
        out.truncate(k);
        out
    }

    fn flush(&self) {
        self.flush_checked().expect("vector flush failed");
    }
    fn flush_checked(&self) -> std::io::Result<()> {
        let _mutation = self.mutation.lock().unwrap();
        self.store.sync()?;
        save_upper(
            &self.upper_path,
            &self.upper.lock().unwrap(),
            *self.entry.lock().unwrap(),
        )?;
        File::open(&self.upper_path)?.sync_data()?;
        if let Ok(directory) = File::open(&self.store.dir) {
            directory.sync_all()?;
        }
        Ok(())
    }
}

/// 上层图快照编解码：entry(flag+node+level) + upper 条目(node,level,n,邻居)，crc + 原子写。
fn save_upper(
    path: &Path,
    upper: &HashMap<(u32, u8), Vec<u32>>,
    entry: Option<(u32, u8)>,
) -> std::io::Result<()> {
    let mut b = Vec::new();
    match entry {
        Some((n, l)) => {
            b.push(1);
            b.extend_from_slice(&n.to_le_bytes());
            b.push(l);
        }
        None => b.push(0),
    }
    b.extend_from_slice(&(upper.len() as u64).to_le_bytes());
    for (&(node, level), adj) in upper {
        b.extend_from_slice(&node.to_le_bytes());
        b.push(level);
        b.extend_from_slice(&(adj.len() as u16).to_le_bytes());
        for &nb in adj {
            b.extend_from_slice(&nb.to_le_bytes());
        }
    }
    let crc = yt_wal::crc32(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, &b)?;
    std::fs::rename(&tmp, path)
}

fn load_upper(path: &Path) -> (HashMap<(u32, u8), Vec<u32>>, Option<(u32, u8)>) {
    let bytes = std::fs::read(path).unwrap_or_default();
    let mut empty = (HashMap::new(), None);
    if bytes.len() < 4 {
        return empty;
    }
    let body = &bytes[..bytes.len() - 4];
    if yt_wal::crc32(body) != u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap()) {
        return empty; // 损坏 → 当空（上层图是派生数据，最坏退化成慢一点的导航）
    }
    let mut p = 0usize;
    let entry = if body[p] == 1 {
        p += 1;
        let n = u32::from_le_bytes(body[p..p + 4].try_into().unwrap());
        p += 4;
        let l = body[p];
        p += 1;
        Some((n, l))
    } else {
        p += 1;
        None
    };
    let cnt = u64::from_le_bytes(body[p..p + 8].try_into().unwrap()) as usize;
    p += 8;
    let mut upper = HashMap::with_capacity(cnt);
    for _ in 0..cnt {
        if p + 7 > body.len() {
            return empty;
        }
        let node = u32::from_le_bytes(body[p..p + 4].try_into().unwrap());
        p += 4;
        let level = body[p];
        p += 1;
        let n = u16::from_le_bytes(body[p..p + 2].try_into().unwrap()) as usize;
        p += 2;
        let mut adj = Vec::with_capacity(n);
        for _ in 0..n {
            if p + 4 > body.len() {
                return empty;
            }
            adj.push(u32::from_le_bytes(body[p..p + 4].try_into().unwrap()));
            p += 4;
        }
        upper.insert((node, level), adj);
    }
    empty = (upper, entry);
    empty
}
