/// 磁盘型图存储：定长节点文件 + 定长向量文件 + 元页 + 向量缓冲。
pub struct DiskGraphStore {
    dir: PathBuf,
    nodes: File,
    vectors: File,
    tenants: File,
    legacy_tenant_slots: Mutex<Option<(u32, u32)>>,
    dim: usize,
    m: usize,
    max_deg: usize,
    metric: Metric,
    node_rec_size: usize,
    /// 已分配节点数（= nodes 文件长度 / 记录长，开盘时据此恢复，无需单独持久）。
    count: AtomicU64,
    cache: Mutex<VecCache>,
    node_cache: Mutex<NodeCache>,
}

impl DiskGraphStore {
    /// 打开/创建索引目录。`dim`/`m` 首次创建时定型（重开从元页读回，`cfg.m` 仅作创建默认）。
    /// `cfg.vector_cache_bytes` = 向量缓冲池内存预算（控制常驻内存，对齐 graph_index 的 vector_buffers）。
    pub fn open(dir: impl AsRef<Path>, dim: usize, cfg: DiskGraphConfig) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let meta_path = dir.join("meta");

        // 元页：有则读回（dim/m/metric 以盘上为准），无则按传入值创建并落盘。
        let (dim, m, metric) = match Meta::load(&meta_path) {
            Some(meta) => (meta.dim, meta.m, meta.metric),
            None if meta_path.exists() => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid vector metadata",
                ))
            }
            None => {
                Meta {
                    dim,
                    m: cfg.m,
                    metric: cfg.metric,
                }
                .store(&meta_path)?;
                (dim, cfg.m, cfg.metric)
            }
        };

        let max_deg = (2 * m).max(2);
        let node_rec_size = NODE_HEADER + 4 * max_deg;

        let nodes = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.join("nodes"))?;
        let vectors = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.join("vectors"))?;

        let tenants = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.join("tenants"))?;

        // 节点数从 nodes 文件长度恢复（撕裂的尾部不足一条则忽略）。
        let count = nodes.metadata()?.len() / node_rec_size as u64;
        let tenant_slots = tenants.metadata()?.len() / 9;
        let format_path = dir.join("tenant_format");
        let modern = format_path.exists();
        if modern && (std::fs::read(&format_path)? != b"YT-TENANTS-1" || tenant_slots < count) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "missing or invalid modern vector tenant data",
            ));
        }
        // 旧格式没有租户字段。仅记下缺失范围；首次向量操作才扫描节点和真实span解析。
        let legacy_tenant_slots =
            (!modern && tenant_slots < count).then_some((tenant_slots as u32, count as u32));
        if count == 0 && !modern {
            std::fs::write(&format_path, b"YT-TENANTS-1")?;
        }

        Ok(Self {
            dir,
            nodes,
            vectors,
            tenants,
            legacy_tenant_slots: Mutex::new(legacy_tenant_slots),
            dim,
            m,
            max_deg,
            metric,
            node_rec_size,
            count: AtomicU64::new(count),
            cache: Mutex::new(VecCache::new(cfg.vector_cache_bytes)),
            // 节点记录（图拓扑）缓存，消除每次访问 pread。默认上限 1M 条（~小几百 MB），上量可调。
            node_cache: Mutex::new(NodeCache::new(1 << 20)),
        })
    }

    fn write_tenant(&self, id: u32, tenant_id: Option<u64>) -> std::io::Result<()> {
        let mut tenant = [0u8; 9];
        if let Some(value) = tenant_id {
            tenant[0] = 1;
            tenant[1..].copy_from_slice(&value.to_le_bytes());
        }
        write_all_at(&self.tenants, &tenant, u64::from(id) * 9)?;
        self.node_cache.lock().unwrap().map.remove(&id);
        Ok(())
    }
    pub fn dim(&self) -> usize {
        self.dim
    }
    pub fn m(&self) -> usize {
        self.m
    }
    pub fn max_deg(&self) -> usize {
        self.max_deg
    }
    pub fn metric(&self) -> Metric {
        self.metric
    }
    pub fn len(&self) -> u64 {
        self.count.load(Ordering::Acquire)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 加一个节点：先写向量、再写节点记录（崩在两者之间则该槽未计数、下次复用），返回 node_id。
    /// 维度不符（≠ dim）拒绝。`level` = HNSW 层级。
    pub fn add_node(
        &self,
        trace_id: u64,
        span_id: u64,
        vector: &[f32],
        level: u8,
    ) -> std::io::Result<Option<u32>> {
        self.add_node_scoped(None, trace_id, span_id, vector, level)
    }

    pub fn add_node_scoped(
        &self,
        tenant_id: Option<u64>,
        trace_id: u64,
        span_id: u64,
        vector: &[f32],
        level: u8,
    ) -> std::io::Result<Option<u32>> {
        if vector.len() != self.dim {
            return Ok(None);
        }
        let id = self.count.load(Ordering::Acquire);
        let mut tenant = [0u8; 9];
        if let Some(value) = tenant_id {
            tenant[0] = 1;
            tenant[1..].copy_from_slice(&value.to_le_bytes());
        }
        write_all_at(&self.tenants, &tenant, id * 9)?;
        self.write_vector(id, vector)?;
        self.write_node(id, trace_id, span_id, false, level, &[])?;
        // 两个文件都落盘后才提交计数（读者据此判可见）。
        self.count.store(id + 1, Ordering::Release);
        self.cache
            .lock()
            .unwrap()
            .put(id, Arc::from(vector.to_vec()));
        Ok(Some(id as u32))
    }

    /// 原地改写某节点的**底层邻边**（保留 level/软删）。截到 `max_deg`。
    pub fn set_neighbors(&self, id: u32, neighbors: &[u32]) -> std::io::Result<()> {
        let rec = self.read_node(id)?;
        self.write_node(
            id as u64,
            rec.trace_id,
            rec.span_id,
            rec.deleted,
            rec.level,
            neighbors,
        )
    }

    /// 标记软删（保留 level/邻边）。
    pub fn mark_deleted(&self, id: u32) -> std::io::Result<()> {
        let rec = self.read_node(id)?;
        self.write_node(
            id as u64,
            rec.trace_id,
            rec.span_id,
            true,
            rec.level,
            &rec.neighbors,
        )
    }

    /// 改某节点的 HNSW 层级（保留邻边/软删）。
    pub fn set_level(&self, id: u32, level: u8) -> std::io::Result<()> {
        let rec = self.read_node(id)?;
        self.write_node(
            id as u64,
            rec.trace_id,
            rec.span_id,
            rec.deleted,
            level,
            &rec.neighbors,
        )
    }

    /// 读节点记录（拷贝出 `NodeRec`，给需要拥有所有权的调用方）。
    pub fn read_node(&self, id: u32) -> std::io::Result<NodeRec> {
        self.node_arc(id).map(|a| (*a).clone())
    }

    /// 读节点记录（`Arc<NodeRec>`，热路径用，命中只加引用计数、不复制邻居 Vec）。
    pub fn node_arc(&self, id: u32) -> std::io::Result<Arc<NodeRec>> {
        if let Some(a) = self.node_cache.lock().unwrap().get(id) {
            return Ok(a);
        }
        let mut buf = vec![0u8; self.node_rec_size];
        read_exact_at(&self.nodes, &mut buf, id as u64 * self.node_rec_size as u64)?;
        let mut decoded = decode_node(&buf);
        decoded.tenant_id = self.read_tenant(id as u64)?;
        let a = Arc::new(decoded);
        self.node_cache.lock().unwrap().put(id, a.clone());
        Ok(a)
    }

    fn read_tenant(&self, id: u64) -> std::io::Result<Option<u64>> {
        // 旧节点没有该边车；空洞为零，仍表示无租户。新节点先写租户，再提交 nodes 槽。
        if self.tenants.metadata()?.len() <= id * 9 {
            return Ok(None);
        }
        let mut bytes = [0u8; 9];
        read_exact_at(&self.tenants, &mut bytes, id * 9)?;
        match bytes[0] {
            0 => Ok(None),
            1 => Ok(Some(u64::from_le_bytes(bytes[1..].try_into().unwrap()))),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid vector tenant identity",
            )),
        }
    }

    /// 读向量（`Arc<[f32]>`，热路径用，命中只加引用计数、不复制）。
    pub fn read_vector_arc(&self, id: u32) -> std::io::Result<Arc<[f32]>> {
        if let Some(v) = self.cache.lock().unwrap().get(id as u64) {
            return Ok(v);
        }
        let mut buf = vec![0u8; self.dim * 4];
        read_exact_at(&self.vectors, &mut buf, id as u64 * self.dim as u64 * 4)?;
        let v: Arc<[f32]> = buf
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        self.cache.lock().unwrap().put(id as u64, v.clone());
        Ok(v)
    }

    /// 读向量（拷成 `Vec<f32>`，给外部 API / 测试用）。
    pub fn read_vector(&self, id: u32) -> std::io::Result<Vec<f32>> {
        self.read_vector_arc(id).map(|v| v.to_vec())
    }

    /// 缓冲命中/未命中计数（测"向量不全量常驻"用）。
    pub fn cache_stats(&self) -> (u64, u64) {
        let c = self.cache.lock().unwrap();
        (c.hits, c.misses)
    }

    /// 缓冲池当前常驻字节 / 预算字节（观测"只用了 1G 显存似的"那种内存上界）。
    pub fn cache_mem(&self) -> (usize, usize) {
        let c = self.cache.lock().unwrap();
        (c.cur_bytes, c.budget_bytes)
    }

    /// 刷盘（fsync 向量 + 节点文件）。写操作本身不刷，由调用方在一批写完后 `sync` 一次（批量、快）。
    /// 同进程内重开读页缓存不需要它；它保证的是**崩溃后落盘**。
    pub fn sync(&self) -> std::io::Result<()> {
        self.tenants.sync_data()?;
        self.vectors.sync_data()?;
        self.nodes.sync_data()
    }

    fn write_vector(&self, id: u64, vector: &[f32]) -> std::io::Result<()> {
        let mut buf = Vec::with_capacity(self.dim * 4);
        for &x in vector {
            buf.extend_from_slice(&x.to_le_bytes());
        }
        write_all_at(&self.vectors, &buf, id * self.dim as u64 * 4)
    }

    fn write_node(
        &self,
        id: u64,
        trace_id: u64,
        span_id: u64,
        deleted: bool,
        level: u8,
        neighbors: &[u32],
    ) -> std::io::Result<()> {
        let nb: Vec<u32> = neighbors.iter().take(self.max_deg).copied().collect();
        let buf = encode_node(
            self.node_rec_size,
            self.max_deg,
            trace_id,
            span_id,
            deleted,
            level,
            &nb,
        );
        write_all_at(&self.nodes, &buf, id * self.node_rec_size as u64)?;
        // 写穿：节点缓存同步更新，读路径直接命中、不回盘。
        self.node_cache.lock().unwrap().put(
            id as u32,
            Arc::new(NodeRec {
                tenant_id: self.read_tenant(id)?,
                trace_id,
                span_id,
                deleted,
                level,
                neighbors: nb,
            }),
        );
        Ok(())
    }
}

#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(unix)]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    use std::os::windows::fs::FileExt;

    while !buf.is_empty() {
        let n = file.seek_read(buf, offset)?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        offset += n as u64;
        let tmp = buf;
        buf = &mut tmp[n..];
    }
    Ok(())
}

#[cfg(windows)]
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    use std::os::windows::fs::FileExt;

    while !buf.is_empty() {
        let n = file.seek_write(buf, offset)?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }
        offset += n as u64;
        buf = &buf[n..];
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};

    let mut cloned = file.try_clone()?;
    cloned.seek(SeekFrom::Start(offset))?;
    cloned.read_exact(buf)
}

#[cfg(not(any(unix, windows)))]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let mut cloned = file.try_clone()?;
    cloned.seek(SeekFrom::Start(offset))?;
    cloned.write_all(buf)
}

const NODE_HEADER: usize = 8 + 8 + 1 + 1 + 2; // trace + span + deleted + level + neighbor_count

fn encode_node(
    rec_size: usize,
    max_deg: usize,
    trace_id: u64,
    span_id: u64,
    deleted: bool,
    level: u8,
    neighbors: &[u32],
) -> Vec<u8> {
    let mut b = vec![0u8; rec_size];
    b[0..8].copy_from_slice(&trace_id.to_le_bytes());
    b[8..16].copy_from_slice(&span_id.to_le_bytes());
    b[16] = deleted as u8;
    b[17] = level; // 原 pad 字节
    let n = neighbors.len().min(max_deg);
    b[18..20].copy_from_slice(&(n as u16).to_le_bytes());
    for (i, &nb) in neighbors.iter().take(max_deg).enumerate() {
        let o = NODE_HEADER + i * 4;
        b[o..o + 4].copy_from_slice(&nb.to_le_bytes());
    }
    b
}

fn decode_node(b: &[u8]) -> NodeRec {
    let trace_id = u64::from_le_bytes(b[0..8].try_into().unwrap());
    let span_id = u64::from_le_bytes(b[8..16].try_into().unwrap());
    let deleted = b[16] != 0;
    let level = b[17];
    let n = u16::from_le_bytes(b[18..20].try_into().unwrap()) as usize;
    let mut neighbors = Vec::with_capacity(n);
    for i in 0..n {
        let o = NODE_HEADER + i * 4;
        if o + 4 > b.len() {
            break;
        }
        neighbors.push(u32::from_le_bytes(b[o..o + 4].try_into().unwrap()));
    }
    NodeRec {
        tenant_id: None,
        trace_id,
        span_id,
        deleted,
        level,
        neighbors,
    }
}

// ───────────────────────── 元页 ─────────────────────────

struct Meta {
    dim: usize,
    m: usize,
    metric: Metric,
}

impl Meta {
    fn load(path: &Path) -> Option<Meta> {
        let bytes = std::fs::read(path).ok()?;
        if bytes.len() < 4 + 4 + 4 + 4 + 1 + 4 {
            return None;
        }
        let crc_stored = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().ok()?);
        if crc_stored != yt_wal::crc32(&bytes[..bytes.len() - 4]) {
            return None;
        }
        if u32::from_le_bytes(bytes[0..4].try_into().ok()?) != MAGIC {
            return None;
        }
        let dim = u32::from_le_bytes(bytes[8..12].try_into().ok()?) as usize;
        let m = u32::from_le_bytes(bytes[12..16].try_into().ok()?) as usize;
        let metric = match bytes[16] {
            1 => Metric::Cosine,
            2 => Metric::InnerProduct,
            _ => Metric::L2, // 旧索引（VERSION 1 未写 metric 字节）回退默认 L2。
        };
        Some(Meta { dim, m, metric })
    }

    fn store(&self, path: &Path) -> std::io::Result<()> {
        let mut b = Vec::new();
        b.extend_from_slice(&MAGIC.to_le_bytes());
        b.extend_from_slice(&VERSION.to_le_bytes());
        b.extend_from_slice(&(self.dim as u32).to_le_bytes());
        b.extend_from_slice(&(self.m as u32).to_le_bytes());
        b.push(self.metric as u8);
        let crc = yt_wal::crc32(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        // 原子写：tmp + rename。
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &b)?;
        std::fs::rename(&tmp, path)
    }
}
