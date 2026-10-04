// ───────────────────────── 引擎用：惰性磁盘图索引（首个向量定维度） ─────────────────────────

/// 向量是独立于 WAL/manifest 的数据，因此用自己的节点长度和提交版本检查跨进程变化。
/// 打开仅恢复图入口；身份目录仍在第一次查询/写入时加载，避免启动扫描大型节点文件。
pub struct DurableGraphIndex {
    dir: PathBuf,
    cfg: DiskGraphConfig,
    inner: Mutex<(Option<Arc<DiskGraphIndex>>, (u64, u64))>,
}

impl DurableGraphIndex {
    pub fn open(dir: impl AsRef<Path>, cfg: DiskGraphConfig) -> Self {
        let dir = dir.as_ref().to_path_buf();
        let inner = if dir.join("meta").exists() {
            DiskGraphIndex::open(&dir, 0, cfg).ok().map(Arc::new)
        } else {
            None
        };
        let fingerprint = Self::fingerprint(&dir);
        Self {
            dir,
            cfg,
            inner: Mutex::new((inner, fingerprint)),
        }
    }
    fn fingerprint(dir: &Path) -> (u64, u64) {
        let nodes = std::fs::metadata(dir.join("nodes"))
            .map(|m| m.len())
            .unwrap_or(0);
        let generation = std::fs::read(dir.join("generation"))
            .ok()
            .and_then(|b| b.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0);
        (nodes, generation)
    }
    fn handle(&self) -> Option<Arc<DiskGraphIndex>> {
        self.inner.lock().unwrap().0.clone()
    }
}

impl GraphIndex for DurableGraphIndex {
    fn needs_legacy_tenant_migration(&self) -> bool {
        self.handle()
            .is_some_and(|index| index.needs_legacy_tenant_migration())
    }
    fn legacy_embedding_keys(&self) -> std::io::Result<Vec<(u64, u64)>> {
        self.handle()
            .map(|index| index.legacy_embedding_keys())
            .unwrap_or_else(|| Ok(Vec::new()))
    }
    fn migrate_legacy_embeddings(
        &self,
        tenants: &HashMap<(u64, u64), Option<u64>>,
    ) -> std::io::Result<()> {
        if let Some(index) = self.handle() {
            index.migrate_legacy_embeddings(tenants)?;
        }
        self.inner.lock().unwrap().1 = Self::fingerprint(&self.dir);
        Ok(())
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
        // 引擎已持跨进程写锁；先重新核对独立版本，不能沿用旧节点计数占磁盘槽。
        self.reload_if_changed_checked()?;
        let mut state = self.inner.lock().unwrap();
        if state.0.is_none() {
            state.0 = Some(Arc::new(DiskGraphIndex::open(
                &self.dir,
                embedding.len(),
                self.cfg,
            )?));
        }
        let result = state
            .0
            .as_ref()
            .unwrap()
            .index_embedding_scoped(tenant_id, trace_id, span_id, embedding);
        state.1 = Self::fingerprint(&self.dir);
        result
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
        self.handle()
            .map(|i| i.search_scoped(query, k, filter))
            .unwrap_or_default()
    }
    fn flush(&self) {
        if let Some(index) = self.handle() {
            index.flush();
        }
    }
    fn flush_checked(&self) -> std::io::Result<()> {
        if let Some(index) = self.handle() {
            index.flush_checked()?;
        }
        Ok(())
    }
    fn reload_if_changed(&self) {
        self.reload_if_changed_checked()
            .expect("vector reload failed");
    }
    fn reload_if_changed_checked(&self) -> std::io::Result<()> {
        let fingerprint = Self::fingerprint(&self.dir);
        let mut state = self.inner.lock().unwrap();
        if state.1 != fingerprint || (state.0.is_none() && self.dir.join("meta").exists()) {
            let index = DiskGraphIndex::open(&self.dir, 0, self.cfg)?;
            state.0 = Some(Arc::new(index));
            state.1 = fingerprint;
        }
        Ok(())
    }
    fn reload(&self) {
        self.reload_checked().expect("vector reload failed");
    }
    fn reload_checked(&self) -> std::io::Result<()> {
        let reopened = if self.dir.join("meta").exists() {
            Some(Arc::new(DiskGraphIndex::open(&self.dir, 0, self.cfg)?))
        } else {
            None
        };
        *self.inner.lock().unwrap() = (reopened, Self::fingerprint(&self.dir));
        Ok(())
    }
}
