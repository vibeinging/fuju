// ───────────────────────── 单写者协调器 ─────────────────────────

/// 所有 manifest 提交的串行入口。持有 WAL + current 指针 + 段存储。
pub struct WriteCoordinator {
    /// 单写者锁：flush/compaction/delete/upgrade 全过这把锁。
    write_lock: Mutex<()>,
    current: Arc<Current>,
    wal: Mutex<Wal>,
    /// 活内存表（带双水位）。读路径的四源之一。
    memtable: Mutex<MemTable>,
    segments: Arc<dyn SegmentStore>,
    /// 等回收的 dead 资源（compaction 摘下的旧段）。
    dead_set: Mutex<Vec<DeadResource>>,
    /// 段文件 buffer pin 计数（GC 条件 (2)）。
    buffer_pins: BufferPins,
    /// BM25 中文倒排（检索）。真实实现接团队自有 BM25。
    bm25: Arc<dyn Bm25Index>,
    /// 向量 ANN（找相似）。真实实现接团队 graph_index。
    graph: Arc<dyn GraphIndex>,
    /// 内存表行数超过此值就自动刷盘（兜住内存上界，OPEN-2）。
    flush_threshold: AtomicUsize,
    /// 段身份分配器（单写者下无并发分配竞争，永不复用）。
    next_segment_id: Mutex<u64>,
    next_chunk_id: Mutex<u64>,
    /// 评测数据集（按名）。元数据,不进 trace 存储;eval 的"燃料"与回归基准。
    datasets: Mutex<BTreeMap<String, Dataset>>,
    /// 人工/自动标注：给 trace/span 记录审核结果、路径质量、失败原因等。
    annotations: Mutex<Vec<TraceAnnotation>>,
    /// 数据集关联：把 trace/span 与外部回归集、eval 样本、split 绑定起来。
    dataset_associations: Mutex<Vec<DatasetAssociation>>,
    /// metadata 查询倒排：annotation/dataset association 用它先拿候选 id，再做最终校验。
    metadata_index: Mutex<MetadataIndex>,
    /// retention 执行审计：记录谁按什么条件删过、删了多少、跳过了哪些热 trace。
    retention_audits: Mutex<Vec<RetentionAuditRecord>>,
    /// retention policy：保存可重复执行的 TTL 策略；引擎不自动起后台线程。
    retention_policies: Mutex<Vec<RetentionPolicy>>,
    next_annotation_id: Mutex<u64>,
    next_dataset_association_id: Mutex<u64>,
    next_retention_audit_id: Mutex<u64>,
    next_retention_policy_id: Mutex<u64>,
    /// manifest 持久化路径。Some = 每次 commit 后原子写盘（重启不丢）；None = 纯内存。
    manifest_path: Option<std::path::PathBuf>,
    /// 业务侧元数据账本路径。Some = annotation/dataset link 原子写盘；None = 纯内存。
    metadata_path: Option<std::path::PathBuf>,
    /// 向量独立落盘路径。Some = `index_embedding` 追加写盘、`recover` 重载（向量不在 trace 数据里,
    /// 段重建不出来,只能单独持久）；None = 纯内存。
    vector_path: Option<std::path::PathBuf>,
    /// BM25 持久倒排缓存路径。只缓存已进入 segment 的文本倒排；WAL tail 重启时仍从 WAL 叠加。
    bm25_path: Option<std::path::PathBuf>,
    /// 段级 key bloom 持久缓存路径。用于全文检索候选 key join 时跳过无关段。
    seg_key_bloom_path: Option<std::path::PathBuf>,
    /// attrs 过滤边车缓存路径。只缓存已进入 segment 的过滤小字段；WAL tail 重启时仍从 WAL 叠加。
    filter_attrs_path: Option<std::path::PathBuf>,
    /// trace rollup 缓存路径。只缓存已进入 segment 的小字段 rollup；WAL tail 重启时仍从 WAL 叠加。
    trace_rollup_path: Option<std::path::PathBuf>,
    /// 检索过滤的属性边车：(trace,span) → 可过滤元数据（带过滤 ANN 的 payload）。
    /// 派生数据：摄入时建,`recover` 时优先从 `filter_attrs.dat` 恢复，坏了再从段重建。
    filter_attrs: Mutex<FilterAttrsIndex>,
    /// trace aggregate 物化 rollup：span 级小字段汇总，不存大文本/logs。
    trace_rollup: Mutex<TraceAggregateRollupIndex>,
    /// rollup / attrs 是否已经装进当前进程。持久重启只恢复控制面，两个大索引按读路径惰性加载。
    read_model_load_state: Mutex<ReadModelLoadState>,
    /// 控制台会话边车索引：摄入时**增量差量**维护（O(1)/事件），delete/upgrade 标脏、下次读重建。
    session_idx: Mutex<SessionIndex>,
    /// **段折叠缓存**：不可变段首次解码后缓存（行 + (trace,span)→行号 索引），检索路径只取候选行、
    /// 不再每查重读+重解码整段。段 unlink（compaction/GC）时失效。LRU、按总行数封顶。
    seg_fold_cache: Mutex<SegFoldCache>,
    /// **段级 key Bloom**（对齐 ClickHouse bloom_filter 跳过索引）：seg_id → 该段 (trace,span) 的 bloom。
    /// 检索折叠定位时，bloom 判"这个段肯定没有任何候选 key" → 整段跳过，不碰折叠缓存。派生数据：flush
    /// 时建、recover 时优先从 `segment_bloom.dat` 恢复，坏了再从段重建。每段几 KB，常驻内存可控。
    seg_key_bloom: Mutex<HashMap<u64, Arc<KeyBloom>>>,
    /// clean reopen 后第一次点查只需要加载小型 segment bloom，不应连带加载 BM25。
    /// 独立锁把并发首查合并成一次 sidecar 读取，同时不占用写锁或跨进程写锁。
    seg_key_bloom_load_lock: Mutex<()>,
    /// 同一 manifest 的 sidecar 已确认不可用时不再每次点查重复读取；manifest 更新或缓存重建后重试。
    seg_key_bloom_load_failed_for: Mutex<Option<(u64, u64)>>,
    #[cfg(test)]
    seg_key_bloom_load_count: AtomicUsize,
    /// 全文检索必需的段扫描派生索引（BM25 / seg_key_bloom）是否还没从历史 segment 补建。
    /// open/recover 命中 rollup/filter 但缺 `bm25.dat` 或 `segment_bloom.dat` 时，先快速可用；
    /// 需要全文检索时再补。控制台 session index 有独立 dirty 标记，不混在这里。
    segment_scan_indexes_stale: Mutex<bool>,
    /// **GC 日志**（崩溃安全）：Some = reclaim 走"MARK→fsync→unlink→DONE→fsync"，崩溃在中途重启补删；
    /// None = 纯内存态（非持久模式，reclaim 直接删，旧路径）。
    gc_log: Mutex<Option<gc_log::GcLog>>,
    /// 数据目录路径（持久模式 = Some）。`backup_snapshot` 用它知道拷哪些文件。
    dir: Option<std::path::PathBuf>,
    /// 持久模式下的跨进程写锁管理器。None = 纯内存 / 测试模式，不做进程间协调。
    process_lock: Option<Arc<process_lock::ProcessLockManager>>,
}

/// 递归拷贝目录（备份用，零依赖）。
fn copy_dir_recursive(src: &std::path::Path, dest: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dest)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let from = entry.path();
        let to = dest.join(entry.file_name());
        let ft = entry.file_type()?;
        if ft.is_dir() {
            copy_dir_recursive(&from, &to)?;
        } else {
            std::fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

/// 段级 key Bloom 过滤器（双哈希 + 位组，std-only，无依赖）。`maybe_contains` 假阳允许、假阴不允许：
/// 返回 false = **肯定没有**（可放心跳段），返回 true = 可能有（要进一步查）。约 10 bit/key、7 个哈希。
struct KeyBloom {
    bits: Vec<u64>,
    mask: usize, // m_bits-1（m_bits 取 2 的幂，用 & 代 %）
    k: u32,
}

impl KeyBloom {
    fn build<I: IntoIterator<Item = (u64, u64)>>(keys: I, n_hint: usize) -> Self {
        let m_bits = (n_hint.max(1) * 10).next_power_of_two().max(64);
        let mut b = KeyBloom {
            bits: vec![0u64; m_bits / 64],
            mask: m_bits - 1,
            k: SEG_BLOOM_HASH_COUNT,
        };
        for key in keys {
            b.insert(key);
        }
        b
    }
    fn pair(key: (u64, u64)) -> (u64, u64) {
        let h1 = splitmix64m(key.0 ^ key.1.rotate_left(32));
        let h2 = splitmix64m(key.0.wrapping_add(0x9E37_79B9_7F4A_7C15) ^ key.1) | 1; // 奇数，保证步长与 m 互质
        (h1, h2)
    }
    fn insert(&mut self, key: (u64, u64)) {
        let (h1, h2) = Self::pair(key);
        for i in 0..self.k as u64 {
            let p = (h1.wrapping_add(i.wrapping_mul(h2)) as usize) & self.mask;
            self.bits[p >> 6] |= 1u64 << (p & 63);
        }
    }
    fn maybe_contains(&self, key: (u64, u64)) -> bool {
        let (h1, h2) = Self::pair(key);
        (0..self.k as u64).all(|i| {
            let p = (h1.wrapping_add(i.wrapping_mul(h2)) as usize) & self.mask;
            self.bits[p >> 6] & (1u64 << (p & 63)) != 0
        })
    }
    fn from_bits(bits: Vec<u64>, k: u32) -> Option<Self> {
        if k != SEG_BLOOM_HASH_COUNT || bits.is_empty() || !bits.len().is_power_of_two() {
            return None;
        }
        let m_bits = bits.len().checked_mul(64)?;
        Some(Self {
            bits,
            mask: m_bits - 1,
            k,
        })
    }
}

fn splitmix64m(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

/// 一个段解码折叠后的缓存：全部行 + (trace,span)→行号 索引（行号=段内顺序，删除位图照行号生效）。
struct SegFold {
    rows: Vec<FoldInput>,
    by_key: HashMap<(u64, u64), Vec<u32>>,
}

/// 段折叠缓存（LRU，按总缓存行数封顶；段不可变，命中即用、unlink 时移除）。
struct SegFoldCache {
    cap_rows: usize,
    cur_rows: usize,
    map: HashMap<u64, (Arc<SegFold>, u64)>,
    tick: u64,
}

impl SegFoldCache {
    fn new(cap_rows: usize) -> Self {
        Self {
            cap_rows: cap_rows.max(1),
            cur_rows: 0,
            map: HashMap::new(),
            tick: 0,
        }
    }
    fn remove(&mut self, seg: u64) {
        if let Some((sf, _)) = self.map.remove(&seg) {
            self.cur_rows -= sf.rows.len();
        }
    }
    fn evict(&mut self) {
        let target = (self.cap_rows * 9 / 10).max(1);
        let mut by_tick: Vec<(u64, u64, usize)> = self
            .map
            .iter()
            .map(|(&seg, (sf, t))| (*t, seg, sf.rows.len()))
            .collect();
        by_tick.sort_unstable_by_key(|x| x.0);
        for (_, seg, n) in by_tick {
            if self.cur_rows <= target || self.map.len() <= 1 {
                break;
            }
            self.map.remove(&seg);
            self.cur_rows -= n;
        }
    }
}

/// 一个 span 在边车里的当前聚合（last-non-null 口径，与折叠一致）。用于算会话级差量。
#[derive(Default, Clone)]
struct SpanAgg {
    tenant: Option<u64>,
    event_order: SpanEventOrder,
    /// 折叠结果没有每字段seq；下次写入先失效，再从同一快照重建，不能猜增量顺序。
    from_fold: bool,
    session: Option<u64>,
    external_session: Option<String>,
    in_tok: u64,
    out_tok: u64,
    cache_read: Option<u64>,
    cache_write: Option<u64>,
    is_llm: bool,
    error: bool,
    has_start: bool,
    has_end: bool,
    agent: Option<String>,
    trace: u64,
}

/// 一个会话在边车里的增量聚合。
#[derive(Default, Clone)]
struct SessionAgg {
    traces: std::collections::HashSet<u64>,
    external_session: Option<String>,
    in_tok: u64,
    out_tok: u64,
    cache_read: u64,
    cache_write: u64,
    cache_read_reported: usize,
    cache_write_reported: usize,
    total_llm: usize,
    error_spans: usize,
    running_spans: usize,
    title: String,
    first_trace: u64,
    first_trace_set: bool,
}

/// 控制台会话边车：span 级聚合 + 会话级增量聚合 + 排序结果缓存。
#[derive(Default)]
struct SessionIndex {
    span: HashMap<ScopedSpanKey, SpanAgg>,
    sess: BTreeMap<(Option<u64>, u64), SessionAgg>,
    /// delete/upgrade 改了段（不走 index_record）→ 标脏，下次读全量重建。
    dirty: bool,
    /// 任何改动 +1；排序结果缓存据此判失效。
    ver: u64,
    cache: Option<(u64, Vec<ConsoleSession>)>,
}

impl SessionIndex {
    fn apply_record(&mut self, r: &WalRecord) {
        if self.dirty {
            return;
        }
        let key = (r.fields.tenant_id, r.trace_id, r.span_id);
        let mut new = self.span.get(&key).cloned().unwrap_or_default();
        if new.from_fold {
            self.dirty = true;
            self.cache = None;
            return;
        }
        if !new.event_order.accept(r.identity.event_id().0) {
            return;
        }
        let seq = r.identity.seq;
        new.trace = r.trace_id;
        new.tenant = r.fields.tenant_id;
        macro_rules! field {
            ($src:ident,$dst:ident) => {
                if let Some(value) = r.fields.$src.as_ref() {
                    if new.event_order.field(stringify!($src), seq) {
                        new.$dst = value.clone();
                    }
                }
            };
        }
        if let Some(value) = r.fields.session_id {
            if new.event_order.field("session_id", seq) {
                new.session = Some(value);
            }
        }
        if let Some(value) = r.fields.external_session_id.as_ref() {
            if new.event_order.field("external_session_id", seq) {
                new.external_session = Some(value.clone());
            }
        }
        field!(input_tokens, in_tok);
        field!(output_tokens, out_tok);
        if let Some(value) = r.fields.cache_read_tokens {
            if new.event_order.field("cache_read_tokens", seq) {
                new.cache_read = Some(value);
            }
        }
        if let Some(value) = r.fields.cache_write_tokens {
            if new.event_order.field("cache_write_tokens", seq) {
                new.cache_write = Some(value);
            }
        }
        if r.fields.model.is_some()
            || r.fields.input_tokens.is_some()
            || r.fields.output_tokens.is_some()
            || r.fields.cache_read_tokens.is_some()
            || r.fields.cache_write_tokens.is_some()
        {
            new.is_llm = true;
        }
        if let Some(status) = r.fields.status {
            if new.event_order.field("status", seq) {
                new.error = status != 0;
            }
        }
        match r.identity.event_type {
            EventType::SpanStart => new.has_start = true,
            EventType::SpanEnd => new.has_end = true,
            _ => {}
        }
        if let Some(agent) = r.fields.agent_name.as_ref() {
            if new.event_order.field("agent_name", seq) {
                new.agent = Some(agent.clone());
            }
        }
        self.apply_span(key, new);
    }

    /// 把一个 span 的"当前聚合 → 新聚合"差量应用到会话级（增量、O(1)）。
    fn apply_span(&mut self, key: ScopedSpanKey, new: SpanAgg) {
        let old = self.span.get(&key).cloned().unwrap_or_default();
        if old.session != new.session {
            if let Some(os) = old.session {
                self.sub(os, &old);
            }
            if let Some(ns) = new.session {
                self.add(ns, &new);
            }
        } else if let Some(s) = new.session {
            // 同会话：只动 token / error 差量。
            let e = self.sess.entry((new.tenant, s)).or_default();
            if e.external_session.is_none() {
                e.external_session = new.external_session.clone();
            }
            e.in_tok = (e.in_tok as i64 + new.in_tok as i64 - old.in_tok as i64).max(0) as u64;
            e.out_tok = (e.out_tok as i64 + new.out_tok as i64 - old.out_tok as i64).max(0) as u64;
            e.cache_read = (e.cache_read as i128 + new.cache_read.unwrap_or(0) as i128
                - old.cache_read.unwrap_or(0) as i128)
                .max(0) as u64;
            e.cache_write = (e.cache_write as i128 + new.cache_write.unwrap_or(0) as i128
                - old.cache_write.unwrap_or(0) as i128)
                .max(0) as u64;
            e.cache_read_reported = (e.cache_read_reported as i64 + new.cache_read.is_some() as i64
                - old.cache_read.is_some() as i64)
                .max(0) as usize;
            e.cache_write_reported = (e.cache_write_reported as i64
                + new.cache_write.is_some() as i64
                - old.cache_write.is_some() as i64)
                .max(0) as usize;
            e.total_llm =
                (e.total_llm as i64 + new.is_llm as i64 - old.is_llm as i64).max(0) as usize;
            e.error_spans =
                (e.error_spans as i64 + new.error as i64 - old.error as i64).max(0) as usize;
            e.running_spans = (e.running_spans as i64 + (new.has_start && !new.has_end) as i64
                - (old.has_start && !old.has_end) as i64)
                .max(0) as usize;
            if e.title.is_empty() {
                if let Some(a) = &new.agent {
                    e.title = a.clone();
                }
            }
        }
        self.span.insert(key, new);
        self.ver += 1;
        self.cache = None;
    }

    fn add(&mut self, sid: u64, s: &SpanAgg) {
        let e = self.sess.entry((s.tenant, sid)).or_default();
        e.in_tok += s.in_tok;
        e.out_tok += s.out_tok;
        e.cache_read += s.cache_read.unwrap_or(0);
        e.cache_write += s.cache_write.unwrap_or(0);
        e.cache_read_reported += s.cache_read.is_some() as usize;
        e.cache_write_reported += s.cache_write.is_some() as usize;
        e.total_llm += s.is_llm as usize;
        e.error_spans += s.error as usize;
        e.running_spans += (s.has_start && !s.has_end) as usize;
        e.traces.insert(s.trace);
        if e.external_session.is_none() {
            e.external_session = s.external_session.clone();
        }
        if !e.first_trace_set || s.trace < e.first_trace {
            e.first_trace = s.trace;
            e.first_trace_set = true;
        }
        if e.title.is_empty() {
            if let Some(a) = &s.agent {
                e.title = a.clone();
            }
        }
    }

    fn sub(&mut self, sid: u64, s: &SpanAgg) {
        if let Some(e) = self.sess.get_mut(&(s.tenant, sid)) {
            e.in_tok = e.in_tok.saturating_sub(s.in_tok);
            e.out_tok = e.out_tok.saturating_sub(s.out_tok);
            e.cache_read = e.cache_read.saturating_sub(s.cache_read.unwrap_or(0));
            e.cache_write = e.cache_write.saturating_sub(s.cache_write.unwrap_or(0));
            e.cache_read_reported = e
                .cache_read_reported
                .saturating_sub(s.cache_read.is_some() as usize);
            e.cache_write_reported = e
                .cache_write_reported
                .saturating_sub(s.cache_write.is_some() as usize);
            e.total_llm = e.total_llm.saturating_sub(s.is_llm as usize);
            e.error_spans = e.error_spans.saturating_sub(s.error as usize);
            e.running_spans = e
                .running_spans
                .saturating_sub((s.has_start && !s.has_end) as usize);
            // traces / first_trace 不在此精确回收（会话切换极罕见）；delete/upgrade 走标脏重建纠正。
        }
    }

    /// 从折叠 span 全量重建（delete/upgrade 标脏后、或首次）。
    fn rebuild(&mut self, spans: &[FoldedSpan]) {
        self.span.clear();
        self.sess.clear();
        for s in spans {
            let sa = SpanAgg {
                event_order: SpanEventOrder::default(),
                from_fold: true,
                tenant: s.tenant_id,
                session: s.session_id,
                external_session: s.external_session_id.clone(),
                in_tok: s.input_tokens.unwrap_or(0),
                out_tok: s.output_tokens.unwrap_or(0),
                cache_read: s.cache_read_tokens,
                cache_write: s.cache_write_tokens,
                is_llm: s.model.is_some()
                    || s.input_tokens.is_some()
                    || s.output_tokens.is_some()
                    || s.cache_read_tokens.is_some()
                    || s.cache_write_tokens.is_some(),
                error: s.status.unwrap_or(0) != 0,
                has_start: s.has_start,
                has_end: s.has_end,
                agent: s.agent_name.clone(),
                trace: s.trace_id,
            };
            if let Some(sid) = sa.session {
                self.add(sid, &sa);
            }
            self.span.insert((s.tenant_id, s.trace_id, s.span_id), sa);
        }
        self.dirty = false;
        self.ver += 1;
        self.cache = None;
    }

    /// 产出按 session_id 降序的会话行（带缓存，ver 没变直接复用）。
    fn rows(&mut self) -> Vec<ConsoleSession> {
        if let Some((v, c)) = &self.cache {
            if *v == self.ver {
                return c.clone();
            }
        }
        let mut out: Vec<ConsoleSession> = self
            .sess
            .iter()
            .map(|(sid, a)| ConsoleSession {
                session_id: sid.1,
                external_session_id: a.external_session.clone(),
                title: if a.title.is_empty() {
                    format!("会话 {}", sid.1)
                } else {
                    a.title.clone()
                },
                turn_count: a.traces.len(),
                input_tokens: a.in_tok,
                output_tokens: a.out_tok,
                cache_read_tokens: (a.cache_read_reported > 0).then_some(a.cache_read),
                cache_write_tokens: (a.cache_write_reported > 0).then_some(a.cache_write),
                cache_read_reported_spans: a.cache_read_reported,
                cache_write_reported_spans: a.cache_write_reported,
                total_llm_spans: a.total_llm,
                has_error: a.error_spans > 0,
                has_running: a.running_spans > 0,
                first_trace_id: a.first_trace,
            })
            .collect();
        out.sort_by(|a, b| b.session_id.cmp(&a.session_id));
        self.cache = Some((self.ver, out.clone()));
        out
    }
}
