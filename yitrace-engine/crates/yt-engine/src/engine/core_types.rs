// ───────────────────────── 段生命周期 ─────────────────────────

/// 段五态（草案 1 §D1.2）。building/sealed 不进 manifest；dead 已从 manifest 移除、等回收。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegLifecycle {
    Building,
    Sealed,
    Live,
    Compacting,
    Dead,
}

/// 持久读模型的进程内加载状态。
///
/// 数据本体仍由 segment + WAL + manifest 保证；这些索引只是可重建的加速层。持久模式重启时先把
/// 三组状态标成未就绪，数据库立即可用，真正需要某条读路径或第一次写入时再加载对应缓存。
#[derive(Debug, Clone, Copy)]
struct ReadModelLoadState {
    rollup_ready: bool,
    filter_attrs_ready: bool,
}

impl ReadModelLoadState {
    fn ready() -> Self {
        Self {
            rollup_ready: true,
            filter_attrs_ready: true,
        }
    }

    fn deferred() -> Self {
        Self {
            rollup_ready: false,
            filter_attrs_ready: false,
        }
    }
}

// ───────────────────────── 外部件接口边界 ─────────────────────────

/// **折叠列投影**：聚合/列表类查询声明它要读哪些**可折叠值列**。
///
/// 身份与分组列（trace_id/span_id/ts/seq/event_type/ext_span_id）**恒读**——折叠去重、组内定序、
/// 分组都要用，不在投影里。投影只挑可折叠值列，主要价值是让**列式段（Vortex）跳过不读的列**，
/// 尤其两个大文本列 `input_text`/`output_text`（多数聚合/成本/会话查询根本不碰原文）。
///
/// 行式/内存源忽略投影（数据本就全在手边、没有列 I/O 可省）；只有列式段从中受益。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Projection(u32);

impl Projection {
    pub const STATUS: u32 = 1 << 0;
    pub const DURATION_NS: u32 = 1 << 1;
    pub const PARENT_SPAN_ID: u32 = 1 << 2;
    pub const INPUT_TOKENS: u32 = 1 << 3;
    pub const OUTPUT_TOKENS: u32 = 1 << 4;
    pub const SESSION_ID: u32 = 1 << 5;
    pub const AGENT_NAME: u32 = 1 << 6;
    pub const TOOL_NAME: u32 = 1 << 7;
    pub const MODEL: u32 = 1 << 8;
    pub const INPUT_TEXT: u32 = 1 << 9;
    pub const OUTPUT_TEXT: u32 = 1 << 10;
    pub const EVAL_SCORE: u32 = 1 << 11;
    pub const EVAL_LABEL: u32 = 1 << 12;
    pub const LOGS: u32 = 1 << 13;
    pub const TENANT_ID: u32 = 1 << 14;
    pub const EXTERNAL_IDS: u32 = 1 << 15;
    pub const ATTRS: u32 = 1 << 16;
    pub const SPAN_NAME: u32 = 1 << 17;
    pub const DISPLAY_NAME: u32 = 1 << 18;
    pub const CACHE_READ_TOKENS: u32 = 1 << 19;
    pub const CACHE_WRITE_TOKENS: u32 = 1 << 20;

    const MASK: u32 = (1 << 21) - 1;

    /// 全列（含两个大文本列）。普通读 / trace 详情 / eval 打分 / 数据集采集要原文，用这个。
    pub const ALL: Projection = Projection(Self::MASK);

    /// 选定若干列（位或）。如 `Projection::of(Projection::AGENT_NAME | Projection::INPUT_TOKENS)`。
    pub const fn of(cols: u32) -> Self {
        Projection(cols & Self::MASK)
    }

    /// 该投影是否要某列（传列常量）。
    pub const fn has(self, col: u32) -> bool {
        self.0 & col != 0
    }

    /// 是否要全部列——存储据此走"读全列"快路（与历史行为完全一致），不必逐列裁剪。
    pub const fn is_all(self) -> bool {
        self.0 == Self::MASK
    }

    /// 原始位（列式存储据此判断每列读不读）。
    pub const fn bits(self) -> u32 {
        self.0
    }
}

/// 列式不可变段存储。真实实现接 **Vortex**（layouts + zone-map + 统计）；
/// 删除/manifest/版本不归它管（那是本引擎自己的事，见 yt-core::manifest）。
#[derive(Default)]
pub struct KeyedSegmentScan {
    pub rows: Vec<(u32, FoldInput)>,
    /// true 表示存储层按磁盘 key 目录点查，没有先解码整段。
    pub used_point_index: bool,
    /// 本次真正解码的物理记录数。
    pub decoded_rows: usize,
    /// 点查实际读取的索引字节数，包含首次完整校验索引的成本。
    pub index_bytes_read: u64,
    /// 点查实际读取的段数据字节数，包含首次完整校验段 CRC 的成本。
    pub data_bytes_read: u64,
    /// 本次首次校验通过的索引数。
    pub indexes_validated: usize,
    /// 本次缺失或损坏后重建成功的索引数。
    pub indexes_rebuilt: usize,
}

/// 按 `(trace_id, span_id)` 点读出来的原始事件。
///
/// `FoldInput` 不保留事件时间和原始日志，单 Span 详情需要这份原始记录来返回 `logEvents`，
/// 因此不能复用 `KeyedSegmentScan` 后再猜回原始事件。
#[derive(Default)]
pub struct KeyedRecordScan {
    pub rows: Vec<(u32, WalRecord)>,
    pub used_point_index: bool,
    pub decoded_rows: usize,
    pub index_bytes_read: u64,
    pub data_bytes_read: u64,
    pub indexes_validated: usize,
    pub indexes_rebuilt: usize,
}

/// 顺序读取完整 segment 时的物理读统计。派生 sidecar 迁移会走这条路径，查询计划必须把
/// 一次性全段读取成本如实返回，不能伪装成只读了目标 Span。
#[derive(Default)]
pub struct FullRecordScan {
    pub rows: Vec<WalRecord>,
    pub data_bytes_read: u64,
}

#[derive(Default)]
struct FoldQueryStats {
    scanned_segments: usize,
    point_lookup_segments: usize,
    decoded_segment_rows: usize,
    decoded_memtable_rows: usize,
    index_bytes_read: u64,
    data_bytes_read: u64,
    indexes_validated: usize,
    indexes_rebuilt: usize,
    fallback_reason: Option<String>,
}

pub trait SegmentStore: Send + Sync {
    /// 把一批已 ack 事件写成段 `seg`（building→sealed）。
    /// seg 由协调器分配（单写者、全局唯一、永不复用），不由存储自选。
    fn flush_to_segment(&self, seg: SegmentId, records: &[WalRecord]);
    /// 新写入接缝允许普通 I/O 错误返回；已有纯内存适配器保持兼容。
    fn try_flush_to_segment(&self, seg: SegmentId, records: &[WalRecord]) -> std::io::Result<()> {
        self.flush_to_segment(seg, records);
        Ok(())
    }
    /// 扫一个段，返回 (段内行号, 折叠输入)。读路径据行号查 deletion_vec 跳过已删行。
    /// 真实实现是 Vortex 段扫描 + 谓词/zone 剪枝下推；这里是接口边界。
    fn scan_fold_inputs(&self, seg: SegmentId) -> Vec<(u32, FoldInput)>;
    /// 扫一个段的原始记录（compaction 重建新段用）。
    fn scan_records(&self, seg: SegmentId) -> Vec<WalRecord>;
    /// 与 `scan_records` 相同，但返回真实物理读取量。默认存储只能报告解码结果；文件段覆盖它，
    /// 精确报告读入并校验的 segment 字节数。
    fn scan_records_with_stats(&self, seg: SegmentId) -> FullRecordScan {
        FullRecordScan {
            rows: self.scan_records(seg),
            data_bytes_read: 0,
        }
    }
    /// 物理删除一个 dead 段文件（仅在 §D1.4 三条水位放行后调用）。
    fn unlink_segment(&self, seg: SegmentId);

    /// 可选：只解码命中 key 的折叠输入，并保留段内行号供 deletion vector 校验。
    /// 默认回退到引擎的段折叠缓存；文件段可用它避免把整段大字段常驻内存。
    fn scan_fold_inputs_for_keys(
        &self,
        _seg: SegmentId,
        _keys: &HashSet<(u64, u64)>,
    ) -> Option<KeyedSegmentScan> {
        None
    }

    /// 可选：只读取命中 key 的原始事件。单 Span 日志详情使用它，避免为一条日志解码整个段。
    /// 返回的物理行号必须和段 deletion vector 使用的行号一致。
    fn scan_records_for_keys(
        &self,
        _seg: SegmentId,
        _keys: &HashSet<(u64, u64)>,
    ) -> Option<KeyedRecordScan> {
        None
    }

    /// 可选：**投影扫描**，只解码 `proj` 选中的可折叠值列（身份/分组列恒读），返回**带物理行号**的
    /// `FoldInput`。投影只裁列、不丢行，故行号完整、与删除位图共存安全——**任何查询都能用**。
    /// 默认 `None` = 不支持，引擎回退 `scan_fold_inputs` 读全列。列式存储（Vortex）覆盖它，让聚合/列表
    /// 查询跳过不读的大文本列（上列式最大的单点收益）。
    fn scan_fold_inputs_projected(
        &self,
        _seg: SegmentId,
        _proj: Projection,
    ) -> Option<Vec<(u32, FoldInput)>> {
        None
    }

    /// 可选：**按时间范围下推扫描 + 投影**，返回 `ts ∈ [from, to]` 命中行的 `FoldInput`（不带物理行号），
    /// 只解码 `proj` 选中的列。默认 `None` = 不支持下推，引擎回退全扫。列式存储（Vortex）覆盖它，把时间
    /// 过滤推进文件扫描、只解码命中行的命中列。
    /// **注意**：下推丢了物理行号，而删除按物理行号定位，二者不能共存——引擎只在「段无删除」时用它。
    fn scan_fold_inputs_in_time(
        &self,
        _seg: SegmentId,
        _from: i64,
        _to: i64,
        _proj: Projection,
    ) -> Option<Vec<FoldInput>> {
        None
    }
}

/// dead_set 里的一个待回收资源（草案 1 §D1.4）。
/// 目前只建段；deletion / upgrade 块同理共用此水位（留扩展）。
struct DeadResource {
    seg: SegmentId,
    /// 该资源变 dead 的 manifest 版本号。
    v_dead: u64,
}

/// compaction 计划：选了哪些输入段 + 选段瞬间各段的 (deletion_seq, upgrade_seq)。
/// `compaction_finish` 据此判断选段后是否有并发删除/补写打进来（OPEN-3）。
pub struct CompactionPlan {
    inputs: Vec<SegmentId>,
    seqs_at_select: HashMap<u64, (u64, u64)>,
}

/// 段文件的 buffer pin 计数（GC 安全条件 (2)：字节级最后保险）。
/// 真实实现复用 vector_smgr 的 pin/release；这里用计数表骨架。
#[derive(Default)]
struct BufferPins {
    counts: Mutex<HashMap<u64, u32>>,
}
impl BufferPins {
    fn pin(&self, seg: SegmentId) {
        *self.counts.lock().unwrap().entry(seg.get()).or_insert(0) += 1;
    }
    fn unpin(&self, seg: SegmentId) {
        let mut g = self.counts.lock().unwrap();
        if let Some(c) = g.get_mut(&seg.get()) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                g.remove(&seg.get());
            }
        }
    }
    fn is_pinned(&self, seg: SegmentId) -> bool {
        self.counts
            .lock()
            .unwrap()
            .get(&seg.get())
            .map_or(false, |&c| c > 0)
    }
}

/// BM25 中文倒排。真实实现 = 团队自有 BM25（cppjieba 分词 FFI + Rust 重写的倒排 + block-max-WAND）。
/// 这是「FFI 复用评分/分词、重写存储」的落点（决策文档 §2.1）。接口按 span 维度（检索返回的是 trace/span）。
pub trait Bm25Index: Send + Sync {
    /// 派生索引必须明确支持租户分区；旧 adapter 只允许无租户写入。
    fn supports_tenant_scope(&self) -> bool {
        false
    }
    fn empty_like(&self) -> Option<Arc<dyn Bm25Index>> {
        None
    }
    fn index_text_scoped(&self, _tenant: Option<u64>, trace: u64, span: u64, text: &str) {
        self.index_text(trace, span, text)
    }
    fn index_event_scoped(
        &self,
        tenant: Option<u64>,
        event_id: u64,
        trace_id: u64,
        span_id: u64,
        text: &str,
    ) {
        // 写入口在拿写锁前检查 supports_tenant_scope；此默认路径只供旧无租户适配器。
        let _ = tenant;
        self.index_event(event_id, trace_id, span_id, text);
    }
    fn mark_event_scoped(&self, _tenant: Option<u64>, event_id: u64) {
        self.mark_event(event_id);
    }
    /// 无过滤查询允许原生实现复用每租户缓存，评分仍保持分区独立。
    fn search_all_scoped(&self, query: &str, k: usize) -> Vec<(Option<u64>, u64, u64, f32)> {
        self.search_scoped(query, k, &|_, _, _| true)
    }
    fn search_scoped(
        &self,
        query: &str,
        k: usize,
        filter: &dyn Fn(Option<u64>, u64, u64) -> bool,
    ) -> Vec<(Option<u64>, u64, u64, f32)> {
        self.search_filtered(query, k, &|t, s| filter(None, t, s))
            .into_iter()
            .map(|(t, s, score)| (None, t, s, score))
            .collect()
    }

    /// 把某 span 的文本喂进倒排（ingest/flush 时调用）。真实实现走 jieba 分词 + 段内倒排。
    fn index_text(&self, trace_id: u64, span_id: u64, text: &str);
    /// 按确定性 event_id 幂等建索引。默认适配器仍按普通文本处理；原生持久索引会记住
    /// 已见 event_id，SDK 重试和 WAL 重放不会重复增加词频。
    fn index_event(&self, event_id: u64, trace_id: u64, span_id: u64, text: &str) {
        let _ = event_id;
        self.index_text(trace_id, span_id, text);
    }
    /// 只登记已见 event_id，不增加文本。用于从折叠 span 重建文本后补齐幂等状态。
    fn mark_event(&self, _event_id: u64) {}
    /// 中文检索，返回 (trace_id, span_id, 评分)，按分降序、取前 k。
    /// 真实实现作为 DataFusion 自定义扫描节点下推（@~@ + LIMIT）。
    fn search(&self, query: &str, k: usize) -> Vec<(u64, u64, f32)>;
    /// 带 key 过滤的中文检索。默认实现逐步扩大候选窗，旧适配器无需立即修改；原生实现应在
    /// 倒排打分阶段应用过滤，避免同一批高频词候选被重复打分。
    fn search_filtered(
        &self,
        query: &str,
        k: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(u64, u64, f32)> {
        if k == 0 {
            return Vec::new();
        }
        let mut pool = k.max(50);
        loop {
            let mut candidates = self.search(query, pool);
            let exhausted = candidates.len() < pool;
            candidates.retain(|&(trace_id, span_id, _)| filter(trace_id, span_id));
            if candidates.len() >= k || exhausted {
                candidates.truncate(k);
                return candidates;
            }
            let next = pool.saturating_mul(4);
            if next == pool {
                return candidates;
            }
            pool = next;
        }
    }
    /// 清空派生倒排。多进程 embedded 刷新时会从持久段 + WAL tail 重建。
    fn clear(&self) {}
    /// 加载持久化倒排。自定义 BM25 实现可不支持；不支持时引擎会回退到段扫描重建。
    fn load_cache(
        &self,
        _path: &std::path::Path,
        _manifest_version: u64,
        _memtable_watermark: u64,
    ) -> bool {
        false
    }
    /// 保存持久化倒排。返回 Ok(false) 表示当前实现不支持持久化。
    fn save_cache(
        &self,
        _path: &std::path::Path,
        _manifest_version: u64,
        _memtable_watermark: u64,
    ) -> std::io::Result<bool> {
        Ok(false)
    }
}

/// graph_index 向量 ANN。真实实现 = 团队自有图索引（algorithm/distance/PQ 经 C ABI FFI 复用）。
/// 「带过滤 ANN」目前是半成品（PoC C 要验进图过滤能否把召回拉回来），这里把 filter 作为一等参数。
pub type ScopedSpanKey = (Option<u64>, u64, u64);

pub trait GraphIndex: Send + Sync {
    fn supports_tenant_scope(&self) -> bool {
        false
    }
    /// 给某 span 建/更新向量（向量由外部 embedder 算，不是每个 span 都有）。
    fn index_embedding(&self, trace_id: u64, span_id: u64, embedding: Vec<f32>);
    /// 带过滤的近邻搜索：`filter` 是下推进图搜索的谓词（service/time/status…）。
    /// 返回 (trace_id, span_id, 距离)，按距离升序、取前 k。真实实现把 filter 接进 search_layer 的导航。
    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(u64, u64, f32)>;
    /// 租户属于向量身份。旧适配器只能处理无租户数据，不能把有租户向量写成全局数据。
    fn index_embedding_scoped(
        &self,
        tenant_id: Option<u64>,
        trace_id: u64,
        span_id: u64,
        embedding: Vec<f32>,
    ) -> std::io::Result<()> {
        if tenant_id.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "graph adapter does not support tenant-scoped embeddings",
            ));
        }
        self.index_embedding(trace_id, span_id, embedding);
        Ok(())
    }
    fn search_scoped(
        &self,
        query: &[f32],
        k: usize,
        filter: &dyn Fn(Option<u64>, u64, u64) -> bool,
    ) -> Vec<(Option<u64>, u64, u64, f32)> {
        self.search(query, k, &|t, s| filter(None, t, s))
            .into_iter()
            .map(|(t, s, d)| (None, t, s, d))
            .collect()
    }
    /// 只有旧格式缺租户记录的节点才需要源数据解析；现代明确None节点不参与。
    fn needs_legacy_tenant_migration(&self) -> bool {
        false
    }
    fn legacy_embedding_keys(&self) -> std::io::Result<Vec<(u64, u64)>> {
        Ok(Vec::new())
    }
    fn migrate_legacy_embeddings(
        &self,
        _tenants: &HashMap<(u64, u64), Option<u64>>,
    ) -> std::io::Result<()> {
        Ok(())
    }
    /// 独立向量写入不推进 WAL/manifest，因此必须另检查向量文件变化；只按需重开。
    fn reload_if_changed(&self) {}
    fn reload_if_changed_checked(&self) -> std::io::Result<()> {
        self.reload_if_changed();
        Ok(())
    }
    /// 落盘点（提交时调）：插入只写不刷的实现（如磁盘索引）在此批量 fsync。内存实现默认空操作。
    /// 我们的场景 **append 极多、删除少** —— 插入走"只写不刷"，靠这里在提交点批量持久，吞吐才扛得住。
    fn flush(&self) {}
    fn flush_checked(&self) -> std::io::Result<()> {
        self.flush();
        Ok(())
    }
    /// 清空内存图。多进程 embedded 刷新时会从持久向量文件或磁盘图索引重建。
    fn clear(&self) {}
    /// 多进程 embedded 刷新时调用。持久图索引可重新打开元页/图文件，内存实现默认空操作。
    fn reload(&self) {}
    fn reload_checked(&self) -> std::io::Result<()> {
        self.reload();
        Ok(())
    }
}

/// 朴素内存 BM25 骨架：按 span 存文本，检索按「查询子串命中数」打分。
/// 真实实现换成团队自有 BM25（jieba 词级分词 + block-max-WAND 评分）。这里只为把检索路径打通可测。
#[derive(Default)]
pub struct InMemoryBm25 {
    docs: Mutex<BTreeMap<(u64, u64), String>>,
}
impl Bm25Index for InMemoryBm25 {
    fn index_text(&self, trace_id: u64, span_id: u64, text: &str) {
        let mut g = self.docs.lock().unwrap();
        let doc = g.entry((trace_id, span_id)).or_default();
        doc.push_str(text);
        doc.push(' ');
    }
    fn search(&self, query: &str, k: usize) -> Vec<(u64, u64, f32)> {
        // 朴素：每个查询词（空白切）在文档里出现就 +1 分。中文用子串命中（真实实现是 jieba 词级）。
        let qtokens: Vec<&str> = query.split_whitespace().collect();
        let g = self.docs.lock().unwrap();
        let mut scored: Vec<(u64, u64, f32)> = g
            .iter()
            .filter_map(|(&(t, s), text)| {
                let score = qtokens.iter().filter(|q| text.contains(**q)).count() as f32;
                (score > 0.0).then_some((t, s, score))
            })
            .collect();
        scored.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap());
        scored.truncate(k);
        scored
    }

    fn search_filtered(
        &self,
        query: &str,
        k: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(u64, u64, f32)> {
        let qtokens: Vec<&str> = query.split_whitespace().collect();
        let g = self.docs.lock().unwrap();
        let mut scored: Vec<_> = g
            .iter()
            .filter(|&(&(trace_id, span_id), _)| filter(trace_id, span_id))
            .filter_map(|(&(trace_id, span_id), text)| {
                let score = qtokens
                    .iter()
                    .filter(|token| text.contains(**token))
                    .count() as f32;
                (score > 0.0).then_some((trace_id, span_id, score))
            })
            .collect();
        scored.sort_by(|a, b| b.2.total_cmp(&a.2).then((a.0, a.1).cmp(&(b.0, b.1))));
        scored.truncate(k);
        scored
    }

    fn clear(&self) {
        self.docs.lock().unwrap().clear();
    }
}

/// 朴素内存向量索引骨架：暴力 L2 距离。真实实现换团队 graph_index（图式 ANN + 带过滤导航）。
#[derive(Default)]
pub struct InMemoryGraphIndex {
    vecs: Mutex<BTreeMap<ScopedSpanKey, Vec<f32>>>,
}
impl GraphIndex for InMemoryGraphIndex {
    fn supports_tenant_scope(&self) -> bool {
        true
    }
    fn index_embedding(&self, trace_id: u64, span_id: u64, embedding: Vec<f32>) {
        self.index_embedding_scoped(None, trace_id, span_id, embedding)
            .unwrap();
    }
    fn index_embedding_scoped(
        &self,
        tenant_id: Option<u64>,
        trace_id: u64,
        span_id: u64,
        embedding: Vec<f32>,
    ) -> std::io::Result<()> {
        self.vecs
            .lock()
            .unwrap()
            .insert((tenant_id, trace_id, span_id), embedding);
        Ok(())
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
        let vectors = self.vecs.lock().unwrap();
        let mut scored: Vec<_> = vectors
            .iter()
            .filter(|(&(tenant, t, s), _)| filter(tenant, t, s))
            .map(|(&(tenant, t, s), v)| (tenant, t, s, l2_distance(query, v)))
            .collect();
        scored.sort_by(|a, b| {
            a.3.total_cmp(&b.3)
                .then((a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)))
        });
        scored.truncate(k);
        scored
    }
    fn clear(&self) {
        self.vecs.lock().unwrap().clear();
    }
}

fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

/// 内存段存储（默认实现 / demo / 测试用）。真实实现换 Vortex 列式不可变段。
/// `unlink` 真删 —— 配合回收水位，过早回收会让读者读到空（被压测当场抓住）。
#[derive(Default)]
pub struct InMemorySegmentStore {
    rows: Mutex<BTreeMap<u64, Vec<WalRecord>>>,
}
impl SegmentStore for InMemorySegmentStore {
    fn flush_to_segment(&self, seg: SegmentId, records: &[WalRecord]) {
        self.rows
            .lock()
            .unwrap()
            .insert(seg.get(), records.to_vec());
    }
    fn scan_fold_inputs(&self, seg: SegmentId) -> Vec<(u32, FoldInput)> {
        self.rows
            .lock()
            .unwrap()
            .get(&seg.get())
            .map(|rs| {
                rs.iter()
                    .enumerate()
                    .map(|(i, r)| (i as u32, r.to_fold_input()))
                    .collect()
            })
            .unwrap_or_default()
    }
    fn scan_records(&self, seg: SegmentId) -> Vec<WalRecord> {
        self.rows
            .lock()
            .unwrap()
            .get(&seg.get())
            .cloned()
            .unwrap_or_default()
    }
    fn unlink_segment(&self, seg: SegmentId) {
        self.rows.lock().unwrap().remove(&seg.get());
    }
}

/// 一批记录的时间范围（zone-map）。空批返回 (0, 0)。
fn ts_range(records: &[WalRecord]) -> (i64, i64) {
    let mut it = records.iter().map(|r| r.ts);
    match it.next() {
        None => (0, 0),
        Some(first) => it.fold((first, first), |(lo, hi), t| (lo.min(t), hi.max(t))),
    }
}

/// 读一条/一批 trace 的查询条件。时间窗 + 可选 trace_id。
pub struct TraceQuery {
    /// None = 所有 trace。
    pub trace_id: Option<u64>,
    /// 时间窗 [from, to]（闭区间）。
    pub time_from: i64,
    pub time_to: i64,
    /// **租户隔离**：设了它，只读该租户的 span。服务层须按鉴权身份注入（与检索路径一致）。
    pub tenant_id: Option<u64>,
}

impl TraceQuery {
    /// 全开窗、所有 trace（等价于不剪枝）。
    pub fn all() -> Self {
        Self {
            trace_id: None,
            time_from: i64::MIN,
            time_to: i64::MAX,
            tenant_id: None,
        }
    }
    pub fn trace(trace_id: u64, time_from: i64, time_to: i64) -> Self {
        Self {
            trace_id: Some(trace_id),
            time_from,
            time_to,
            tenant_id: None,
        }
    }
    /// 限定租户（链式）。
    pub fn for_tenant(mut self, tenant_id: u64) -> Self {
        self.tenant_id = Some(tenant_id);
        self
    }
}
