//! yt-core —— 核心类型。
//!
//! 把加固设计文档（…segment-snapshot-hardened.md）第 0 部分「共享底座」的数据模型
//! 变成 Rust 类型：三类不可变标识、确定性 event_id、不可变 Manifest（写时复制）、
//! 与 deletion / upgrade 两个对称的不可变块。
//!
//! 设计要点（务必对照文档）：
//! - Manifest 是「值」不是可变结构：commit = 在旧值上写时复制生成新版本，再原子换指针。
//! - deletion_vec 与 upgrade_ref 结构完全对称：都是 `Arc<不可变块>` + 单调 `_seq`，
//!   新补写一律生成新块、绝不原地改旧块。
//! - event_id 必须是确定性的：`hash(ext_span_id, seq, event_type)`，重传/崩溃重放算出同一个，
//!   绝不用引擎侧生成的雪花号。身份字段冻结，不可被 upgrade 覆盖。
#![allow(dead_code)]

/// 三类不可变标识：全单调递增、单写者分配、永不复用（GC 后也不复用，避免 ABA）。
pub mod ids {
    macro_rules! id_newtype {
        ($(#[$m:meta])* $name:ident) => {
            $(#[$m])*
            #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub struct $name(pub u64);
            impl $name {
                pub const fn new(v: u64) -> Self { Self(v) }
                pub const fn get(self) -> u64 { self.0 }
            }
        };
    }

    id_newtype!(
        /// 物理段身份，全局唯一、永不复用。
        SegmentId
    );
    id_newtype!(
        /// deletion 块 / upgrade 块的身份，永不复用。
        ChunkId
    );
    id_newtype!(
        /// 写前日志的提交点。
        WalLsn
    );
    id_newtype!(
        /// 全局回收纪元（epoch）。
        Epoch
    );

    /// manifest 版本号。当前约定 `snapshot_id == manifest_version`（一对一，见 OPEN-1）。
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct ManifestVersion(pub u64);
    impl ManifestVersion {
        pub const fn new(v: u64) -> Self {
            Self(v)
        }
        pub const fn next(self) -> Self {
            Self(self.0 + 1)
        }
        pub const fn get(self) -> u64 {
            self.0
        }
    }

    /// 读者 pin 的版本状态。
    /// 关键（D2.1）：读者「先登记 slot、后解引用 current」，登记到落定之间是 `Tentative`，
    /// 回收线程对 Tentative slot 按 `observed_epoch` 设回收下限，堵住中间残窗。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum PinnedVersion {
        Tentative { observed_epoch: u64 },
        Fixed(ManifestVersion),
    }
}

/// 事件身份与确定性 event_id（M.7）。
pub mod event {
    /// 事件类型。`tag()` 进 event_id 哈希，必须稳定、不可改。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum EventType {
        SpanStart,
        SpanEnd,
        Attr,
        Log,
        Error,
        Other(u8),
    }
    impl EventType {
        /// 进 event_id 哈希的稳定字节。改了会让历史数据去重失效，等同破坏审计可复现。
        pub fn tag(self) -> u8 {
            match self {
                EventType::SpanStart => 1,
                EventType::SpanEnd => 2,
                EventType::Attr => 3,
                EventType::Log => 4,
                EventType::Error => 5,
                EventType::Other(b) => 0x80 | (b & 0x7f),
            }
        }

        /// tag 反向映射（摄入端据 SDK 线格式重建类型）。`tag(from_tag(t)) == t` 对所有 t 成立。
        pub fn from_tag(tag: u8) -> EventType {
            match tag {
                1 => EventType::SpanStart,
                2 => EventType::SpanEnd,
                3 => EventType::Attr,
                4 => EventType::Log,
                5 => EventType::Error,
                b => EventType::Other(b & 0x7f),
            }
        }
    }

    /// 确定性事件 id。折叠去重的唯一键。
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct EventId(pub u64);

    /// 冻结的身份字段。upgrade 只能补写「非身份属性」，这三样定版后即不可改（M.7）。
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct EventIdentity {
        /// 上游 span 身份（来自打点 SDK，跨进程稳定）。
        pub ext_span_id: String,
        /// 上报序：客户端给出、原样持久化进 WAL、重放严禁重补。客户端没给时用 `derive_seq`。
        pub seq: u64,
        pub event_type: EventType,
    }
    impl EventIdentity {
        /// `event_id = fnv1a64(ext_span_id ++ seq ++ event_type)`。
        /// 纯确定性：同一物理事件，无论何时、第几次重放，恒得同一个 id。
        pub fn event_id(&self) -> EventId {
            let mut h = Fnv64::new();
            h.write(self.ext_span_id.as_bytes());
            h.write(&self.seq.to_le_bytes());
            h.write(&[self.event_type.tag()]);
            EventId(h.finish())
        }
    }

    /// 客户端未给 seq 时的稳定派生量：`hash(ts_normalized, payload_canonical)`。
    /// 绝不用「ingest 到达序」——那会让重放后 seq 漂移、event_id 变化、折叠翻倍。
    pub fn derive_seq(ts_normalized_nanos: u64, payload_canonical: &[u8]) -> u64 {
        let mut h = Fnv64::new();
        h.write(&ts_normalized_nanos.to_le_bytes());
        h.write(payload_canonical);
        h.finish()
    }

    /// 确定性 FNV-1a 64（跨进程/跨语言一致）。给需要"把任意字节稳定映成 u64"的地方复用，
    /// 例如 OTLP 适配器把字符串会话 id 哈希成 u64 —— 避免再抄一份哈希常量。
    pub fn fnv1a64(bytes: &[u8]) -> u64 {
        let mut h = Fnv64::new();
        h.write(bytes);
        h.finish()
    }

    /// FNV-1a 64 位。选它是因为确定性、零依赖、跨进程跨版本稳定。
    /// （真实实现可换更快的稳定哈希，但绝不能换成随机种子的 SipHash/RandomState。）
    struct Fnv64(u64);
    impl Fnv64 {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;
        fn new() -> Self {
            Self(Self::OFFSET)
        }
        fn write(&mut self, bytes: &[u8]) {
            for &b in bytes {
                self.0 ^= b as u64;
                self.0 = self.0.wrapping_mul(Self::PRIME);
            }
        }
        fn finish(&self) -> u64 {
            self.0
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn event_id_is_deterministic_and_seq_sensitive() {
            let a = EventIdentity {
                ext_span_id: "span-1".into(),
                seq: 7,
                event_type: EventType::SpanEnd,
            };
            let b = a.clone();
            // 同一身份 → 同一 id（这正是重放幂等依赖的性质）
            assert_eq!(a.event_id(), b.event_id());
            // seq 不同 → id 不同（防同 span 不同上报被折成一条）
            let c = EventIdentity {
                seq: 8,
                ..a.clone()
            };
            assert_ne!(a.event_id(), c.event_id());
            // event_type 不同 → id 不同
            let d = EventIdentity {
                event_type: EventType::SpanStart,
                ..a.clone()
            };
            assert_ne!(a.event_id(), d.event_id());
        }

        #[test]
        fn derive_seq_is_stable() {
            assert_eq!(derive_seq(123, b"payload"), derive_seq(123, b"payload"));
            assert_ne!(derive_seq(123, b"payload"), derive_seq(124, b"payload"));
        }
    }
}

/// deletion 位图 与 upgrade 块 —— 两个对称的不可变块（M.2）。
pub mod chunk {
    use super::fold::SpanFields;
    use super::ids::ChunkId;
    use std::collections::BTreeMap;

    /// per-段删除位图。占位实现用 `Vec<u64>` 位集；真实实现换 `roaring::RoaringBitmap`。
    /// 不可变：每次 delete 提交生成一个新的 `DeletionVec`（新 chunk_id），旧的留给旧版本读者。
    #[derive(Debug, Clone, Default)]
    pub struct DeletionVec {
        pub chunk_id: Option<ChunkId>,
        bits: Vec<u64>,
    }
    impl DeletionVec {
        pub fn empty() -> Self {
            Self::default()
        }

        /// 原始位字（manifest 持久化用）。
        pub fn bits(&self) -> &[u64] {
            &self.bits
        }

        /// 从持久化的位字重建（manifest 恢复用）。
        pub fn from_bits(chunk_id: Option<ChunkId>, bits: Vec<u64>) -> Self {
            Self { chunk_id, bits }
        }

        /// 写时复制地标记一行删除，返回新块（绝不原地改 self）。
        pub fn with_deleted(&self, row: u32, new_chunk_id: ChunkId) -> Self {
            let mut bits = self.bits.clone();
            let word = (row / 64) as usize;
            if word >= bits.len() {
                bits.resize(word + 1, 0);
            }
            bits[word] |= 1u64 << (row % 64);
            Self {
                chunk_id: Some(new_chunk_id),
                bits,
            }
        }

        pub fn is_deleted(&self, row: u32) -> bool {
            let word = (row / 64) as usize;
            self.bits
                .get(word)
                .map_or(false, |w| (w >> (row % 64)) & 1 == 1)
        }

        pub fn count(&self) -> u32 {
            self.bits.iter().map(|w| w.count_ones()).sum()
        }
    }

    /// upgrade 旁路列块（晚到属性补写 / 升格 patch）。与 `DeletionVec` 同等保护级别：
    /// `Arc<不可变块>` + 单调 `upgrade_seq`，新补写生成新 chunk_id 的新块、绝不原地改旧块。
    ///
    /// 按 (trace_id, span_id) 定位补写**非身份属性**（status/duration/logs）。身份字段冻结（M.7），
    /// 不进这里。真实实现是列式补写存储；这里用 map 骨架。
    #[derive(Debug, Clone, Default)]
    pub struct UpgradeColChunk {
        pub chunk_id: Option<ChunkId>,
        patches: BTreeMap<(u64, u64), SpanFields>,
    }
    impl UpgradeColChunk {
        pub fn empty() -> Self {
            Self::default()
        }

        /// 从持久化的补写表重建（manifest 恢复用）。
        pub fn from_patches(
            chunk_id: Option<ChunkId>,
            patches: BTreeMap<(u64, u64), SpanFields>,
        ) -> Self {
            Self { chunk_id, patches }
        }

        /// 写时复制地加一条补写，返回新块（绝不原地改 self）。同一 span 多次补写按非空覆盖 + 日志并集。
        /// 覆盖逻辑统一走 `SpanFields::merge_from`（含 eval 分数/标签等所有可补写字段，不再各列子集）。
        pub fn with_patch(
            &self,
            trace_id: u64,
            span_id: u64,
            fields: SpanFields,
            new_chunk_id: ChunkId,
        ) -> Self {
            let mut patches = self.patches.clone();
            patches
                .entry((trace_id, span_id))
                .or_default()
                .merge_from(&fields);
            Self {
                chunk_id: Some(new_chunk_id),
                patches,
            }
        }

        /// 某 span 的补写（若有）。
        pub fn patch_for(&self, trace_id: u64, span_id: u64) -> Option<&SpanFields> {
            self.patches.get(&(trace_id, span_id))
        }

        /// 遍历所有补写。
        pub fn iter(&self) -> impl Iterator<Item = (&(u64, u64), &SpanFields)> {
            self.patches.iter()
        }
    }
}

/// 不可变 Manifest（M.2）。
pub mod manifest {
    use super::chunk::{DeletionVec, UpgradeColChunk};
    use super::ids::{ManifestVersion, SegmentId, WalLsn};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    /// manifest 里段只可能是这两态（building/sealed 不进 manifest，dead 已被移除）。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum SegState {
        Live,
        Compacting,
    }

    /// 段元数据。版本间靠 `Arc` 结构共享：换 deletion / 换 upgrade / 翻 state 都是
    /// 「新版本里这个 entry 指向新块」，绝不改旧版本的任何字节。
    #[derive(Debug, Clone)]
    pub struct SegmentEntry {
        pub segment_id: SegmentId,
        pub level: u8, // 0 / 1 / 2 时间分层
        pub state: SegState,
        pub min_ts: i64,
        pub max_ts: i64,
        pub deletion_vec: Arc<DeletionVec>,
        pub deletion_seq: u64,
        pub upgrade_ref: Option<Arc<UpgradeColChunk>>,
        pub upgrade_seq: u64,
    }

    /// 一个不可变的 manifest 版本。
    #[derive(Debug, Clone)]
    pub struct Manifest {
        pub version: ManifestVersion,
        /// key = segment_id.get()，便于有序遍历与裁剪。
        pub segments: BTreeMap<u64, SegmentEntry>,
        /// 本版本「已吸收进段」的最大 WAL LSN（两层事务接缝，M.6）。
        pub memtable_watermark: WalLsn,
        /// 发布本版本时的全局 epoch 值。
        pub epoch: u64,
    }

    impl Manifest {
        pub fn empty() -> Self {
            Self {
                version: ManifestVersion::new(0),
                segments: BTreeMap::new(),
                memtable_watermark: WalLsn::new(0),
                epoch: 0,
            }
        }

        /// 写时复制出下一版本的「草稿」：克隆段集合、版本号 +1，由调用方继续改段集合后提交。
        pub fn cow_next(&self, epoch: u64) -> Self {
            Self {
                version: self.version.next(),
                segments: self.segments.clone(), // BTreeMap clone；SegmentEntry 内部 Arc 共享，浅拷贝
                memtable_watermark: self.memtable_watermark,
                epoch,
            }
        }
    }
}

/// 四源折叠归并（草案 2 §D2.2 第 5 步）。
///
/// 这是读一条完整 trace 的核心逻辑，做成纯函数便于充分测试：
/// - **去重键 = event_id（确定性）**：同一事件无论来自 MemTable 还是段、无论重传几次，只算一次。
/// - **归并/排序**：按 (trace_id, span_id) 分组，组内按 seq 升序定「最后」。
/// - **折叠**：status / duration 走 last-non-null-wins；logs 走 union（保序去重）。
///
/// 这里只实现「算法」。把四个源（MemTable 行、段行、deletion、upgrade）解码成 `FoldInput`
/// 喂进来，是上层 `MergeOnReadExec` 的事（仍是 TODO，需要给行解码出结构化字段）。
pub mod fold {
    use super::event::{EventId, EventIdentity, EventType};
    use std::collections::{BTreeMap, HashSet};

    /// 一个事件携带的可折叠字段（按 span 维度）。真实实现字段更多。
    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct SpanFields {
        /// last-non-null-wins。
        pub status: Option<u8>,
        /// last-non-null-wins。
        pub duration_ns: Option<u64>,
        /// 父 span（trace 是棵树：谁调用了谁）。span 创建时定，folded 取 last-non-null。
        pub parent_span_id: Option<u64>,
        /// LLM 输入 token（last-non-null）。Agent 可观测性的核心成本指标。
        pub input_tokens: Option<u64>,
        /// LLM 输出 token（last-non-null）。
        pub output_tokens: Option<u64>,
        /// 输入 token 中由供应商缓存读取的部分（last-non-null）。None 表示供应商未返回。
        pub cache_read_tokens: Option<u64>,
        /// 输入 token 中新写入供应商缓存的部分（last-non-null）。None 表示供应商未返回。
        pub cache_write_tokens: Option<u64>,
        /// 会话 id（多轮对话/agent 会话；同一 session 串起多条 trace）。last-non-null。
        pub session_id: Option<u64>,
        /// 租户 id（**逻辑隔离维度**）：多租户共享一套索引，靠每个查询强制带 tenant 过滤隔离。
        /// span 创建时定，folded 取 last-non-null。服务层须按鉴权身份给每个查询注入它（见检索路径）。
        pub tenant_id: Option<u64>,
        /// 外部 trace id 原文。内部 trace_id 仍是 u64（数字直用、字符串稳定 hash），这里用于展示和回查。
        pub external_trace_id: Option<String>,
        /// 外部 span id 原文。内部 span_id 用于索引和折叠，外部 id 用于跨系统关联。
        pub external_span_id: Option<String>,
        /// 外部父 span id 原文。与 parent_span_id 的关系同上。
        pub external_parent_span_id: Option<String>,
        /// 外部 session id 原文。AgenticData 等系统使用 UUID 时不会只剩 hash。
        pub external_session_id: Option<String>,
        /// span 技术名。SDK 的 `span(name)` 和 OTLP span name 落到这里，不再混进 logs。
        pub span_name: Option<String>,
        /// 给前端用户看的名字。只负责展示，不参与身份、聚合或 event_id。
        pub display_name: Option<String>,
        /// agent 名（成本/可观测按 agent 下钻）。last-non-null。
        pub agent_name: Option<String>,
        /// 工具名（tool/function call span）。last-non-null。
        pub tool_name: Option<String>,
        /// 模型名（成本按模型归因）。last-non-null。
        pub model: Option<String>,
        /// LLM 输入文本（prompt/问题）。eval 时 judge 的上文。last-non-null。
        pub input_text: Option<String>,
        /// LLM 输出文本（模型答案）。eval 打分的主要对象。last-non-null。
        pub output_text: Option<String>,
        /// 评测分（千分制 0..=1000，整数以保住 Eq；展示时除以 10 得百分）。
        /// 由 scorer 事后算出、走 upgrade 通道补写回这条 span。last-non-null。
        pub eval_score: Option<u32>,
        /// 评测标签（如「通过」「未通过」或 scorer 名）。last-non-null。
        pub eval_label: Option<String>,
        /// union（保序去重）。
        pub logs: Vec<String>,
        /// 原始/扩展属性。key 为属性名，value 是已校验的 JSON 字面量；折叠时同 key 后到覆盖。
        pub attrs: BTreeMap<String, String>,
    }

    impl SpanFields {
        /// 把 `other` 按 **last-non-null-wins**（标量非空才覆盖）+ **logs 保序并集** 叠到 `self` 上。
        /// 这是「晚到属性补写 / upgrade 叠加」的唯一权威实现 —— `with_patch`、读路径的 upgrade 归并都调它，
        /// 不再各写一份（各写一份曾导致只覆盖部分字段、新字段被悄悄丢掉）。
        pub fn merge_from(&mut self, other: &SpanFields) {
            if other.status.is_some() {
                self.status = other.status;
            }
            if other.duration_ns.is_some() {
                self.duration_ns = other.duration_ns;
            }
            if other.parent_span_id.is_some() {
                self.parent_span_id = other.parent_span_id;
            }
            if other.input_tokens.is_some() {
                self.input_tokens = other.input_tokens;
            }
            if other.output_tokens.is_some() {
                self.output_tokens = other.output_tokens;
            }
            if other.cache_read_tokens.is_some() {
                self.cache_read_tokens = other.cache_read_tokens;
            }
            if other.cache_write_tokens.is_some() {
                self.cache_write_tokens = other.cache_write_tokens;
            }
            if other.session_id.is_some() {
                self.session_id = other.session_id;
            }
            if other.tenant_id.is_some() {
                self.tenant_id = other.tenant_id;
            }
            if other.external_trace_id.is_some() {
                self.external_trace_id = other.external_trace_id.clone();
            }
            if other.external_span_id.is_some() {
                self.external_span_id = other.external_span_id.clone();
            }
            if other.external_parent_span_id.is_some() {
                self.external_parent_span_id = other.external_parent_span_id.clone();
            }
            if other.external_session_id.is_some() {
                self.external_session_id = other.external_session_id.clone();
            }
            if other.span_name.is_some() {
                self.span_name = other.span_name.clone();
            }
            if other.display_name.is_some() {
                self.display_name = other.display_name.clone();
            }
            if other.agent_name.is_some() {
                self.agent_name = other.agent_name.clone();
            }
            if other.tool_name.is_some() {
                self.tool_name = other.tool_name.clone();
            }
            if other.model.is_some() {
                self.model = other.model.clone();
            }
            if other.input_text.is_some() {
                self.input_text = other.input_text.clone();
            }
            if other.output_text.is_some() {
                self.output_text = other.output_text.clone();
            }
            if other.eval_score.is_some() {
                self.eval_score = other.eval_score;
            }
            if other.eval_label.is_some() {
                self.eval_label = other.eval_label.clone();
            }
            for l in &other.logs {
                if !self.logs.contains(l) {
                    self.logs.push(l.clone());
                }
            }
            for (k, v) in &other.attrs {
                self.attrs.insert(k.clone(), v.clone());
            }
        }
    }

    /// 进折叠的一个事件 = 归并坐标 + 身份(定 event_id/去重) + 可折叠字段。
    #[derive(Clone, Debug)]
    pub struct FoldInput {
        pub trace_id: u64,
        pub span_id: u64,
        /// 身份：`event_id()` 是去重键，`seq` 定组内先后。身份字段冻结，不被 upgrade 覆盖（M.7）。
        pub identity: EventIdentity,
        pub fields: SpanFields,
    }

    /// 折叠后的一个 span。
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct FoldedSpan {
        pub trace_id: u64,
        pub span_id: u64,
        /// 去重后的事件中是否真正出现过 SPAN_START，不能用名称等可选字段代替判断。
        pub has_start: bool,
        /// 去重后的事件中是否真正出现过 SPAN_END，不能用 duration/status 等可选字段代替判断。
        pub has_end: bool,
        pub parent_span_id: Option<u64>,
        pub status: Option<u8>,
        pub duration_ns: Option<u64>,
        pub input_tokens: Option<u64>,
        pub output_tokens: Option<u64>,
        pub cache_read_tokens: Option<u64>,
        pub cache_write_tokens: Option<u64>,
        pub session_id: Option<u64>,
        pub tenant_id: Option<u64>,
        pub external_trace_id: Option<String>,
        pub external_span_id: Option<String>,
        pub external_parent_span_id: Option<String>,
        pub external_session_id: Option<String>,
        pub span_name: Option<String>,
        pub display_name: Option<String>,
        pub agent_name: Option<String>,
        pub tool_name: Option<String>,
        pub model: Option<String>,
        pub input_text: Option<String>,
        pub output_text: Option<String>,
        pub eval_score: Option<u32>,
        pub eval_label: Option<String>,
        pub logs: Vec<String>,
        pub attrs: BTreeMap<String, String>,
        /// 去重后真正纳入折叠的事件数。
        pub event_count: usize,
    }

    impl FoldedSpan {
        /// 只根据到达的原始事件类型返回生命周期，不依赖可选的耗时和状态字段。
        pub fn lifecycle(&self) -> &'static str {
            match (self.has_start, self.has_end) {
                (true, true) => "completed",
                (true, false) => "running",
                (false, true) => "orphan_end",
                (false, false) => "incomplete",
            }
        }

        /// 把一份 upgrade 补写（`SpanFields`）叠到已折叠出的 span 上：last-non-null-wins + logs 并集。
        /// 与 `SpanFields::merge_from` 同口径；`FoldedSpan` 是字段被摊平的同一组字段，故单独一份。
        pub fn apply_patch(&mut self, p: &SpanFields) {
            if p.status.is_some() {
                self.status = p.status;
            }
            if p.duration_ns.is_some() {
                self.duration_ns = p.duration_ns;
            }
            if p.parent_span_id.is_some() {
                self.parent_span_id = p.parent_span_id;
            }
            if p.input_tokens.is_some() {
                self.input_tokens = p.input_tokens;
            }
            if p.output_tokens.is_some() {
                self.output_tokens = p.output_tokens;
            }
            if p.cache_read_tokens.is_some() {
                self.cache_read_tokens = p.cache_read_tokens;
            }
            if p.cache_write_tokens.is_some() {
                self.cache_write_tokens = p.cache_write_tokens;
            }
            if p.session_id.is_some() {
                self.session_id = p.session_id;
            }
            if p.tenant_id.is_some() {
                self.tenant_id = p.tenant_id;
            }
            if p.external_trace_id.is_some() {
                self.external_trace_id = p.external_trace_id.clone();
            }
            if p.external_span_id.is_some() {
                self.external_span_id = p.external_span_id.clone();
            }
            if p.external_parent_span_id.is_some() {
                self.external_parent_span_id = p.external_parent_span_id.clone();
            }
            if p.external_session_id.is_some() {
                self.external_session_id = p.external_session_id.clone();
            }
            if p.span_name.is_some() {
                self.span_name = p.span_name.clone();
            }
            if p.display_name.is_some() {
                self.display_name = p.display_name.clone();
            }
            if p.agent_name.is_some() {
                self.agent_name = p.agent_name.clone();
            }
            if p.tool_name.is_some() {
                self.tool_name = p.tool_name.clone();
            }
            if p.model.is_some() {
                self.model = p.model.clone();
            }
            if p.input_text.is_some() {
                self.input_text = p.input_text.clone();
            }
            if p.output_text.is_some() {
                self.output_text = p.output_text.clone();
            }
            if p.eval_score.is_some() {
                self.eval_score = p.eval_score;
            }
            if p.eval_label.is_some() {
                self.eval_label = p.eval_label.clone();
            }
            for l in &p.logs {
                if !self.logs.contains(l) {
                    self.logs.push(l.clone());
                }
            }
            for (k, v) in &p.attrs {
                self.attrs.insert(k.clone(), v.clone());
            }
        }
    }

    /// 折叠。输出按 (trace_id, span_id) 升序，确定可复算（审计可复现的形式）。
    pub fn fold_events(events: impl IntoIterator<Item = FoldInput>) -> Vec<FoldedSpan> {
        // 事件算法不改；只把租户放到本地去重域，避免另一租户的相同上报被吃掉。
        let mut seen: HashSet<(Option<u64>, EventId)> = HashSet::new();
        // 2. 租户是 span 身份的一部分；先分组再折叠，绝不能把不同租户原文并起来。
        let mut groups: BTreeMap<(Option<u64>, u64, u64), Vec<FoldInput>> = BTreeMap::new();
        for e in events {
            let eid = e.identity.event_id();
            if !seen.insert((e.fields.tenant_id, eid)) {
                continue; // 已见过这个 event_id，丢弃（去重）
            }
            groups
                .entry((e.fields.tenant_id, e.trace_id, e.span_id))
                .or_default()
                .push(e);
        }

        // 3. 组内按 seq 升序，last-non-null-wins + logs union。
        let mut out = Vec::with_capacity(groups.len());
        for ((_tenant, trace_id, span_id), mut evs) in groups {
            evs.sort_by_key(|e| e.identity.seq);
            let event_count = evs.len();
            let mut has_start = false;
            let mut has_end = false;
            let mut status = None;
            let mut duration_ns = None;
            let mut parent_span_id = None;
            let mut input_tokens = None;
            let mut output_tokens = None;
            let mut cache_read_tokens = None;
            let mut cache_write_tokens = None;
            let mut session_id = None;
            let mut tenant_id = None;
            let mut external_trace_id: Option<String> = None;
            let mut external_span_id: Option<String> = None;
            let mut external_parent_span_id: Option<String> = None;
            let mut external_session_id: Option<String> = None;
            let mut span_name: Option<String> = None;
            let mut display_name: Option<String> = None;
            let mut agent_name: Option<String> = None;
            let mut tool_name: Option<String> = None;
            let mut model: Option<String> = None;
            let mut input_text: Option<String> = None;
            let mut output_text: Option<String> = None;
            let mut eval_score: Option<u32> = None;
            let mut eval_label: Option<String> = None;
            let mut logs: Vec<String> = Vec::new();
            let mut logset: HashSet<&str> = HashSet::new();
            let mut attrs: BTreeMap<String, String> = BTreeMap::new();
            for e in &evs {
                match e.identity.event_type {
                    EventType::SpanStart => has_start = true,
                    EventType::SpanEnd => has_end = true,
                    _ => {}
                }
                if e.fields.status.is_some() {
                    status = e.fields.status; // 非空才覆盖 → 后到的非空值胜出，null 不抹掉已有值
                }
                if e.fields.duration_ns.is_some() {
                    duration_ns = e.fields.duration_ns;
                }
                if e.fields.parent_span_id.is_some() {
                    parent_span_id = e.fields.parent_span_id;
                }
                if e.fields.input_tokens.is_some() {
                    input_tokens = e.fields.input_tokens;
                }
                if e.fields.output_tokens.is_some() {
                    output_tokens = e.fields.output_tokens;
                }
                if e.fields.cache_read_tokens.is_some() {
                    cache_read_tokens = e.fields.cache_read_tokens;
                }
                if e.fields.cache_write_tokens.is_some() {
                    cache_write_tokens = e.fields.cache_write_tokens;
                }
                if e.fields.session_id.is_some() {
                    session_id = e.fields.session_id;
                }
                if e.fields.tenant_id.is_some() {
                    tenant_id = e.fields.tenant_id;
                }
                if e.fields.external_trace_id.is_some() {
                    external_trace_id = e.fields.external_trace_id.clone();
                }
                if e.fields.external_span_id.is_some() {
                    external_span_id = e.fields.external_span_id.clone();
                }
                if e.fields.external_parent_span_id.is_some() {
                    external_parent_span_id = e.fields.external_parent_span_id.clone();
                }
                if e.fields.external_session_id.is_some() {
                    external_session_id = e.fields.external_session_id.clone();
                }
                if e.fields.span_name.is_some() {
                    span_name = e.fields.span_name.clone();
                }
                if e.fields.display_name.is_some() {
                    display_name = e.fields.display_name.clone();
                }
                if e.fields.agent_name.is_some() {
                    agent_name = e.fields.agent_name.clone();
                }
                if e.fields.tool_name.is_some() {
                    tool_name = e.fields.tool_name.clone();
                }
                if e.fields.model.is_some() {
                    model = e.fields.model.clone();
                }
                if e.fields.input_text.is_some() {
                    input_text = e.fields.input_text.clone();
                }
                if e.fields.output_text.is_some() {
                    output_text = e.fields.output_text.clone();
                }
                if e.fields.eval_score.is_some() {
                    eval_score = e.fields.eval_score;
                }
                if e.fields.eval_label.is_some() {
                    eval_label = e.fields.eval_label.clone();
                }
                for l in &e.fields.logs {
                    if logset.insert(l.as_str()) {
                        logs.push(l.clone()); // 保序去重
                    }
                }
                for (k, v) in &e.fields.attrs {
                    attrs.insert(k.clone(), v.clone());
                }
            }
            out.push(FoldedSpan {
                trace_id,
                span_id,
                has_start,
                has_end,
                parent_span_id,
                status,
                duration_ns,
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
                session_id,
                tenant_id,
                external_trace_id,
                external_span_id,
                external_parent_span_id,
                external_session_id,
                span_name,
                display_name,
                agent_name,
                tool_name,
                model,
                input_text,
                output_text,
                eval_score,
                eval_label,
                logs,
                attrs,
                event_count,
            });
        }
        out.sort_by_key(|span| (span.trace_id, span.span_id, span.tenant_id));
        out
    }

    #[cfg(test)]
    mod tests {
        use super::super::event::{EventIdentity, EventType};
        use super::*;

        fn ev(
            trace: u64,
            span: u64,
            seq: u64,
            status: Option<u8>,
            dur: Option<u64>,
            logs: &[&str],
        ) -> FoldInput {
            FoldInput {
                trace_id: trace,
                span_id: span,
                identity: EventIdentity {
                    ext_span_id: format!("{trace}-{span}"),
                    seq,
                    event_type: EventType::Attr,
                },
                fields: SpanFields {
                    status,
                    duration_ns: dur,
                    logs: logs.iter().map(|s| s.to_string()).collect(),
                    ..Default::default()
                },
            }
        }

        #[test]
        fn assembles_one_span_from_partial_events() {
            // start 事件给 status，end 事件给 duration → 折叠后两者都在。
            let folded = fold_events([
                ev(1, 10, 1, Some(0), None, &["开始"]),
                ev(1, 10, 2, None, Some(500), &["结束"]),
            ]);
            assert_eq!(folded.len(), 1);
            assert_eq!(folded[0].status, Some(0));
            assert_eq!(folded[0].duration_ns, Some(500));
            assert_eq!(folded[0].logs, vec!["开始", "结束"]);
            assert_eq!(folded[0].event_count, 2);
        }

        #[test]
        fn lifecycle_comes_from_event_types_not_optional_fields() {
            let mut start = ev(1, 10, 1, None, None, &[]);
            start.identity.event_type = EventType::SpanStart;
            let only_start = fold_events([start.clone()]);
            assert!(only_start[0].has_start);
            assert!(!only_start[0].has_end);

            // end 故意不带 duration/status：生命周期不能靠这些可选字段猜。
            let mut end = ev(1, 10, 2, None, None, &[]);
            end.identity.event_type = EventType::SpanEnd;
            let completed = fold_events([start, end]);
            assert!(completed[0].has_start);
            assert!(completed[0].has_end);
            assert_eq!(completed[0].duration_ns, None);
        }

        #[test]
        fn last_non_null_wins_even_when_input_out_of_order() {
            // 两个事件都给 status，seq 大的胜出；故意把 seq 大的放前面，验证内部按 seq 排序。
            let folded = fold_events([
                ev(1, 10, 5, Some(2), None, &[]), // 后发生（seq 大）
                ev(1, 10, 1, Some(0), None, &[]), // 先发生（seq 小）
            ]);
            assert_eq!(
                folded[0].status,
                Some(2),
                "seq 大的（后发生的）非空值应胜出"
            );
        }

        #[test]
        fn span_and_display_names_fold_independently() {
            let mut start = ev(1, 10, 1, None, None, &[]);
            start.fields.span_name = Some("risk.review".into());
            start.fields.display_name = Some("风险审核".into());
            let mut patch = ev(1, 10, 2, None, None, &[]);
            patch.fields.display_name = Some("人工风险审核".into());

            let folded = fold_events([patch, start]);
            assert_eq!(folded[0].span_name.as_deref(), Some("risk.review"));
            assert_eq!(folded[0].display_name.as_deref(), Some("人工风险审核"));
            assert!(folded[0].logs.is_empty(), "名称不能再混入日志");
        }

        #[test]
        fn null_does_not_clobber_existing_value() {
            // 先有 status=Some，后来事件 status=None：不能把已有值抹成空。
            let folded = fold_events([
                ev(1, 10, 1, Some(7), None, &[]),
                ev(1, 10, 2, None, None, &[]),
            ]);
            assert_eq!(folded[0].status, Some(7));
        }

        #[test]
        fn dedups_by_event_id_across_sources() {
            // 同一事件（同 identity → 同 event_id）出现两次（如既在 MemTable 又在段、或重放）：只算一次。
            let dup = ev(1, 10, 1, Some(0), Some(100), &["x"]);
            let folded = fold_events([dup.clone(), dup, ev(1, 10, 2, Some(1), None, &["x", "y"])]);
            assert_eq!(folded.len(), 1);
            // event_count = 2（重复的那条被去掉），不是 3
            assert_eq!(folded[0].event_count, 2);
            // logs union 保序去重："x" 只一份
            assert_eq!(folded[0].logs, vec!["x", "y"]);
            // status 最后非空 = seq2 的 1
            assert_eq!(folded[0].status, Some(1));
            assert_eq!(folded[0].duration_ns, Some(100));
        }

        #[test]
        fn groups_multiple_spans_in_deterministic_order() {
            let folded = fold_events([
                ev(2, 1, 1, None, None, &[]),
                ev(1, 9, 1, None, None, &[]),
                ev(1, 2, 1, None, None, &[]),
            ]);
            let keys: Vec<(u64, u64)> = folded.iter().map(|s| (s.trace_id, s.span_id)).collect();
            assert_eq!(
                keys,
                vec![(1, 2), (1, 9), (2, 1)],
                "输出按 (trace_id, span_id) 升序，确定可复算"
            );
        }
    }
}

/// 多路检索结果融合（Reciprocal Rank Fusion）。
///
/// 把 BM25 关键词排序和向量语义排序融成一路：一个 span 的融合分 = Σ 1/(c + 它在各路里的名次)。
/// 同时被多路命中的 span 分更高 —— 这正是「关键词 + 语义混合召回」要的效果。纯函数，便于单测。
pub mod rank {
    use std::collections::BTreeMap;

    /// 输入若干路排序（每路是按相关性从高到低的 (trace_id, span_id) 列表），输出融合后从高到低的
    /// (key, 融合分)。`c` 是 RRF 常数（经验值 60）。同分按 key 升序定序（确定可复算）。
    pub fn rrf_fuse(rankings: &[Vec<(u64, u64)>], c: f32) -> Vec<((u64, u64), f32)> {
        let mut scores: BTreeMap<(u64, u64), f32> = BTreeMap::new();
        for ranking in rankings {
            for (i, &key) in ranking.iter().enumerate() {
                let rank = i as f32 + 1.0; // 名次从 1 起
                *scores.entry(key).or_insert(0.0) += 1.0 / (c + rank);
            }
        }
        let mut v: Vec<((u64, u64), f32)> = scores.into_iter().collect();
        // 分降序；同分按 key 升序（BTreeMap 已使同分项按 key 有序，稳定排序保持之）。
        v.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        v
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn item_in_both_lists_wins() {
            // A 把 (1,1) 排第一，B 把 (2,2) 排第一，但 (3,3) 两路都命中 → 融合后 (3,3) 居首。
            let a = vec![(1, 1), (3, 3)];
            let b = vec![(2, 2), (3, 3)];
            let fused = rrf_fuse(&[a, b], 60.0);
            assert_eq!(fused[0].0, (3, 3), "被两路同时命中的应排最前");
            // (1,1) 与 (2,2) 同分，按 key 升序 → (1,1) 在前
            assert_eq!(fused[1].0, (1, 1));
            assert_eq!(fused[2].0, (2, 2));
        }

        #[test]
        fn higher_rank_scores_more() {
            // 单路里名次越靠前分越高。
            let a = vec![(1, 1), (2, 2), (3, 3)];
            let fused = rrf_fuse(&[a], 60.0);
            assert_eq!(fused[0].0, (1, 1));
            assert!(fused[0].1 > fused[1].1 && fused[1].1 > fused[2].1);
        }
    }
}
