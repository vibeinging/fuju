//! yt-segstore-vortex —— **列式段存储（Vortex）**，实现引擎的 `SegmentStore` trait。
//!
//! 一个段 = 一个 `.vortex` 文件，SpanFields 的每个字段一**列**（StructLayout：按列存、只读子集列线性、
//! 随机访问任意列常数时间）。`input_text`/`output_text` 是大列，列式让"数 token / 列表 / 聚合"等查询
//! 完全不碰它们——这是上列式最大的单点收益。
//!
//! 已落地：写读 round-trip + **谓词下推**（`scan().with_filter(...)` 按时间窗剪行）+ **投影下推**
//! （`scan().with_projection(select(...))` 只解码命中列，聚合查询跳过大文本列）。写入用 Vortex 默认
//! BtrBlocks 压缩策略（字符串列走 FSST/dict），大文本列在盘上是压缩态。
//! 决策与计划见 `docs/design/2026-06-22_列式段存储-vortex-选型与落地计划.md`。

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tokio::runtime::Runtime;

use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
use vortex::array::arrow::IntoArrowArray;
use vortex::array::stream::ArrayStreamExt;
use vortex::array::{ArrayRef, IntoArray};
use vortex::buffer::ByteBufferMut;
use vortex::error::{VortexError, VortexResult};
use vortex::expr::{and, col, eq, gt_eq, lit, lt_eq, or, root, select, Expression};
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;

use arrow::array::{
    Array, AsArray, Int64Array, StringViewArray, UInt32Array, UInt64Array, UInt8Array,
};

use yt_core::event::{EventIdentity, EventType};
use yt_core::fold::{FoldInput, SpanFields};
use yt_core::ids::SegmentId;
use yt_engine::{KeyedRecordScan, KeyedSegmentScan, Projection, SegmentStore};
use yt_wal::WalRecord;

/// logs（Vec<String>）压成单列：转义后用记录分隔符 `\u{1e}` 连接。**对任意内容可逆**——金融系统日志
/// 可能含二进制错误码/协议帧/NUL/换行，所以分隔符与转义符在内容里出现时都被转义（NUL 不再是特殊字符）。
/// 按 `char` 处理，多字节 UTF-8（中文）安全。空 logs → None（不占列）。
/// （比真正的 list<utf8> 列省事，且对当前一段一文件的布局够用；要列内按元素下推再升级 list。）
const LOG_SEP: char = '\u{1e}';
const LOG_ESC: char = '\\';
const KEY_INDEX_MAGIC: u64 = 0x5954_564B_4559_4931; // "YTVKEYI1"
const MAX_POINT_LOOKUP_KEYS: usize = 4096;

/// 把一条 span 的 logs 编码成单列字符串；空 → None。
fn encode_logs(logs: &[String]) -> Option<String> {
    if logs.is_empty() {
        return None;
    }
    let mut s = String::new();
    for (i, l) in logs.iter().enumerate() {
        if i > 0 {
            s.push(LOG_SEP);
        }
        for c in l.chars() {
            if c == LOG_ESC || c == LOG_SEP {
                s.push(LOG_ESC); // 内容里的分隔符/转义符 → 转义,解码时还原
            }
            s.push(c);
        }
    }
    Some(s)
}

/// 解码单列字符串回 logs（与 `encode_logs` 互逆）。
fn decode_logs(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut esc = false;
    for c in s.chars() {
        if esc {
            cur.push(c);
            esc = false;
        } else if c == LOG_ESC {
            esc = true;
        } else if c == LOG_SEP {
            out.push(std::mem::take(&mut cur));
        } else {
            cur.push(c);
        }
    }
    out.push(cur);
    out
}

/// 投影 → 要 `select` 的列名。身份/分组列（trace_id/span_id/ts/seq/event_type/ext_span_id）**恒选**
/// （折叠去重/定序/分组要用）；可折叠值列按 `proj` 的位选。`proj.is_all()` → `None` = 不裁列、读全表
/// （与历史行为字节一致）。**投影下推的省点全在这**：聚合不选 input_text/output_text，Vortex 连解码都不做。
fn projected_field_names(proj: Projection) -> Option<Vec<&'static str>> {
    if proj.is_all() {
        return None;
    }
    let mut cols = vec![
        "trace_id",
        "span_id",
        "ts",
        "seq",
        "event_type",
        "ext_span_id",
    ];
    for (bit, name) in [
        (Projection::STATUS, "status"),
        (Projection::DURATION_NS, "duration_ns"),
        (Projection::PARENT_SPAN_ID, "parent_span_id"),
        (Projection::INPUT_TOKENS, "input_tokens"),
        (Projection::OUTPUT_TOKENS, "output_tokens"),
        (Projection::CACHE_READ_TOKENS, "cache_read_tokens"),
        (Projection::CACHE_WRITE_TOKENS, "cache_write_tokens"),
        (Projection::SESSION_ID, "session_id"),
        (Projection::TENANT_ID, "tenant_id"),
        (Projection::SPAN_NAME, "span_name"),
        (Projection::DISPLAY_NAME, "display_name"),
        (Projection::AGENT_NAME, "agent_name"),
        (Projection::TOOL_NAME, "tool_name"),
        (Projection::MODEL, "model"),
        (Projection::INPUT_TEXT, "input_text"),
        (Projection::OUTPUT_TEXT, "output_text"),
        (Projection::EVAL_SCORE, "eval_score"),
        (Projection::EVAL_LABEL, "eval_label"),
        (Projection::LOGS, "logs"),
    ] {
        if proj.has(bit) {
            cols.push(name);
        }
    }
    Some(cols)
}

/// 列式段存储到一个目录，每段一个 `.vortex` 文件。
pub struct VortexSegmentStore {
    dir: PathBuf,
    session: VortexSession,
    rt: Runtime,
    key_indexes: Mutex<HashMap<u64, Arc<KeyIndex>>>,
}

struct KeyIndex {
    by_key: HashMap<(u64, u64), Vec<u32>>,
}

impl VortexSegmentStore {
    pub fn open(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        // with_tokio 抓当前 tokio 运行时句柄,必须在运行时上下文里调 → 进 rt.enter() 再配。
        let session = {
            let _enter = rt.enter();
            VortexSession::default().with_tokio()
        };
        Ok(Self {
            dir,
            session,
            rt,
            key_indexes: Mutex::new(HashMap::new()),
        })
    }

    fn seg_path(&self, seg: SegmentId) -> PathBuf {
        self.dir.join(format!("seg-{}.vortex", seg.get()))
    }

    fn key_index_path(&self, seg: SegmentId) -> PathBuf {
        self.dir.join(format!("seg-{}.keys", seg.get()))
    }

    fn write_key_index(
        &self,
        seg: SegmentId,
        records: &[WalRecord],
        data_len: u64,
        data_crc: u32,
    ) -> std::io::Result<Arc<KeyIndex>> {
        let mut bytes = Vec::with_capacity(28 + records.len() * 16 + 4);
        bytes.extend_from_slice(&KEY_INDEX_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&(records.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&data_len.to_le_bytes());
        bytes.extend_from_slice(&data_crc.to_le_bytes());
        let mut by_key: HashMap<(u64, u64), Vec<u32>> = HashMap::new();
        for (row, record) in records.iter().enumerate() {
            bytes.extend_from_slice(&record.trace_id.to_le_bytes());
            bytes.extend_from_slice(&record.span_id.to_le_bytes());
            by_key
                .entry((record.trace_id, record.span_id))
                .or_default()
                .push(row as u32);
        }
        let crc = yt_wal::crc32(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        let path = self.key_index_path(seg);
        let tmp = path.with_extension("keys.tmp");
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(tmp, path)?;
        let index = Arc::new(KeyIndex { by_key });
        self.key_indexes
            .lock()
            .unwrap()
            .insert(seg.get(), Arc::clone(&index));
        Ok(index)
    }

    fn read_key_index(&self, seg: SegmentId) -> Option<(Arc<KeyIndex>, u64)> {
        let bytes = std::fs::read(self.key_index_path(seg)).ok()?;
        if bytes.len() < 32 {
            return None;
        }
        let payload_len = bytes.len() - 4;
        let stored_crc = u32::from_le_bytes(bytes[payload_len..].try_into().ok()?);
        if yt_wal::crc32(&bytes[..payload_len]) != stored_crc {
            return None;
        }
        let magic = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
        let count = u64::from_le_bytes(bytes[8..16].try_into().ok()?) as usize;
        let data_len = u64::from_le_bytes(bytes[16..24].try_into().ok()?);
        let data_crc = u32::from_le_bytes(bytes[24..28].try_into().ok()?);
        if magic != KEY_INDEX_MAGIC || payload_len != 28usize.checked_add(count.checked_mul(16)?)? {
            return None;
        }
        // 首次加载必须把目录绑定到实际 Vortex 文件。有效但错配的 sidecar 不能参与 deletion vector 行号判断。
        let data = std::fs::read(self.seg_path(seg)).ok()?;
        if data.len() as u64 != data_len || yt_wal::crc32(&data) != data_crc {
            return None;
        }
        let mut by_key: HashMap<(u64, u64), Vec<u32>> = HashMap::new();
        for row in 0..count {
            let offset = 28 + row * 16;
            let trace_id = u64::from_le_bytes(bytes[offset..offset + 8].try_into().ok()?);
            let span_id = u64::from_le_bytes(bytes[offset + 8..offset + 16].try_into().ok()?);
            by_key
                .entry((trace_id, span_id))
                .or_default()
                .push(row as u32);
        }
        let index = Arc::new(KeyIndex { by_key });
        self.key_indexes
            .lock()
            .unwrap()
            .insert(seg.get(), Arc::clone(&index));
        Some((index, bytes.len() as u64))
    }

    fn key_index(&self, seg: SegmentId) -> Option<(Arc<KeyIndex>, u64, usize)> {
        if let Some(index) = self.key_indexes.lock().unwrap().get(&seg.get()).cloned() {
            return Some((index, 0, 0));
        }
        if let Some((index, bytes)) = self.read_key_index(seg) {
            return Some((index, bytes, 0));
        }
        // 旧段首次点读只读取身份列，补建小型 key 目录；后续查询不再扫描所有身份行。
        let data = std::fs::read(self.seg_path(seg)).ok()?;
        let records = self.read_filtered(seg, None, Projection::of(0));
        let index = self
            .write_key_index(seg, &records, data.len() as u64, yt_wal::crc32(&data))
            .ok()?;
        Some((index, 0, 1))
    }

    fn point_records(&self, seg: SegmentId, keys: &HashSet<(u64, u64)>) -> Option<KeyedRecordScan> {
        if keys.len() > MAX_POINT_LOOKUP_KEYS {
            return None;
        }
        let (index, index_bytes_read, indexes_rebuilt) = self.key_index(seg)?;
        let mut rows_by_key = keys
            .iter()
            .filter_map(|key| {
                index
                    .by_key
                    .get(key)
                    .map(|rows| (*key, VecDeque::from(rows.clone())))
            })
            .collect::<HashMap<_, _>>();
        let filter = keys
            .iter()
            .copied()
            .map(|(trace_id, span_id)| {
                and(
                    eq(col("trace_id"), lit(trace_id)),
                    eq(col("span_id"), lit(span_id)),
                )
            })
            .reduce(or);
        let records = match filter {
            Some(filter) => self.read_filtered(seg, Some(filter), Projection::ALL),
            None => Vec::new(),
        };
        let mut rows = Vec::with_capacity(records.len());
        for record in records {
            let row = rows_by_key
                .get_mut(&(record.trace_id, record.span_id))?
                .pop_front()?;
            rows.push((row, record));
        }
        let decoded_rows = rows.len();
        Some(KeyedRecordScan {
            rows,
            used_point_index: true,
            decoded_rows,
            index_bytes_read,
            data_bytes_read: std::fs::metadata(self.seg_path(seg))
                .map(|meta| meta.len())
                .unwrap_or(0),
            indexes_validated: usize::from(indexes_rebuilt == 0),
            indexes_rebuilt,
        })
    }

    /// 把一批记录建成列式 StructArray（每字段一列）。
    fn build_struct(records: &[WalRecord]) -> StructArray {
        // 原始列辅助：Option<T> 迭代 → 可空原始列。非空列也用 Some 走同一路，保持代码统一。
        macro_rules! u64col {
            ($f:expr) => {
                PrimitiveArray::from_option_iter(records.iter().map(|r| $f(r))).into_array()
            };
        }
        let trace_id =
            PrimitiveArray::from_option_iter(records.iter().map(|r| Some(r.trace_id))).into_array();
        let span_id =
            PrimitiveArray::from_option_iter(records.iter().map(|r| Some(r.span_id))).into_array();
        let ts = PrimitiveArray::from_option_iter(records.iter().map(|r| Some(r.ts))).into_array();
        let seq = PrimitiveArray::from_option_iter(records.iter().map(|r| Some(r.identity.seq)))
            .into_array();
        let event_type = PrimitiveArray::from_option_iter(
            records.iter().map(|r| Some(r.identity.event_type.tag())),
        )
        .into_array();
        let ext_span_id =
            VarBinViewArray::from_iter_str(records.iter().map(|r| r.identity.ext_span_id.clone()))
                .into_array();

        let status =
            PrimitiveArray::from_option_iter(records.iter().map(|r| r.fields.status)).into_array();
        let duration_ns = u64col!(|r: &WalRecord| r.fields.duration_ns);
        let parent_span_id = u64col!(|r: &WalRecord| r.fields.parent_span_id);
        let input_tokens = u64col!(|r: &WalRecord| r.fields.input_tokens);
        let output_tokens = u64col!(|r: &WalRecord| r.fields.output_tokens);
        let cache_read_tokens = u64col!(|r: &WalRecord| r.fields.cache_read_tokens);
        let cache_write_tokens = u64col!(|r: &WalRecord| r.fields.cache_write_tokens);
        let session_id = u64col!(|r: &WalRecord| r.fields.session_id);
        let tenant_id = u64col!(|r: &WalRecord| r.fields.tenant_id);
        let eval_score =
            PrimitiveArray::from_option_iter(records.iter().map(|r| r.fields.eval_score))
                .into_array();

        let strcol = |f: &dyn Fn(&WalRecord) -> Option<String>| {
            VarBinViewArray::from_iter_nullable_str(records.iter().map(f)).into_array()
        };
        let span_name = strcol(&|r| r.fields.span_name.clone());
        let display_name = strcol(&|r| r.fields.display_name.clone());
        let agent_name = strcol(&|r| r.fields.agent_name.clone());
        let tool_name = strcol(&|r| r.fields.tool_name.clone());
        let model = strcol(&|r| r.fields.model.clone());
        let input_text = strcol(&|r| r.fields.input_text.clone());
        let output_text = strcol(&|r| r.fields.output_text.clone());
        let eval_label = strcol(&|r| r.fields.eval_label.clone());
        let logs = strcol(&|r| encode_logs(&r.fields.logs));

        StructArray::from_fields(&[
            ("trace_id", trace_id),
            ("span_id", span_id),
            ("ts", ts),
            ("seq", seq),
            ("event_type", event_type),
            ("ext_span_id", ext_span_id),
            ("status", status),
            ("duration_ns", duration_ns),
            ("parent_span_id", parent_span_id),
            ("input_tokens", input_tokens),
            ("output_tokens", output_tokens),
            ("cache_read_tokens", cache_read_tokens),
            ("cache_write_tokens", cache_write_tokens),
            ("session_id", session_id),
            ("tenant_id", tenant_id),
            ("eval_score", eval_score),
            ("span_name", span_name),
            ("display_name", display_name),
            ("agent_name", agent_name),
            ("tool_name", tool_name),
            ("model", model),
            ("input_text", input_text),
            ("output_text", output_text),
            ("eval_label", eval_label),
            ("logs", logs),
        ])
        .expect("build struct array")
    }

    /// 从读回的 Arrow StructArray 逐行重建 WalRecord。**投影感知**：身份/分组列恒在；可折叠值列可能因
    /// 投影被裁掉（`column_by_name` 返回 None）→ 该字段整列当 None，不 panic。这样同一段读回路径既服务
    /// 全列读、也服务投影读。
    fn rows_from_arrow(st: &arrow::array::StructArray) -> Vec<WalRecord> {
        let n = st.len();
        // 身份/分组列：任何投影都选了它们，恒在 → 直接取。
        let u64req = |name: &str| {
            st.column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .clone()
        };
        let trace_id = u64req("trace_id");
        let span_id = u64req("span_id");
        let ts = st
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        let seq = u64req("seq");
        let event_type = st
            .column_by_name("event_type")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .clone();
        let ext_span_id = st
            .column_by_name("ext_span_id")
            .unwrap()
            .as_string_view()
            .clone();

        // 可折叠值列：可能被投影裁掉 → Option<列>，缺列即全行 None。
        let optu64 = |name: &str| {
            st.column_by_name(name)
                .map(|c| c.as_any().downcast_ref::<UInt64Array>().unwrap().clone())
        };
        let duration_ns = optu64("duration_ns");
        let parent_span_id = optu64("parent_span_id");
        let input_tokens = optu64("input_tokens");
        let output_tokens = optu64("output_tokens");
        let cache_read_tokens = optu64("cache_read_tokens");
        let cache_write_tokens = optu64("cache_write_tokens");
        let session_id = optu64("session_id");
        let tenant_id = optu64("tenant_id");
        let status = st
            .column_by_name("status")
            .map(|c| c.as_any().downcast_ref::<UInt8Array>().unwrap().clone());
        let eval_score = st
            .column_by_name("eval_score")
            .map(|c| c.as_any().downcast_ref::<UInt32Array>().unwrap().clone());
        let optsv = |name: &str| st.column_by_name(name).map(|c| c.as_string_view().clone());
        let span_name = optsv("span_name");
        let display_name = optsv("display_name");
        let agent_name = optsv("agent_name");
        let tool_name = optsv("tool_name");
        let model = optsv("model");
        let input_text = optsv("input_text");
        let output_text = optsv("output_text");
        let eval_label = optsv("eval_label");
        let logs = optsv("logs");

        // 缺列 → None；在列但该行为 null → None；否则取值。
        let gu64 = |a: &Option<UInt64Array>, i: usize| {
            a.as_ref().filter(|x| !x.is_null(i)).map(|x| x.value(i))
        };
        let gu8 = |a: &Option<UInt8Array>, i: usize| {
            a.as_ref().filter(|x| !x.is_null(i)).map(|x| x.value(i))
        };
        let gu32 = |a: &Option<UInt32Array>, i: usize| {
            a.as_ref().filter(|x| !x.is_null(i)).map(|x| x.value(i))
        };
        let gstr = |a: &Option<StringViewArray>, i: usize| {
            a.as_ref()
                .filter(|x| !x.is_null(i))
                .map(|x| x.value(i).to_string())
        };

        (0..n)
            .map(|i| WalRecord {
                trace_id: trace_id.value(i),
                span_id: span_id.value(i),
                ts: ts.value(i),
                identity: EventIdentity {
                    ext_span_id: ext_span_id.value(i).to_string(),
                    seq: seq.value(i),
                    event_type: EventType::from_tag(event_type.value(i)),
                },
                fields: SpanFields {
                    status: gu8(&status, i),
                    duration_ns: gu64(&duration_ns, i),
                    parent_span_id: gu64(&parent_span_id, i),
                    input_tokens: gu64(&input_tokens, i),
                    output_tokens: gu64(&output_tokens, i),
                    cache_read_tokens: gu64(&cache_read_tokens, i),
                    cache_write_tokens: gu64(&cache_write_tokens, i),
                    session_id: gu64(&session_id, i),
                    tenant_id: gu64(&tenant_id, i),
                    eval_score: gu32(&eval_score, i),
                    span_name: gstr(&span_name, i),
                    display_name: gstr(&display_name, i),
                    agent_name: gstr(&agent_name, i),
                    tool_name: gstr(&tool_name, i),
                    model: gstr(&model, i),
                    input_text: gstr(&input_text, i),
                    output_text: gstr(&output_text, i),
                    eval_label: gstr(&eval_label, i),
                    logs: match gstr(&logs, i) {
                        None => Vec::new(),
                        Some(s) => decode_logs(&s),
                    },
                    ..Default::default()
                },
            })
            .collect()
    }

    /// 读段（可选谓词下推 + 投影下推）。`filter=Some(expr)` 把过滤**推进 Vortex 文件扫描**
    /// （`scan().with_filter`），只解码命中行/块；`proj` 非全列时再 `with_projection(select(...))` 把列也裁掉，
    /// 不读的列（尤其大文本列）连解码都不做。都不在 Rust 后置全读再筛。
    fn read_filtered(
        &self,
        seg: SegmentId,
        filter: Option<Expression>,
        proj: Projection,
    ) -> Vec<WalRecord> {
        let path = self.seg_path(seg);
        if !path.exists() {
            return Vec::new();
        }
        let fallback_filter = filter.clone();
        let arr: VortexResult<ArrayRef> = self.rt.block_on(async {
            // open_path 通过 VortexReadAt 按需读取页；谓词点查不再先把整个段复制进内存。
            let scan = self.session.open_options().open_path(&path).await?.scan()?;
            let scan = match filter {
                Some(f) => scan.with_filter(f),
                None => scan,
            };
            let scan = match projected_field_names(proj) {
                Some(cols) => scan.with_projection(select(cols, root())),
                None => scan, // 全列读,不裁
            };
            scan.into_array_stream()?.read_all().await
        });
        let arr = match arr {
            Ok(a) => a,
            Err(e) => {
                if !proj.is_all() {
                    // 旧 Vortex 段没有后来新增的可选列时，窄投影会报列不存在。
                    // 回退全列读取后，rows_from_arrow 会把缺列还原成 None，保证旧段可读。
                    return self.read_filtered(seg, fallback_filter, Projection::ALL);
                }
                eprintln!("[vortex-segstore] scan seg {} 失败: {e}", seg.get());
                return Vec::new();
            }
        };
        let arrow = arr.into_arrow_preferred().expect("vortex→arrow");
        let st = arrow
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .expect("struct array");
        Self::rows_from_arrow(st)
    }

    /// **按 ts 范围下推过滤**（谓词进文件扫描）+ 可选投影：只返回 `ts ∈ [from, to]` 的行、只解码 `proj` 的列。
    /// 这是列式剪枝的主路 —— 读路径按时间窗只碰相关行/块、按投影只碰相关列，大段里查一小段时间不全扫、不全解。
    pub fn scan_in_time(
        &self,
        seg: SegmentId,
        from: i64,
        to: i64,
        proj: Projection,
    ) -> Vec<WalRecord> {
        let filter = and(gt_eq(col("ts"), lit(from)), lt_eq(col("ts"), lit(to)));
        self.read_filtered(seg, Some(filter), proj)
    }
}

impl SegmentStore for VortexSegmentStore {
    fn flush_to_segment(&self, seg: SegmentId, records: &[WalRecord]) {
        self.try_flush_to_segment(seg, records)
            .expect("Vortex segment write failed");
    }

    fn try_flush_to_segment(&self, seg: SegmentId, records: &[WalRecord]) -> std::io::Result<()> {
        use std::io::Write;
        if records.is_empty() {
            return Ok(());
        }
        let st = Self::build_struct(records);
        let path = self.seg_path(seg);
        let buf: ByteBufferMut = self
            .rt
            .block_on(async {
                let mut buf = ByteBufferMut::empty();
                self.session
                    .write_options()
                    .write(&mut buf, st.into_array().to_array_stream())
                    .await?;
                Ok::<ByteBufferMut, VortexError>(buf)
            })
            .map_err(|err| std::io::Error::other(err.to_string()))?;
        let tmp = path.with_extension("tmp");
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(buf.as_slice())?;
        file.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        // 点读目录是可重建的派生索引，写失败不影响已持久化的主数据段。
        let _ = self.write_key_index(
            seg,
            records,
            buf.len() as u64,
            yt_wal::crc32(buf.as_slice()),
        );
        Ok(())
    }

    fn scan_records(&self, seg: SegmentId) -> Vec<WalRecord> {
        // compaction 重建新段要全字段 → 读全列。
        self.read_filtered(seg, None, Projection::ALL)
    }

    fn scan_fold_inputs(&self, seg: SegmentId) -> Vec<(u32, FoldInput)> {
        self.scan_records(seg)
            .iter()
            .enumerate()
            .map(|(i, r)| (i as u32, r.to_fold_input()))
            .collect()
    }

    fn scan_fold_inputs_for_keys(
        &self,
        seg: SegmentId,
        keys: &HashSet<(u64, u64)>,
    ) -> Option<KeyedSegmentScan> {
        let scan = self.point_records(seg, keys)?;
        Some(KeyedSegmentScan {
            rows: scan
                .rows
                .into_iter()
                .map(|(row, record)| (row, record.to_fold_input()))
                .collect(),
            used_point_index: scan.used_point_index,
            decoded_rows: scan.decoded_rows,
            index_bytes_read: scan.index_bytes_read,
            data_bytes_read: scan.data_bytes_read,
            indexes_validated: scan.indexes_validated,
            indexes_rebuilt: scan.indexes_rebuilt,
        })
    }

    fn scan_records_for_keys(
        &self,
        seg: SegmentId,
        keys: &HashSet<(u64, u64)>,
    ) -> Option<KeyedRecordScan> {
        self.point_records(seg, keys)
    }

    fn unlink_segment(&self, seg: SegmentId) {
        let _ = std::fs::remove_file(self.seg_path(seg));
        let _ = std::fs::remove_file(self.key_index_path(seg));
        self.key_indexes.lock().unwrap().remove(&seg.get());
    }

    /// 覆盖默认（None）：**投影下推**——只解码 `proj` 的列，不丢行 → 带物理行号返回（删除位图照常生效）。
    /// 行号 = 段内顺序；投影只裁列、行顺序不变，所以 enumerate 出来的行号与全列读一致。
    fn scan_fold_inputs_projected(
        &self,
        seg: SegmentId,
        proj: Projection,
    ) -> Option<Vec<(u32, FoldInput)>> {
        Some(
            self.read_filtered(seg, None, proj)
                .iter()
                .enumerate()
                .map(|(i, r)| (i as u32, r.to_fold_input()))
                .collect(),
        )
    }

    /// 覆盖默认（None）：把时间过滤 + 投影**真下推**进 Vortex 文件扫描，返回命中行的 FoldInput。
    /// 引擎只在「段无删除」时调它（见 trait 文档），故这里不管删除位图。
    fn scan_fold_inputs_in_time(
        &self,
        seg: SegmentId,
        from: i64,
        to: i64,
        proj: Projection,
    ) -> Option<Vec<FoldInput>> {
        Some(
            self.scan_in_time(seg, from, to, proj)
                .iter()
                .map(|r| r.to_fold_input())
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use yt_core::event::EventType;

    fn temp_dir() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "yt_vortex_{}_{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    fn rec(trace: u64, span: u64, seq: u64) -> WalRecord {
        WalRecord {
            trace_id: trace,
            span_id: span,
            ts: seq as i64 * 100,
            identity: EventIdentity {
                ext_span_id: format!("{trace}-{span}"),
                seq,
                event_type: EventType::SpanEnd,
            },
            fields: SpanFields::default(),
        }
    }

    #[test]
    fn columnar_segment_round_trips_all_fields() {
        let dir = temp_dir();
        let store = VortexSegmentStore::open(&dir).unwrap();
        let seg = SegmentId::new(1);

        let mut a = rec(1, 10, 1);
        a.fields.status = Some(0);
        a.fields.input_tokens = Some(1200);
        a.fields.span_name = Some("risk.review".into());
        a.fields.display_name = Some("风险审核".into());
        a.fields.agent_name = Some("风控".into());
        a.fields.output_text = Some("疑似盗刷".into());
        a.fields.logs = vec!["开始".into(), "研判".into()];
        let mut b = rec(2, 20, 1);
        b.fields.status = Some(1);
        b.fields.eval_score = Some(800);
        b.fields.eval_label = Some("未通过".into());
        // b 的 token/agent 留空,验证可空列

        store.flush_to_segment(seg, &[a.clone(), b.clone()]);

        let back = store.scan_records(seg);
        assert_eq!(back.len(), 2);
        // 逐字段一致
        assert_eq!(back[0].trace_id, 1);
        assert_eq!(back[0].identity.ext_span_id, "1-10");
        assert_eq!(back[0].fields.status, Some(0));
        assert_eq!(back[0].fields.input_tokens, Some(1200));
        assert_eq!(back[0].fields.span_name.as_deref(), Some("risk.review"));
        assert_eq!(back[0].fields.display_name.as_deref(), Some("风险审核"));
        assert_eq!(back[0].fields.agent_name.as_deref(), Some("风控"));
        assert_eq!(back[0].fields.output_text.as_deref(), Some("疑似盗刷"));
        assert_eq!(back[0].fields.logs, vec!["开始", "研判"]);
        // 可空列:b 的 token/agent 是 None,logs 空
        assert_eq!(back[1].trace_id, 2);
        assert_eq!(back[1].fields.status, Some(1));
        assert_eq!(back[1].fields.input_tokens, None);
        assert_eq!(back[1].fields.agent_name, None);
        assert!(back[1].fields.logs.is_empty());
        assert_eq!(back[1].fields.eval_score, Some(800));
        assert_eq!(back[1].fields.eval_label.as_deref(), Some("未通过"));

        // fold input 行号映射
        let folds = store.scan_fold_inputs(seg);
        assert_eq!(folds.len(), 2);
        assert_eq!(folds[1].0, 1);
        assert_eq!(folds[1].1.trace_id, 2);

        // unlink
        store.unlink_segment(seg);
        assert!(store.scan_records(seg).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn keyed_scan_decodes_only_target_records_and_preserves_physical_rows() {
        let dir = temp_dir();
        let store = VortexSegmentStore::open(&dir).unwrap();
        let seg = SegmentId::new(9);
        let mut first = rec(1, 7, 1);
        first.fields.logs = vec!["first".into()];
        let mut second = rec(1, 7, 2);
        second.fields.logs = vec!["second".into()];
        store.flush_to_segment(seg, &[rec(1, 1, 1), first, second, rec(2, 7, 1)]);

        let keys = HashSet::from([(1, 7)]);
        let raw = store.scan_records_for_keys(seg, &keys).unwrap();
        assert!(raw.used_point_index);
        assert_eq!(raw.decoded_rows, 2);
        assert_eq!(
            raw.rows.iter().map(|(row, _)| *row).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(raw.rows[0].1.fields.logs, vec!["first"]);
        assert_eq!(raw.rows[1].1.fields.logs, vec!["second"]);

        std::fs::remove_file(store.key_index_path(seg)).unwrap();
        drop(store);
        let reopened = VortexSegmentStore::open(&dir).unwrap();
        let rebuilt = reopened.scan_fold_inputs_for_keys(seg, &keys).unwrap();
        assert_eq!(rebuilt.decoded_rows, 2);
        assert_eq!(rebuilt.indexes_rebuilt, 1);
        assert!(reopened.key_index_path(seg).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn keyed_scan_rebuilds_corrupt_or_mismatched_sidecar() {
        let dir = temp_dir();
        let first = SegmentId::new(10);
        let second = SegmentId::new(11);
        let store = VortexSegmentStore::open(&dir).unwrap();
        store.flush_to_segment(first, &[rec(1, 1, 1), rec(1, 2, 2)]);
        store.flush_to_segment(second, &[rec(2, 1, 1), rec(2, 2, 2)]);

        let mut corrupt = std::fs::read(store.key_index_path(first)).unwrap();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xff;
        std::fs::write(store.key_index_path(first), corrupt).unwrap();
        std::fs::copy(store.key_index_path(first), store.key_index_path(second)).unwrap();
        drop(store);

        let reopened = VortexSegmentStore::open(&dir).unwrap();
        let first_scan = reopened
            .scan_records_for_keys(first, &HashSet::from([(1, 2)]))
            .unwrap();
        assert_eq!(first_scan.indexes_rebuilt, 1);
        assert_eq!(first_scan.rows[0].0, 1);

        // 把 first 的有效目录错配给 second；自身 CRC 正确，但数据 fingerprint 不同，仍必须重建。
        std::fs::copy(
            reopened.key_index_path(first),
            reopened.key_index_path(second),
        )
        .unwrap();
        let reopened_again = VortexSegmentStore::open(&dir).unwrap();
        let second_scan = reopened_again
            .scan_records_for_keys(second, &HashSet::from([(2, 2)]))
            .unwrap();
        assert_eq!(second_scan.indexes_rebuilt, 1);
        assert_eq!(second_scan.rows[0].0, 1);

        let cached = reopened_again
            .scan_records_for_keys(second, &HashSet::from([(2, 1)]))
            .unwrap();
        assert_eq!(cached.index_bytes_read, 0, "后续点读复用已校验内存目录");
        assert_eq!(cached.decoded_rows, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn predicate_pushdown_filters_by_time_range() {
        // 谓词下推:按 ts 范围过滤,只读命中行(过滤进 Vortex 扫描,不在 Rust 后置)。
        let dir = temp_dir();
        let store = VortexSegmentStore::open(&dir).unwrap();
        let seg = SegmentId::new(2);
        // 5 行,ts = 100,200,300,400,500(rec 里 ts = seq*100)
        let rows: Vec<WalRecord> = (1..=5).map(|i| rec(1, i, i)).collect();
        store.flush_to_segment(seg, &rows);

        // 全读 5 行
        assert_eq!(store.scan_records(seg).len(), 5);

        // ts ∈ [200,400] → 只剩 3 行(ts=200,300,400)
        let hit = store.scan_in_time(seg, 200, 400, Projection::ALL);
        let ts: Vec<i64> = hit.iter().map(|r| r.ts).collect();
        assert_eq!(ts, vec![200, 300, 400], "下推过滤只返回时间窗内的行");

        // 窗口外 → 空
        assert!(store
            .scan_in_time(seg, 1000, 2000, Projection::ALL)
            .is_empty());
        // 单点窗口
        let one = store.scan_in_time(seg, 300, 300, Projection::ALL);
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].ts, 300);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn logs_encoding_survives_separator_nul_and_cjk() {
        // logs 编码对任意内容可逆：含分隔符/转义符本身、NUL、二进制、换行、中文都 round-trip。
        let cases: Vec<Vec<String>> = vec![
            vec![],
            vec!["".into()],
            vec!["开始".into(), "研判".into()],
            vec!["含分隔符\u{1e}和转义符\\的日志".into()],
            vec!["二进制\u{0}错误码\u{0}帧".into()], // NUL —— 老的 NUL 连接会在这切坏
            vec!["多行\n日志\r\n带制表\t符".into(), "第二条".into()],
            vec!["协议帧\u{1e}\\\u{0}\u{1f}混合".into()],
        ];
        for logs in cases {
            let round = match encode_logs(&logs) {
                None => Vec::new(),
                Some(s) => decode_logs(&s),
            };
            assert_eq!(round, logs, "logs 编解码可逆: {logs:?}");
        }
    }

    #[test]
    fn logs_round_trip_through_segment_with_nul() {
        // 端到端：带 NUL 的 logs 写进列式段、读回一致（不是只测内存编解码）。
        let dir = temp_dir();
        let store = VortexSegmentStore::open(&dir).unwrap();
        let seg = SegmentId::new(11);
        let mut a = rec(1, 10, 1);
        a.fields.logs = vec!["帧\u{0}头".into(), "正常日志".into()];
        store.flush_to_segment(seg, &[a]);
        let back = store.scan_records(seg);
        assert_eq!(
            back[0].fields.logs,
            vec!["帧\u{0}头", "正常日志"],
            "含 NUL 的 logs 过段不丢不错切"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn segment_compresses_repetitive_text() {
        // Vortex 默认写策略是 BtrBlocks（字符串走 FSST/dict）—— 高度重复的大文本列应被压到远小于原文。
        // 这条同时是"压缩确实开着"的回归守卫（若未来误关压缩,文件会暴涨,这里会失败）。
        // ⚠️ 阈值 1/5 是按**当前默认压缩策略（BtrBlocks + FSST）对高度重复文本**定的经验值,不是协议保证。
        //    若升级 Vortex 后默认策略变了（或我们改用 with_strategy 自定义压缩器）,这个硬阈值可能误伤,
        //    需同步重测一组真实样本再调——它守的是"压缩没被关掉",不是某个固定压缩比。
        let dir = temp_dir();
        let store = VortexSegmentStore::open(&dir).unwrap();
        let seg = SegmentId::new(12);
        let big = "疑似盗刷,建议拦截并人工复核。".repeat(50); // 单行约 1.5KB
        let raw_per_row = big.len();
        let rows: Vec<WalRecord> = (1..=200)
            .map(|i| {
                let mut r = rec(1, i, i);
                r.fields.output_text = Some(big.clone());
                r
            })
            .collect();
        store.flush_to_segment(seg, &rows);
        let file = store.seg_path(seg);
        let on_disk = std::fs::metadata(&file).unwrap().len() as usize;
        let raw_total = raw_per_row * rows.len(); // 仅这一列的原始字节量
        assert!(
            on_disk < raw_total / 5,
            "高度重复文本应被压到原文的 1/5 以下：盘上 {on_disk} vs 原文 {raw_total}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn projection_reads_only_selected_columns() {
        // 投影下推:只 select 命中列,被裁掉的列(尤其大文本)读回即 None,身份/选中列照常。
        let dir = temp_dir();
        let store = VortexSegmentStore::open(&dir).unwrap();
        let seg = SegmentId::new(7);

        let mut a = rec(1, 10, 1);
        a.fields.span_name = Some("risk.review".into());
        a.fields.display_name = Some("风险审核".into());
        a.fields.agent_name = Some("风控".into());
        a.fields.input_tokens = Some(100);
        a.fields.output_tokens = Some(20);
        a.fields.input_text = Some("很长的提示词……".into());
        a.fields.output_text = Some("很长的回答正文……".into());
        a.fields.logs = vec!["开始".into()];
        store.flush_to_segment(seg, &[a]);

        // 窄投影:只要 agent + token(成本下钻的列)。
        let proj = Projection::of(
            Projection::AGENT_NAME | Projection::INPUT_TOKENS | Projection::OUTPUT_TOKENS,
        );
        let folds = store.scan_fold_inputs_projected(seg, proj).unwrap();
        assert_eq!(folds.len(), 1);
        assert_eq!(folds[0].0, 0, "投影不丢行 → 物理行号完整");
        let f = &folds[0].1;
        // 身份恒在
        assert_eq!(f.trace_id, 1);
        assert_eq!(f.identity.ext_span_id, "1-10");
        // 选中列读得到
        assert_eq!(f.fields.agent_name.as_deref(), Some("风控"));
        assert_eq!(f.fields.input_tokens, Some(100));
        assert_eq!(f.fields.output_tokens, Some(20));
        // 未选列(被裁掉)读回 None —— 大文本列连解码都没做
        assert_eq!(f.fields.input_text, None, "投影外的大文本列不读 → None");
        assert_eq!(f.fields.output_text, None, "投影外的大文本列不读 → None");
        assert!(f.fields.logs.is_empty(), "投影外的 logs 列不读 → 空");
        assert_eq!(f.fields.span_name, None, "投影外的名称列不读 → None");
        assert_eq!(f.fields.display_name, None, "投影外的展示名列不读 → None");

        let name_proj = Projection::of(Projection::SPAN_NAME | Projection::DISPLAY_NAME);
        let names = store.scan_fold_inputs_projected(seg, name_proj).unwrap();
        assert_eq!(names[0].1.fields.span_name.as_deref(), Some("risk.review"));
        assert_eq!(names[0].1.fields.display_name.as_deref(), Some("风险审核"));
        assert_eq!(
            names[0].1.fields.agent_name, None,
            "名称投影不应读取 agent 列"
        );

        // 对照:全列读回原文都在。
        let all = store.scan_records(seg);
        assert_eq!(
            all[0].fields.output_text.as_deref(),
            Some("很长的回答正文……")
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn engine_uses_vortex_pushdown_end_to_end() {
        // 端到端:用 VortexSegmentStore 起引擎,灌数据 flush 进列式段,带时间窗读 → 引擎走真 Vortex 下推。
        use std::sync::Arc;
        use yt_core::ids::WalLsn;
        use yt_engine::{TraceQuery, WriteCoordinator};

        let dir = temp_dir();
        let store = Arc::new(VortexSegmentStore::open(&dir).unwrap());
        let wc = WriteCoordinator::new(store);

        let rows: Vec<WalRecord> = (1..=3).map(|i| rec(1, i, i)).collect(); // ts = 100,200,300
        wc.ingest(rows.clone());
        wc.commit_flush(&rows, WalLsn::new(3)); // 写进 .vortex 段、内存表回收

        let snap = wc.pin_snapshot();
        // 全开窗:3 条都在(从列式段读回)
        assert_eq!(wc.read_spans_query(&snap, &TraceQuery::all()).0.len(), 3);
        // 时间窗 [150,250]:引擎走 Vortex 下推,只回 ts=200 的 span2
        let (hit, _) = wc.read_spans_query(
            &snap,
            &TraceQuery {
                trace_id: None,
                time_from: 150,
                time_to: 250,
                tenant_id: None,
            },
        );
        assert_eq!(hit.len(), 1, "Vortex 下推穿过引擎读路径,行级时间过滤");
        assert_eq!(hit[0].span_id, 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn engine_applies_deletion_vector_to_vortex_point_rows() {
        use std::sync::Arc;
        use yt_core::ids::WalLsn;
        use yt_engine::WriteCoordinator;

        let dir = temp_dir();
        let store = Arc::new(VortexSegmentStore::open(&dir).unwrap());
        let wc = WriteCoordinator::new(store);
        let mut deleted_log = rec(1, 7, 1);
        deleted_log.fields.logs = vec!["must-not-leak".into()];
        let rows = vec![rec(1, 1, 1), deleted_log, rec(1, 7, 2)];
        wc.ingest(rows.clone());
        wc.commit_flush(&rows, WalLsn::new(3));
        wc.commit_delete(SegmentId::new(1), 1);

        let snap = wc.pin_snapshot();
        let (span, stats) = wc.console_span_for_tenant(&snap, 1, 7, None);
        assert_eq!(span.as_ref().map(|span| span.span_id), Some(7));
        assert_eq!(stats.point_lookup_segments, 1);
        assert_eq!(stats.decoded_segment_rows, 2);

        let keys = HashSet::from([(1, 7)]);
        let (logs, log_stats) = wc.log_events_for_trace_keys_with_stats(&snap, 1, &keys);
        assert!(logs.get(&7).is_none(), "被删除物理行的日志不能泄漏");
        assert_eq!(log_stats.point_lookup_segments, 1);
        assert_eq!(log_stats.decoded_segment_rows, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
