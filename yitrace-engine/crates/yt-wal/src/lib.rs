//! yt-wal —— 写前日志。支持两种后端：
//! - **内存**（`Wal::new`）：测试用，不落盘。
//! - **文件**（`Wal::open(path)`）：**真落盘 + fsync**，进程崩溃/重启后能重放（§M.6）。
//!
//! 崩溃安全的帧格式（只用标准库，零依赖）：
//!   每批一帧 = `[first_lsn u64][payload_len u32][payload][crc32 u32][marker=1 u8]`
//!   - 整帧（含 crc+marker）写完并 **fsync** 之后才回 ack。
//!   - 重放时遇到第一个撕裂/损坏帧（短读 / marker≠1 / crc 不符）即停 —— 那批从未 ack，丢弃合法。
//!     → 不丢已 ack、不重放半截批。
//!
//! payload 是该批记录的自研二进制编码（定长字段 LE + 长度前缀字符串），同样零依赖。
#![allow(dead_code)]

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use yt_core::event::{EventIdentity, EventType};
use yt_core::fold::{FoldInput, SpanFields};
use yt_core::ids::WalLsn;

const BATCH_MAGIC_V2: u64 = 0x3254_4259_5741_4C59; // Versioned batch sentinel, distinct from realistic record counts.
const SPAN_FIELDS_MAGIC: u64 = 0x3244_4C46_5459_5954; // Versioned SpanFields sentinel for standalone upgrade payloads.
const WAL_STATE_MAGIC: u64 = 0x3154_5357_5459_5954; // "TYYTWST1"
const WAL_STATE_VERSION: u32 = 1;
const WAL_STATE_LEN: usize = 40;

/// 一条 WAL 记录 = 一个事件。
#[derive(Clone)]
pub struct WalRecord {
    pub trace_id: u64,
    pub span_id: u64,
    pub ts: i64,
    pub identity: EventIdentity,
    pub fields: SpanFields,
}

impl WalRecord {
    pub fn to_fold_input(&self) -> FoldInput {
        FoldInput {
            trace_id: self.trace_id,
            span_id: self.span_id,
            identity: self.identity.clone(),
            fields: self.fields.clone(),
        }
    }
}

/// 内存模式下的一批（保留 crc+marker 以复用 is_committed 语义）。
struct MemBatch {
    first_lsn: u64,
    records: Vec<WalRecord>,
    crc32: u32,
    committed: bool,
}

enum Backing {
    Mem(Vec<MemBatch>),
    File {
        file: File,
        path: PathBuf,
        state_path: PathBuf,
        scanned_len: usize,
        checkpoint_len: usize,
        checkpoint_lsn: u64,
    },
}

pub struct Wal {
    next_lsn: u64,
    backing: Backing,
    /// 写入或同步失败后，文件尾是否提交已不确定，必须重开恢复才能再写。
    write_failed: bool,
}

impl Default for Wal {
    fn default() -> Self {
        Self::new()
    }
}

impl Wal {
    /// 内存模式（测试用，不落盘）。
    pub fn new() -> Self {
        Self {
            next_lsn: 1,
            backing: Backing::Mem(Vec::new()),
            write_failed: false,
        }
    }

    /// 文件模式：真落盘。打开已有文件并扫描出 next_lsn（恢复用），之后 append+fsync。
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let state_path = path.with_extension("state");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        let file_len = file.metadata()?.len() as usize;
        let checkpoint = load_wal_state(&state_path)
            .filter(|state| state.scanned_len <= file_len)
            .unwrap_or_default();
        let existing = read_file_from(&path, checkpoint.scanned_len)?;
        let (frames, consumed) = parse_frames_with_consumed(&existing);
        let scanned_len = checkpoint.scanned_len.saturating_add(consumed);
        // checkpoint 之前的帧已经随 manifest flush 持久化；这里只解码 checkpoint 后的尾部。
        let next_lsn = frames
            .last()
            .map(|(first, recs)| first + (recs.len() as u64).max(1))
            .unwrap_or(checkpoint.next_lsn.max(1));
        // 解析停止处之后是不完整帧；若继续 append，其后的成功提交也会被下次恢复忽略。
        if scanned_len < file_len {
            file.set_len(scanned_len as u64)?;
            file.sync_data()?;
        }
        Ok(Self {
            next_lsn,
            write_failed: false,
            backing: Backing::File {
                file,
                path,
                state_path,
                scanned_len,
                checkpoint_len: checkpoint.scanned_len,
                checkpoint_lsn: checkpoint.checkpoint_lsn,
            },
        })
    }

    /// 追加一批并提交（组提交）。文件模式 fsync 后才返回 → 之后调用方才回 ack。
    pub fn append_committed(&mut self, records: Vec<WalRecord>) -> WalLsn {
        self.try_append_committed(records)
            .expect("yiTrace WAL append failed")
    }

    pub fn try_append_committed(&mut self, records: Vec<WalRecord>) -> std::io::Result<WalLsn> {
        self.try_append_committed_records(&records)
    }

    /// 文件后端只需借用待编码的记录，避免引擎为了先写 WAL 复制整批大文本。
    pub fn try_append_committed_records(
        &mut self,
        records: &[WalRecord],
    ) -> std::io::Result<WalLsn> {
        if self.write_failed {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "WAL write previously failed; reopen before writing",
            ));
        }
        let first = self.next_lsn;
        let n = records.len() as u64;
        match &mut self.backing {
            Backing::Mem(batches) => {
                let crc = crc32_bytes(&encode_batch(&records));
                batches.push(MemBatch {
                    first_lsn: first,
                    records: records.to_vec(),
                    crc32: crc,
                    committed: true,
                });
            }
            Backing::File {
                file, scanned_len, ..
            } => {
                let payload = encode_batch(&records);
                let crc = crc32_bytes(&payload);
                let mut frame = Vec::with_capacity(payload.len() + 17);
                frame.extend_from_slice(&first.to_le_bytes());
                frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                frame.extend_from_slice(&payload);
                frame.extend_from_slice(&crc.to_le_bytes());
                frame.push(1u8); // commit marker
                if let Err(err) = file.write_all(&frame).and_then(|_| file.sync_data()) {
                    self.write_failed = true;
                    return Err(err);
                }
                *scanned_len = scanned_len.saturating_add(frame.len());
            }
        }
        self.next_lsn += n.max(1);
        Ok(WalLsn::new(self.next_lsn - 1))
    }

    /// 崩溃重放：返回「已 ack」批次里 LSN 在 `from`(不含) 之后的每条记录，带其 LSN。
    /// 文件模式重新读盘解析；撕裂尾被丢弃。返回 owned（文件模式无法借用）。
    pub fn replay_after(&self, from: WalLsn) -> Vec<(u64, WalRecord)> {
        self.try_replay_after(from)
            .expect("yiTrace WAL replay failed")
    }

    pub fn try_replay_after(&self, from: WalLsn) -> std::io::Result<Vec<(u64, WalRecord)>> {
        let from = from.get();
        if from >= self.committed_tail().get() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut push = |first: u64, recs: &[WalRecord]| {
            for (i, r) in recs.iter().enumerate() {
                let lsn = first + i as u64;
                if lsn > from {
                    out.push((lsn, r.clone()));
                }
            }
        };
        match &self.backing {
            Backing::Mem(batches) => {
                for b in batches {
                    if b.committed && b.crc32 == crc32_bytes(&encode_batch(&b.records)) {
                        push(b.first_lsn, &b.records);
                    }
                }
            }
            Backing::File {
                path,
                checkpoint_len,
                checkpoint_lsn,
                ..
            } => {
                let offset = if from >= *checkpoint_lsn {
                    *checkpoint_len
                } else {
                    0
                };
                let bytes = read_file_from(path, offset)?;
                for (first, recs) in parse_frames(&bytes) {
                    push(first, &recs);
                }
            }
        }
        Ok(out)
    }

    pub fn committed_tail(&self) -> WalLsn {
        WalLsn::new(self.next_lsn - 1)
    }

    /// manifest 已把 `through` 之前的 WAL 吸收到 segment 后，记录对应文件偏移。
    /// 状态文件只是恢复加速器；缺失、损坏或落后时会从 WAL 正文回退校验。
    pub fn checkpoint(&mut self, through: WalLsn) -> std::io::Result<()> {
        let next_lsn = self.next_lsn;
        let Backing::File {
            state_path,
            scanned_len,
            checkpoint_len,
            checkpoint_lsn,
            ..
        } = &mut self.backing
        else {
            return Ok(());
        };
        if through.get() != next_lsn.saturating_sub(1) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "WAL checkpoint must match committed tail",
            ));
        }
        let state = WalState {
            scanned_len: *scanned_len,
            checkpoint_lsn: through.get(),
            next_lsn,
        };
        save_wal_state(state_path, state)?;
        *checkpoint_len = *scanned_len;
        *checkpoint_lsn = through.get();
        Ok(())
    }

    /// 文件模式下重新扫描 WAL，吸收其它进程已经提交的帧，更新本进程的 next_lsn。
    /// 内存模式无跨进程来源，保持原值。
    pub fn refresh_from_disk(&mut self) -> WalLsn {
        self.refresh_from_disk_after(WalLsn::new(u64::MAX)).0
    }

    pub fn try_refresh_from_disk(&mut self) -> std::io::Result<WalLsn> {
        self.try_refresh_from_disk_after(WalLsn::new(u64::MAX))
            .map(|(tail, _)| tail)
    }

    /// 文件模式增量刷新：只解析上次已确认帧之后的新字节，并返回 `from` 之后的新记录。
    /// 如果文件被截断或本地扫描位置失效，则退回全量扫描。
    pub fn refresh_from_disk_after(&mut self, from: WalLsn) -> (WalLsn, Vec<(u64, WalRecord)>) {
        self.try_refresh_from_disk_after(from)
            .expect("yiTrace WAL refresh failed")
    }

    /// 调用方须串行化该文件的所有 writer；失败过的 handle 不自动确认未知提交。
    pub fn try_refresh_from_disk_after(
        &mut self,
        from: WalLsn,
    ) -> std::io::Result<(WalLsn, Vec<(u64, WalRecord)>)> {
        if self.write_failed {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "WAL write previously failed; reopen before refresh",
            ));
        }
        let from = from.get();
        let mut changed = Vec::new();
        if let Backing::File {
            file,
            path,
            scanned_len,
            checkpoint_len,
            checkpoint_lsn,
            ..
        } = &mut self.backing
        {
            let file_len = std::fs::metadata(&*path)?.len() as usize;
            if file_len == *scanned_len {
                return Ok((self.committed_tail(), changed));
            }

            if file_len < *scanned_len {
                let existing = std::fs::read(&*path)?;
                let (frames, consumed) = parse_frames_with_consumed(&existing);
                *scanned_len = consumed;
                self.next_lsn = update_next_lsn_from_frames(&frames);
                *checkpoint_len = 0;
                *checkpoint_lsn = 0;
                if consumed < file_len {
                    file.set_len(consumed as u64)?;
                    file.sync_data()?;
                }
                collect_after(&mut changed, from, &frames);
                return Ok((self.committed_tail(), changed));
            }

            let existing = read_file_from(path, *scanned_len)?;
            let (frames, consumed) = parse_frames_with_consumed(&existing);
            *scanned_len = (*scanned_len).saturating_add(consumed);
            if *scanned_len < file_len {
                file.set_len(*scanned_len as u64)?;
                file.sync_data()?;
            }
            if let Some(next) = frames
                .last()
                .map(|(first, recs)| first + (recs.len() as u64).max(1))
            {
                self.next_lsn = next;
            }
            collect_after(&mut changed, from, &frames);
        }
        Ok((self.committed_tail(), changed))
    }
}

#[derive(Clone, Copy)]
struct WalState {
    scanned_len: usize,
    checkpoint_lsn: u64,
    next_lsn: u64,
}

impl Default for WalState {
    fn default() -> Self {
        Self {
            scanned_len: 0,
            checkpoint_lsn: 0,
            next_lsn: 1,
        }
    }
}

fn read_file_from(path: &Path, offset: usize) -> std::io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset as u64))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn load_wal_state(path: &Path) -> Option<WalState> {
    let bytes = std::fs::read(path).ok()?;
    if bytes.len() != WAL_STATE_LEN {
        return None;
    }
    let payload = &bytes[..WAL_STATE_LEN - 4];
    let expected = u32::from_le_bytes(bytes[WAL_STATE_LEN - 4..].try_into().ok()?);
    if crc32_bytes(payload) != expected {
        return None;
    }
    let magic = u64::from_le_bytes(payload[0..8].try_into().ok()?);
    let version = u32::from_le_bytes(payload[8..12].try_into().ok()?);
    if magic != WAL_STATE_MAGIC || version != WAL_STATE_VERSION {
        return None;
    }
    Some(WalState {
        scanned_len: u64::from_le_bytes(payload[12..20].try_into().ok()?) as usize,
        checkpoint_lsn: u64::from_le_bytes(payload[20..28].try_into().ok()?),
        next_lsn: u64::from_le_bytes(payload[28..36].try_into().ok()?),
    })
}

fn save_wal_state(path: &Path, state: WalState) -> std::io::Result<()> {
    let mut bytes = Vec::with_capacity(WAL_STATE_LEN);
    bytes.extend_from_slice(&WAL_STATE_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&WAL_STATE_VERSION.to_le_bytes());
    bytes.extend_from_slice(&(state.scanned_len as u64).to_le_bytes());
    bytes.extend_from_slice(&state.checkpoint_lsn.to_le_bytes());
    bytes.extend_from_slice(&state.next_lsn.to_le_bytes());
    let crc = crc32_bytes(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    let tmp = path.with_extension("state.tmp");
    let mut file = File::create(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(tmp, path)
}

// ───────────────────────── 帧解析 ─────────────────────────

/// 解析文件里所有「已提交」帧（crc 通过 + marker=1）。遇撕裂/损坏即停（视为未 ack 的尾）。
fn parse_frames(bytes: &[u8]) -> Vec<(u64, Vec<WalRecord>)> {
    parse_frames_with_consumed(bytes).0
}

fn parse_frames_with_consumed(bytes: &[u8]) -> (Vec<(u64, Vec<WalRecord>)>, usize) {
    let mut out = Vec::new();
    let mut i = 0usize;
    loop {
        let frame_start = i;
        if i + 12 > bytes.len() {
            break; // 不足 first_lsn(8)+len(4)
        }
        let first = u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        i += 8;
        let len = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        if i + len + 5 > bytes.len() {
            i = frame_start;
            break; // payload+crc(4)+marker(1) 不全 → 撕裂尾
        }
        let payload = &bytes[i..i + len];
        i += len;
        let crc = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap());
        i += 4;
        let marker = bytes[i];
        i += 1;
        if marker != 1 || crc != crc32_bytes(payload) {
            i = frame_start;
            break; // 未提交 / 损坏 → 停
        }
        match decode_batch(payload) {
            Some(recs) => out.push((first, recs)),
            None => {
                i = frame_start;
                break;
            }
        }
    }
    (out, i)
}

fn update_next_lsn_from_frames(frames: &[(u64, Vec<WalRecord>)]) -> u64 {
    frames
        .last()
        .map(|(first, recs)| first + (recs.len() as u64).max(1))
        .unwrap_or(1)
}

fn collect_after(out: &mut Vec<(u64, WalRecord)>, from: u64, frames: &[(u64, Vec<WalRecord>)]) {
    for (first, recs) in frames {
        for (i, r) in recs.iter().enumerate() {
            let lsn = first + i as u64;
            if lsn > from {
                out.push((lsn, r.clone()));
            }
        }
    }
}

// ───────────────────────── 二进制编解码（std-only） ─────────────────────────

fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_le_bytes());
}
fn put_str(b: &mut Vec<u8>, s: &str) {
    put_u64(b, s.len() as u64);
    b.extend_from_slice(s.as_bytes());
}
fn put_opt_u64(b: &mut Vec<u8>, v: Option<u64>) {
    match v {
        Some(x) => {
            b.push(1);
            put_u64(b, x);
        }
        None => b.push(0),
    }
}
fn put_opt_u8(b: &mut Vec<u8>, v: Option<u8>) {
    match v {
        Some(x) => {
            b.push(1);
            b.push(x);
        }
        None => b.push(0),
    }
}
fn put_opt_str(b: &mut Vec<u8>, v: &Option<String>) {
    match v {
        Some(s) => {
            b.push(1);
            put_str(b, s);
        }
        None => b.push(0),
    }
}
fn put_bytes(b: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(b, bytes.len() as u64);
    b.extend_from_slice(bytes);
}

/// 把一批记录编码成自研二进制（定长 LE + 长度前缀字符串）。WAL 用它，**段落盘也复用同一套编码**
/// （`FileSegmentStore`），避免两处各写一份记录序列化。
pub fn encode_records(records: &[WalRecord]) -> Vec<u8> {
    encode_batch(records)
}

/// 一条记录在 `encode_records` 结果里的字节范围。
///
/// Segment 点查目录只保存这些范围，不复制记录内容。`offset` 从 batch payload 开头计算。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncodedRecordRange {
    pub trace_id: u64,
    pub span_id: u64,
    pub offset: u64,
    pub len: u32,
}

/// 只读取 batch 的结构字段，找出每条记录的位置，不解码字符串和 SpanFields。
///
/// 只接受当前 v2 编码；旧 segment 没有点查目录时由上层回退整段扫描。
pub fn encoded_record_ranges(payload: &[u8]) -> Option<Vec<EncodedRecordRange>> {
    let mut c = Cur { b: payload, i: 0 };
    if c.u64()? != BATCH_MAGIC_V2 || c.u64()? != 2 {
        return None;
    }
    let n = usize::try_from(c.u64()?).ok()?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let start = c.i;
        let trace_id = c.u64()?;
        let span_id = c.u64()?;
        c.u64()?; // ts
        c.skip_string()?;
        c.u64()?; // seq
        c.u8()?; // event_type
        c.skip_bytes()?; // encoded SpanFields
        let len = u32::try_from(c.i.checked_sub(start)?).ok()?;
        out.push(EncodedRecordRange {
            trace_id,
            span_id,
            offset: start as u64,
            len,
        });
    }
    (c.i == payload.len()).then_some(out)
}

/// 解码由 `encoded_record_ranges` 返回的一条记录切片。
pub fn decode_record(bytes: &[u8]) -> Option<WalRecord> {
    let mut c = Cur { b: bytes, i: 0 };
    let record = decode_record_v2_from(&mut c)?;
    (c.i == bytes.len()).then_some(record)
}

/// `encode_records` 的逆。损坏/截断返回 None。
pub fn decode_records(payload: &[u8]) -> Option<Vec<WalRecord>> {
    decode_batch(payload)
}

/// CRC32（IEEE），段文件完整性校验复用 WAL 同一实现。
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = Crc32::new();
    crc.update(data);
    crc.finish()
}

fn crc32_bytes(data: &[u8]) -> u32 {
    crc32(data)
}

/// SpanFields 的二进制编码（唯一一份）—— WAL、段落盘、manifest 持久化都复用它，避免字段列表抄多份。
fn encode_span_fields_into(b: &mut Vec<u8>, f: &SpanFields) {
    put_u64(b, SPAN_FIELDS_MAGIC);
    put_u64(b, 4);
    put_opt_u8(b, f.status);
    put_opt_u64(b, f.duration_ns);
    put_opt_u64(b, f.parent_span_id);
    put_opt_u64(b, f.input_tokens);
    put_opt_u64(b, f.output_tokens);
    put_opt_u64(b, f.cache_read_tokens);
    put_opt_u64(b, f.cache_write_tokens);
    put_opt_u64(b, f.session_id);
    put_opt_u64(b, f.tenant_id);
    put_opt_str(b, &f.external_trace_id);
    put_opt_str(b, &f.external_span_id);
    put_opt_str(b, &f.external_parent_span_id);
    put_opt_str(b, &f.external_session_id);
    put_opt_str(b, &f.span_name);
    put_opt_str(b, &f.display_name);
    put_opt_str(b, &f.agent_name);
    put_opt_str(b, &f.tool_name);
    put_opt_str(b, &f.model);
    put_opt_str(b, &f.input_text);
    put_opt_str(b, &f.output_text);
    put_opt_u64(b, f.eval_score.map(|v| v as u64));
    put_opt_str(b, &f.eval_label);
    put_u64(b, f.logs.len() as u64);
    for l in &f.logs {
        put_str(b, l);
    }
    put_u64(b, f.attrs.len() as u64);
    for (k, v) in &f.attrs {
        put_str(b, k);
        put_str(b, v);
    }
}

fn decode_span_fields_v4_from(c: &mut Cur) -> Option<SpanFields> {
    let status = c.opt_u8()?;
    let duration_ns = c.opt_u64()?;
    let parent_span_id = c.opt_u64()?;
    let input_tokens = c.opt_u64()?;
    let output_tokens = c.opt_u64()?;
    let cache_read_tokens = c.opt_u64()?;
    let cache_write_tokens = c.opt_u64()?;
    let session_id = c.opt_u64()?;
    let tenant_id = c.opt_u64()?;
    let external_trace_id = c.opt_str()?;
    let external_span_id = c.opt_str()?;
    let external_parent_span_id = c.opt_str()?;
    let external_session_id = c.opt_str()?;
    let span_name = c.opt_str()?;
    let display_name = c.opt_str()?;
    let agent_name = c.opt_str()?;
    let tool_name = c.opt_str()?;
    let model = c.opt_str()?;
    let input_text = c.opt_str()?;
    let output_text = c.opt_str()?;
    let eval_score = c.opt_u64()?.map(|v| v as u32);
    let eval_label = c.opt_str()?;
    let log_n = c.u64()? as usize;
    let mut logs = Vec::with_capacity(log_n);
    for _ in 0..log_n {
        logs.push(c.string()?);
    }
    let attr_n = c.u64()? as usize;
    let mut attrs = std::collections::BTreeMap::new();
    for _ in 0..attr_n {
        attrs.insert(c.string()?, c.string()?);
    }
    Some(SpanFields {
        status,
        duration_ns,
        parent_span_id,
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
    })
}

fn decode_span_fields_v3_from(c: &mut Cur) -> Option<SpanFields> {
    let status = c.opt_u8()?;
    let duration_ns = c.opt_u64()?;
    let parent_span_id = c.opt_u64()?;
    let input_tokens = c.opt_u64()?;
    let output_tokens = c.opt_u64()?;
    let session_id = c.opt_u64()?;
    let tenant_id = c.opt_u64()?;
    let external_trace_id = c.opt_str()?;
    let external_span_id = c.opt_str()?;
    let external_parent_span_id = c.opt_str()?;
    let external_session_id = c.opt_str()?;
    let span_name = c.opt_str()?;
    let display_name = c.opt_str()?;
    let agent_name = c.opt_str()?;
    let tool_name = c.opt_str()?;
    let model = c.opt_str()?;
    let input_text = c.opt_str()?;
    let output_text = c.opt_str()?;
    let eval_score = c.opt_u64()?.map(|v| v as u32);
    let eval_label = c.opt_str()?;
    let log_n = c.u64()? as usize;
    let mut logs = Vec::with_capacity(log_n);
    for _ in 0..log_n {
        logs.push(c.string()?);
    }
    let attr_n = c.u64()? as usize;
    let mut attrs = std::collections::BTreeMap::new();
    for _ in 0..attr_n {
        attrs.insert(c.string()?, c.string()?);
    }
    Some(SpanFields {
        status,
        duration_ns,
        parent_span_id,
        input_tokens,
        output_tokens,
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
        ..Default::default()
    })
}

fn decode_span_fields_v2_from(c: &mut Cur) -> Option<SpanFields> {
    let status = c.opt_u8()?;
    let duration_ns = c.opt_u64()?;
    let parent_span_id = c.opt_u64()?;
    let input_tokens = c.opt_u64()?;
    let output_tokens = c.opt_u64()?;
    let session_id = c.opt_u64()?;
    let tenant_id = c.opt_u64()?;
    let external_trace_id = c.opt_str()?;
    let external_span_id = c.opt_str()?;
    let external_parent_span_id = c.opt_str()?;
    let external_session_id = c.opt_str()?;
    let agent_name = c.opt_str()?;
    let tool_name = c.opt_str()?;
    let model = c.opt_str()?;
    let input_text = c.opt_str()?;
    let output_text = c.opt_str()?;
    let eval_score = c.opt_u64()?.map(|v| v as u32);
    let eval_label = c.opt_str()?;
    let log_n = c.u64()? as usize;
    let mut logs = Vec::with_capacity(log_n);
    for _ in 0..log_n {
        logs.push(c.string()?);
    }
    let attr_n = c.u64()? as usize;
    let mut attrs = std::collections::BTreeMap::new();
    for _ in 0..attr_n {
        attrs.insert(c.string()?, c.string()?);
    }
    Some(SpanFields {
        status,
        duration_ns,
        parent_span_id,
        input_tokens,
        output_tokens,
        session_id,
        tenant_id,
        external_trace_id,
        external_span_id,
        external_parent_span_id,
        external_session_id,
        agent_name,
        tool_name,
        model,
        input_text,
        output_text,
        eval_score,
        eval_label,
        logs,
        attrs,
        ..Default::default()
    })
}

fn decode_span_fields_legacy_from(c: &mut Cur) -> Option<SpanFields> {
    let status = c.opt_u8()?;
    let duration_ns = c.opt_u64()?;
    let parent_span_id = c.opt_u64()?;
    let input_tokens = c.opt_u64()?;
    let output_tokens = c.opt_u64()?;
    let session_id = c.opt_u64()?;
    let tenant_id = c.opt_u64()?;
    let agent_name = c.opt_str()?;
    let tool_name = c.opt_str()?;
    let model = c.opt_str()?;
    let input_text = c.opt_str()?;
    let output_text = c.opt_str()?;
    let eval_score = c.opt_u64()?.map(|v| v as u32);
    let eval_label = c.opt_str()?;
    let log_n = c.u64()? as usize;
    let mut logs = Vec::with_capacity(log_n);
    for _ in 0..log_n {
        logs.push(c.string()?);
    }
    Some(SpanFields {
        status,
        duration_ns,
        parent_span_id,
        input_tokens,
        output_tokens,
        session_id,
        tenant_id,
        agent_name,
        tool_name,
        model,
        input_text,
        output_text,
        eval_score,
        eval_label,
        logs,
        ..Default::default()
    })
}

/// 把一组 `SpanFields` 字段编成独立字节块（manifest 持久化 upgrade 补写块时用）。
pub fn encode_span_fields(f: &SpanFields) -> Vec<u8> {
    let mut b = Vec::new();
    encode_span_fields_into(&mut b, f);
    b
}

/// `encode_span_fields` 的逆。
pub fn decode_span_fields(bytes: &[u8]) -> Option<SpanFields> {
    let mut c = Cur { b: bytes, i: 0 };
    if c.peek_u64() == Some(SPAN_FIELDS_MAGIC) {
        c.u64()?;
        match c.u64()? {
            2 => decode_span_fields_v2_from(&mut c),
            3 => decode_span_fields_v3_from(&mut c),
            4 => decode_span_fields_v4_from(&mut c),
            _ => None,
        }
    } else {
        decode_span_fields_legacy_from(&mut c)
    }
}

fn encode_batch(records: &[WalRecord]) -> Vec<u8> {
    let mut b = Vec::new();
    put_u64(&mut b, BATCH_MAGIC_V2);
    put_u64(&mut b, 2);
    put_u64(&mut b, records.len() as u64);
    for r in records {
        put_u64(&mut b, r.trace_id);
        put_u64(&mut b, r.span_id);
        put_u64(&mut b, r.ts as u64); // i64 位模式
        put_str(&mut b, &r.identity.ext_span_id);
        put_u64(&mut b, r.identity.seq);
        b.push(r.identity.event_type.tag());
        let fields = encode_span_fields(&r.fields);
        put_bytes(&mut b, &fields);
    }
    b
}

struct Cur<'a> {
    b: &'a [u8],
    i: usize,
}
impl<'a> Cur<'a> {
    fn peek_u64(&self) -> Option<u64> {
        let e = self.i + 8;
        let s = self.b.get(self.i..e)?;
        Some(u64::from_le_bytes(s.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        let e = self.i + 8;
        let s = self.b.get(self.i..e)?;
        self.i = e;
        Some(u64::from_le_bytes(s.try_into().ok()?))
    }
    fn u8(&mut self) -> Option<u8> {
        let v = *self.b.get(self.i)?;
        self.i += 1;
        Some(v)
    }
    fn string(&mut self) -> Option<String> {
        let n = self.u64()? as usize;
        let e = self.i + n;
        let s = self.b.get(self.i..e)?;
        self.i = e;
        Some(String::from_utf8_lossy(s).into_owned())
    }
    fn bytes(&mut self) -> Option<&'a [u8]> {
        let n = self.u64()? as usize;
        let e = self.i + n;
        let s = self.b.get(self.i..e)?;
        self.i = e;
        Some(s)
    }
    fn skip_string(&mut self) -> Option<()> {
        let n = usize::try_from(self.u64()?).ok()?;
        self.i = self.i.checked_add(n)?;
        (self.i <= self.b.len()).then_some(())
    }
    fn skip_bytes(&mut self) -> Option<()> {
        self.skip_string()
    }
    fn opt_u64(&mut self) -> Option<Option<u64>> {
        if self.u8()? == 1 {
            Some(Some(self.u64()?))
        } else {
            Some(None)
        }
    }
    fn opt_u8(&mut self) -> Option<Option<u8>> {
        if self.u8()? == 1 {
            Some(Some(self.u8()?))
        } else {
            Some(None)
        }
    }
    fn opt_str(&mut self) -> Option<Option<String>> {
        if self.u8()? == 1 {
            Some(Some(self.string()?))
        } else {
            Some(None)
        }
    }
}

fn decode_batch(payload: &[u8]) -> Option<Vec<WalRecord>> {
    let mut c = Cur { b: payload, i: 0 };
    let first = c.u64()?;
    if first == BATCH_MAGIC_V2 {
        return decode_batch_v2(&mut c);
    }
    decode_batch_legacy(&mut c, first as usize)
}

fn decode_batch_v2(c: &mut Cur) -> Option<Vec<WalRecord>> {
    let ver = c.u64()?;
    if ver != 2 {
        return None;
    }
    let n = c.u64()? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(decode_record_v2_from(c)?);
    }
    Some(out)
}

fn decode_record_v2_from(c: &mut Cur) -> Option<WalRecord> {
    let trace_id = c.u64()?;
    let span_id = c.u64()?;
    let ts = c.u64()? as i64;
    let ext = c.string()?;
    let seq = c.u64()?;
    let event_type = EventType::from_tag(c.u8()?);
    let fields = decode_span_fields(c.bytes()?)?;
    Some(WalRecord {
        trace_id,
        span_id,
        ts,
        identity: EventIdentity {
            ext_span_id: ext,
            seq,
            event_type,
        },
        fields,
    })
}

fn decode_batch_legacy(c: &mut Cur, n: usize) -> Option<Vec<WalRecord>> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let trace_id = c.u64()?;
        let span_id = c.u64()?;
        let ts = c.u64()? as i64;
        let ext = c.string()?;
        let seq = c.u64()?;
        let event_type = EventType::from_tag(c.u8()?);
        let fields = decode_span_fields_legacy_from(c)?;
        out.push(WalRecord {
            trace_id,
            span_id,
            ts,
            identity: EventIdentity {
                ext_span_id: ext,
                seq,
                event_type,
            },
            fields,
        });
    }
    Some(out)
}

/// CRC32（IEEE，反射多项式 0xEDB8_8320）slicing-by-8 查表实现。8 张表在首用时一次性算好，
/// 每轮处理 8 字节；校验值和历史逐字节实现完全一致，保持 std-only 且不改变 WAL/segment 格式。
fn crc32_tables() -> &'static [[u32; 256]; 8] {
    static TABLES: std::sync::OnceLock<[[u32; 256]; 8]> = std::sync::OnceLock::new();
    TABLES.get_or_init(|| {
        let mut tables = [[0u32; 256]; 8];
        let mut i = 0usize;
        while i < 256 {
            let mut crc = i as u32;
            let mut j = 0;
            while j < 8 {
                let mask = (crc & 1).wrapping_neg();
                crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
                j += 1;
            }
            tables[0][i] = crc;
            i += 1;
        }
        let mut slice = 1usize;
        while slice < 8 {
            let mut i = 0usize;
            while i < 256 {
                let previous = tables[slice - 1][i];
                tables[slice][i] = (previous >> 8) ^ tables[0][(previous & 0xff) as usize];
                i += 1;
            }
            slice += 1;
        }
        tables
    })
}

/// 可分块更新的 IEEE CRC32。WAL 一次性校验和 segment 流式校验共用这一份实现，
/// 避免两套 slicing-by-8 表和循环以后发生漂移。
pub struct Crc32(u32);

impl Crc32 {
    pub fn new() -> Self {
        Self(0xffff_ffff)
    }

    pub fn update(&mut self, mut bytes: &[u8]) {
        let tables = crc32_tables();
        while bytes.len() >= 8 {
            let first = u32::from_le_bytes(bytes[..4].try_into().unwrap());
            let current = self.0 ^ first;
            self.0 = tables[7][(current & 0xff) as usize]
                ^ tables[6][((current >> 8) & 0xff) as usize]
                ^ tables[5][((current >> 16) & 0xff) as usize]
                ^ tables[4][(current >> 24) as usize]
                ^ tables[3][bytes[4] as usize]
                ^ tables[2][bytes[5] as usize]
                ^ tables[1][bytes[6] as usize]
                ^ tables[0][bytes[7] as usize];
            bytes = &bytes[8..];
        }
        for &byte in bytes {
            self.0 = (self.0 >> 8) ^ tables[0][((self.0 ^ byte as u32) & 0xff) as usize];
        }
    }

    pub fn finish(self) -> u32 {
        !self.0
    }
}

impl Default for Crc32 {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use yt_core::event::EventType;

    fn rec(span: &str, seq: u64) -> WalRecord {
        WalRecord {
            trace_id: 1,
            span_id: seq,
            ts: seq as i64,
            identity: EventIdentity {
                ext_span_id: span.into(),
                seq,
                event_type: EventType::SpanEnd,
            },
            fields: SpanFields {
                logs: vec![format!("日志{seq}")],
                ..Default::default()
            },
        }
    }

    fn temp_path() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "yt_wal_{}_{}.wal",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn crc32_matches_ieee_known_vectors() {
        // 查表实现必须与 IEEE CRC32 标准逐字节一致（换实现不能改校验和,否则老 WAL/段 全部读不回）。
        assert_eq!(crc32(b""), 0x0000_0000);
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926, "标准测试向量");
        assert_eq!(
            crc32(b"The quick brown fox jumps over the lazy dog"),
            0x414F_A339
        );
    }

    #[test]
    fn crc32_slicing_by_8_matches_slow_reference() {
        fn slow(data: &[u8]) -> u32 {
            let mut crc = 0xffff_ffffu32;
            for &byte in data {
                crc ^= byte as u32;
                for _ in 0..8 {
                    let mask = (crc & 1).wrapping_neg();
                    crc = (crc >> 1) ^ (0xedb8_8320 & mask);
                }
            }
            !crc
        }

        let bytes = (0..4_097)
            .map(|i| ((i * 197 + i / 11) & 0xff) as u8)
            .collect::<Vec<_>>();
        for len in 0..=bytes.len() {
            assert_eq!(crc32(&bytes[..len]), slow(&bytes[..len]), "len={len}");
        }
    }

    #[test]
    fn mem_replay_after_watermark() {
        let mut wal = Wal::new();
        wal.append_committed(vec![rec("a", 1)]);
        let l2 = wal.append_committed(vec![rec("b", 2), rec("c", 3)]);
        assert_eq!(wal.committed_tail(), l2);
        let all: Vec<_> = wal
            .replay_after(WalLsn::new(0))
            .into_iter()
            .map(|(l, _)| l)
            .collect();
        assert_eq!(all, vec![1, 2, 3]);
        let after: Vec<_> = wal
            .replay_after(WalLsn::new(1))
            .into_iter()
            .map(|(_, r)| r.identity.seq)
            .collect();
        assert_eq!(after, vec![2, 3]);
    }

    #[test]
    fn file_wal_survives_reopen_real_disk() {
        // 真落盘：写 → drop(模拟崩溃) → 重开同一文件 → 重放,记录还在。
        let path = temp_path();
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append_committed(vec![rec("反洗钱", 1), rec("盗刷", 2)]);
            wal.append_committed(vec![rec("转账", 3)]);
            assert_eq!(wal.committed_tail(), WalLsn::new(3));
            // drop → File 关闭。之前每批都 fsync 过。
        }
        // 重开（相当于重启进程）
        let wal2 = Wal::open(&path).unwrap();
        assert_eq!(
            wal2.committed_tail(),
            WalLsn::new(3),
            "重开后 next_lsn 从盘上恢复"
        );
        let recs = wal2.replay_after(WalLsn::new(0));
        let seqs: Vec<u64> = recs.iter().map(|(l, _)| *l).collect();
        assert_eq!(seqs, vec![1, 2, 3], "三条记录从磁盘重放回来");
        // 内容也对（含中文 + 逐字段）
        assert_eq!(recs[0].1.identity.ext_span_id, "反洗钱");
        assert_eq!(recs[0].1.fields.logs, vec!["日志1"]);
        assert_eq!(recs[1].1.identity.ext_span_id, "盗刷");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn checkpoint_reopens_from_tail_and_preserves_unflushed_records() {
        let path = temp_path();
        let state_path = path.with_extension("state");
        {
            let mut wal = Wal::open(&path).unwrap();
            let flushed = wal.append_committed(vec![rec("flushed", 1), rec("flushed-2", 2)]);
            wal.checkpoint(flushed).unwrap();
            wal.append_committed(vec![rec("tail", 3)]);
        }

        let wal = Wal::open(&path).unwrap();
        assert_eq!(wal.committed_tail(), WalLsn::new(3));
        let tail = wal.replay_after(WalLsn::new(2));
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].0, 3);
        assert_eq!(tail[0].1.identity.ext_span_id, "tail");
        assert!(wal.replay_after(WalLsn::new(3)).is_empty());

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(state_path);
    }

    #[test]
    fn corrupt_checkpoint_falls_back_to_full_wal_scan() {
        let path = temp_path();
        let state_path = path.with_extension("state");
        {
            let mut wal = Wal::open(&path).unwrap();
            let tail = wal.append_committed(vec![rec("a", 1), rec("b", 2)]);
            wal.checkpoint(tail).unwrap();
        }
        std::fs::write(&state_path, b"broken").unwrap();

        let wal = Wal::open(&path).unwrap();
        assert_eq!(wal.committed_tail(), WalLsn::new(2));
        assert_eq!(wal.replay_after(WalLsn::new(0)).len(), 2);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(state_path);
    }

    #[test]
    fn file_wal_incremental_refresh_sees_only_new_frames() {
        let path = temp_path();
        let mut wal_a = Wal::open(&path).unwrap();
        let mut wal_b = Wal::open(&path).unwrap();

        wal_a.append_committed(vec![rec("a", 1)]);
        let (tail_b, rows_b) = wal_b.refresh_from_disk_after(WalLsn::new(0));
        assert_eq!(tail_b, WalLsn::new(1));
        assert_eq!(rows_b.len(), 1);
        assert_eq!(rows_b[0].0, 1);
        assert_eq!(rows_b[0].1.identity.ext_span_id, "a");

        wal_b.append_committed(vec![rec("b", 2), rec("c", 3)]);
        let (tail_a, rows_a) = wal_a.refresh_from_disk_after(WalLsn::new(1));
        assert_eq!(tail_a, WalLsn::new(3));
        assert_eq!(
            rows_a
                .iter()
                .map(|(lsn, r)| (*lsn, r.identity.ext_span_id.as_str()))
                .collect::<Vec<_>>(),
            vec![(2, "b"), (3, "c")]
        );

        let (tail_again, rows_again) = wal_a.refresh_from_disk_after(WalLsn::new(3));
        assert_eq!(tail_again, WalLsn::new(3));
        assert!(rows_again.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn span_fields_v4_roundtrips_names_attrs_and_cache_tokens() {
        let mut attrs = std::collections::BTreeMap::new();
        attrs.insert("project_id".to_string(), "\"agentic-data\"".to_string());
        attrs.insert("retry".to_string(), "true".to_string());
        let fields = SpanFields {
            external_trace_id: Some("run-uuid".into()),
            external_span_id: Some("span-uuid".into()),
            external_session_id: Some("session-uuid".into()),
            span_name: Some("risk.review".into()),
            display_name: Some("风险审核".into()),
            attrs,
            cache_read_tokens: Some(0),
            cache_write_tokens: Some(300),
            logs: vec!["ok".into()],
            ..Default::default()
        };
        let decoded = decode_span_fields(&encode_span_fields(&fields)).unwrap();
        assert_eq!(decoded.external_trace_id.as_deref(), Some("run-uuid"));
        assert_eq!(decoded.external_span_id.as_deref(), Some("span-uuid"));
        assert_eq!(decoded.external_session_id.as_deref(), Some("session-uuid"));
        assert_eq!(decoded.span_name.as_deref(), Some("risk.review"));
        assert_eq!(decoded.display_name.as_deref(), Some("风险审核"));
        assert_eq!(
            decoded.attrs.get("project_id").map(String::as_str),
            Some("\"agentic-data\"")
        );
        assert_eq!(decoded.attrs.get("retry").map(String::as_str), Some("true"));
        assert_eq!(decoded.logs, vec!["ok"]);
        assert_eq!(decoded.cache_read_tokens, Some(0));
        assert_eq!(decoded.cache_write_tokens, Some(300));
    }

    #[test]
    fn span_fields_v2_decodes_with_empty_names() {
        // 固定写出旧版 v2 布局，保证升级后仍能读取历史 WAL/segment 字段块。
        let mut bytes = Vec::new();
        put_u64(&mut bytes, SPAN_FIELDS_MAGIC);
        put_u64(&mut bytes, 2);
        put_opt_u8(&mut bytes, Some(0));
        put_opt_u64(&mut bytes, Some(10));
        put_opt_u64(&mut bytes, None);
        put_opt_u64(&mut bytes, None);
        put_opt_u64(&mut bytes, None);
        put_opt_u64(&mut bytes, None);
        put_opt_u64(&mut bytes, Some(1));
        put_opt_str(&mut bytes, &Some("run-old".into()));
        put_opt_str(&mut bytes, &Some("span-old".into()));
        put_opt_str(&mut bytes, &None);
        put_opt_str(&mut bytes, &None);
        put_opt_str(&mut bytes, &Some("旧 Agent".into()));
        put_opt_str(&mut bytes, &Some("lookup".into()));
        put_opt_str(&mut bytes, &None);
        put_opt_str(&mut bytes, &None);
        put_opt_str(&mut bytes, &None);
        put_opt_u64(&mut bytes, None);
        put_opt_str(&mut bytes, &None);
        put_u64(&mut bytes, 1);
        put_str(&mut bytes, "旧日志");
        put_u64(&mut bytes, 0);

        let decoded = decode_span_fields(&bytes).expect("v2 fields should decode");
        assert_eq!(decoded.external_trace_id.as_deref(), Some("run-old"));
        assert_eq!(decoded.agent_name.as_deref(), Some("旧 Agent"));
        assert_eq!(decoded.tool_name.as_deref(), Some("lookup"));
        assert_eq!(decoded.logs, vec!["旧日志"]);
        assert_eq!(decoded.span_name, None);
        assert_eq!(decoded.display_name, None);
    }

    #[test]
    fn encoded_record_ranges_decode_each_record_without_batch_scan() {
        let records = vec![rec("first", 1), rec("second", 2), rec("third", 3)];
        let payload = encode_records(&records);
        let ranges = encoded_record_ranges(&payload).unwrap();
        assert_eq!(ranges.len(), records.len());

        for (expected, range) in records.iter().zip(ranges) {
            let start = range.offset as usize;
            let end = start + range.len as usize;
            let decoded = decode_record(&payload[start..end]).unwrap();
            assert_eq!(decoded.trace_id, expected.trace_id);
            assert_eq!(decoded.span_id, expected.span_id);
            assert_eq!(decoded.ts, expected.ts);
            assert_eq!(decoded.identity, expected.identity);
            assert_eq!(decoded.fields, expected.fields);
            assert_eq!(range.trace_id, expected.trace_id);
            assert_eq!(range.span_id, expected.span_id);
        }
    }

    #[test]
    fn torn_tail_is_dropped() {
        // 模拟「最后一帧只写了一半就崩了」：截断文件尾部 → 该批视为未 ack,重放丢弃,前面的不受影响。
        let path = temp_path();
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append_committed(vec![rec("ok", 1)]);
            wal.append_committed(vec![rec("half", 2)]);
        }
        // 砍掉文件最后 3 字节（破坏第二帧的 crc/marker）
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.truncate(bytes.len() - 3);
        std::fs::write(&path, &bytes).unwrap();

        let wal = Wal::open(&path).unwrap();
        let seqs: Vec<u64> = wal
            .replay_after(WalLsn::new(0))
            .iter()
            .map(|(l, _)| *l)
            .collect();
        assert_eq!(seqs, vec![1], "撕裂的第二帧被丢弃,第一帧完好");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn durability_regression_append_after_torn_tail_survives_reopen() {
        let path = temp_path();
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append_committed(vec![rec("first", 1)]);
        }
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[0u8; 7])
            .unwrap();
        {
            let mut wal = Wal::open(&path).unwrap();
            assert_eq!(wal.append_committed(vec![rec("second", 2)]).get(), 2);
        }
        let wal = Wal::open(&path).unwrap();
        assert_eq!(
            wal.replay_after(WalLsn::new(0))
                .iter()
                .map(|(lsn, _)| *lsn)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn durability_regression_failed_write_does_not_advance_tail() {
        let path = temp_path();
        let mut wal = Wal::open(&path).unwrap();
        if let Backing::File { file, .. } = &mut wal.backing {
            *file = File::open(&path).unwrap();
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            wal.append_committed(vec![rec("failed", 1)])
        }));
        assert!(
            result.is_err(),
            "failed write must be reported rather than acknowledged"
        );
        assert_eq!(wal.committed_tail().get(), 0);
        assert!(Wal::open(&path)
            .unwrap()
            .replay_after(WalLsn::new(0))
            .is_empty());
        let _ = std::fs::remove_file(path);
    }
}
