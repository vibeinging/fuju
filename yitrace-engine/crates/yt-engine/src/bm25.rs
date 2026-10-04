//! bm25.rs —— **真的 BM25 中文倒排索引**（替掉 `InMemoryBm25` 的子串匹配占位），验证自研路线里
//! "原生中文检索" 这条差异化能不能立住。
//!
//! 三件事是真的（不是占位）：
//! 1. **分词可替换**：分词从索引里解耦成 [`Tokenizer`] 接缝。引擎默认注入纯 Rust
//!    [`crate::ChineseTokenizer`]；`CjkBigramTokenizer` 是零依赖兜底。外部分词库只需要实现
//!    `Tokenizer` 就能接入，**索引/评分这套自有逻辑一行不动**。
//! 2. **BM25 打分**：真倒排（token → 每文档词频）+ idf + 文档长度归一。按相关性排序，不是子串"有/无"。
//! 3. **bigram 召回正确**：相邻汉字两两成词（"疑似盗刷" → 疑似/似盗/盗刷），是 Elasticsearch CJK
//!    analyzer 同款；词级分词是精度升级，不是召回前置（bigram 已能正确召回+排序）。
//!
//! 为什么这比子串强（模块自带会失败的测试证明）：查 "盗刷风控" 这种**非连续多概念**中文串，子串占位
//! （`InMemoryBm25` 按空白切，整串当一个 token）要求文档里出现连续 "盗刷风控" 才命中 → 一条都召不回；
//! BM25 按 bigram 把它拆成 盗刷/刷风/风控，命中"盗刷"和"风控"两概念的文档排第一，按 tf-idf 给出相关性序。
#![allow(dead_code)]

use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};

use crate::bm25_disk::{self, CacheMetadata, DiskBlockMeta, DiskBm25Cache};
use crate::Bm25Index;

const K1: f32 = 1.5;
const B: f32 = 0.75;
const DISK_QUERY_BLOCK_BATCH: usize = 64;
const DEFAULT_QUERY_CACHE_BYTES: usize = 16 * 1024 * 1024;

type SearchHit = (u64, u64, f32);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct QueryCacheKey {
    query: String,
    k: usize,
}

struct QueryCacheEntry {
    hits: Arc<Vec<SearchHit>>,
    byte_len: usize,
    recently_used: bool,
}

struct QueryResultCache {
    entries: HashMap<QueryCacheKey, QueryCacheEntry>,
    clock: VecDeque<QueryCacheKey>,
    bytes: usize,
    budget: usize,
}

impl Default for QueryResultCache {
    fn default() -> Self {
        Self::with_budget(query_cache_budget())
    }
}

impl QueryResultCache {
    fn with_budget(budget: usize) -> Self {
        Self {
            entries: HashMap::new(),
            clock: VecDeque::new(),
            bytes: 0,
            budget,
        }
    }

    fn get(&mut self, key: &QueryCacheKey) -> Option<Vec<SearchHit>> {
        let entry = self.entries.get_mut(key)?;
        entry.recently_used = true;
        Some(entry.hits.as_ref().clone())
    }

    fn insert(&mut self, key: QueryCacheKey, hits: &[SearchHit]) {
        if self.entries.contains_key(&key) {
            return;
        }
        let hits = Arc::new(hits.to_vec());
        let key_bytes = std::mem::size_of::<QueryCacheKey>().saturating_add(key.query.capacity());
        let byte_len = key_bytes
            .saturating_mul(2)
            .saturating_add(
                hits.capacity()
                    .saturating_mul(std::mem::size_of::<SearchHit>()),
            )
            .saturating_add(std::mem::size_of::<QueryCacheEntry>());
        if byte_len > self.budget {
            return;
        }
        while self.bytes.saturating_add(byte_len) > self.budget {
            let Some(oldest) = self.clock.pop_front() else {
                break;
            };
            if let Some(entry) = self.entries.get_mut(&oldest) {
                if entry.recently_used {
                    entry.recently_used = false;
                    self.clock.push_back(oldest);
                    continue;
                }
            }
            if let Some(removed) = self.entries.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(removed.byte_len);
            }
        }
        self.bytes = self.bytes.saturating_add(byte_len);
        self.clock.push_back(key.clone());
        self.entries.insert(
            key,
            QueryCacheEntry {
                hits,
                byte_len,
                recently_used: false,
            },
        );
    }

    fn clear(&mut self) {
        if self.entries.is_empty() {
            return;
        }
        self.entries.clear();
        self.clock.clear();
        self.bytes = 0;
    }
}

fn query_cache_budget() -> usize {
    std::env::var("YT_BM25_QUERY_CACHE_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_QUERY_CACHE_BYTES)
}

/// f32 全序包装（NaN 也定序），WAND 的 top-k 阈值堆用。
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

/// Top-k 堆的根保存最差结果，避免高频词把所有命中文档先堆进内存再排序。
#[derive(Clone, Copy, PartialEq)]
struct RankedScore {
    doc: (u64, u64),
    score: OrdF32,
}
impl Eq for RankedScore {}
impl PartialOrd for RankedScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for RankedScore {
    fn cmp(&self, other: &Self) -> Ordering {
        // 分数低、或同分但 doc 较大的结果更差，因此在 BinaryHeap 根部优先淘汰。
        other
            .score
            .cmp(&self.score)
            .then_with(|| self.doc.cmp(&other.doc))
    }
}

fn push_topk(heap: &mut BinaryHeap<RankedScore>, k: usize, doc: (u64, u64), score: f32) {
    let item = RankedScore {
        doc,
        score: OrdF32(score),
    };
    if heap.len() < k {
        heap.push(item);
    } else if heap.peek().is_some_and(|worst| item < *worst) {
        heap.pop();
        heap.push(item);
    }
}

/// **分词接缝**：把一段文本切成检索词。索引与评分对分词只认这个 trait —— 换分词器
/// 只换实现、不动倒排逻辑。实现方负责大小写归一、标点处理等；返回的每个 token 原样进倒排。
pub trait Tokenizer: Send + Sync {
    fn tokenize(&self, text: &str) -> Vec<String>;
}

/// 兜底分词器：无词典 CJK bigram + ASCII/数字按串小写化。零依赖、std-only。
#[derive(Default)]
pub struct CjkBigramTokenizer;

impl Tokenizer for CjkBigramTokenizer {
    fn tokenize(&self, text: &str) -> Vec<String> {
        tokenize(text)
    }
}

/// CJK 统一表意文字主区（验证够用；扩展区/标点另算）。
fn is_cjk(c: char) -> bool {
    ('\u{4e00}'..='\u{9fff}').contains(&c)
}

/// 一串连续汉字 → 相邻 bigram；单字则保留单字。
fn push_cjk_bigrams(run: &[char], out: &mut Vec<String>) {
    match run.len() {
        0 => {}
        1 => out.push(run[0].to_string()),
        _ => {
            for w in run.windows(2) {
                out.push(w.iter().collect());
            }
        }
    }
}

/// 分词：连续汉字走 bigram，ASCII/数字按串成词并小写化，其余字符当分隔。
pub fn tokenize(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cjk: Vec<char> = Vec::new();
    let mut ascii = String::new();
    for c in text.chars() {
        if is_cjk(c) {
            if !ascii.is_empty() {
                out.push(std::mem::take(&mut ascii).to_lowercase());
            }
            cjk.push(c);
        } else if c.is_alphanumeric() {
            if !cjk.is_empty() {
                push_cjk_bigrams(&cjk, &mut out);
                cjk.clear();
            }
            ascii.push(c);
        } else {
            if !ascii.is_empty() {
                out.push(std::mem::take(&mut ascii).to_lowercase());
            }
            if !cjk.is_empty() {
                push_cjk_bigrams(&cjk, &mut out);
                cjk.clear();
            }
        }
    }
    if !ascii.is_empty() {
        out.push(ascii.to_lowercase());
    }
    push_cjk_bigrams(&cjk, &mut out);
    out
}

struct Bm25State {
    /// token → (文档 → 词频)。增量建图用（HashMap 插入快）。
    postings: HashMap<String, HashMap<(u64, u64), u32>>,
    /// 文档 → 词数（BM25 长度归一用）。
    doc_len: HashMap<(u64, u64), u32>,
    /// 所有文档词数之和（算 avgdl）。
    total_len: u64,
    /// 当前增量里已经建过索引的确定性 event_id。磁盘基线保留排序表，这里只存新写入。
    seen_events: HashSet<u64>,
    /// WAND 用：token → 分块的有序 postings（脏时从 `postings` 重建、查询期缓存，build-then-query 摊销）。
    sorted: HashMap<String, Postings>,
    dirty: bool,
    /// 已提交的倒排基线。打开时只保留文档长度和词目录，postings 按查询词读取。
    disk: Option<DiskBm25Cache>,
    /// 磁盘基线被 clear/load 替换时递增。锁外打分按它检查文档目录和 posting 块仍属同一代。
    disk_epoch: u64,
    /// 只缓存无过滤查询；索引一有变化就递增并清空，避免并发查询把旧结果写回缓存。
    query_epoch: u64,
    query_cache: QueryResultCache,
}

impl Default for Bm25State {
    fn default() -> Self {
        Self {
            postings: HashMap::new(),
            doc_len: HashMap::new(),
            total_len: 0,
            seen_events: HashSet::new(),
            sorted: HashMap::new(),
            dirty: false,
            disk: None,
            disk_epoch: 0,
            query_epoch: 0,
            query_cache: QueryResultCache::default(),
        }
    }
}

/// 每块 128 篇文档，存 max_tf/min_dl 算块上界（block-max-WAND：块上界 < 阈值 → 整块跳）。
const BLOCK_SIZE: usize = 128;

/// 一个 token 的分块有序 postings。
struct Postings {
    docs: Arc<Vec<((u64, u64), u32)>>, // 按 doc 升序
    blocks: Vec<BlockMeta>,
}

/// 一次查询命中的 postings。文档长度与 docs 对齐，避免逐文档二分查找磁盘 doc table。
struct QueryPostings {
    docs: Arc<Vec<((u64, u64), u32)>>,
    doc_lens: Vec<u32>,
    blocks: Vec<BlockMeta>,
}
struct BlockMeta {
    end: usize,  // 该块覆盖 docs[start..end]（end 为下个块起点）
    max_tf: u32, // 块内最大词频
    min_dl: u32, // 块内最短文档长度（norm 在 tf 大、dl 小时最大 → (max_tf,min_dl) 给块上界）
}

impl Bm25State {
    /// 把增量 postings 合并进有序主索引。主索引与增量只保留一份 doc/tf，避免持久缓存加载后
    /// 同时常驻 HashMap postings 和排序 Vec postings 两份百万级数据。
    fn ensure_sorted(&mut self) {
        if !self.dirty {
            return;
        }
        for (tok, delta_map) in std::mem::take(&mut self.postings) {
            let mut delta: Vec<((u64, u64), u32)> = delta_map.into_iter().collect();
            delta.sort_unstable_by_key(|&(doc, _)| doc);
            let base = self
                .sorted
                .remove(&tok)
                .map(|postings| {
                    Arc::try_unwrap(postings.docs).unwrap_or_else(|docs| (*docs).clone())
                })
                .unwrap_or_default();
            let docs = merge_sorted_postings(base, delta);
            let blocks = build_blocks(&docs, &self.doc_len)
                .expect("BM25 postings doc must have a document length");
            self.sorted.insert(
                tok,
                Postings {
                    docs: Arc::new(docs),
                    blocks,
                },
            );
        }
        self.dirty = false;
    }

    fn doc_count(&self) -> usize {
        let disk_count = self.disk.as_ref().map_or(0, DiskBm25Cache::doc_count);
        disk_count
            + self
                .doc_len
                .keys()
                .filter(|&&doc| {
                    self.disk
                        .as_ref()
                        .and_then(|disk| disk.doc_len(doc))
                        .is_none()
                })
                .count()
    }

    fn has_event(&self, event_id: u64) -> bool {
        self.seen_events.contains(&event_id)
            || self
                .disk
                .as_ref()
                .is_some_and(|disk| disk.contains_event(event_id))
    }

    fn mark_event(&mut self, event_id: u64) -> bool {
        if self.has_event(event_id) {
            return false;
        }
        self.seen_events.insert(event_id);
        true
    }

    fn doc_len(&self, doc: (u64, u64)) -> Option<u32> {
        let base = self
            .disk
            .as_ref()
            .and_then(|disk| disk.doc_len(doc))
            .unwrap_or(0);
        let delta = self.doc_len.get(&doc).copied().unwrap_or(0);
        let len = base.saturating_add(delta);
        (len > 0).then_some(len)
    }

    fn postings_for(&mut self, token: &str) -> Option<Arc<Vec<((u64, u64), u32)>>> {
        self.ensure_sorted();
        let delta = self
            .sorted
            .get(token)
            .map(|postings| Arc::clone(&postings.docs));
        let base = self
            .disk
            .as_mut()
            .and_then(|disk| disk.load_postings(token));
        match (base, delta) {
            (Some(base), Some(delta)) => Some(Arc::new(merge_sorted_postings(
                Arc::unwrap_or_clone(base),
                Arc::unwrap_or_clone(delta),
            ))),
            (Some(base), None) => Some(base),
            (None, Some(delta)) => Some(delta),
            (None, None) => None,
        }
    }

    fn doc_lens_for_sorted(&self, docs: &[((u64, u64), u32)]) -> Option<Vec<u32>> {
        let Some(disk) = self.disk.as_ref() else {
            return docs
                .iter()
                .map(|&(doc, _)| self.doc_len.get(&doc).copied())
                .collect();
        };
        let base = disk.docs();
        let mut base_index = 0usize;
        let mut out = Vec::with_capacity(docs.len());
        for &(doc, _) in docs {
            while base_index < base.len() && base[base_index].0 < doc {
                base_index += 1;
            }
            let base_len = if base_index < base.len() && base[base_index].0 == doc {
                base[base_index].1
            } else {
                0
            };
            let delta_len = self.doc_len.get(&doc).copied().unwrap_or(0);
            let len = base_len.saturating_add(delta_len);
            if len == 0 {
                return None;
            }
            out.push(len);
        }
        Some(out)
    }
}

fn merge_sorted_postings(
    base: Vec<((u64, u64), u32)>,
    delta: Vec<((u64, u64), u32)>,
) -> Vec<((u64, u64), u32)> {
    let mut out = Vec::with_capacity(base.len().saturating_add(delta.len()));
    let mut base = base.into_iter().peekable();
    let mut delta = delta.into_iter().peekable();
    loop {
        match (base.peek(), delta.peek()) {
            (Some(&(base_doc, _)), Some(&(delta_doc, _))) if base_doc < delta_doc => {
                out.push(base.next().unwrap());
            }
            (Some(&(base_doc, _)), Some(&(delta_doc, _))) if base_doc > delta_doc => {
                out.push(delta.next().unwrap());
            }
            (Some(_), Some(_)) => {
                let (doc, base_tf) = base.next().unwrap();
                let (_, delta_tf) = delta.next().unwrap();
                out.push((doc, base_tf.saturating_add(delta_tf)));
            }
            (Some(_), None) => {
                out.extend(base.by_ref());
                break;
            }
            (None, Some(_)) => {
                out.extend(delta.by_ref());
                break;
            }
            (None, None) => break,
        }
    }
    out
}

/// BM25 词频长度归一（tf·(k1+1) / (tf + k1·(1-b+b·dl/avgdl))）。上确界 = k1+1（tf→∞）。
pub(crate) fn bm25_norm(tf: f32, dl: f32, avgdl: f32) -> f32 {
    tf * (K1 + 1.0) / (tf + K1 * (1.0 - B + B * dl / avgdl))
}

/// 真 BM25 中文倒排索引。实现引擎的 `Bm25Index` trait，可直接替掉 `InMemoryBm25`。
/// 分词器可注入：`new()` 用兜底 bigram，`with_tokenizer` 可换任意词级分词器（同一套倒排/评分）。
pub struct Bm25TextIndex {
    state: Mutex<Bm25State>,
    tokenizer: Arc<dyn Tokenizer>,
    tenant_indexes: Mutex<std::collections::BTreeMap<u64, Arc<Bm25TextIndex>>>,
    query_inflight: Mutex<HashSet<QueryCacheKey>>,
    query_ready: Condvar,
}

struct QueryInflightGuard<'a> {
    key: QueryCacheKey,
    inflight: &'a Mutex<HashSet<QueryCacheKey>>,
    ready: &'a Condvar,
}

impl Drop for QueryInflightGuard<'_> {
    fn drop(&mut self) {
        let mut inflight = self.inflight.lock().unwrap_or_else(|err| err.into_inner());
        inflight.remove(&self.key);
        self.ready.notify_all();
    }
}

impl Default for Bm25TextIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl Bm25TextIndex {
    /// 默认 bigram 分词。
    pub fn new() -> Self {
        Self::with_tokenizer(Box::new(CjkBigramTokenizer))
    }

    /// 注入自定义分词器。倒排与 BM25 评分不变。
    pub fn with_tokenizer(tokenizer: Box<dyn Tokenizer>) -> Self {
        Self {
            state: Mutex::new(Bm25State::default()),
            tokenizer: Arc::from(tokenizer),
            tenant_indexes: Mutex::new(std::collections::BTreeMap::new()),
            query_inflight: Mutex::new(HashSet::new()),
            query_ready: Condvar::new(),
        }
    }
}

struct DiskQueryTerm {
    token: String,
    idf: f32,
    blocks: Vec<DiskBlockMeta>,
}

/// clean reopen 的磁盘 block-max 路径。锁只保护共享文件和 CLOCK 缓存，每个块的过滤、
/// 打分和 top-k 都在锁外完成，多个查询不会再因为一个全局锁完全串行。
fn disk_query_terms(
    state: &Bm25State,
    tokens: &[String],
    doc_count: usize,
) -> Option<(Vec<DiskQueryTerm>, Arc<Vec<((u64, u64), u32)>>, u64)> {
    let disk = state.disk.as_ref()?;
    let docs = disk.docs_arc();
    let mut terms = Vec::new();
    for token in tokens {
        let Some(df) = disk.token_doc_freq(token) else {
            continue;
        };
        let df = df as f32;
        let idf = (1.0 + (doc_count as f32 - df + 0.5) / (df + 0.5)).ln();
        terms.push(DiskQueryTerm {
            token: token.clone(),
            idf,
            blocks: disk.block_metadata(token)?,
        });
    }
    if terms.is_empty() {
        return Some((terms, docs, state.disk_epoch));
    }
    let layout = &terms[0].blocks;
    if terms.iter().skip(1).any(|term| {
        term.blocks.len() != layout.len()
            || term.blocks.iter().zip(layout).any(|(left, right)| {
                left.first_doc != right.first_doc || left.last_doc != right.last_doc
            })
    }) {
        return None;
    }

    Some((terms, docs, state.disk_epoch))
}

impl Bm25TextIndex {
    fn search_disk_block_max(
        &self,
        tokens: &[String],
        k: usize,
        doc_count: usize,
        avgdl: f32,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Option<Vec<(u64, u64, f32)>> {
        let (terms, doc_lengths, disk_epoch) = {
            let state = self.state.lock().unwrap();
            disk_query_terms(&state, tokens, doc_count)?
        };
        if terms.is_empty() {
            return Some(Vec::new());
        }
        let layout = terms[0].blocks.clone();

        // 文档顺序不代表得分顺序。先处理上界最高的块，通常用一个块就能建立接近
        // 最终值的 top-k 门槛；后续低上界块可以直接停止，不必按文件顺序扫到高分块。
        // 同上界时先处理 doc 较小的块，既符合稳定排序，也让相同得分的尾块可安全跳过。
        let mut block_order: Vec<(usize, f32)> = (0..layout.len())
            .map(|block| {
                let upper = terms
                    .iter()
                    .map(|term| term.idf * term.blocks[block].max_norm)
                    .sum();
                (block, upper)
            })
            .collect();
        block_order.sort_unstable_by(|&(left_block, left_upper), &(right_block, right_upper)| {
            right_upper.total_cmp(&left_upper).then(
                layout[left_block]
                    .first_doc
                    .cmp(&layout[right_block].first_doc),
            )
        });

        let mut topk = BinaryHeap::new();
        let mut batch_start = 0usize;
        while batch_start < block_order.len() {
            // 首块单独处理，先尽快建立 top-k 阈值；否则首批会在没有阈值时把大量
            // 本可跳过的块提前读进缓存。
            let batch_end = if batch_start == 0 {
                1
            } else {
                (batch_start + DISK_QUERY_BLOCK_BATCH).min(block_order.len())
            };
            let wanted = block_order[batch_start..batch_end]
                .iter()
                .copied()
                .take_while(|&(block, upper)| {
                    !upper_cannot_enter(&topk, k, upper, layout[block].first_doc)
                })
                .collect::<Vec<_>>();
            if wanted.is_empty() {
                break;
            }
            let loaded = {
                let mut state = self.state.lock().unwrap();
                if state.disk_epoch != disk_epoch {
                    return None;
                }
                let disk = state.disk.as_mut()?;
                let mut loaded = Vec::with_capacity(wanted.len());
                for (block, upper) in wanted {
                    let mut postings = Vec::with_capacity(terms.len());
                    for term in &terms {
                        postings.push(disk.load_block(&term.token, block)?);
                    }
                    loaded.push((block, upper, postings));
                }
                loaded
            };
            for (block, upper, postings_by_term) in loaded {
                if upper_cannot_enter(&topk, k, upper, layout[block].first_doc) {
                    break;
                }
                // 每个词的 posting 本身已有序，直接做多路归并。旧路径每 128 条建一个
                // HashMap 再排序，百万数据会重复几千次分配和哈希。
                let mut positions = vec![0usize; postings_by_term.len()];
                let first = postings_by_term
                    .iter()
                    .filter_map(|postings| postings.first().map(|posting| posting.0))
                    .min()?;
                let mut doc_index = doc_lengths
                    .binary_search_by_key(&first, |&(doc, _)| doc)
                    .ok()?;
                loop {
                    let next_doc = postings_by_term
                        .iter()
                        .zip(&positions)
                        .filter_map(|(postings, &position)| {
                            postings.get(position).map(|posting| posting.0)
                        })
                        .min();
                    let Some(doc) = next_doc else { break };
                    while doc_index < doc_lengths.len() && doc_lengths[doc_index].0 < doc {
                        doc_index += 1;
                    }
                    if doc_index >= doc_lengths.len() || doc_lengths[doc_index].0 != doc {
                        return None;
                    }
                    let mut score = 0.0f32;
                    for ((term, postings), position) in
                        terms.iter().zip(&postings_by_term).zip(&mut positions)
                    {
                        if postings
                            .get(*position)
                            .is_some_and(|posting| posting.0 == doc)
                        {
                            let tf = postings[*position].1;
                            score += term.idf
                                * bm25_norm(tf as f32, doc_lengths[doc_index].1 as f32, avgdl);
                            *position += 1;
                        }
                    }
                    if filter(doc.0, doc.1) {
                        push_topk(&mut topk, k, doc, score);
                    }
                }
            }
            batch_start = batch_end;
        }

        let mut scored: Vec<_> = topk
            .into_iter()
            .map(|item| (item.doc.0, item.doc.1, item.score.0))
            .collect();
        scored.sort_by(|a, b| b.2.total_cmp(&a.2).then((a.0, a.1).cmp(&(b.0, b.1))));
        Some(scored)
    }
}

fn upper_cannot_enter(
    heap: &BinaryHeap<RankedScore>,
    k: usize,
    upper: f32,
    first_doc: (u64, u64),
) -> bool {
    if heap.len() < k {
        return false;
    }
    let worst = heap.peek().unwrap();
    match upper.total_cmp(&worst.score.0) {
        Ordering::Less => true,
        Ordering::Equal => first_doc > worst.doc,
        Ordering::Greater => false,
    }
}

fn index_tokens(st: &mut Bm25State, trace_id: u64, span_id: u64, toks: Vec<String>) {
    st.query_epoch = st.query_epoch.wrapping_add(1);
    st.query_cache.clear();
    let doc = (trace_id, span_id);
    st.total_len += toks.len() as u64;
    *st.doc_len.entry(doc).or_insert(0) += toks.len() as u32;
    for token in toks {
        *st.postings
            .entry(token)
            .or_default()
            .entry(doc)
            .or_insert(0) += 1;
    }
    st.dirty = true;
}

impl Bm25Index for Bm25TextIndex {
    fn supports_tenant_scope(&self) -> bool {
        true
    }
    fn empty_like(&self) -> Option<Arc<dyn Bm25Index>> {
        Some(Arc::new(self.new_partition()))
    }
    fn index_text_scoped(&self, tenant: Option<u64>, trace: u64, span: u64, text: &str) {
        match tenant {
            Some(id) => self.partition(id).index_text(trace, span, text),
            None => self.index_text(trace, span, text),
        }
    }
    fn index_event_scoped(&self, tenant: Option<u64>, eid: u64, t: u64, sp: u64, text: &str) {
        match tenant {
            Some(tenant) => self.partition(tenant).index_event(eid, t, sp, text),
            None => self.index_event(eid, t, sp, text),
        }
    }
    fn mark_event_scoped(&self, tenant: Option<u64>, eid: u64) {
        match tenant {
            Some(tenant) => self.partition(tenant).mark_event(eid),
            None => self.mark_event(eid),
        }
    }
    fn search_all_scoped(&self, query: &str, k: usize) -> Vec<(Option<u64>, u64, u64, f32)> {
        let mut hits = self
            .search_partition(query, k)
            .into_iter()
            .map(|(t, s, score)| (None, t, s, score))
            .collect::<Vec<_>>();
        let children = self
            .tenant_indexes
            .lock()
            .unwrap()
            .iter()
            .map(|(&tenant, child)| (tenant, child.clone()))
            .collect::<Vec<_>>();
        for (tenant, child) in children {
            hits.extend(
                child
                    .search_partition(query, k)
                    .into_iter()
                    .map(|(t, s, score)| (Some(tenant), t, s, score)),
            );
        }
        hits.sort_by(|a, b| {
            b.3.total_cmp(&a.3)
                .then((a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)))
        });
        hits.truncate(k);
        hits
    }
    fn search_scoped(
        &self,
        query: &str,
        k: usize,
        filter: &dyn Fn(Option<u64>, u64, u64) -> bool,
    ) -> Vec<(Option<u64>, u64, u64, f32)> {
        let children = self
            .tenant_indexes
            .lock()
            .unwrap()
            .iter()
            .map(|(&id, index)| (id, index.clone()))
            .collect::<Vec<_>>();
        let mut hits = self
            .search_filtered_partition(query, k, &|t, s| filter(None, t, s))
            .into_iter()
            .map(|(t, s, score)| (None, t, s, score))
            .collect::<Vec<_>>();
        for (tenant, index) in children {
            hits.extend(
                index
                    .search_filtered_partition(query, k, &|t, s| filter(Some(tenant), t, s))
                    .into_iter()
                    .map(|(t, s, score)| (Some(tenant), t, s, score)),
            );
        }
        hits.sort_by(|a, b| {
            b.3.total_cmp(&a.3)
                .then((a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)))
        });
        hits.truncate(k);
        hits
    }

    fn index_text(&self, trace_id: u64, span_id: u64, text: &str) {
        let toks = self.tokenizer.tokenize(text);
        if toks.is_empty() {
            return;
        }
        let mut st = self.state.lock().unwrap();
        index_tokens(&mut st, trace_id, span_id, toks);
    }

    fn index_event(&self, event_id: u64, trace_id: u64, span_id: u64, text: &str) {
        let toks = self.tokenizer.tokenize(text);
        let mut st = self.state.lock().unwrap();
        if !st.mark_event(event_id) || toks.is_empty() {
            return;
        }
        index_tokens(&mut st, trace_id, span_id, toks);
    }

    fn mark_event(&self, event_id: u64) {
        self.state.lock().unwrap().mark_event(event_id);
    }

    /// **block-max-WAND**（DAAT + 上界剪枝 + 块跳过）。上界低于当前第 k 高分时直接剪枝；
    /// 上界同分时结合稳定排序的 doc id，只跳过不可能替换当前结果的后续块。
    /// 候选全量打分后**终排（分降序、(trace,span) 升序）取 top-k**，与暴力逐位一致（有测试钉死）。
    /// 单词查询走块跳过（块上界 = idf·norm(max_tf,min_dl) < θ → 整块跳）；多词查询走 term 级 WAND（剪掉只命中弱词的文档）。
    fn search(&self, query: &str, k: usize) -> Vec<(u64, u64, f32)> {
        let mut hits = self.search_partition(query, k);
        for child in self.partition_handles() {
            hits.extend(child.search_partition(query, k));
        }
        Self::rank_legacy(hits, k)
    }

    fn search_filtered(
        &self,
        query: &str,
        k: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(u64, u64, f32)> {
        let mut hits = self.search_filtered_partition(query, k, filter);
        for child in self.partition_handles() {
            hits.extend(child.search_filtered_partition(query, k, filter));
        }
        Self::rank_legacy(hits, k)
    }

    fn clear(&self) {
        self.tenant_indexes.lock().unwrap().clear();
        let mut state = self.state.lock().unwrap();
        let disk_epoch = state.disk_epoch.wrapping_add(1);
        let query_epoch = state.query_epoch.wrapping_add(1);
        *state = Bm25State {
            disk_epoch,
            query_epoch,
            ..Bm25State::default()
        };
    }

    fn load_cache(&self, path: &Path, version: u64, watermark: u64) -> bool {
        let Ok(meta) = std::fs::metadata(path) else {
            return false;
        };
        if meta.len() < 36 || meta.len() > 8_000_036 {
            return false;
        }
        let Ok(bytes) = std::fs::read(path) else {
            return false;
        };
        if bytes.len() < 36 || &bytes[..8] != b"YTBMTS02" {
            return false;
        }
        let payload_len = bytes.len() - 4;
        let expected = u32::from_le_bytes(bytes[payload_len..].try_into().unwrap());
        if yt_wal::crc32(&bytes[..payload_len]) != expected {
            return false;
        }
        let u64_at = |n: usize| u64::from_le_bytes(bytes[n..n + 8].try_into().unwrap());
        let count = u64_at(24) as usize;
        if u64_at(8) != version
            || u64_at(16) != watermark
            || count > 1_000_000
            || bytes.len() != 36 + count * 8
        {
            return false;
        }
        if !self.load_plain_cache(&path.with_extension("none.dat"), version, watermark) {
            return false;
        }
        let mut loaded = std::collections::BTreeMap::new();
        for i in 0..count {
            let tenant = u64_at(32 + i * 8);
            if loaded.contains_key(&tenant) {
                return false;
            }
            let child = self.new_partition();
            if !child.load_plain_cache(&Self::tenant_cache_path(path, tenant), version, watermark) {
                return false;
            }
            loaded.insert(tenant, Arc::new(child));
        }
        *self.tenant_indexes.lock().unwrap() = loaded;
        self.share_partition_budgets();
        true
    }
    fn save_cache(&self, path: &Path, version: u64, watermark: u64) -> std::io::Result<bool> {
        self.save_plain_cache(&path.with_extension("none.dat"), version, watermark)?;
        let children = self.tenant_indexes.lock().unwrap();
        let mut bytes = b"YTBMTS02".to_vec();
        bytes.extend_from_slice(&version.to_le_bytes());
        bytes.extend_from_slice(&watermark.to_le_bytes());
        bytes.extend_from_slice(&(children.len() as u64).to_le_bytes());
        for (&tenant, child) in children.iter() {
            child.save_plain_cache(&Self::tenant_cache_path(path, tenant), version, watermark)?;
            bytes.extend_from_slice(&tenant.to_le_bytes());
        }
        let checksum = yt_wal::crc32(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        let temporary = path.with_extension("tenants.tmp");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&temporary)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        std::fs::rename(temporary, path)?;
        Ok(true)
    }
}

impl Bm25TextIndex {
    /// 旧裸查询聚合所有租户，但不合并相同pair的两条文档；带tenant接口保留完整身份。
    fn partition_handles(&self) -> Vec<Arc<Self>> {
        self.tenant_indexes
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }
    fn rank_legacy(mut hits: Vec<(u64, u64, f32)>, k: usize) -> Vec<(u64, u64, f32)> {
        hits.sort_by(|a, b| b.2.total_cmp(&a.2).then((a.0, a.1).cmp(&(b.0, b.1))));
        hits.truncate(k);
        hits
    }
    fn search_partition(&self, query: &str, k: usize) -> Vec<(u64, u64, f32)> {
        if k == 0 {
            return Vec::new();
        }
        let key = QueryCacheKey {
            query: query.to_owned(),
            k,
        };
        let query_epoch = loop {
            let mut inflight = self.query_inflight.lock().unwrap();
            if inflight.contains(&key) {
                drop(self.query_ready.wait(inflight).unwrap());
                continue;
            }

            // 在登记计算者前再查一次，补上“首次查缓存”和“拿到 inflight 锁”之间
            // 另一个线程已经算完的窗口。
            let mut state = self.state.lock().unwrap();
            if let Some(hits) = state.query_cache.get(&key) {
                return hits;
            }
            let query_epoch = state.query_epoch;
            inflight.insert(key.clone());
            break query_epoch;
        };
        let _inflight_guard = QueryInflightGuard {
            key: key.clone(),
            inflight: &self.query_inflight,
            ready: &self.query_ready,
        };

        let hits = self.search_filtered_partition(query, k, &|_, _| true);
        {
            let mut state = self.state.lock().unwrap();
            if state.query_epoch == query_epoch {
                state.query_cache.insert(key, &hits);
            }
        }
        hits
    }
    fn search_filtered_partition(
        &self,
        query: &str,
        k: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(u64, u64, f32)> {
        let (n, avgdl, clean_disk) = {
            let st = self.state.lock().unwrap();
            let n = st.doc_count();
            let avgdl = if n == 0 {
                0.0
            } else {
                st.total_len as f32 / n as f32
            };
            let clean_disk = st.doc_len.is_empty()
                && st.sorted.is_empty()
                && st.postings.is_empty()
                && st.disk.is_some();
            (n, avgdl, clean_disk)
        };
        if n == 0 || k == 0 {
            return Vec::new();
        }

        // 查询词去重 + 排序（确定性求和顺序：按 token 序加各词贡献，与暴力一致）。
        let mut toks: Vec<String> = self.tokenizer.tokenize(query);
        toks.sort_unstable();
        toks.dedup();

        // clean reopen 没有 WAL 增量时，直接用磁盘块目录做 block-max。只有块可能进入
        // top-k 才读取 postings；块布局不一致的多词查询回到下面通用 WAND 路径。
        if clean_disk {
            if let Some(scored) = self.search_disk_block_max(&toks, k, n, avgdl, filter) {
                return scored;
            }
        }

        let mut st = self.state.lock().unwrap();

        // 只读取查询命中的词。持久基线 postings 从磁盘按偏移分页，WAL tail 从内存增量合并。
        let mut hits: Vec<(f32, QueryPostings)> = Vec::new();
        for tok in &toks {
            if let Some(docs) = st.postings_for(tok) {
                let df = docs.len() as f32;
                let idf = (1.0 + (n as f32 - df + 0.5) / (df + 0.5)).ln();
                let Some(doc_lens) = st.doc_lens_for_sorted(&docs) else {
                    return Vec::new();
                };
                let blocks = build_blocks_from_lens(&docs, &doc_lens);
                hits.push((
                    idf,
                    QueryPostings {
                        docs,
                        doc_lens,
                        blocks,
                    },
                ));
            }
        }
        if hits.is_empty() {
            return Vec::new();
        }

        let mut topk: BinaryHeap<RankedScore> = BinaryHeap::new();
        let theta = |h: &BinaryHeap<RankedScore>| {
            if h.len() >= k {
                h.peek().unwrap().score.0
            } else {
                f32::NEG_INFINITY
            }
        };

        if hits.len() == 1 {
            // 单词：block-max 块跳过。块上界 < θ → 整块不打分。
            let (idf, pp) = &hits[0];
            let mut i = 0usize;
            for blk in &pp.blocks {
                let bmax = *idf * bm25_norm(blk.max_tf as f32, blk.min_dl as f32, avgdl);
                if bmax < theta(&topk) {
                    i = blk.end;
                    continue; // 整块跳
                }
                for (offset, &(doc, tf)) in pp.docs[i..blk.end].iter().enumerate() {
                    if !filter(doc.0, doc.1) {
                        continue;
                    }
                    let dl = pp.doc_lens[i + offset] as f32;
                    let sc = *idf * bm25_norm(tf as f32, dl, avgdl);
                    push_topk(&mut topk, k, doc, sc);
                }
                i = blk.end;
            }
        } else {
            // 多词：term 级 WAND（DAAT，上界 = idf·(k1+1)，按 doc 序选 pivot、剪枝）。
            struct Cur<'a> {
                docs: &'a [((u64, u64), u32)],
                doc_lens: &'a [u32],
                idf: f32,
                maxi: f32,
                pos: usize,
            }
            let mut curs: Vec<Cur> = hits
                .iter()
                .map(|(idf, pp)| Cur {
                    docs: &pp.docs,
                    doc_lens: &pp.doc_lens,
                    idf: *idf,
                    maxi: *idf * (K1 + 1.0),
                    pos: 0,
                })
                .collect();
            loop {
                curs.retain(|c| c.pos < c.docs.len());
                if curs.is_empty() {
                    break;
                }
                let mut order: Vec<usize> = (0..curs.len()).collect();
                order.sort_by_key(|&i| curs[i].docs[curs[i].pos].0);
                let th = theta(&topk);
                let mut acc = 0.0f32;
                let mut pivot: Option<usize> = None;
                for (oi, &ci) in order.iter().enumerate() {
                    acc += curs[ci].maxi;
                    if acc >= th {
                        pivot = Some(oi);
                        break;
                    }
                }
                let Some(poi) = pivot else { break };
                let pivot_doc = curs[order[poi]].docs[curs[order[poi]].pos].0;
                let first_doc = curs[order[0]].docs[curs[order[0]].pos].0;
                if first_doc == pivot_doc {
                    if filter(pivot_doc.0, pivot_doc.1) {
                        let mut sc = 0.0f32;
                        let dl = curs
                            .iter()
                            .find(|c| c.pos < c.docs.len() && c.docs[c.pos].0 == pivot_doc)
                            .map(|c| c.doc_lens[c.pos] as f32)
                            .unwrap();
                        for c in curs.iter() {
                            if c.pos < c.docs.len() && c.docs[c.pos].0 == pivot_doc {
                                sc += c.idf * bm25_norm(c.docs[c.pos].1 as f32, dl, avgdl);
                            }
                        }
                        push_topk(&mut topk, k, pivot_doc, sc);
                    }
                    for c in curs.iter_mut() {
                        if c.pos < c.docs.len() && c.docs[c.pos].0 == pivot_doc {
                            c.pos += 1;
                        }
                    }
                } else {
                    for &ci in order.iter().take(poi + 1) {
                        if curs[ci].docs[curs[ci].pos].0 < pivot_doc {
                            let c = &mut curs[ci];
                            while c.pos < c.docs.len() && c.docs[c.pos].0 < pivot_doc {
                                c.pos += 1;
                            }
                            break;
                        }
                    }
                }
            }
        }

        // 候选打分过程中只保留 top-k，最后做稳定终排。
        let mut scored: Vec<(u64, u64, f32)> = topk
            .into_iter()
            .map(|item| (item.doc.0, item.doc.1, item.score.0))
            .collect();
        scored.sort_by(|a, b| {
            b.2.partial_cmp(&a.2)
                .unwrap()
                .then((a.0, a.1).cmp(&(b.0, b.1)))
        });
        scored.truncate(k);
        scored
    }
    fn search_exact_partition_for_eval(
        &self,
        query: &str,
        k: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(u64, u64, f32)> {
        let mut st = self.state.lock().unwrap();
        let n = st.doc_count();
        if n == 0 || k == 0 {
            return Vec::new();
        }
        let avgdl = st.total_len as f32 / n as f32;
        let mut toks: Vec<String> = self.tokenizer.tokenize(query);
        toks.sort_unstable();
        toks.dedup();
        let mut hits = Vec::new();
        for tok in &toks {
            if let Some(docs) = st.postings_for(tok) {
                let df = docs.len() as f32;
                let idf = (1.0 + (n as f32 - df + 0.5) / (df + 0.5)).ln();
                let doc_lens = st.doc_lens_for_sorted(&docs).unwrap();
                hits.push((idf, docs, doc_lens));
            }
        }
        if hits.is_empty() {
            return Vec::new();
        }

        // posting 按 doc 有序。这里逐个 doc 完整求和，只用 top-k 堆限制结果内存；与
        // 在线路径不同，它不会根据任何上界提前停止或跳块。
        let mut positions = vec![0usize; hits.len()];
        let mut topk = BinaryHeap::new();
        loop {
            let next_doc = hits
                .iter()
                .zip(&positions)
                .filter_map(|((_, docs, _), &position)| docs.get(position).map(|posting| posting.0))
                .min();
            let Some(doc) = next_doc else { break };
            let mut score = 0.0f32;
            for ((idf, docs, doc_lens), position) in hits.iter().zip(&mut positions) {
                if docs.get(*position).is_some_and(|posting| posting.0 == doc) {
                    let tf = docs[*position].1;
                    let dl = doc_lens[*position];
                    score += *idf * bm25_norm(tf as f32, dl as f32, avgdl);
                    *position += 1;
                }
            }
            if filter(doc.0, doc.1) {
                push_topk(&mut topk, k, doc, score);
            }
        }

        let mut scored = topk
            .into_iter()
            .map(|item| (item.doc.0, item.doc.1, item.score.0))
            .collect::<Vec<_>>();
        scored.sort_by(|a, b| b.2.total_cmp(&a.2).then((a.0, a.1).cmp(&(b.0, b.1))));
        scored
    }
    fn new_partition(&self) -> Self {
        Self {
            state: Mutex::new(Bm25State::default()),
            tokenizer: self.tokenizer.clone(),
            tenant_indexes: Mutex::new(std::collections::BTreeMap::new()),
            query_inflight: Mutex::new(HashSet::new()),
            query_ready: Condvar::new(),
        }
    }
    fn partition(&self, tenant: u64) -> Arc<Self> {
        let (partition, created) = {
            let mut children = self.tenant_indexes.lock().unwrap();
            if let Some(index) = children.get(&tenant) {
                (index.clone(), false)
            } else {
                let index = Arc::new(self.new_partition());
                children.insert(tenant, index.clone());
                (index, true)
            }
        };
        if created {
            self.share_partition_budgets();
        }
        partition
    }

    /// 租户各自计算词频，但所有分区共享原有缓存预算，不能随租户数放大内存上限。
    fn share_partition_budgets(&self) {
        let children = self.tenant_indexes.lock().unwrap();
        let count = children.len() + 1;
        let postings_budget = std::env::var("YT_BM25_POSTINGS_CACHE_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(64 * 1024 * 1024)
            / count;
        let query_budget = std::env::var("YT_BM25_QUERY_CACHE_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(16 * 1024 * 1024)
            / count;
        for index in std::iter::once(self).chain(children.values().map(Arc::as_ref)) {
            let mut state = index.state.lock().unwrap();
            state.query_cache.budget = query_budget;
            if state.query_cache.bytes > query_budget {
                state.query_cache.clear();
            }
            if let Some(disk) = state.disk.as_mut() {
                disk.set_cache_budget(postings_budget);
            }
        }
    }
    fn tenant_cache_path(path: &Path, tenant: u64) -> std::path::PathBuf {
        path.with_extension(format!("tenant-{tenant}.dat"))
    }
    fn load_plain_cache(
        &self,
        path: &Path,
        manifest_version: u64,
        memtable_watermark: u64,
    ) -> bool {
        let Some(disk) = DiskBm25Cache::open(path, manifest_version, memtable_watermark) else {
            return false;
        };
        let total_len = disk.total_len();
        let mut state = self.state.lock().unwrap();
        let disk_epoch = state.disk_epoch.wrapping_add(1);
        let query_epoch = state.query_epoch.wrapping_add(1);
        *state = Bm25State {
            total_len,
            disk: Some(disk),
            disk_epoch,
            query_epoch,
            ..Bm25State::default()
        };
        true
    }

    fn save_plain_cache(
        &self,
        path: &Path,
        manifest_version: u64,
        memtable_watermark: u64,
    ) -> std::io::Result<bool> {
        self.state
            .lock()
            .unwrap()
            .save_cache(path, manifest_version, memtable_watermark)?;
        Ok(true)
    }
}

impl Bm25TextIndex {
    /// Eval 用完整评分：扫描查询词的全部 posting，不走 WAND、block 跳过、结果缓存或
    /// singleflight。它是优化查询的慢速正确性基准，不应放进在线请求路径。
    pub fn search_exact_for_eval(&self, query: &str, k: usize) -> Vec<(u64, u64, f32)> {
        self.search_exact_filtered_for_eval(query, k, &|_, _| true)
    }

    /// 带过滤的 Eval 完整评分。内存只保留每个查询词的 posting 和 top-k，不为全部命中
    /// 文档建立分数 HashMap，因此百万档高频词也可以运行。
    pub fn search_exact_filtered_for_eval(
        &self,
        query: &str,
        k: usize,
        filter: &dyn Fn(u64, u64) -> bool,
    ) -> Vec<(u64, u64, f32)> {
        let mut hits = self.search_exact_partition_for_eval(query, k, filter);
        for child in self.partition_handles() {
            hits.extend(child.search_exact_partition_for_eval(query, k, filter));
        }
        Self::rank_legacy(hits, k)
    }
}

impl Bm25State {
    fn save_cache(
        &mut self,
        path: &Path,
        manifest_version: u64,
        memtable_watermark: u64,
    ) -> std::io::Result<()> {
        self.ensure_sorted();
        let mut docs = self
            .disk
            .as_ref()
            .map(|disk| disk.docs().to_vec())
            .unwrap_or_default();
        let mut delta_docs: Vec<_> = self.doc_len.iter().map(|(&doc, &len)| (doc, len)).collect();
        delta_docs.sort_unstable_by_key(|&(doc, _)| doc);
        docs = merge_sorted_doc_lengths(docs, delta_docs);

        let mut token_set = BTreeSet::new();
        if let Some(disk) = self.disk.as_ref() {
            token_set.extend(disk.token_names().cloned());
        }
        token_set.extend(self.sorted.keys().cloned());
        let tokens: Vec<String> = token_set.into_iter().collect();
        let mut event_ids = self
            .disk
            .as_ref()
            .map(|disk| disk.event_ids().to_vec())
            .unwrap_or_default();
        event_ids.extend(self.seen_events.iter().copied());
        event_ids.sort_unstable();
        event_ids.dedup();
        bm25_disk::write_atomic(
            path,
            CacheMetadata {
                manifest_version,
                memtable_watermark,
                total_len: self.total_len,
            },
            &docs,
            &event_ids,
            &tokens,
            |token| {
                let delta = self
                    .sorted
                    .get(token)
                    .map(|postings| Arc::clone(&postings.docs));
                let base = self
                    .disk
                    .as_mut()
                    .and_then(|disk| disk.load_postings(token));
                Ok(match (base, delta) {
                    (Some(base), Some(delta)) => merge_sorted_postings(
                        Arc::unwrap_or_clone(base),
                        Arc::unwrap_or_clone(delta),
                    ),
                    (Some(base), None) => Arc::unwrap_or_clone(base),
                    (None, Some(delta)) => Arc::unwrap_or_clone(delta),
                    (None, None) => Vec::new(),
                })
            },
        )
    }
}

fn merge_sorted_doc_lengths(
    base: Vec<((u64, u64), u32)>,
    delta: Vec<((u64, u64), u32)>,
) -> Vec<((u64, u64), u32)> {
    let mut out = Vec::with_capacity(base.len().saturating_add(delta.len()));
    let mut base = base.into_iter().peekable();
    let mut delta = delta.into_iter().peekable();
    loop {
        match (base.peek(), delta.peek()) {
            (Some(&(base_doc, _)), Some(&(delta_doc, _))) if base_doc < delta_doc => {
                out.push(base.next().unwrap());
            }
            (Some(&(base_doc, _)), Some(&(delta_doc, _))) if base_doc > delta_doc => {
                out.push(delta.next().unwrap());
            }
            (Some(_), Some(_)) => {
                let (doc, base_len) = base.next().unwrap();
                let (_, delta_len) = delta.next().unwrap();
                out.push((doc, base_len.saturating_add(delta_len)));
            }
            (Some(_), None) => {
                out.extend(base.by_ref());
                break;
            }
            (None, Some(_)) => {
                out.extend(delta.by_ref());
                break;
            }
            (None, None) => break,
        }
    }
    out
}

fn build_blocks(
    docs: &[((u64, u64), u32)],
    doc_len: &HashMap<(u64, u64), u32>,
) -> Option<Vec<BlockMeta>> {
    build_blocks_with(docs, |doc| doc_len.get(&doc).copied())
}

fn build_blocks_with(
    docs: &[((u64, u64), u32)],
    mut doc_len: impl FnMut((u64, u64)) -> Option<u32>,
) -> Option<Vec<BlockMeta>> {
    let mut blocks = Vec::new();
    let mut i = 0;
    while i < docs.len() {
        let end = (i + BLOCK_SIZE).min(docs.len());
        let mut max_tf = 0u32;
        let mut min_dl = u32::MAX;
        for &(doc, tf) in &docs[i..end] {
            max_tf = max_tf.max(tf);
            min_dl = min_dl.min(doc_len(doc)?);
        }
        blocks.push(BlockMeta {
            end,
            max_tf,
            min_dl,
        });
        i = end;
    }
    Some(blocks)
}

fn build_blocks_from_lens(docs: &[((u64, u64), u32)], doc_lens: &[u32]) -> Vec<BlockMeta> {
    debug_assert_eq!(docs.len(), doc_lens.len());
    let mut blocks = Vec::new();
    let mut start = 0;
    while start < docs.len() {
        let end = (start + BLOCK_SIZE).min(docs.len());
        let max_tf = docs[start..end]
            .iter()
            .map(|&(_, tf)| tf)
            .max()
            .unwrap_or(0);
        let min_dl = doc_lens[start..end].iter().copied().min().unwrap_or(0);
        blocks.push(BlockMeta {
            end,
            max_tf,
            min_dl,
        });
        start = end;
    }
    blocks
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryBm25;

    #[test]
    fn cjk_bigram_tokenizer() {
        assert_eq!(tokenize("疑似盗刷"), vec!["疑似", "似盗", "盗刷"]);
        assert_eq!(tokenize("好"), vec!["好"]); // 单字保留
                                                // 中英混排 + 标点切分
        assert_eq!(tokenize("风控 GPT4"), vec!["风控", "gpt4"]);
        assert_eq!(tokenize("盗刷,转账"), vec!["盗刷", "转账"]);
    }

    #[test]
    fn bm25_ranks_by_relevance_where_substring_returns_nothing() {
        // 验证核心:非连续多概念中文查询,真 BM25 能召回并排序,子串占位一条都召不回。
        let bm = Bm25TextIndex::new();
        bm.index_text(1, 1, "风控系统实时拦截了一笔疑似盗刷的交易"); // 含 盗刷 + 风控 两概念
        bm.index_text(2, 2, "用户正常登录并完成转账"); // 都不含
        bm.index_text(3, 3, "这是一笔疑似盗刷"); // 只含 盗刷
        bm.index_text(4, 4, "风控规则已更新"); // 只含 风控

        // 查 "盗刷风控"(非连续):bigram = 盗刷/刷风/风控。
        let hits = bm.search("盗刷风控", 10);
        assert_eq!((hits[0].0, hits[0].1), (1, 1), "两概念都命中的文档排第一");
        // (2,2) 都不含 → 不出现。
        assert!(!hits.iter().any(|&(t, _, _)| t == 2), "无关文档不召回");
        // 只含单概念的 (3,3)/(4,4) 排在后面、分更低。
        assert!(hits[0].2 > hits[1].2, "多概念命中分更高");

        // 对照:子串占位查同一串 —— 没有文档含连续"盗刷风控" → 召回为空。
        let sub = InMemoryBm25::default();
        sub.index_text(1, 1, "风控系统实时拦截了一笔疑似盗刷的交易");
        sub.index_text(3, 3, "这是一笔疑似盗刷");
        sub.index_text(4, 4, "风控规则已更新");
        assert!(
            sub.search("盗刷风控", 10).is_empty(),
            "子串匹配召不回非连续多概念查询"
        );
    }

    #[test]
    fn bm25_term_frequency_and_length_norm() {
        // 同一查询词,词频高的文档排前(且长度归一:短文档同 tf 占便宜)。
        let bm = Bm25TextIndex::new();
        bm.index_text(1, 1, "盗刷盗刷盗刷"); // 盗刷 出现多次,文档短
        bm.index_text(2, 2, "盗刷 以及一大段无关的正常交易日志内容填充长度"); // 一次,文档长
        let hits = bm.search("盗刷", 10);
        assert_eq!((hits[0].0, hits[0].1), (1, 1), "高词频+短文档排第一");
        assert_eq!(hits.len(), 2);
        assert!(hits[0].2 > hits[1].2);
    }

    #[test]
    fn wand_matches_exhaustive_on_random_corpus() {
        // WAND 必须与暴力全量打分**逐位一致**（剪枝只跳掉绝不进 top-k 的文档）。
        // 随机语料 + 多词查询，扫多个 k 对比。确定性 LCG，不依赖 rand。
        let words = [
            "盗刷", "风控", "交易", "转账", "登录", "异常", "拦截", "模型", "会话", "超时",
        ];
        let mut seed = 0x1234_5678u64;
        let mut next = || {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (seed >> 33) as usize
        };
        let bm = Bm25TextIndex::new();
        // 800 文档，每篇 3~7 个随机词（空格分隔，bigram 分词会再切，但稳定可复算）。
        for i in 0..800u64 {
            let len = 3 + next() % 5;
            let text: Vec<&str> = (0..len).map(|_| words[next() % words.len()]).collect();
            bm.index_text(i / 10, i, &text.join(" "));
        }
        // 多组随机多词查询 × 多个 k。
        for _ in 0..60 {
            let qlen = 1 + next() % 3;
            let q: Vec<&str> = (0..qlen).map(|_| words[next() % words.len()]).collect();
            let query = q.join(" ");
            for &k in &[1usize, 5, 10, 50] {
                let wand = bm.search(&query, k);
                let exhaustive = bm.search_exact_for_eval(&query, k);
                assert_eq!(wand, exhaustive, "WAND≠暴力: query={query:?} k={k}");
                let filter = |trace_id: u64, span_id: u64| trace_id % 3 == 0 && span_id % 5 != 0;
                assert_eq!(
                    bm.search_filtered(&query, k, &filter),
                    bm.search_exact_filtered_for_eval(&query, k, &filter),
                    "过滤 WAND≠暴力: query={query:?} k={k}"
                );
            }
        }
    }

    #[test]
    fn empty_index_returns_nothing() {
        let bm = Bm25TextIndex::new();
        assert!(bm.search("盗刷", 5).is_empty());
    }

    #[test]
    fn unfiltered_query_cache_is_bounded_and_invalidated_on_write() {
        let bm = Bm25TextIndex::new();
        for doc in 1..=512 {
            bm.index_text(doc, 1, "common phrase filler");
        }
        let before = bm.search("common phrase", 10);
        assert_eq!(bm.search("common phrase", 10), before);
        {
            let state = bm.state.lock().unwrap();
            assert_eq!(state.query_cache.entries.len(), 1);
            assert!(state
                .query_cache
                .entries
                .values()
                .all(|entry| entry.recently_used));
        }

        bm.index_text(999, 1, "common common common common phrase");
        assert!(bm.state.lock().unwrap().query_cache.entries.is_empty());
        let after = bm.search("common phrase", 10);
        assert_ne!(after, before);
        assert_eq!((after[0].0, after[0].1), (999, 1));

        let mut cache = QueryResultCache::with_budget(usize::MAX);
        cache.insert(
            QueryCacheKey {
                query: "first".to_owned(),
                k: 1,
            },
            &[(1, 1, 1.0)],
        );
        cache.budget = cache.bytes;
        cache.insert(
            QueryCacheKey {
                query: "other".to_owned(),
                k: 1,
            },
            &[(2, 2, 2.0)],
        );
        assert_eq!(cache.entries.len(), 1);
        assert!(cache.entries.keys().any(|key| key.query == "other"));
        assert!(cache.bytes <= cache.budget);
    }

    #[test]
    fn concurrent_identical_queries_share_one_computation() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
        use std::sync::Barrier;

        struct SlowQueryTokenizer {
            query_calls: Arc<AtomicUsize>,
        }
        impl Tokenizer for SlowQueryTokenizer {
            fn tokenize(&self, text: &str) -> Vec<String> {
                if text == "needle" {
                    self.query_calls.fetch_add(1, AtomicOrdering::Relaxed);
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                text.split_whitespace().map(str::to_owned).collect()
            }
        }

        let query_calls = Arc::new(AtomicUsize::new(0));
        let bm = Arc::new(Bm25TextIndex::with_tokenizer(Box::new(
            SlowQueryTokenizer {
                query_calls: Arc::clone(&query_calls),
            },
        )));
        for doc in 1..=512 {
            bm.index_text(doc, 1, "needle filler");
        }
        let barrier = Arc::new(Barrier::new(16));
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let bm = Arc::clone(&bm);
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    assert_eq!(bm.search("needle", 10).len(), 10);
                });
            }
        });
        assert_eq!(
            query_calls.load(AtomicOrdering::Relaxed),
            1,
            "并发相同查询只能计算一次"
        );
    }

    /// 接缝验证：注入一个"只认整词、不切 bigram"的分词器，索引/评分逻辑照旧走，
    /// 但召回行为随分词器改变 —— 证明换分词器（→ jieba）只换这一层。
    #[test]
    fn injected_tokenizer_changes_segmentation_only() {
        struct WordTokenizer; // 按空白切，整段中文当一个词（模拟"词级"的极端：不拆 bigram）
        impl Tokenizer for WordTokenizer {
            fn tokenize(&self, text: &str) -> Vec<String> {
                text.split_whitespace().map(|w| w.to_lowercase()).collect()
            }
        }

        let bm = Bm25TextIndex::with_tokenizer(Box::new(WordTokenizer));
        bm.index_text(1, 1, "盗刷 风控");
        bm.index_text(2, 2, "盗刷风控"); // 无空格 → 在该分词器下是一个整词

        // 查 "风控"：只有 (1,1) 把它切成独立词 → 命中；(2,2) 整串是一个词，不含 "风控" 这个 token。
        let hits = bm.search("风控", 10);
        assert_eq!(hits.len(), 1);
        assert_eq!(
            (hits[0].0, hits[0].1),
            (1, 1),
            "分词器决定切分，索引只认 token"
        );

        // 同一份数据走默认 bigram：两条都把 风控 切出来 → 都召回（对照，证明只有分词层变了）。
        let bg = Bm25TextIndex::new();
        bg.index_text(1, 1, "盗刷 风控");
        bg.index_text(2, 2, "盗刷风控");
        assert_eq!(bg.search("风控", 10).len(), 2, "bigram 下两条都含 风控");
    }

    #[test]
    fn bm25_cache_roundtrip_preserves_results() {
        let dir = std::env::temp_dir().join(format!(
            "yt_bm25_cache_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bm25.dat");

        let bm = Bm25TextIndex::new();
        bm.index_text(1, 10, "风控系统拦截疑似盗刷");
        bm.index_text(2, 20, "正常登录完成转账");
        bm.index_text(3, 30, "疑似盗刷需要人工复核");
        let before = bm.search("盗刷 风控", 10);
        assert!(bm.save_cache(&path, 7, 3).unwrap());

        let loaded = Bm25TextIndex::new();
        assert!(loaded.load_cache(&path, 7, 3));
        assert_eq!(loaded.search("盗刷 风控", 10), before);
        let only_odd_traces = |trace_id: u64, _: u64| trace_id % 2 == 1;
        assert_eq!(
            loaded.search_filtered("盗刷 风控", 10, &only_odd_traces),
            loaded.search_exact_filtered_for_eval("盗刷 风控", 10, &only_odd_traces)
        );

        // 持久主索引只保留排序 postings；WAL tail 增量要能合并回主索引，不能覆盖历史文档。
        loaded.index_text(1, 10, "补充风控证据");
        loaded.index_text(4, 40, "新发现盗刷风险");
        let fresh = Bm25TextIndex::new();
        fresh.index_text(1, 10, "风控系统拦截疑似盗刷");
        fresh.index_text(2, 20, "正常登录完成转账");
        fresh.index_text(3, 30, "疑似盗刷需要人工复核");
        fresh.index_text(1, 10, "补充风控证据");
        fresh.index_text(4, 40, "新发现盗刷风险");
        assert_eq!(
            loaded.search("盗刷 风控", 10),
            fresh.search("盗刷 风控", 10)
        );
        assert!(
            !loaded.load_cache(&path, 8, 3),
            "manifest version mismatch must reject stale cache"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn event_index_is_idempotent_before_and_after_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "yt_bm25_event_dedup_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bm25.dat");

        let built = Bm25TextIndex::new();
        built.index_event(101, 1, 10, "任务执行 支付风控");
        let once = built.search_exact_for_eval("支付风控", 10);
        built.index_event(101, 1, 10, "任务执行 支付风控");
        assert_eq!(
            built.search_exact_for_eval("支付风控", 10),
            once,
            "同一 event_id 重试不能重复增加词频"
        );
        built.index_event(102, 2, 20, "任务执行 普通检查");
        assert!(built.save_cache(&path, 7, 2).unwrap());

        let reopened = Bm25TextIndex::new();
        assert!(reopened.load_cache(&path, 7, 2));
        let before_retry = reopened.search_exact_for_eval("支付风控", 10);
        reopened.index_event(101, 1, 10, "任务执行 支付风控");
        assert_eq!(
            reopened.search_exact_for_eval("支付风控", 10),
            before_retry,
            "磁盘 event_id 表必须挡住重开后的迟到重试"
        );

        reopened.index_event(103, 1, 10, "支付风控");
        assert_ne!(
            reopened.search_exact_for_eval("支付风控", 10),
            before_retry,
            "不同 event_id 仍必须正常增加文本"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn disk_block_max_is_stable_under_parallel_queries() {
        let dir = std::env::temp_dir().join(format!(
            "yt_bm25_parallel_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bm25.dat");

        let built = Bm25TextIndex::new();
        for doc in 1..=4096 {
            let text = if doc % 97 == 0 {
                "任务执行 月蚀校验码"
            } else {
                "任务执行 工具调用"
            };
            built.index_text(doc, 1, text);
        }
        assert!(built.save_cache(&path, 5, 4096).unwrap());

        let loaded = Arc::new(Bm25TextIndex::new());
        assert!(loaded.load_cache(&path, 5, 4096));
        let expected_common = loaded.search("任务执行", 10);
        let expected_rare = loaded.search("月蚀校验码", 10);
        std::thread::scope(|scope| {
            for worker in 0..16 {
                let loaded = Arc::clone(&loaded);
                let expected_common = expected_common.clone();
                let expected_rare = expected_rare.clone();
                scope.spawn(move || {
                    for round in 0..20 {
                        if (worker + round) % 2 == 0 {
                            assert_eq!(loaded.search("任务执行", 10), expected_common);
                        } else {
                            assert_eq!(loaded.search("月蚀校验码", 10), expected_rare);
                        }
                    }
                });
            }
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disk_block_max_skips_equal_score_tail_blocks() {
        let dir = std::env::temp_dir().join(format!(
            "yt_bm25_block_max_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bm25.dat");

        let built = Bm25TextIndex::new();
        for doc in 1..=1024 {
            built.index_text(doc, 1, "common phrase");
        }
        assert!(built.save_cache(&path, 3, 1024).unwrap());

        let loaded = Bm25TextIndex::new();
        assert!(loaded.load_cache(&path, 3, 1024));
        let hits = loaded.search("common phrase", 10);
        assert_eq!(
            hits.iter().map(|hit| hit.0).collect::<Vec<_>>(),
            (1..=10).collect::<Vec<_>>()
        );
        let state = loaded.state.lock().unwrap();
        let disk = state.disk.as_ref().unwrap();
        assert_eq!(
            disk.cached_block_count(),
            2,
            "两个词都只应读取首块，后续同分且 doc 更大的块应由磁盘上界跳过"
        );
        drop(state);

        assert_eq!(hits, loaded.search_exact_for_eval("common phrase", 10));
        let tail_only = |trace_id: u64, _: u64| trace_id >= 900;
        let tail_hits = loaded.search_filtered("common phrase", 10, &tail_only);
        assert_eq!(
            tail_hits.iter().map(|hit| hit.0).collect::<Vec<_>>(),
            (900..910).collect::<Vec<_>>()
        );
        assert_eq!(
            tail_hits,
            loaded.search_exact_filtered_for_eval("common phrase", 10, &tail_only)
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn disk_block_max_reads_highest_upper_bound_block_first() {
        let dir = std::env::temp_dir().join(format!(
            "yt_bm25_best_block_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bm25.dat");

        let built = Bm25TextIndex::new();
        for doc in 1..=1024 {
            let text = if doc <= 896 {
                "common phrase filler filler filler filler filler filler"
            } else {
                "common phrase"
            };
            built.index_text(doc, 1, text);
        }
        assert!(built.save_cache(&path, 4, 1024).unwrap());

        let loaded = Bm25TextIndex::new();
        assert!(loaded.load_cache(&path, 4, 1024));
        let hits = loaded.search("common phrase", 10);
        assert_eq!(
            hits.iter().map(|hit| hit.0).collect::<Vec<_>>(),
            (897..907).collect::<Vec<_>>()
        );
        let state = loaded.state.lock().unwrap();
        let disk = state.disk.as_ref().unwrap();
        assert_eq!(
            disk.cached_block_count(),
            2,
            "两个查询词都只应读取文件尾部的最高上界块"
        );
        drop(state);

        assert_eq!(hits, loaded.search_exact_for_eval("common phrase", 10));
        let _ = std::fs::remove_dir_all(dir);
    }
    #[test]
    fn legacy_queries_aggregate_tenants_and_keep_scoped_cache_invalidation() {
        let bm = Bm25TextIndex::new();
        bm.index_event_scoped(Some(1), 99, 1, 1, "alpha");
        bm.index_event_scoped(Some(2), 99, 1, 1, "beta");
        assert_eq!(bm.search("alpha beta", 10).len(), 2);
        assert_eq!(
            bm.search("alpha beta", 10),
            bm.search_exact_for_eval("alpha beta", 10)
        );
        assert_eq!(bm.search_filtered("beta", 10, &|_, _| true).len(), 1);
        assert_eq!(bm.search_all_scoped("fresh", 10).len(), 0);
        bm.index_event_scoped(Some(2), 100, 2, 2, "fresh");
        assert_eq!(bm.search_all_scoped("fresh", 10)[0].0, Some(2));
        assert_eq!(bm.search("fresh", 10).len(), 1);
        let dir = std::env::temp_dir().join(format!(
            "yt_bm25_tenant_catalog_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bm25.dat");
        bm.save_cache(&path, 1, 3).unwrap();
        let loaded = Bm25TextIndex::new();
        assert!(loaded.load_cache(&path, 1, 3));
        assert_eq!(loaded.search("alpha beta", 10), bm.search("alpha beta", 10));
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[32] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        assert!(
            !Bm25TextIndex::new().load_cache(&path, 1, 3),
            "损坏目录必须从原始事件重建"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
