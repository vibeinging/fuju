//! BM25 持久缓存的分块读取层。
//!
//! 打开时只读取文档长度和词/块目录。每块保存文档范围和真实最大 BM25 norm，查询先用
//! 上界判断，再按需读取 128 条 postings，避免高频词总是整条倒排进内存。

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

pub(crate) type Doc = (u64, u64);
pub(crate) type Posting = (Doc, u32);

const MAGIC: u32 = 0x5954_424d; // "YTBM"
const VERSION: u32 = 4;
const HEADER_LEN: u64 = 8 + 11 * 8;
const DOC_RECORD_LEN: u64 = 20;
const POSTING_RECORD_LEN: u64 = 20;
pub(crate) const BLOCK_SIZE: usize = 128;
const DEFAULT_CACHE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub(crate) struct DiskBlockMeta {
    pub(crate) offset: u64,
    pub(crate) count: u32,
    pub(crate) max_norm: f32,
    pub(crate) first_doc: Doc,
    pub(crate) last_doc: Doc,
}

#[derive(Clone, Debug)]
struct PostingRef {
    count: u64,
    blocks: Vec<DiskBlockMeta>,
}

struct CachedPostings {
    postings: Arc<Vec<Posting>>,
    byte_len: usize,
    recently_used: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum CacheKey {
    Block { token: String, block: usize },
    Whole(String),
}

#[derive(Clone, Copy)]
pub(crate) struct CacheMetadata {
    pub(crate) manifest_version: u64,
    pub(crate) memtable_watermark: u64,
    pub(crate) total_len: u64,
}

pub(crate) struct DiskBm25Cache {
    file: File,
    docs: Arc<Vec<(Doc, u32)>>,
    event_ids: Arc<Vec<u64>>,
    total_len: u64,
    directory: HashMap<String, PostingRef>,
    cached: HashMap<CacheKey, CachedPostings>,
    lru: VecDeque<CacheKey>,
    cached_bytes: usize,
    cache_budget: usize,
}

impl DiskBm25Cache {
    pub(crate) fn open(
        path: &Path,
        manifest_version: u64,
        memtable_watermark: u64,
    ) -> Option<Self> {
        Self::open_with_budget(
            path,
            manifest_version,
            memtable_watermark,
            postings_cache_budget(),
        )
    }

    fn open_with_budget(
        path: &Path,
        manifest_version: u64,
        memtable_watermark: u64,
        cache_budget: usize,
    ) -> Option<Self> {
        let mut file = File::open(path).ok()?;
        let file_len = file.metadata().ok()?.len();
        if file_len < HEADER_LEN {
            return None;
        }
        let mut header = [0u8; HEADER_LEN as usize];
        file.read_exact(&mut header).ok()?;
        let mut cur = SliceCursor::new(&header);
        if cur.u32()? != MAGIC || cur.u32()? != VERSION {
            return None;
        }
        if cur.u64()? != manifest_version || cur.u64()? != memtable_watermark {
            return None;
        }
        let total_len = cur.u64()?;
        let doc_count = cur.u64()?;
        let event_count = cur.u64()?;
        let token_count = cur.u64()?;
        let docs_offset = cur.u64()?;
        let event_ids_offset = cur.u64()?;
        let postings_offset = cur.u64()?;
        let directory_offset = cur.u64()?;
        let directory_len = cur.u64()?;

        let docs_len = doc_count.checked_mul(DOC_RECORD_LEN)?;
        let docs_end = docs_offset.checked_add(docs_len)?;
        let event_ids_len = event_count.checked_mul(8)?;
        let event_ids_end = event_ids_offset.checked_add(event_ids_len)?;
        let directory_end = directory_offset.checked_add(directory_len)?;
        if docs_offset != HEADER_LEN
            || docs_end != event_ids_offset
            || event_ids_end != postings_offset
            || postings_offset > directory_offset
            || directory_end != file_len
        {
            return None;
        }

        let mut docs_bytes = vec![0u8; usize::try_from(docs_len).ok()?];
        file.seek(SeekFrom::Start(docs_offset)).ok()?;
        file.read_exact(&mut docs_bytes).ok()?;
        let mut docs_cur = SliceCursor::new(&docs_bytes);
        let mut docs = Vec::with_capacity(usize::try_from(doc_count).ok()?);
        let mut previous = None;
        for _ in 0..doc_count {
            let doc = (docs_cur.u64()?, docs_cur.u64()?);
            let len = docs_cur.u32()?;
            if len == 0 || previous.is_some_and(|prev| prev >= doc) {
                return None;
            }
            previous = Some(doc);
            docs.push((doc, len));
        }

        let mut event_bytes = vec![0u8; usize::try_from(event_ids_len).ok()?];
        file.seek(SeekFrom::Start(event_ids_offset)).ok()?;
        file.read_exact(&mut event_bytes).ok()?;
        let mut event_cur = SliceCursor::new(&event_bytes);
        let mut event_ids = Vec::with_capacity(usize::try_from(event_count).ok()?);
        let mut previous_event = None;
        for _ in 0..event_count {
            let event_id = event_cur.u64()?;
            if previous_event.is_some_and(|previous| previous >= event_id) {
                return None;
            }
            previous_event = Some(event_id);
            event_ids.push(event_id);
        }

        let mut directory_bytes = vec![0u8; usize::try_from(directory_len).ok()?];
        file.seek(SeekFrom::Start(directory_offset)).ok()?;
        file.read_exact(&mut directory_bytes).ok()?;
        let mut dir_cur = SliceCursor::new(&directory_bytes);
        let mut directory = HashMap::with_capacity(usize::try_from(token_count).ok()?);
        for _ in 0..token_count {
            let token = dir_cur.string_u32()?;
            let count = dir_cur.u64()?;
            let block_count = usize::try_from(dir_cur.u64()?).ok()?;
            if count == 0 || block_count == 0 || directory.contains_key(&token) {
                return None;
            }
            let mut blocks = Vec::with_capacity(block_count);
            let mut counted = 0u64;
            let mut previous_doc = None;
            for _ in 0..block_count {
                let offset = dir_cur.u64()?;
                let block_rows = dir_cur.u32()?;
                let max_norm = f32::from_bits(dir_cur.u32()?);
                let first_doc = (dir_cur.u64()?, dir_cur.u64()?);
                let last_doc = (dir_cur.u64()?, dir_cur.u64()?);
                let bytes = u64::from(block_rows).checked_mul(POSTING_RECORD_LEN)?;
                let end = offset.checked_add(bytes)?;
                if block_rows == 0
                    || block_rows as usize > BLOCK_SIZE
                    || !max_norm.is_finite()
                    || max_norm <= 0.0
                    || first_doc > last_doc
                    || previous_doc.is_some_and(|previous| previous >= first_doc)
                    || offset < postings_offset
                    || end > directory_offset
                {
                    return None;
                }
                previous_doc = Some(last_doc);
                counted = counted.checked_add(u64::from(block_rows))?;
                blocks.push(DiskBlockMeta {
                    offset,
                    count: block_rows,
                    max_norm,
                    first_doc,
                    last_doc,
                });
            }
            if counted != count {
                return None;
            }
            directory.insert(token, PostingRef { count, blocks });
        }
        if !dir_cur.is_finished() {
            return None;
        }

        Some(Self {
            file,
            docs: Arc::new(docs),
            event_ids: Arc::new(event_ids),
            total_len,
            directory,
            cached: HashMap::new(),
            lru: VecDeque::new(),
            cached_bytes: 0,
            cache_budget,
        })
    }

    pub(crate) fn doc_count(&self) -> usize {
        self.docs.len()
    }

    pub(crate) fn total_len(&self) -> u64 {
        self.total_len
    }

    pub(crate) fn contains_event(&self, event_id: u64) -> bool {
        self.event_ids.binary_search(&event_id).is_ok()
    }

    pub(crate) fn event_ids(&self) -> &[u64] {
        self.event_ids.as_slice()
    }

    pub(crate) fn doc_len(&self, doc: Doc) -> Option<u32> {
        self.docs
            .binary_search_by_key(&doc, |&(key, _)| key)
            .ok()
            .map(|index| self.docs[index].1)
    }

    pub(crate) fn docs(&self) -> &[(Doc, u32)] {
        self.docs.as_slice()
    }

    pub(crate) fn docs_arc(&self) -> Arc<Vec<(Doc, u32)>> {
        Arc::clone(&self.docs)
    }

    pub(crate) fn token_names(&self) -> impl Iterator<Item = &String> {
        self.directory.keys()
    }

    pub(crate) fn token_doc_freq(&self, token: &str) -> Option<u64> {
        self.directory.get(token).map(|entry| entry.count)
    }

    pub(crate) fn block_metadata(&self, token: &str) -> Option<Vec<DiskBlockMeta>> {
        Some(self.directory.get(token)?.blocks.clone())
    }

    pub(crate) fn set_cache_budget(&mut self, budget: usize) {
        self.cache_budget = budget;
        if self.cached_bytes > budget {
            self.cached.clear();
            self.lru.clear();
            self.cached_bytes = 0;
        }
    }
    pub(crate) fn load_block(&mut self, token: &str, block: usize) -> Option<Arc<Vec<Posting>>> {
        let key = CacheKey::Block {
            token: token.to_owned(),
            block,
        };
        if let Some(cached) = self.cached.get_mut(&key) {
            cached.recently_used = true;
            return Some(Arc::clone(&cached.postings));
        }
        let meta = *self.directory.get(token)?.blocks.get(block)?;
        let byte_len = usize::try_from(u64::from(meta.count) * POSTING_RECORD_LEN).ok()?;
        let mut bytes = vec![0u8; byte_len];
        self.file.seek(SeekFrom::Start(meta.offset)).ok()?;
        self.file.read_exact(&mut bytes).ok()?;
        let mut cur = SliceCursor::new(&bytes);
        let mut postings = Vec::with_capacity(meta.count as usize);
        let mut previous = None;
        let mut doc_index = self
            .docs
            .binary_search_by_key(&meta.first_doc, |&(doc, _)| doc)
            .ok()?;
        for _ in 0..meta.count {
            let doc = (cur.u64()?, cur.u64()?);
            let tf = cur.u32()?;
            while doc_index < self.docs.len() && self.docs[doc_index].0 < doc {
                doc_index += 1;
            }
            if tf == 0
                || previous.is_some_and(|prev| prev >= doc)
                || doc_index >= self.docs.len()
                || self.docs[doc_index].0 != doc
            {
                return None;
            }
            previous = Some(doc);
            doc_index += 1;
            postings.push((doc, tf));
        }
        if postings.first().map(|posting| posting.0) != Some(meta.first_doc)
            || postings.last().map(|posting| posting.0) != Some(meta.last_doc)
        {
            return None;
        }
        let postings = Arc::new(postings);
        self.insert_cache(key, Arc::clone(&postings));
        Some(postings)
    }

    pub(crate) fn load_postings(&mut self, token: &str) -> Option<Arc<Vec<Posting>>> {
        let cache_key = CacheKey::Whole(token.to_owned());
        if let Some(cached) = self.cached.get_mut(&cache_key) {
            cached.recently_used = true;
            return Some(Arc::clone(&cached.postings));
        }
        // 通用 WAND 需要完整词表时做一次连续读，不能退化成每 128 条一次 seek/read。
        // block-max 快路仍通过 `load_block` 只取可能进入 top-k 的块。
        let entry = self.directory.get(token)?.clone();
        let offset = entry.blocks.first()?.offset;
        let byte_len = usize::try_from(entry.count.checked_mul(POSTING_RECORD_LEN)?).ok()?;
        let mut bytes = vec![0u8; byte_len];
        self.file.seek(SeekFrom::Start(offset)).ok()?;
        self.file.read_exact(&mut bytes).ok()?;
        let mut cur = SliceCursor::new(&bytes);
        let mut postings = Vec::with_capacity(usize::try_from(entry.count).ok()?);
        let mut previous = None;
        let mut doc_index = 0usize;
        for _ in 0..entry.count {
            let doc = (cur.u64()?, cur.u64()?);
            let tf = cur.u32()?;
            while doc_index < self.docs.len() && self.docs[doc_index].0 < doc {
                doc_index += 1;
            }
            if tf == 0
                || previous.is_some_and(|prev| prev >= doc)
                || doc_index >= self.docs.len()
                || self.docs[doc_index].0 != doc
            {
                return None;
            }
            previous = Some(doc);
            postings.push((doc, tf));
        }
        let postings = Arc::new(postings);
        self.insert_cache(cache_key, Arc::clone(&postings));
        Some(postings)
    }

    fn insert_cache(&mut self, key: CacheKey, postings: Arc<Vec<Posting>>) {
        // 磁盘一条 posting 是 20 字节，但 Rust 元组通常有对齐填充。缓存预算按 Vec
        // 真正保留的容量计费，不能拿序列化大小冒充堆占用。
        let byte_len = postings
            .capacity()
            .saturating_mul(std::mem::size_of::<Posting>());
        if byte_len > self.cache_budget {
            return;
        }
        while self.cached_bytes.saturating_add(byte_len) > self.cache_budget {
            let Some(oldest) = self.lru.pop_front() else {
                break;
            };
            if let Some(cached) = self.cached.get_mut(&oldest) {
                if cached.recently_used {
                    cached.recently_used = false;
                    self.lru.push_back(oldest);
                    continue;
                }
            }
            if let Some(removed) = self.cached.remove(&oldest) {
                self.cached_bytes = self.cached_bytes.saturating_sub(removed.byte_len);
            }
        }
        self.cached.insert(
            key.clone(),
            CachedPostings {
                postings,
                byte_len,
                recently_used: false,
            },
        );
        self.lru.push_back(key);
        self.cached_bytes = self.cached_bytes.saturating_add(byte_len);
    }

    #[cfg(test)]
    pub(crate) fn cached_block_count(&self) -> usize {
        self.cached
            .keys()
            .filter(|key| matches!(key, CacheKey::Block { .. }))
            .count()
    }
}

fn postings_cache_budget() -> usize {
    std::env::var("YT_BM25_POSTINGS_CACHE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_CACHE_BYTES)
}

pub(crate) fn write_atomic<F>(
    path: &Path,
    metadata: CacheMetadata,
    docs: &[(Doc, u32)],
    event_ids: &[u64],
    tokens: &[String],
    mut postings_for: F,
) -> std::io::Result<()>
where
    F: FnMut(&str) -> std::io::Result<Vec<Posting>>,
{
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    std::fs::create_dir_all(parent)?;
    let tmp = path.with_extension("tmp");
    let mut file = BufWriter::with_capacity(8 * 1024 * 1024, File::create(&tmp)?);
    file.write_all(&[0u8; HEADER_LEN as usize])?;

    let mut previous_doc = None;
    for &(doc, len) in docs {
        if len == 0 || previous_doc.is_some_and(|prev| prev >= doc) {
            return Err(invalid("BM25 documents are not strictly sorted"));
        }
        previous_doc = Some(doc);
        file.write_all(&doc.0.to_le_bytes())?;
        file.write_all(&doc.1.to_le_bytes())?;
        file.write_all(&len.to_le_bytes())?;
    }

    let mut previous_event = None;
    for &event_id in event_ids {
        if previous_event.is_some_and(|previous| previous >= event_id) {
            return Err(invalid("BM25 event ids are not strictly sorted"));
        }
        previous_event = Some(event_id);
        file.write_all(&event_id.to_le_bytes())?;
    }

    let postings_offset = file.stream_position()?;
    let avgdl = if docs.is_empty() {
        0.0
    } else {
        metadata.total_len as f32 / docs.len() as f32
    };
    let mut directory = Vec::with_capacity(tokens.len());
    for token in tokens {
        let postings = postings_for(token)?;
        if postings.is_empty() {
            return Err(invalid("BM25 token has no postings"));
        }
        let mut blocks = Vec::with_capacity(postings.len().div_ceil(BLOCK_SIZE));
        let mut doc_index = 0usize;
        let mut previous = None;
        for chunk in postings.chunks(BLOCK_SIZE) {
            let offset = file.stream_position()?;
            let mut max_norm = 0.0f32;
            for &(doc, tf) in chunk {
                while doc_index < docs.len() && docs[doc_index].0 < doc {
                    doc_index += 1;
                }
                if tf == 0
                    || previous.is_some_and(|prev| prev >= doc)
                    || doc_index >= docs.len()
                    || docs[doc_index].0 != doc
                {
                    return Err(invalid("BM25 postings are not strictly sorted"));
                }
                previous = Some(doc);
                max_norm = max_norm.max(crate::bm25::bm25_norm(
                    tf as f32,
                    docs[doc_index].1 as f32,
                    avgdl,
                ));
                file.write_all(&doc.0.to_le_bytes())?;
                file.write_all(&doc.1.to_le_bytes())?;
                file.write_all(&tf.to_le_bytes())?;
            }
            blocks.push(DiskBlockMeta {
                offset,
                count: chunk.len() as u32,
                max_norm,
                first_doc: chunk[0].0,
                last_doc: chunk[chunk.len() - 1].0,
            });
        }
        directory.push((token, postings.len() as u64, blocks));
    }

    let directory_offset = file.stream_position()?;
    for (token, count, blocks) in directory {
        let token_len =
            u32::try_from(token.len()).map_err(|_| invalid("BM25 token is too long"))?;
        file.write_all(&token_len.to_le_bytes())?;
        file.write_all(token.as_bytes())?;
        file.write_all(&count.to_le_bytes())?;
        file.write_all(&(blocks.len() as u64).to_le_bytes())?;
        for block in blocks {
            file.write_all(&block.offset.to_le_bytes())?;
            file.write_all(&block.count.to_le_bytes())?;
            file.write_all(&block.max_norm.to_bits().to_le_bytes())?;
            file.write_all(&block.first_doc.0.to_le_bytes())?;
            file.write_all(&block.first_doc.1.to_le_bytes())?;
            file.write_all(&block.last_doc.0.to_le_bytes())?;
            file.write_all(&block.last_doc.1.to_le_bytes())?;
        }
    }
    let end = file.stream_position()?;

    file.seek(SeekFrom::Start(0))?;
    file.write_all(&MAGIC.to_le_bytes())?;
    file.write_all(&VERSION.to_le_bytes())?;
    file.write_all(&metadata.manifest_version.to_le_bytes())?;
    file.write_all(&metadata.memtable_watermark.to_le_bytes())?;
    file.write_all(&metadata.total_len.to_le_bytes())?;
    file.write_all(&(docs.len() as u64).to_le_bytes())?;
    file.write_all(&(event_ids.len() as u64).to_le_bytes())?;
    file.write_all(&(tokens.len() as u64).to_le_bytes())?;
    file.write_all(&HEADER_LEN.to_le_bytes())?;
    file.write_all(&(HEADER_LEN + docs.len() as u64 * DOC_RECORD_LEN).to_le_bytes())?;
    file.write_all(&postings_offset.to_le_bytes())?;
    file.write_all(&directory_offset.to_le_bytes())?;
    file.write_all(&(end - directory_offset).to_le_bytes())?;
    file.flush()?;
    file.get_ref().sync_all()?;
    drop(file);
    crate::test_failpoints::before_sidecar_rename("bm25", path);
    std::fs::rename(tmp, path)
}

fn invalid(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

struct SliceCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> SliceCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(len)?;
        if end > self.bytes.len() {
            return None;
        }
        let value = &self.bytes[self.pos..end];
        self.pos = end;
        Some(value)
    }

    fn u32(&mut self) -> Option<u32> {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(self.take(4)?);
        Some(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> Option<u64> {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(self.take(8)?);
        Some(u64::from_le_bytes(bytes))
    }

    fn string_u32(&mut self) -> Option<String> {
        let len = usize::try_from(self.u32()?).ok()?;
        String::from_utf8(self.take(len)?.to_vec()).ok()
    }

    fn is_finished(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "yt_bm25_disk_{name}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn opens_metadata_and_pages_only_requested_blocks() {
        let path = temp_path("page");
        let docs: Vec<_> = (1..=300).map(|doc| ((doc, 1), 4)).collect();
        let tokens = vec!["a".to_owned(), "b".to_owned()];
        write_atomic(
            &path,
            CacheMetadata {
                manifest_version: 7,
                memtable_watermark: 300,
                total_len: 1200,
            },
            &docs,
            &[],
            &tokens,
            |token| match token {
                "a" => Ok(docs.iter().map(|&(doc, _)| (doc, 2)).collect()),
                "b" => Ok(vec![((2, 1), 1)]),
                _ => unreachable!(),
            },
        )
        .unwrap();

        let one_full_block_bytes = BLOCK_SIZE * std::mem::size_of::<Posting>();
        let mut cache =
            DiskBm25Cache::open_with_budget(&path, 7, 300, one_full_block_bytes).unwrap();
        assert_eq!(cache.doc_count(), 300);
        assert_eq!(cache.block_metadata("a").unwrap().len(), 3);
        assert_eq!(cache.cached_block_count(), 0);
        assert_eq!(cache.load_block("a", 0).unwrap().len(), 128);
        assert_eq!(cache.cached_block_count(), 1);
        for _ in 0..100 {
            assert_eq!(cache.load_block("a", 0).unwrap().len(), 128);
        }
        assert_eq!(cache.lru.len(), 1, "cache hits must not grow the clock");
        assert_eq!(&*cache.load_block("b", 0).unwrap(), &[((2, 1), 1)]);
        assert_eq!(cache.cached_block_count(), 1, "budget must evict a block");
        assert!(DiskBm25Cache::open(&path, 8, 300).is_none());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn rejects_truncated_cache() {
        let path = temp_path("truncated");
        std::fs::write(&path, [0u8; 16]).unwrap();
        assert!(DiskBm25Cache::open(&path, 1, 1).is_none());
        let _ = std::fs::remove_file(path);
    }
}
