impl WriteCoordinator {
    /// 内存 WAL（测试/开发，不落盘）。
    pub fn new(segments: Arc<dyn SegmentStore>) -> Arc<Self> {
        Self::build(segments, Wal::new())
    }

    /// 文件 WAL（真落盘）：重启后用同一路径 `open` + `recover()` 可从盘上重放(WAL 持久化)。
    /// 注意：段/manifest 不持久化,崩溃后靠 WAL 全量重放进 MemTable 恢复。要"flush 后重启不丢"用 `open_durable`。
    pub fn open(
        segments: Arc<dyn SegmentStore>,
        wal_path: impl AsRef<std::path::Path>,
    ) -> std::io::Result<Arc<Self>> {
        Ok(Self::build_full(
            segments,
            Wal::open(wal_path)?,
            Manifest::empty(),
            1,
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ))
    }

    /// **全持久化引擎**：一个目录下放段(`segments/`)+ WAL(`wal.log`)+ manifest(`manifest.dat`)。
    /// 重启用同一目录 `open_durable` + `recover()`：先从 manifest 重建段集合(指向盘上段文件)、再 WAL 重放
    /// 水位之后的尾巴 —— **flush 过的数据(水位之前、WAL 不再重放)从持久段读回,真正重启不丢**。
    pub fn open_durable(dir: impl AsRef<std::path::Path>) -> std::io::Result<Arc<Self>> {
        Self::open_durable_inner(dir, None, None, None)
    }

    /// open_durable 的内部实现，多收可选索引覆盖 + 磁盘向量索引参数（[`CoordinatorBuilder`] 用它注入）。
    fn open_durable_inner(
        dir: impl AsRef<std::path::Path>,
        bm25: Option<Arc<dyn Bm25Index>>,
        graph: Option<Arc<dyn GraphIndex>>,
        vec_cfg: Option<DiskGraphConfig>,
    ) -> std::io::Result<Arc<Self>> {
        let open_started = std::time::Instant::now();
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let process_lock = Arc::new(process_lock::ProcessLockManager::new(dir));
        let lock_started = std::time::Instant::now();
        let _open_guard = process_lock
            .acquire("open")
            .map_err(|e| std::io::Error::new(e.kind(), format!("open durable lock failed: {e}")))?;
        let _write_guard = process_lock.acquire("write").map_err(|e| {
            std::io::Error::new(e.kind(), format!("open durable write lock failed: {e}"))
        })?;
        let lock_us = lock_started.elapsed().as_micros() as u64;
        let storage_started = std::time::Instant::now();
        let segments = Arc::new(FileSegmentStore::open(dir.join("segments"))?);
        let wal = Wal::open(dir.join("wal.log"))?;
        let storage_us = storage_started.elapsed().as_micros() as u64;
        let manifest_path = dir.join("manifest.dat");
        let metadata_path = dir.join("metadata.dat");
        let gc_log_path = dir.join("gc.log");
        // 有持久 manifest 就从它恢复段集合与 id 计数器；否则从空开始。
        let manifest_started = std::time::Instant::now();
        let (manifest, next_seg, next_chunk) = match persist::try_load(&manifest_path)? {
            Some(s) => (s.manifest, s.next_segment_id, s.next_chunk_id),
            None => (Manifest::empty(), 1, 1),
        };
        let manifest_us = manifest_started.elapsed().as_micros() as u64;
        // 默认向量索引 = **磁盘图索引**（向量+图都落盘、重启不 rebuild、append 友好），不用 vecstore。
        // 注入了自定义 graph（可能内存型）则保留 vecstore 重建路径（向后兼容）。
        let graph_started = std::time::Instant::now();
        let (graph, vector_path): (Option<Arc<dyn GraphIndex>>, Option<std::path::PathBuf>) =
            match graph {
                Some(g) => (Some(g), Some(dir.join("vectors.dat"))),
                None => {
                    let disk =
                        DurableGraphIndex::open(dir.join("vecindex"), vec_cfg.unwrap_or_default());
                    (Some(Arc::new(disk) as Arc<dyn GraphIndex>), None)
                }
            };
        let graph_us = graph_started.elapsed().as_micros() as u64;
        let build_started = std::time::Instant::now();
        let coord = Self::build_full(
            segments,
            wal,
            manifest,
            next_seg,
            next_chunk,
            Some(manifest_path),
            vector_path,
            bm25,
            graph,
            metadata::load(&metadata_path),
            Some(metadata_path),
            Some(dir.to_path_buf()),
            Some(process_lock),
        );
        let build_us = build_started.elapsed().as_micros() as u64;
        // 打开 GC 日志，先补删上次崩溃残留的"MARK 没 DONE"段（崩溃安全），再装上。
        let gc_started = std::time::Instant::now();
        let entries = gc_log::GcLog::scan(&gc_log_path).unwrap_or_default();
        for seg in gc_log::pending_deletions(&entries) {
            // 段文件可能已删了一半（崩溃在 unlink 中）；补删幂等（不存在就跳过）。
            coord.segments.unlink_segment(SegmentId(seg));
            // 这些段上次崩溃前 manifest 已不引用（reclaim 前提），不用动 manifest。
            // 段 id 不复用、dead_set 是内存态重启后清空，所以不用动 dead_set。
        }
        // 重置 gc.log：已补删的不再记；之后 reclaim 重新记新意图。truncate 即可。
        let _ = std::fs::write(&gc_log_path, b"");
        // GC 日志和 WAL/manifest 同等重要（崩溃安全的承重组件）——打开失败必须 fail-fast，
        // 不能静默降级成"无 GC 日志、reclaim 直接删"（那样崩溃恢复失效且无人知晓）。
        let log = gc_log::GcLog::open(&gc_log_path)?;
        *coord.gc_log.lock().unwrap() = Some(log);
        let gc_us = gc_started.elapsed().as_micros() as u64;
        let total_us = open_started.elapsed().as_micros() as u64;
        olog::log(
            olog::Level::Info,
            "open_durable_done",
            &[
                ("total_us", &total_us),
                ("lock_us", &lock_us),
                ("storage_us", &storage_us),
                ("manifest_us", &manifest_us),
                ("graph_us", &graph_us),
                ("build_us", &build_us),
                ("gc_us", &gc_us),
            ],
        );
        Ok(coord)
    }

    fn build(segments: Arc<dyn SegmentStore>, wal: Wal) -> Arc<Self> {
        Self::build_full(
            segments,
            wal,
            Manifest::empty(),
            1,
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build_full(
        segments: Arc<dyn SegmentStore>,
        wal: Wal,
        manifest: Manifest,
        next_segment_id: u64,
        next_chunk_id: u64,
        manifest_path: Option<std::path::PathBuf>,
        vector_path: Option<std::path::PathBuf>,
        bm25: Option<Arc<dyn Bm25Index>>,
        graph: Option<Arc<dyn GraphIndex>>,
        metadata_state: Option<metadata::MetadataState>,
        metadata_path: Option<std::path::PathBuf>,
        dir: Option<std::path::PathBuf>,
        process_lock: Option<Arc<process_lock::ProcessLockManager>>,
    ) -> Arc<Self> {
        let metadata_state = metadata_state.unwrap_or_default();
        let bm25_path = dir.as_ref().map(|dir| dir.join("bm25.dat"));
        let seg_key_bloom_path = dir.as_ref().map(|dir| dir.join("segment_bloom.dat"));
        let filter_attrs_path = dir.as_ref().map(|dir| dir.join("filter_attrs.dat"));
        let trace_rollup_path = dir.as_ref().map(|dir| dir.join("trace_rollup.dat"));
        let metadata_index = MetadataIndex::build(
            &metadata_state.annotations,
            &metadata_state.dataset_associations,
            &metadata_state.retention_audits,
            &metadata_state.retention_policies,
        );
        Arc::new(Self {
            write_lock: Mutex::new(()),
            current: Current::new(manifest),
            wal: Mutex::new(wal),
            memtable: Mutex::new(MemTable::new()),
            segments,
            dead_set: Mutex::new(Vec::new()),
            buffer_pins: BufferPins::default(),
            // 默认 BM25 用纯 Rust 中文词级分词（jieba 全量词典，开箱即生产级）/ 图式 ANN；
            // 可被 builder 注入覆盖（团队 jieba FFI、bigram、或叠了自有词典的 ChineseTokenizer）。
            bm25: bm25.unwrap_or_else(|| {
                Arc::new(Bm25TextIndex::with_tokenizer(Box::new(
                    ChineseTokenizer::full(),
                )))
            }),
            graph: graph.unwrap_or_else(|| Arc::new(GraphAnnIndex::default())),
            flush_threshold: AtomicUsize::new(4096),
            next_segment_id: Mutex::new(next_segment_id),
            next_chunk_id: Mutex::new(next_chunk_id),
            datasets: Mutex::new(BTreeMap::new()),
            annotations: Mutex::new(metadata_state.annotations),
            dataset_associations: Mutex::new(metadata_state.dataset_associations),
            metadata_index: Mutex::new(metadata_index),
            retention_audits: Mutex::new(metadata_state.retention_audits),
            retention_policies: Mutex::new(metadata_state.retention_policies),
            next_annotation_id: Mutex::new(metadata_state.next_annotation_id),
            next_dataset_association_id: Mutex::new(metadata_state.next_dataset_association_id),
            next_retention_audit_id: Mutex::new(metadata_state.next_retention_audit_id),
            next_retention_policy_id: Mutex::new(metadata_state.next_retention_policy_id),
            manifest_path,
            metadata_path,
            vector_path,
            bm25_path,
            seg_key_bloom_path,
            filter_attrs_path,
            trace_rollup_path,
            filter_attrs: Mutex::new(FilterAttrsIndex::default()),
            trace_rollup: Mutex::new(TraceAggregateRollupIndex::default()),
            read_model_load_state: Mutex::new(if dir.is_some() {
                ReadModelLoadState::deferred()
            } else {
                ReadModelLoadState::ready()
            }),
            session_idx: Mutex::new(SessionIndex::default()),
            seg_fold_cache: Mutex::new(SegFoldCache::new(2_000_000)), // 缓存上限 ~200 万行
            seg_key_bloom: Mutex::new(HashMap::new()),
            seg_key_bloom_load_lock: Mutex::new(()),
            seg_key_bloom_load_failed_for: Mutex::new(None),
            #[cfg(test)]
            seg_key_bloom_load_count: AtomicUsize::new(0),
            segment_scan_indexes_stale: Mutex::new(false),
            gc_log: Mutex::new(None), // open_durable 设成 Some；非持久模式保持 None
            dir,
            process_lock,
        })
    }

    /// 元数据写失败必须传回调用方；调用方持 writer 锁并恢复未提交的内存变更。
    fn persist_metadata(&self) -> std::io::Result<()> {
        let Some(path) = &self.metadata_path else {
            return Ok(());
        };
        metadata::save(path, &self.capture_metadata_state())
    }

    fn capture_metadata_state(&self) -> metadata::MetadataState {
        metadata::MetadataState {
            annotations: self.annotations.lock().unwrap().clone(),
            dataset_associations: self.dataset_associations.lock().unwrap().clone(),
            retention_audits: self.retention_audits.lock().unwrap().clone(),
            retention_policies: self.retention_policies.lock().unwrap().clone(),
            next_annotation_id: *self.next_annotation_id.lock().unwrap(),
            next_dataset_association_id: *self.next_dataset_association_id.lock().unwrap(),
            next_retention_audit_id: *self.next_retention_audit_id.lock().unwrap(),
            next_retention_policy_id: *self.next_retention_policy_id.lock().unwrap(),
        }
    }

    fn restore_metadata_state(&self, state: metadata::MetadataState) {
        *self.annotations.lock().unwrap() = state.annotations;
        *self.dataset_associations.lock().unwrap() = state.dataset_associations;
        *self.retention_audits.lock().unwrap() = state.retention_audits;
        *self.retention_policies.lock().unwrap() = state.retention_policies;
        *self.next_annotation_id.lock().unwrap() = state.next_annotation_id;
        *self.next_dataset_association_id.lock().unwrap() = state.next_dataset_association_id;
        *self.next_retention_audit_id.lock().unwrap() = state.next_retention_audit_id;
        *self.next_retention_policy_id.lock().unwrap() = state.next_retention_policy_id;
        self.rebuild_metadata_index();
    }

    fn rebuild_metadata_index(&self) {
        let annotations = self.annotations.lock().unwrap().clone();
        let dataset_associations = self.dataset_associations.lock().unwrap().clone();
        let retention_audits = self.retention_audits.lock().unwrap().clone();
        let retention_policies = self.retention_policies.lock().unwrap().clone();
        *self.metadata_index.lock().unwrap() = MetadataIndex::build(
            &annotations,
            &dataset_associations,
            &retention_audits,
            &retention_policies,
        );
    }

    fn try_acquire_process_lock(
        &self,
        name: &str,
    ) -> std::io::Result<Option<process_lock::ProcessLockGuard>> {
        self.process_lock
            .as_ref()
            .map(|mgr| mgr.acquire(name))
            .transpose()
    }

    fn acquire_process_lock(&self, name: &str) -> Option<process_lock::ProcessLockGuard> {
        self.try_acquire_process_lock(name)
            .expect("yiTrace process lock failed")
    }

    fn try_refresh_from_disk_for_read(&self) -> std::io::Result<()> {
        let _process = self.try_acquire_process_lock("write")?;
        let _local = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()
    }

    fn refresh_from_disk_for_read(&self) {
        self.try_refresh_from_disk_for_read()
            .expect("yiTrace read refresh failed");
    }

    pub(crate) fn try_refresh_from_disk_for_api(&self) -> std::io::Result<()> {
        self.try_refresh_from_disk_for_read()
    }

    fn refresh_from_disk_locked(&self) -> std::io::Result<()> {
        if self.manifest_path.is_none() {
            return Ok(());
        }
        self.graph.reload_if_changed_checked()?;
        // 兼容注入内存 graph 的旧持久化路径，向量写入可独立于 manifest/WAL。
        if self.vector_path.is_some() {
            self.reload_legacy_vectors_locked()?;
        }
        let persisted = persist::try_load(self.manifest_path.as_ref().unwrap())?;
        let old_tail = self.current.committed_tail();
        let (tail, tail_records) = self
            .wal
            .lock()
            .unwrap()
            .try_refresh_from_disk_after(WalLsn::new(old_tail))?;
        let disk_version = persisted
            .as_ref()
            .map(|s| s.manifest.version.get())
            .unwrap_or(0);
        let disk_watermark = persisted
            .as_ref()
            .map(|s| s.manifest.memtable_watermark.get())
            .unwrap_or(0);
        let manifest_changed = disk_version != self.current.version()
            || disk_watermark != self.current.memtable_watermark();
        self.refresh_metadata_from_disk_locked();
        if !manifest_changed {
            if tail.get() != old_tail {
                if !tail_records.is_empty() {
                    // 另一个进程只追加了 WAL、尚未 flush。先补齐历史派生索引，再叠加这段增量。
                    self.ensure_all_read_models_current_locked();
                }
                self.apply_wal_tail_records_locked(tail_records);
                self.current.advance_committed_tail(tail);
            }
            return Ok(());
        }
        if let Some(state) = persisted {
            self.current.replace_from_disk(state.manifest);
            *self.next_segment_id.lock().unwrap() = state.next_segment_id;
            *self.next_chunk_id.lock().unwrap() = state.next_chunk_id;
        }
        self.rebuild_volatile_from_current_locked()?;
        Ok(())
    }

    fn apply_wal_tail_records_locked(&self, rows: Vec<(u64, WalRecord)>) {
        if rows.is_empty() {
            return;
        }
        let watermark = self.current.memtable_watermark();
        let mut mt = self.memtable.lock().unwrap();
        for (lsn, r) in rows {
            if lsn <= watermark {
                continue;
            }
            self.index_record(&r);
            if mt.newest_lsn().is_some_and(|last| lsn <= last) {
                continue;
            }
            mt.append(MemRow {
                commit_lsn: lsn,
                trace_id: r.trace_id,
                span_id: r.span_id,
                ts: r.ts,
                identity: r.identity,
                fields: r.fields,
            });
        }
    }

    fn refresh_metadata_from_disk_locked(&self) {
        let Some(path) = &self.metadata_path else {
            return;
        };
        let Some(state) = metadata::load(path) else {
            return;
        };
        *self.annotations.lock().unwrap() = state.annotations;
        *self.dataset_associations.lock().unwrap() = state.dataset_associations;
        *self.retention_audits.lock().unwrap() = state.retention_audits;
        *self.retention_policies.lock().unwrap() = state.retention_policies;
        *self.next_annotation_id.lock().unwrap() = state.next_annotation_id;
        *self.next_dataset_association_id.lock().unwrap() = state.next_dataset_association_id;
        *self.next_retention_audit_id.lock().unwrap() = state.next_retention_audit_id;
        *self.next_retention_policy_id.lock().unwrap() = state.next_retention_policy_id;
        self.rebuild_metadata_index();
    }

    fn clear_volatile_indexes_locked(&self) -> std::io::Result<()> {
        // 外部 flush 后仍有旧 snapshot 读取旧 memtable 区间，按所有本地读者水位保留。
        self.memtable
            .lock()
            .unwrap()
            .evict_up_to(WalLsn::new(self.current.min_retained_watermark()));
        self.clear_segment_scan_indexes_locked();
        let durable = self.manifest_path.is_some();
        *self.segment_scan_indexes_stale.lock().unwrap() = durable;
        self.graph.clear();
        self.graph.reload_checked()?;
        *self.filter_attrs.lock().unwrap() = FilterAttrsIndex::default();
        *self.trace_rollup.lock().unwrap() = TraceAggregateRollupIndex::default();
        *self.read_model_load_state.lock().unwrap() = if durable {
            ReadModelLoadState::deferred()
        } else {
            ReadModelLoadState::ready()
        };
        Ok(())
    }

    fn clear_segment_scan_indexes_locked(&self) {
        self.bm25.clear();
        *self.session_idx.lock().unwrap() = SessionIndex::default();
        *self.seg_fold_cache.lock().unwrap() = SegFoldCache::new(2_000_000);
        self.seg_key_bloom.lock().unwrap().clear();
        *self.seg_key_bloom_load_failed_for.lock().unwrap() = None;
    }

    fn rebuild_volatile_from_current_locked(&self) -> std::io::Result<usize> {
        self.clear_volatile_indexes_locked()?;
        let m = self.current.manifest();
        let segment_derived_dirty = m
            .segments
            .values()
            .any(|entry| entry.deletion_seq > 0 || entry.upgrade_ref.is_some());
        let seg_count = m.segments.len();
        drop(m);
        // 正常重启只恢复控制面。四份大型派生索引由第一次相关查询加载；第一次写入前会全部补齐。
        // delete/upgrade 只会让缓存失效，不影响主数据正确性，惰性加载时会自动走段重建。
        *self.segment_scan_indexes_stale.lock().unwrap() = self.manifest_path.is_some();
        self.session_idx.lock().unwrap().dirty = true;
        self.reload_legacy_vectors_locked()?;
        self.replay_wal_tail_into_memtable_locked()?;
        olog::log(
            olog::Level::Info,
            "recover_lazy_ready",
            &[
                ("segments_deferred", &seg_count),
                ("derived_dirty", &segment_derived_dirty),
            ],
        );
        Ok(0)
    }

    fn reload_legacy_vectors_locked(&self) -> std::io::Result<()> {
        if let Some(p) = &self.vector_path {
            self.graph.clear();
            for ((t, s), v) in vecstore::load(p) {
                self.graph.index_embedding_scoped(None, t, s, v)?;
            }
            for ((tenant, t, s), vector) in
                vecstore::load_scoped(p.with_file_name("vectors_scoped.dat"))
            {
                self.graph.index_embedding_scoped(tenant, t, s, vector)?;
            }
        }
        Ok(())
    }

    fn replay_wal_tail_into_memtable_locked(&self) -> std::io::Result<()> {
        let (records, tail) = {
            let wal = self.wal.lock().unwrap();
            (
                wal.try_replay_after(WalLsn::new(self.current.memtable_watermark()))?,
                wal.committed_tail(),
            )
        };
        // 恢复时 Current 的初始尾为 0；先在 writer 临界区内设置合法快照上界，
        // 使派生模型从 (manifest watermark, committed tail] 读取时不会得到倒置区间。
        self.current.advance_committed_tail(tail);
        if !records.is_empty() {
            // WAL tail 必须叠加到完整派生索引上，不能先写空索引再被持久 cache 覆盖。
            self.ensure_all_read_models_current_locked();
        }
        let mut mt = self.memtable.lock().unwrap();
        for (lsn, r) in records {
            self.index_record(&r);
            if mt.newest_lsn().is_some_and(|last| lsn <= last) {
                continue;
            }
            mt.append(MemRow {
                commit_lsn: lsn,
                trace_id: r.trace_id,
                span_id: r.span_id,
                ts: r.ts,
                identity: r.identity.clone(),
                fields: r.fields.clone(),
            });
        }
        self.current.advance_committed_tail(tail);
        Ok(())
    }

    fn ensure_segment_scan_indexes_current(&self) {
        self.try_ensure_segment_scan_indexes_current()
            .expect("yiTrace index refresh failed");
    }

    fn try_ensure_segment_scan_indexes_current(&self) -> std::io::Result<()> {
        if !*self.segment_scan_indexes_stale.lock().unwrap() {
            return Ok(());
        }
        let _process = self.try_acquire_process_lock("write")?;
        let _local = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        self.ensure_segment_scan_indexes_current_locked();
        Ok(())
    }

    /// 单 Span 点查只需要知道目标 key 可能位于哪些 segment。
    /// clean reopen 时先按当前快照加载小型 bloom sidecar，避免为了定位一个 Span 对所有段做完整 CRC；
    /// BM25 仍保持 deferred，不把全文索引的冷启动成本带进详情读取。
    fn ensure_seg_key_bloom_for_manifest(&self, manifest: &Manifest) -> FoldQueryStats {
        let manifest_key = (manifest.version.get(), manifest.memtable_watermark.get());
        let covers_manifest = || {
            let blooms = self.seg_key_bloom.lock().unwrap();
            manifest
                .segments
                .keys()
                .all(|segment_id| blooms.contains_key(segment_id))
        };
        if covers_manifest() {
            return FoldQueryStats::default();
        }
        if *self.seg_key_bloom_load_failed_for.lock().unwrap() == Some(manifest_key) {
            return FoldQueryStats::default();
        }

        let _load = self.seg_key_bloom_load_lock.lock().unwrap();
        if covers_manifest() {
            return FoldQueryStats::default();
        }
        if *self.seg_key_bloom_load_failed_for.lock().unwrap() == Some(manifest_key) {
            return FoldQueryStats::default();
        }
        // 缺失、旧版或损坏时不能信任派生 sidecar。这里从真实 segment 一次性重建并原子写成
        // 当前格式；这样升级用户只承担一次迁移成本，之后即使进程重启也仍走冷读快路。
        if self.load_seg_key_bloom_segments(manifest) {
            *self.seg_key_bloom_load_failed_for.lock().unwrap() = None;
        } else {
            let (rebuilt, stats) = self.rebuild_and_persist_seg_key_bloom_current();
            if rebuilt && covers_manifest() {
                *self.seg_key_bloom_load_failed_for.lock().unwrap() = None;
            } else {
                *self.seg_key_bloom_load_failed_for.lock().unwrap() = Some(manifest_key);
            }
            return stats;
        }
        FoldQueryStats::default()
    }

    fn rebuild_and_persist_seg_key_bloom_current(&self) -> (bool, FoldQueryStats) {
        self.try_rebuild_and_persist_seg_key_bloom_current()
            .expect("yiTrace bloom refresh failed")
    }

    fn try_rebuild_and_persist_seg_key_bloom_current(
        &self,
    ) -> std::io::Result<(bool, FoldQueryStats)> {
        let mut stats = FoldQueryStats {
            fallback_reason: Some("segment_bloom_migrated".to_string()),
            ..FoldQueryStats::default()
        };
        let Some(path) = &self.seg_key_bloom_path else {
            return Ok((false, stats));
        };
        let _process = self.try_acquire_process_lock("write")?;
        let _local = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        let manifest = self.current.manifest();
        let mut rebuilt = HashMap::new();
        if rebuilt.try_reserve(manifest.segments.len()).is_err() {
            return Ok((false, stats));
        }
        for entry in manifest.segments.values() {
            let scan = self.segments.scan_records_with_stats(entry.segment_id);
            stats.scanned_segments += 1;
            stats.decoded_segment_rows += scan.rows.len();
            stats.data_bytes_read = stats.data_bytes_read.saturating_add(scan.data_bytes_read);
            // 已提交的 segment 不会为空；空结果说明数据段缺失或校验失败，此时不能生成会有
            // 假阴性的 bloom，只能保留逐段点查回退。
            if scan.rows.is_empty() {
                return Ok((false, stats));
            }
            rebuilt.insert(
                entry.segment_id.get(),
                Arc::new(KeyBloom::build(
                    scan.rows
                        .iter()
                        .map(|record| (record.trace_id, record.span_id)),
                    scan.rows.len(),
                )),
            );
        }
        let result = save_seg_key_bloom_cache(
            path,
            manifest.version.get(),
            manifest.memtable_watermark.get(),
            &manifest,
            &rebuilt,
        );
        if let Err(err) = result {
            olog::log(
                olog::Level::Warn,
                "segment_bloom_cache_migrate_failed",
                &[("error", &err.to_string())],
            );
            return Ok((false, stats));
        }
        self.seg_key_bloom.lock().unwrap().extend(rebuilt);
        olog::log(
            olog::Level::Info,
            "segment_bloom_cache_migrated",
            &[
                ("segments", &manifest.segments.len()),
                ("version", &manifest.version.get()),
                ("watermark", &manifest.memtable_watermark.get()),
            ],
        );
        Ok((true, stats))
    }

    fn ensure_segment_scan_indexes_current_locked(&self) {
        if !*self.segment_scan_indexes_stale.lock().unwrap() {
            return;
        }
        let manifest = self.current.manifest();
        let derived_dirty = manifest
            .segments
            .values()
            .any(|entry| entry.deletion_seq > 0 || entry.upgrade_ref.is_some());
        let loaded = !derived_dirty
            && self.load_bm25_segments(&manifest)
            && self.load_seg_key_bloom_segments(&manifest);
        drop(manifest);
        let scanned = if loaded {
            0
        } else {
            self.rebuild_segment_scan_indexes_locked()
        };
        *self.segment_scan_indexes_stale.lock().unwrap() = false;
        if !loaded {
            self.persist_bm25_segments();
            self.persist_seg_key_bloom_segments();
        }
        olog::log(
            olog::Level::Info,
            "segment_scan_indexes_ready",
            &[("segs_scanned", &scanned), ("cache_loaded", &loaded)],
        );
    }

    fn ensure_all_read_models_current_locked(&self) {
        self.ensure_trace_rollup_current_locked();
        self.ensure_filter_attrs_current_locked();
        self.ensure_segment_scan_indexes_current_locked();
    }

    fn rebuild_segment_scan_indexes_locked(&self) -> usize {
        self.clear_segment_scan_indexes_locked();
        let mut scanned = 0usize;
        let m = self.current.manifest();
        let has_patches = m
            .segments
            .values()
            .any(|entry| entry.deletion_seq > 0 || entry.upgrade_ref.is_some());
        for entry in m.segments.values() {
            let recs = self.segments.scan_records(entry.segment_id);
            scanned += 1;
            let bloom = KeyBloom::build(recs.iter().map(|r| (r.trace_id, r.span_id)), recs.len());
            self.seg_key_bloom
                .lock()
                .unwrap()
                .insert(entry.segment_id.get(), Arc::new(bloom));
            for r in &recs {
                self.index_record_without_rollup_and_filter_attrs(r);
            }
        }
        drop(m);

        let mem_rows = {
            let mt = self.memtable.lock().unwrap();
            mt.read_range(
                WalLsn::new(self.current.memtable_watermark()),
                WalLsn::new(self.current.committed_tail()),
            )
            .map(|r| WalRecord {
                trace_id: r.trace_id,
                span_id: r.span_id,
                ts: r.ts,
                identity: r.identity.clone(),
                fields: r.fields.clone(),
            })
            .collect::<Vec<_>>()
        };
        for r in &mem_rows {
            self.index_record_without_rollup_and_filter_attrs(r);
        }
        if has_patches {
            // 原始事件只用于建不可变 key 目录；删除和补写后的文本必须按可见 span 重建。
            self.rebuild_bm25_current();
            self.session_idx.lock().unwrap().dirty = true;
        }
        scanned
    }

    /// 先落盘草案再发布，普通写失败保留之前的 manifest，不允许推进水位或删除状态。
    fn commit_and_persist(&self, draft: Manifest) -> std::io::Result<()> {
        self.graph.flush_checked()?;
        if let Some(path) = &self.manifest_path {
            persist::save(
                path,
                &persist::PersistedState {
                    manifest: draft.clone(),
                    next_segment_id: *self.next_segment_id.lock().unwrap(),
                    next_chunk_id: *self.next_chunk_id.lock().unwrap(),
                },
            )?;
        }
        self.current.commit(draft);
        Ok(())
    }

    /// 兼容入口的 panic 在 try 方法释放锁之后发生，避免普通 I/O 失败污染互斥锁。
    pub fn pin_snapshot(&self) -> Snapshot {
        self.try_pin_snapshot().expect("yiTrace reader pin failed")
    }

    /// 刷新、外部 pin、本地 pin 在同一个 writer 临界区内完成，GC 无法穿过三者之间。
    pub fn try_pin_snapshot(&self) -> std::io::Result<Snapshot> {
        let _process = self.try_acquire_process_lock("write")?;
        let _local = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        if let Some(mgr) = &self.process_lock {
            let guard = mgr.pin_reader()?;
            Ok(self
                .current
                .pin_snapshot_with_external_guard(Box::new(guard)))
        } else {
            Ok(self.current.pin_snapshot())
        }
    }

    pub fn process_lock_metrics(&self) -> Option<ProcessLockMetricsSnapshot> {
        self.process_lock.as_ref().map(|mgr| mgr.metrics_snapshot())
    }

    pub fn process_lock_metrics_json(&self) -> String {
        let Some(m) = self.process_lock_metrics() else {
            return r#"{"enabled":false,"acquire_count":0,"try_acquire_count":0,"wait_count":0,"active_wait_count":0,"wait_ns":0,"wait_ms":0.0,"timeout_count":0,"try_busy_count":0,"stale_lock_cleared_count":0,"reader_pin_count":0,"stale_reader_cleared_count":0}"#.to_string();
        };
        format!(
            "{{\"enabled\":true,\"acquire_count\":{},\"try_acquire_count\":{},\"wait_count\":{},\"active_wait_count\":{},\"wait_ns\":{},\"wait_ms\":{},\"timeout_count\":{},\"try_busy_count\":{},\"stale_lock_cleared_count\":{},\"reader_pin_count\":{},\"stale_reader_cleared_count\":{}}}",
            m.acquire_count,
            m.try_acquire_count,
            m.wait_count,
            m.active_wait_count,
            m.wait_ns,
            (m.wait_ns as f64) / 1_000_000.0,
            m.timeout_count,
            m.try_busy_count,
            m.stale_lock_cleared_count,
            m.reader_pin_count,
            m.stale_reader_cleared_count,
        )
    }
}
