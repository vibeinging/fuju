impl WriteCoordinator {
    /// 给一组 trace 计算时间边界，用于 retention 判断“整条 trace 是否早于 cutoff”。
    pub fn trace_time_bounds(
        &self,
        snap: &Snapshot,
        trace_ids: &HashSet<u64>,
    ) -> BTreeMap<u64, (i64, i64)> {
        self.trace_time_bounds_for_tenant(snap, trace_ids, None)
    }

    pub fn trace_time_bounds_for_tenant(
        &self,
        snap: &Snapshot,
        trace_ids: &HashSet<u64>,
        tenant: Option<u64>,
    ) -> BTreeMap<u64, (i64, i64)> {
        let mut out = BTreeMap::<u64, (i64, i64)>::new();
        if trace_ids.is_empty() {
            return out;
        }
        for entry in snap.manifest.segments.values() {
            for (row, record) in self
                .segments
                .scan_records(entry.segment_id)
                .into_iter()
                .enumerate()
            {
                if entry.deletion_vec.is_deleted(row as u32)
                    || !trace_ids.contains(&record.trace_id)
                    || tenant.is_some_and(|t| record.fields.tenant_id != Some(t))
                {
                    continue;
                }
                let e = out.entry(record.trace_id).or_insert((record.ts, record.ts));
                e.0 = e.0.min(record.ts);
                e.1 = e.1.max(record.ts);
            }
        }
        let mt = self.memtable.lock().unwrap();
        for r in mt.read_range(snap.retained_watermark, snap.live_lsn) {
            if !trace_ids.contains(&r.trace_id)
                || tenant.is_some_and(|t| r.fields.tenant_id != Some(t))
            {
                continue;
            }
            let e = out.entry(r.trace_id).or_insert((r.ts, r.ts));
            e.0 = e.0.min(r.ts);
            e.1 = e.1.max(r.ts);
        }
        out
    }

    /// retention apply：只软删除已经 flush 到 segment 的 trace 行。
    ///
    /// 仍在 MemTable/WAL tail 的 trace 整条跳过，避免“半条 trace 被删、半条还活着”。
    pub fn delete_segment_rows_for_traces(
        &self,
        snap: &Snapshot,
        trace_ids: &HashSet<u64>,
    ) -> RetentionDeleteResult {
        self.delete_segment_rows_for_traces_for_tenant(snap, trace_ids, None)
    }

    pub fn delete_segment_rows_for_traces_for_tenant(
        &self,
        snap: &Snapshot,
        trace_ids: &HashSet<u64>,
        tenant: Option<u64>,
    ) -> RetentionDeleteResult {
        self.try_delete_segment_rows_for_traces_for_tenant(snap, trace_ids, tenant)
            .expect("retention storage failure")
    }
    pub fn try_delete_segment_rows_for_traces_for_tenant(
        &self,
        _snap: &Snapshot,
        trace_ids: &HashSet<u64>,
        tenant: Option<u64>,
    ) -> std::io::Result<RetentionDeleteResult> {
        // 计划之后可能有新写入；锁内重新检查当前热行，避免删除半条 trace。
        let _process = self.try_acquire_process_lock("write")?;
        let _w = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        let snap = self.current.pin_snapshot();
        let mut result = RetentionDeleteResult {
            requested_trace_count: trace_ids.len(),
            ..Default::default()
        };
        if trace_ids.is_empty() {
            return Ok(result);
        }

        let mut live_traces = HashSet::new();
        {
            let mt = self.memtable.lock().unwrap();
            for row in mt.read_range(snap.retained_watermark, snap.live_lsn) {
                if trace_ids.contains(&row.trace_id)
                    && tenant.is_none_or(|t| row.fields.tenant_id == Some(t))
                {
                    live_traces.insert(row.trace_id);
                }
            }
        }
        result.skipped_live_trace_ids = live_traces.iter().copied().collect();
        result.skipped_live_trace_ids.sort_unstable();
        result.skipped_live_trace_count = result.skipped_live_trace_ids.len();

        let deletable: HashSet<u64> = trace_ids.difference(&live_traces).copied().collect();
        let mut rows_by_segment: BTreeMap<u64, Vec<(u32, u64)>> = BTreeMap::new();
        for entry in snap.manifest.segments.values() {
            for (row, fi) in self.segments.scan_fold_inputs(entry.segment_id) {
                if entry.deletion_vec.is_deleted(row)
                    || !deletable.contains(&fi.trace_id)
                    || tenant.is_some_and(|t| fi.fields.tenant_id != Some(t))
                {
                    continue;
                }
                rows_by_segment
                    .entry(entry.segment_id.get())
                    .or_default()
                    .push((row, fi.trace_id));
            }
        }
        if rows_by_segment.is_empty() {
            return Ok(result);
        }

        let mut draft = self.current.cow_next();
        let mut deleted_traces = HashSet::new();
        let mut deleted_rows = 0usize;
        for (seg_id, rows) in rows_by_segment {
            let Some(entry) = draft.segments.get_mut(&seg_id) else {
                continue;
            };
            let mut new_dv = (*entry.deletion_vec).clone();
            for (row, trace_id) in rows {
                if new_dv.is_deleted(row) {
                    continue;
                }
                let chunk_id = {
                    let mut g = self.next_chunk_id.lock().unwrap();
                    let id = *g;
                    *g += 1;
                    yt_core::ids::ChunkId::new(id)
                };
                new_dv = new_dv.with_deleted(row, chunk_id);
                deleted_rows += 1;
                deleted_traces.insert(trace_id);
            }
            entry.deletion_vec = Arc::new(new_dv);
            entry.deletion_seq += 1;
        }
        self.commit_and_persist(draft)?;
        self.session_idx.lock().unwrap().dirty = true;
        self.rebuild_trace_rollup_current();
        self.rebuild_filter_attrs_current();
        self.rebuild_bm25_current();
        *self.segment_scan_indexes_stale.lock().unwrap() = false;
        self.persist_read_model_sidecars();
        result.deleted_trace_ids = deleted_traces.into_iter().collect();
        result.deleted_trace_ids.sort_unstable();
        result.deleted_trace_count = result.deleted_trace_ids.len();
        result.deleted_segment_row_count = deleted_rows;
        Ok(result)
    }

    /// 可选压实：挑出 deletion ratio 达标的段，把删除位物化进新段并尝试安全回收旧段。
    pub fn compact_deleted_segments(
        &self,
        max_segments: usize,
        min_deleted_rows: u32,
        min_deleted_percent: u32,
        reclaim_after: bool,
    ) -> RetentionCompactResult {
        self.try_compact_deleted_segments(
            max_segments,
            min_deleted_rows,
            min_deleted_percent,
            reclaim_after,
        )
        .expect("retention compaction failed")
    }
    pub fn try_compact_deleted_segments(
        &self,
        max_segments: usize,
        min_deleted_rows: u32,
        min_deleted_percent: u32,
        reclaim_after: bool,
    ) -> std::io::Result<RetentionCompactResult> {
        let snap = self.try_pin_snapshot()?;
        let before_live_segment_count = snap.manifest.segments.len();
        let before_dead_segment_count = self.dead_set.lock().unwrap().len();
        let mut selected = Vec::new();
        let mut dropped_deleted_row_count = 0usize;
        let mut rewritten_live_row_count = 0usize;
        for entry in snap.manifest.segments.values() {
            if selected.len() >= max_segments {
                break;
            }
            let rows = self.segments.scan_records(entry.segment_id);
            let total = rows.len() as u32;
            if total == 0 {
                continue;
            }
            let deleted = (0..total)
                .filter(|row| entry.deletion_vec.is_deleted(*row))
                .count() as u32;
            if deleted < min_deleted_rows {
                continue;
            }
            if deleted.saturating_mul(100) < total.saturating_mul(min_deleted_percent) {
                continue;
            }
            selected.push(entry.segment_id);
            dropped_deleted_row_count += deleted as usize;
            rewritten_live_row_count += total.saturating_sub(deleted) as usize;
        }
        drop(snap);

        let selected_segment_ids = selected.iter().map(|s| s.get()).collect::<Vec<_>>();
        for seg in &selected {
            self.try_commit_compaction(&[*seg])?;
        }
        let reclaimed_segment_count = if reclaim_after {
            self.try_reclaim()?
        } else {
            0
        };
        let after = self.try_pin_snapshot()?;
        Ok(RetentionCompactResult {
            before_live_segment_count,
            after_live_segment_count: after.manifest.segments.len(),
            before_dead_segment_count,
            after_dead_segment_count: self.dead_set.lock().unwrap().len(),
            selected_segment_count: selected_segment_ids.len(),
            compacted_segment_count: selected_segment_ids.len(),
            reclaimed_segment_count,
            dropped_deleted_row_count,
            rewritten_live_row_count,
            selected_segment_ids,
        })
    }

    pub fn add_retention_audit(
        &self,
        input: NewRetentionAuditRecord,
        tenant_id: Option<u64>,
    ) -> RetentionAuditRecord {
        self.try_add_retention_audit(input, tenant_id)
            .expect("retention metadata write failed")
    }
    pub fn try_add_retention_audit(
        &self,
        input: NewRetentionAuditRecord,
        tenant_id: Option<u64>,
    ) -> std::io::Result<RetentionAuditRecord> {
        let _process = self.try_acquire_process_lock("write")?;
        let _guard = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        let before = self.capture_metadata_state();
        let audit_id = {
            let mut next = self.next_retention_audit_id.lock().unwrap();
            let id = *next;
            *next = id.saturating_add(1);
            id
        };
        let audit = RetentionAuditRecord {
            audit_id,
            tenant_id,
            created_at_ns: metadata::now_ns(),
            source: input.source,
            reason: input.reason,
            delete_before_ts: input.delete_before_ts,
            query_json: input.query_json,
            protect_annotations: input.protect_annotations,
            protect_dataset_associations: input.protect_dataset_associations,
            protect_snapshots: input.protect_snapshots,
            protect_eval_links: input.protect_eval_links,
            protect_path_memory: input.protect_path_memory,
            compact_requested: input.compact_requested,
            compact_reclaim: input.compact_reclaim,
            candidate_trace_count: input.candidate_trace_count,
            protected_trace_count: input.protected_trace_count,
            deletable_trace_count: input.deletable_trace_count,
            requested_trace_count: input.requested_trace_count,
            deleted_trace_count: input.deleted_trace_count,
            deleted_segment_row_count: input.deleted_segment_row_count,
            skipped_live_trace_count: input.skipped_live_trace_count,
            compacted_segment_count: input.compacted_segment_count,
            reclaimed_segment_count: input.reclaimed_segment_count,
            dropped_deleted_row_count: input.dropped_deleted_row_count,
            rewritten_live_row_count: input.rewritten_live_row_count,
            deletable_trace_ids: input.deletable_trace_ids,
            deleted_trace_ids: input.deleted_trace_ids,
            skipped_live_trace_ids: input.skipped_live_trace_ids,
            trace_id_sample_truncated: input.trace_id_sample_truncated,
        };
        self.retention_audits.lock().unwrap().push(audit.clone());
        self.metadata_index.lock().unwrap().add_audit(&audit);
        if let Err(err) = self.persist_metadata() {
            self.restore_metadata_state(before);
            return Err(err);
        }
        Ok(audit)
    }

    pub fn retention_audits(&self, filter: &RetentionAuditFilter) -> Vec<RetentionAuditRecord> {
        self.refresh_from_disk_for_read();
        let candidate_ids = self.metadata_index.lock().unwrap().audit_candidates(filter);
        let mut out: Vec<RetentionAuditRecord> = self
            .retention_audits
            .lock()
            .unwrap()
            .iter()
            .filter(|a| candidate_ids.contains(&a.audit_id) && retention_audit_matches(a, filter))
            .cloned()
            .collect();
        out.sort_by_key(|a| a.audit_id);
        out
    }

    pub fn add_retention_policy(
        &self,
        input: NewRetentionPolicy,
        tenant_id: Option<u64>,
    ) -> RetentionPolicy {
        self.try_add_retention_policy(input, tenant_id)
            .expect("retention metadata write failed")
    }
    pub fn try_add_retention_policy(
        &self,
        input: NewRetentionPolicy,
        tenant_id: Option<u64>,
    ) -> std::io::Result<RetentionPolicy> {
        let _process = self.try_acquire_process_lock("write")?;
        let _guard = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        let before = self.capture_metadata_state();
        let policy_id = {
            let mut next = self.next_retention_policy_id.lock().unwrap();
            let id = *next;
            *next = id.saturating_add(1);
            id
        };
        let now = metadata::now_ns();
        let policy = RetentionPolicy {
            policy_id,
            tenant_id,
            name: input.name,
            enabled: input.enabled,
            created_at_ns: now,
            updated_at_ns: now,
            last_run_at_ns: None,
            next_run_at_ns: input.next_run_at_ns,
            interval_ns: input.interval_ns,
            source: input.source,
            reason: input.reason,
            query_json: input.query_json,
        };
        self.retention_policies.lock().unwrap().push(policy.clone());
        self.metadata_index.lock().unwrap().add_policy(&policy);
        if let Err(err) = self.persist_metadata() {
            self.restore_metadata_state(before);
            return Err(err);
        }
        Ok(policy)
    }

    pub fn retention_policies(&self, filter: &RetentionPolicyFilter) -> Vec<RetentionPolicy> {
        self.refresh_from_disk_for_read();
        let candidate_ids = self
            .metadata_index
            .lock()
            .unwrap()
            .policy_candidates(filter);
        let mut out: Vec<RetentionPolicy> = self
            .retention_policies
            .lock()
            .unwrap()
            .iter()
            .filter(|p| candidate_ids.contains(&p.policy_id) && retention_policy_matches(p, filter))
            .cloned()
            .collect();
        out.sort_by_key(|p| p.policy_id);
        out
    }

    pub fn mark_retention_policy_ran(
        &self,
        policy_id: u64,
        tenant_id: Option<u64>,
        now_ns: u64,
    ) -> Option<RetentionPolicy> {
        self.try_mark_retention_policy_ran(policy_id, tenant_id, now_ns)
            .expect("retention metadata write failed")
    }
    pub fn try_mark_retention_policy_ran(
        &self,
        policy_id: u64,
        tenant_id: Option<u64>,
        now_ns: u64,
    ) -> std::io::Result<Option<RetentionPolicy>> {
        let _process = self.try_acquire_process_lock("write")?;
        let _guard = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        let before = self.capture_metadata_state();
        let updated = {
            let mut policies = self.retention_policies.lock().unwrap();
            let Some(policy) = policies.iter_mut().find(|p| {
                p.policy_id == policy_id && metadata_tenant_allowed(p.tenant_id, tenant_id)
            }) else {
                return Ok(None);
            };
            policy.last_run_at_ns = Some(now_ns);
            policy.next_run_at_ns = Some(now_ns.saturating_add(policy.interval_ns));
            policy.updated_at_ns = metadata::now_ns();
            policy.clone()
        };
        if let Err(err) = self.persist_metadata() {
            self.restore_metadata_state(before);
            return Err(err);
        }
        Ok(Some(updated))
    }
}
