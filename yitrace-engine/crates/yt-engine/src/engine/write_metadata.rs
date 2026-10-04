impl WriteCoordinator {
    /// 给 trace/span 加一条标注。常见用途：人工确认失败原因、标记好路径、记录审核结论。
    pub fn add_annotation(
        &self,
        input: NewTraceAnnotation,
        tenant_id: Option<u64>,
    ) -> TraceAnnotation {
        self.try_add_annotation(input, tenant_id)
            .expect("yiTrace annotation save failed")
    }

    pub fn try_add_annotation(
        &self,
        mut input: NewTraceAnnotation,
        tenant_id: Option<u64>,
    ) -> std::io::Result<TraceAnnotation> {
        let _process = self.try_acquire_process_lock("write")?;
        let _guard = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        let before = self.capture_metadata_state();
        let annotation_id = {
            let mut next = self.next_annotation_id.lock().unwrap();
            let id = *next;
            *next = id.saturating_add(1);
            id
        };
        let now = metadata::now_ns();
        let target = input.target.unwrap_or(if input.span_id.is_some() {
            AnnotationTarget::Span
        } else {
            AnnotationTarget::Trace
        });
        let annotation = TraceAnnotation {
            annotation_id,
            tenant_id,
            target,
            trace_id: input.trace_id,
            span_id: input.span_id,
            external_trace_id: input.external_trace_id.take(),
            external_span_id: input.external_span_id.take(),
            label: input.label,
            score: input.score,
            reason: input.reason.take(),
            source: input.source.take(),
            created_at_ns: now,
            updated_at_ns: now,
            status: input.status.unwrap_or(AnnotationStatus::Active),
            reviewer: input.reviewer.take(),
            attrs: input.attrs,
        };
        self.annotations.lock().unwrap().push(annotation.clone());
        self.metadata_index
            .lock()
            .unwrap()
            .add_annotation(&annotation);
        if let Err(err) = self.persist_metadata() {
            self.restore_metadata_state(before);
            return Err(err);
        }
        Ok(annotation)
    }

    /// 查询标注。默认隐藏 Deleted；需要回收站视图时设置 `include_deleted=true`。
    pub fn annotations(&self, filter: &TraceAnnotationFilter) -> Vec<TraceAnnotation> {
        self.try_annotations(filter)
            .expect("yiTrace annotation read failed")
    }

    pub fn try_annotations(
        &self,
        filter: &TraceAnnotationFilter,
    ) -> std::io::Result<Vec<TraceAnnotation>> {
        self.try_refresh_from_disk_for_read()?;
        let candidate_ids = self
            .metadata_index
            .lock()
            .unwrap()
            .annotation_candidates(filter);
        let mut out: Vec<TraceAnnotation> = self
            .annotations
            .lock()
            .unwrap()
            .iter()
            .filter(|a| candidate_ids.contains(&a.annotation_id) && annotation_matches(a, filter))
            .cloned()
            .collect();
        out.sort_by_key(|a| a.annotation_id);
        Ok(out)
    }

    /// 更新一条标注。`tenant_id=Some(x)` 时只能改本 tenant 的标注。
    pub fn update_annotation(
        &self,
        annotation_id: u64,
        tenant_id: Option<u64>,
        update: UpdateTraceAnnotation,
    ) -> Option<TraceAnnotation> {
        self.try_update_annotation(annotation_id, tenant_id, update)
            .expect("yiTrace annotation update failed")
    }

    pub fn try_update_annotation(
        &self,
        annotation_id: u64,
        tenant_id: Option<u64>,
        update: UpdateTraceAnnotation,
    ) -> std::io::Result<Option<TraceAnnotation>> {
        let _process = self.try_acquire_process_lock("write")?;
        let _guard = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        let before = self.capture_metadata_state();
        let updated = {
            let mut annotations = self.annotations.lock().unwrap();
            let Some(ann) = annotations.iter_mut().find(|a| {
                a.annotation_id == annotation_id && metadata_tenant_allowed(a.tenant_id, tenant_id)
            }) else {
                return Ok(None);
            };
            if let Some(label) = update.label {
                ann.label = label;
            }
            if let Some(score) = update.score {
                ann.score = score;
            }
            if let Some(reason) = update.reason {
                ann.reason = reason;
            }
            if let Some(source) = update.source {
                ann.source = source;
            }
            if let Some(status) = update.status {
                ann.status = status;
            }
            if let Some(reviewer) = update.reviewer {
                ann.reviewer = reviewer;
            }
            if let Some(attrs) = update.attrs {
                if update.merge_attrs {
                    for (k, v) in attrs {
                        ann.attrs.insert(k, v);
                    }
                } else {
                    ann.attrs = attrs;
                }
            }
            ann.updated_at_ns = metadata::now_ns();
            ann.clone()
        };
        self.rebuild_metadata_index();
        if let Err(err) = self.persist_metadata() {
            self.restore_metadata_state(before);
            return Err(err);
        }
        Ok(Some(updated))
    }

    /// 软删除标注：保留审计记录，把状态改成 Deleted。
    pub fn delete_annotation(
        &self,
        annotation_id: u64,
        tenant_id: Option<u64>,
        reviewer: Option<String>,
        reason: Option<String>,
    ) -> Option<TraceAnnotation> {
        self.update_annotation(
            annotation_id,
            tenant_id,
            UpdateTraceAnnotation {
                status: Some(AnnotationStatus::Deleted),
                reviewer: Some(reviewer),
                reason: Some(reason),
                ..Default::default()
            },
        )
    }

    pub fn try_delete_annotation(
        &self,
        annotation_id: u64,
        tenant_id: Option<u64>,
        reviewer: Option<String>,
        reason: Option<String>,
    ) -> std::io::Result<Option<TraceAnnotation>> {
        self.try_update_annotation(
            annotation_id,
            tenant_id,
            UpdateTraceAnnotation {
                status: Some(AnnotationStatus::Deleted),
                reviewer: Some(reviewer),
                reason: Some(reason),
                ..Default::default()
            },
        )
    }

    /// 把一条 trace/span 和外部数据集样本关联起来。它只记录“关系”，不复制 trace 大字段。
    pub fn add_dataset_association(
        &self,
        input: NewDatasetAssociation,
        tenant_id: Option<u64>,
    ) -> DatasetAssociation {
        self.try_add_dataset_association(input, tenant_id)
            .expect("yiTrace dataset association save failed")
    }

    pub fn try_add_dataset_association(
        &self,
        mut input: NewDatasetAssociation,
        tenant_id: Option<u64>,
    ) -> std::io::Result<DatasetAssociation> {
        let _process = self.try_acquire_process_lock("write")?;
        let _guard = self.write_lock.lock().unwrap();
        self.refresh_from_disk_locked()?;
        let before = self.capture_metadata_state();
        let association_id = {
            let mut next = self.next_dataset_association_id.lock().unwrap();
            let id = *next;
            *next = id.saturating_add(1);
            id
        };
        let association = DatasetAssociation {
            association_id,
            tenant_id,
            dataset_id: input.dataset_id,
            item_id: input.item_id,
            trace_id: input.trace_id,
            span_id: input.span_id,
            external_trace_id: input.external_trace_id.take(),
            external_span_id: input.external_span_id.take(),
            snapshot_id: input.snapshot_id.take(),
            snapshot_hash: input.snapshot_hash.take(),
            eval_run_id: input.eval_run_id.take(),
            split: input.split.take(),
            label: input.label.take(),
            score: input.score,
            created_at_ns: metadata::now_ns(),
            attrs: input.attrs,
        };
        self.dataset_associations
            .lock()
            .unwrap()
            .push(association.clone());
        self.metadata_index
            .lock()
            .unwrap()
            .add_dataset(&association);
        if let Err(err) = self.persist_metadata() {
            self.restore_metadata_state(before);
            return Err(err);
        }
        Ok(association)
    }

    /// 查询 trace/span 到数据集样本的关联。
    pub fn dataset_associations(
        &self,
        filter: &DatasetAssociationFilter,
    ) -> Vec<DatasetAssociation> {
        self.try_dataset_associations(filter)
            .expect("yiTrace dataset association read failed")
    }

    pub fn try_dataset_associations(
        &self,
        filter: &DatasetAssociationFilter,
    ) -> std::io::Result<Vec<DatasetAssociation>> {
        self.try_refresh_from_disk_for_read()?;
        let candidate_ids = self
            .metadata_index
            .lock()
            .unwrap()
            .dataset_candidates(filter);
        let mut out: Vec<DatasetAssociation> = self
            .dataset_associations
            .lock()
            .unwrap()
            .iter()
            .filter(|d| {
                candidate_ids.contains(&d.association_id) && dataset_association_matches(d, filter)
            })
            .cloned()
            .collect();
        out.sort_by_key(|d| d.association_id);
        Ok(out)
    }
}
