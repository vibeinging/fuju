impl EngineJsonApi {
    fn create_annotation_json(&self, body: &str, tenant: Option<u64>) -> (u16, String) {
        let v = match crate::wire::parse(body) {
            Ok(v) => v,
            Err(e) => return (400, format!(r#"{{"error":"{}"}}"#, e.replace('"', "'"))),
        };
        let Some((trace_id, external_trace_id)) =
            json_field_alias(&v, &["trace_id", "traceId"]).and_then(json_id_with_external)
        else {
            return (400, r#"{"error":"missing trace_id"}"#.to_string());
        };
        let span = json_field_alias(&v, &["span_id", "spanId"]).and_then(json_id_with_external);
        let label = json_field_alias(&v, &["label", "name"])
            .and_then(crate::wire::Json::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        if label.is_empty() {
            return (400, r#"{"error":"missing label"}"#.to_string());
        }
        let target = match json_field_alias(&v, &["target", "target_type", "targetType"]) {
            Some(raw) => match raw.as_str().and_then(AnnotationTarget::parse) {
                Some(target) => Some(target),
                None => return (400, r#"{"error":"invalid target"}"#.to_string()),
            },
            None => None,
        };
        let status = match json_raw_field_alias(&v, &["status"]) {
            Some(raw) => match raw.as_str().and_then(AnnotationStatus::parse) {
                Some(status) => Some(status),
                None => return (400, r#"{"error":"invalid status"}"#.to_string()),
            },
            None => None,
        };
        let mut attrs = std::collections::BTreeMap::new();
        collect_metadata_attr_map(&v, &mut attrs);
        let input = NewTraceAnnotation {
            target,
            trace_id,
            span_id: span.as_ref().map(|(id, _)| *id),
            external_trace_id: json_field_alias(&v, &["external_trace_id", "externalTraceId"])
                .and_then(crate::wire::Json::as_str)
                .map(ToString::to_string)
                .or(external_trace_id),
            external_span_id: json_field_alias(&v, &["external_span_id", "externalSpanId"])
                .and_then(crate::wire::Json::as_str)
                .map(ToString::to_string)
                .or_else(|| span.and_then(|(_, ext)| ext)),
            label,
            score: json_field_alias(&v, &["score", "eval_score", "evalScore"])
                .and_then(crate::wire::Json::as_u64)
                .map(|n| n.min(u32::MAX as u64) as u32),
            reason: json_field_alias(&v, &["reason", "comment", "note"])
                .and_then(crate::wire::Json::as_str)
                .map(ToString::to_string),
            source: json_field_alias(&v, &["source", "created_by", "createdBy"])
                .and_then(crate::wire::Json::as_str)
                .map(ToString::to_string),
            status,
            reviewer: json_field_alias(&v, &["reviewer", "reviewed_by", "reviewedBy"])
                .and_then(crate::wire::Json::as_str)
                .map(ToString::to_string),
            attrs,
        };
        match self.coord.try_add_annotation(input, tenant) {
            Ok(annotation) => (200, json_annotation(&annotation)),
            Err(err) => internal_error(err),
        }
    }

    /// GET /v1/annotations?traceId=&label=&attrs={}
    fn annotations_json(&self, query: &str, tenant: Option<u64>) -> (u16, String) {
        let (filter, cursor, limit) = annotation_filter_from_query(query, tenant);
        match self.coord.try_annotations(&filter) {
            Ok(items) => annotations_page_json(items, cursor, limit),
            Err(err) => internal_error(err),
        }
    }

    /// PATCH /v1/annotations/:id：更新审核状态、原因、标签或 attrs。
    fn update_annotation_json(&self, id: &str, body: &str, tenant: Option<u64>) -> (u16, String) {
        let Ok(annotation_id) = id.parse::<u64>() else {
            return (400, r#"{"error":"invalid annotation_id"}"#.to_string());
        };
        let update = match update_annotation_from_body(body) {
            Ok(update) => update,
            Err((status, body)) => return (status, body),
        };
        match self
            .coord
            .try_update_annotation(annotation_id, tenant, update)
        {
            Ok(Some(annotation)) => (200, json_annotation(&annotation)),
            Ok(None) => (404, r#"{"error":"annotation not found"}"#.to_string()),
            Err(err) => internal_error(err),
        }
    }

    /// DELETE /v1/annotations/:id：软删除，默认查询不再返回。
    fn delete_annotation_json(&self, id: &str, body: &str, tenant: Option<u64>) -> (u16, String) {
        let Ok(annotation_id) = id.parse::<u64>() else {
            return (400, r#"{"error":"invalid annotation_id"}"#.to_string());
        };
        let (reviewer, reason) = match delete_annotation_from_body(body) {
            Ok(parts) => parts,
            Err((status, body)) => return (status, body),
        };
        match self
            .coord
            .try_delete_annotation(annotation_id, tenant, reviewer, reason)
        {
            Ok(Some(annotation)) => (200, json_annotation(&annotation)),
            Ok(None) => (404, r#"{"error":"annotation not found"}"#.to_string()),
            Err(err) => internal_error(err),
        }
    }

    /// POST /v1/dataset-associations：把 trace/span 与外部回归集样本绑定。
    fn create_dataset_association_json(&self, body: &str, tenant: Option<u64>) -> (u16, String) {
        let v = match crate::wire::parse(body) {
            Ok(v) => v,
            Err(e) => return (400, format!(r#"{{"error":"{}"}}"#, e.replace('"', "'"))),
        };
        let dataset_id = json_field_alias(&v, &["dataset_id", "datasetId", "dataset"])
            .and_then(crate::wire::Json::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        if dataset_id.is_empty() {
            return (400, r#"{"error":"missing dataset_id"}"#.to_string());
        }
        let item_id = json_field_alias(
            &v,
            &["item_id", "itemId", "dataset_item_id", "datasetItemId"],
        )
        .and_then(crate::wire::Json::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
        if item_id.is_empty() {
            return (400, r#"{"error":"missing item_id"}"#.to_string());
        }
        let Some((trace_id, external_trace_id)) =
            json_field_alias(&v, &["trace_id", "traceId"]).and_then(json_id_with_external)
        else {
            return (400, r#"{"error":"missing trace_id"}"#.to_string());
        };
        let span = json_field_alias(&v, &["span_id", "spanId"]).and_then(json_id_with_external);
        let mut attrs = std::collections::BTreeMap::new();
        collect_metadata_attr_map(&v, &mut attrs);
        let input = NewDatasetAssociation {
            dataset_id,
            item_id,
            trace_id,
            span_id: span.as_ref().map(|(id, _)| *id),
            external_trace_id: json_field_alias(&v, &["external_trace_id", "externalTraceId"])
                .and_then(crate::wire::Json::as_str)
                .map(ToString::to_string)
                .or(external_trace_id),
            external_span_id: json_field_alias(&v, &["external_span_id", "externalSpanId"])
                .and_then(crate::wire::Json::as_str)
                .map(ToString::to_string)
                .or_else(|| span.and_then(|(_, ext)| ext)),
            snapshot_id: json_field_alias(&v, &["snapshot_id", "snapshotId"])
                .and_then(crate::wire::Json::as_str)
                .map(ToString::to_string),
            snapshot_hash: json_field_alias(&v, &["snapshot_hash", "snapshotHash"])
                .and_then(crate::wire::Json::as_str)
                .map(ToString::to_string),
            eval_run_id: json_field_alias(&v, &["eval_run_id", "evalRunId"])
                .and_then(crate::wire::Json::as_str)
                .map(ToString::to_string),
            split: json_field_alias(&v, &["split"])
                .and_then(crate::wire::Json::as_str)
                .map(ToString::to_string),
            label: json_field_alias(&v, &["label"])
                .and_then(crate::wire::Json::as_str)
                .map(ToString::to_string),
            score: json_field_alias(&v, &["score", "eval_score", "evalScore"])
                .and_then(crate::wire::Json::as_u64)
                .map(|n| n.min(u32::MAX as u64) as u32),
            attrs,
        };
        match self.coord.try_add_dataset_association(input, tenant) {
            Ok(assoc) => (200, json_dataset_association(&assoc)),
            Err(err) => internal_error(err),
        }
    }

    /// GET /v1/dataset-associations?datasetId=&itemId=&traceId=
    fn dataset_associations_json(&self, query: &str, tenant: Option<u64>) -> (u16, String) {
        let (filter, cursor, limit) = dataset_filter_from_query(query, tenant);
        match self.coord.try_dataset_associations(&filter) {
            Ok(items) => dataset_associations_page_json(items, cursor, limit),
            Err(err) => internal_error(err),
        }
    }

    // GET /v1/loops：按 loop_id 汇总。
}
