impl EngineJsonApi {
    fn storage_metadata_for_retention(&self, tenant: Option<u64>) -> StorageMetadata {
        StorageMetadata {
            annotations: self.coord.annotations(&crate::TraceAnnotationFilter {
                tenant_id: tenant,
                ..Default::default()
            }),
            dataset_associations: self.coord.dataset_associations(
                &crate::DatasetAssociationFilter {
                    tenant_id: tenant,
                    ..Default::default()
                },
            ),
        }
    }

    fn retention_plan_json(
        &self,
        body: &str,
        tenant: Option<u64>,
        route_apply: bool,
    ) -> (u16, String) {
        let v = match crate::wire::parse(body) {
            Ok(v) => v,
            Err(e) => return (400, format!(r#"{{"error":"{}"}}"#, e.replace('"', "'"))),
        };
        let config = match retention_plan_config_from_json(&v, route_apply) {
            Ok(config) => config,
            Err(e) => return (400, format!(r#"{{"error":"{}"}}"#, e.replace('"', "'"))),
        };
        let outcome = match self.retention_plan_for_config(body, &config, tenant) {
            Ok(outcome) => outcome,
            Err(e) => {
                return (
                    if e.starts_with("storage failure:") {
                        500
                    } else {
                        400
                    },
                    format!(r#"{{"error":"{}"}}"#, e.replace('"', "'")),
                )
            }
        };
        (200, json_retention_plan(&config, &outcome))
    }

    fn retention_plan_for_config(
        &self,
        body: &str,
        config: &RetentionPlanConfig,
        tenant: Option<u64>,
    ) -> Result<RetentionPlanOutcome, String> {
        let read = self.trace_search_spans(body, tenant)?;
        let spans = read.spans;
        let all_trace_ids = spans
            .iter()
            .map(|span| span.trace_id)
            .collect::<std::collections::HashSet<_>>();
        let snap = self.coord.pin_snapshot();
        let bounds = self
            .coord
            .trace_time_bounds_for_tenant(&snap, &all_trace_ids, tenant);
        let mut candidate_trace_ids = std::collections::HashSet::new();
        for trace_id in &all_trace_ids {
            let Some((_, max_ts)) = bounds.get(trace_id) else {
                continue;
            };
            if config
                .cutoff
                .map(|cutoff| *max_ts <= cutoff)
                .unwrap_or(true)
            {
                candidate_trace_ids.insert(*trace_id);
            }
        }

        let metadata = self.storage_metadata_for_retention(tenant);
        let protected = protected_trace_reasons(&candidate_trace_ids, &metadata, config);
        let protected_trace_ids = protected
            .keys()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        let deletable_trace_ids = candidate_trace_ids
            .difference(&protected_trace_ids)
            .copied()
            .collect::<std::collections::HashSet<_>>();

        let candidate_stats = storage_bucket_for_trace_ids(&spans, &candidate_trace_ids);
        let protected_stats = storage_bucket_for_trace_ids(&spans, &protected_trace_ids);
        let deletable_stats = storage_bucket_for_trace_ids(&spans, &deletable_trace_ids);
        let applied = if config.apply {
            Some(
                self.coord
                    .try_delete_segment_rows_for_traces_for_tenant(
                        &snap,
                        &deletable_trace_ids,
                        tenant,
                    )
                    .map_err(|e| format!("storage failure: {e}"))?,
            )
        } else {
            None
        };
        drop(snap);
        let compacted = if config.apply && config.compact_after_apply {
            Some(self.coord.try_compact_deleted_segments(
                config.compact_max_segments,
                config.compact_min_deleted_rows,
                config.compact_min_deleted_percent,
                config.reclaim_after_compact,
            ).map_err(|e|format!("storage failure: retention deletion may already have committed; compaction failed: {e}"))?)
        } else {
            None
        };
        let audit = if config.apply {
            let sample_limit = 100;
            let deletable_sample = sample_u64_set(&deletable_trace_ids, sample_limit);
            let deleted_sample = applied
                .as_ref()
                .map(|result| sample_u64_slice(&result.deleted_trace_ids, sample_limit))
                .unwrap_or_default();
            let skipped_sample = applied
                .as_ref()
                .map(|result| sample_u64_slice(&result.skipped_live_trace_ids, sample_limit))
                .unwrap_or_default();
            let sample_truncated = deletable_trace_ids.len() > deletable_sample.len()
                || applied
                    .as_ref()
                    .map(|result| result.deleted_trace_ids.len() > deleted_sample.len())
                    .unwrap_or(false)
                || applied
                    .as_ref()
                    .map(|result| result.skipped_live_trace_ids.len() > skipped_sample.len())
                    .unwrap_or(false);
            Some(self.coord.try_add_retention_audit(
                crate::NewRetentionAuditRecord {
                    source: config.audit_source.clone(),
                    reason: config.audit_reason.clone(),
                    delete_before_ts: config.cutoff,
                    query_json: config.query_json.clone(),
                    protect_annotations: config.protect_annotations,
                    protect_dataset_associations: config.protect_dataset_associations,
                    protect_snapshots: config.protect_snapshots,
                    protect_eval_links: config.protect_eval_links,
                    protect_path_memory: config.protect_path_memory,
                    compact_requested: config.compact_after_apply,
                    compact_reclaim: config.reclaim_after_compact,
                    candidate_trace_count: candidate_stats.trace_ids.len() as u64,
                    protected_trace_count: protected_stats.trace_ids.len() as u64,
                    deletable_trace_count: deletable_stats.trace_ids.len() as u64,
                    requested_trace_count: applied
                        .as_ref()
                        .map(|result| result.requested_trace_count as u64)
                        .unwrap_or(0),
                    deleted_trace_count: applied
                        .as_ref()
                        .map(|result| result.deleted_trace_count as u64)
                        .unwrap_or(0),
                    deleted_segment_row_count: applied
                        .as_ref()
                        .map(|result| result.deleted_segment_row_count as u64)
                        .unwrap_or(0),
                    skipped_live_trace_count: applied
                        .as_ref()
                        .map(|result| result.skipped_live_trace_count as u64)
                        .unwrap_or(0),
                    compacted_segment_count: compacted
                        .as_ref()
                        .map(|result| result.compacted_segment_count as u64)
                        .unwrap_or(0),
                    reclaimed_segment_count: compacted
                        .as_ref()
                        .map(|result| result.reclaimed_segment_count as u64)
                        .unwrap_or(0),
                    dropped_deleted_row_count: compacted
                        .as_ref()
                        .map(|result| result.dropped_deleted_row_count as u64)
                        .unwrap_or(0),
                    rewritten_live_row_count: compacted
                        .as_ref()
                        .map(|result| result.rewritten_live_row_count as u64)
                        .unwrap_or(0),
                    deletable_trace_ids: deletable_sample,
                    deleted_trace_ids: deleted_sample,
                    skipped_live_trace_ids: skipped_sample,
                    trace_id_sample_truncated: sample_truncated,
                },
                tenant,
            ).map_err(|e|format!("storage failure: retention deletion may already have committed; audit failed: {e}"))?)
        } else {
            None
        };

        Ok(RetentionPlanOutcome {
            candidate_stats,
            protected_stats,
            deletable_stats,
            protected,
            deletable_trace_ids,
            applied,
            compacted,
            audit,
        })
    }

    fn retention_audits_query_json(&self, query: &str, tenant: Option<u64>) -> (u16, String) {
        let mut filter = crate::RetentionAuditFilter {
            tenant_id: tenant,
            ..Default::default()
        };
        let mut cursor = 0usize;
        let mut limit = 50usize;
        for (k, v) in query_pairs(query) {
            match k.as_str() {
                "audit_id" | "auditId" | "id" => filter.audit_id = v.parse::<u64>().ok(),
                "source" | "requestedBy" | "requested_by" | "actor" => filter.source = Some(v),
                "created_after_ns" | "createdAfterNs" | "minCreatedAtNs" => {
                    filter.min_created_at_ns = v.parse::<u64>().ok()
                }
                "created_before_ns" | "createdBeforeNs" | "maxCreatedAtNs" => {
                    filter.max_created_at_ns = v.parse::<u64>().ok()
                }
                "cursor" | "offset" => cursor = v.parse::<usize>().unwrap_or(0),
                "limit" => limit = v.parse::<usize>().unwrap_or(50).clamp(1, 500),
                _ => {}
            }
        }
        (200, self.retention_audits_page_json(filter, cursor, limit))
    }

    fn retention_audits_body_json(&self, body: &str, tenant: Option<u64>) -> (u16, String) {
        let v = match parse_json_body_or_empty(body) {
            Ok(v) => v,
            Err(e) => return (400, format!(r#"{{"error":"{}"}}"#, e.replace('"', "'"))),
        };
        let f = crate::wire::field(&v, "filter").unwrap_or(&v);
        let mut filter = crate::RetentionAuditFilter {
            tenant_id: tenant,
            ..Default::default()
        };
        filter.audit_id =
            json_field_alias(f, &["audit_id", "auditId", "id"]).and_then(crate::wire::Json::as_u64);
        filter.source = json_field_alias(f, &["source", "requestedBy", "requested_by", "actor"])
            .and_then(crate::wire::Json::as_str)
            .map(ToString::to_string);
        filter.min_created_at_ns =
            json_field_alias(f, &["created_after_ns", "createdAfterNs", "minCreatedAtNs"])
                .and_then(crate::wire::Json::as_u64);
        filter.max_created_at_ns = json_field_alias(
            f,
            &["created_before_ns", "createdBeforeNs", "maxCreatedAtNs"],
        )
        .and_then(crate::wire::Json::as_u64);
        let cursor = json_field_alias(&v, &["cursor", "offset"])
            .and_then(crate::wire::Json::as_u64)
            .unwrap_or(0) as usize;
        let limit = json_field_alias(&v, &["limit"])
            .and_then(crate::wire::Json::as_u64)
            .unwrap_or(50)
            .clamp(1, 500) as usize;
        (200, self.retention_audits_page_json(filter, cursor, limit))
    }

    fn retention_audits_page_json(
        &self,
        filter: crate::RetentionAuditFilter,
        cursor: usize,
        limit: usize,
    ) -> String {
        let mut items = self.coord.retention_audits(&filter);
        items.sort_by(|a, b| {
            b.created_at_ns
                .cmp(&a.created_at_ns)
                .then_with(|| b.audit_id.cmp(&a.audit_id))
        });
        let total = items.len();
        let end = cursor.saturating_add(limit).min(total);
        let page = if cursor < total {
            &items[cursor..end]
        } else {
            &[][..]
        };
        let body = page
            .iter()
            .map(json_retention_audit)
            .collect::<Vec<_>>()
            .join(",");
        let next = if end < total {
            end.to_string()
        } else {
            "null".to_string()
        };
        format!(
            r#"{{"items":[{}],"nextCursor":{},"total":{}}}"#,
            body, next, total
        )
    }

    fn create_retention_policy_json(&self, body: &str, tenant: Option<u64>) -> (u16, String) {
        let v = match crate::wire::parse(body) {
            Ok(v) => v,
            Err(e) => return (400, format!(r#"{{"error":"{}"}}"#, e.replace('"', "'"))),
        };
        let name = json_field_alias(&v, &["name", "policyName", "policy_name"])
            .and_then(crate::wire::Json::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        if name.is_empty() {
            return (400, r#"{"error":"missing name"}"#.to_string());
        }
        let interval_ns =
            json_field_alias(&v, &["intervalNs", "interval_ns", "everyNs", "every_ns"])
                .and_then(crate::wire::Json::as_u64)
                .unwrap_or(0);
        if interval_ns == 0 {
            return (400, r#"{"error":"missing intervalNs"}"#.to_string());
        }
        let Some(query) = json_field_alias(
            &v,
            &["query", "retention", "retentionQuery", "retention_query"],
        ) else {
            return (400, r#"{"error":"missing query"}"#.to_string());
        };
        if !matches!(query, crate::wire::Json::Obj(_)) {
            return (400, r#"{"error":"query must be an object"}"#.to_string());
        }
        if !retention_policy_query_has_cutoff(query) {
            return (
                400,
                r#"{"error":"query requires deleteBeforeTs or olderThanNs"}"#.to_string(),
            );
        }
        let now = unix_now_ns_u64_for_http();
        let policy = match self.coord.try_add_retention_policy(
            crate::NewRetentionPolicy {
                name,
                enabled: json_bool_alias(&v, &["enabled"]).unwrap_or(true),
                next_run_at_ns: json_field_alias(&v, &["nextRunAtNs", "next_run_at_ns"])
                    .and_then(crate::wire::Json::as_u64)
                    .or(Some(now)),
                interval_ns,
                source: json_field_alias(
                    &v,
                    &[
                        "source",
                        "requestedBy",
                        "requested_by",
                        "actor",
                        "createdBy",
                    ],
                )
                .and_then(crate::wire::Json::as_str)
                .map(ToString::to_string),
                reason: json_field_alias(&v, &["reason", "comment", "note"])
                    .and_then(crate::wire::Json::as_str)
                    .map(ToString::to_string),
                query_json: query.to_compact_json(),
            },
            tenant,
        ) {
            Ok(policy) => policy,
            Err(e) => {
                return (
                    500,
                    format!(r#"{{"error":"{}"}}"#, e.to_string().replace('"', "'")),
                )
            }
        };
        (200, json_retention_policy(&policy))
    }

    fn retention_policies_query_json(&self, query: &str, tenant: Option<u64>) -> (u16, String) {
        let mut filter = crate::RetentionPolicyFilter {
            tenant_id: tenant,
            ..Default::default()
        };
        let mut cursor = 0usize;
        let mut limit = 50usize;
        for (k, v) in query_pairs(query) {
            match k.as_str() {
                "policy_id" | "policyId" | "id" => filter.policy_id = v.parse::<u64>().ok(),
                "name" | "policyName" | "policy_name" => filter.name = Some(v),
                "enabled" => filter.enabled = Some(query_bool(&v)),
                "cursor" | "offset" => cursor = v.parse::<usize>().unwrap_or(0),
                "limit" => limit = v.parse::<usize>().unwrap_or(50).clamp(1, 500),
                _ => {}
            }
        }
        (
            200,
            self.retention_policies_page_json(filter, cursor, limit),
        )
    }

    fn retention_policies_page_json(
        &self,
        filter: crate::RetentionPolicyFilter,
        cursor: usize,
        limit: usize,
    ) -> String {
        let mut items = self.coord.retention_policies(&filter);
        items.sort_by_key(|p| p.policy_id);
        let total = items.len();
        let end = cursor.saturating_add(limit).min(total);
        let page = if cursor < total {
            &items[cursor..end]
        } else {
            &[][..]
        };
        let body = page
            .iter()
            .map(json_retention_policy)
            .collect::<Vec<_>>()
            .join(",");
        let next = if end < total {
            end.to_string()
        } else {
            "null".to_string()
        };
        format!(
            r#"{{"items":[{}],"nextCursor":{},"total":{}}}"#,
            body, next, total
        )
    }

    fn run_due_retention_policies_json(&self, body: &str, tenant: Option<u64>) -> (u16, String) {
        let v = match parse_json_body_or_empty(body) {
            Ok(v) => v,
            Err(e) => return (400, format!(r#"{{"error":"{}"}}"#, e.replace('"', "'"))),
        };
        let now = json_field_alias(&v, &["nowNs", "now_ns"])
            .and_then(crate::wire::Json::as_u64)
            .unwrap_or_else(unix_now_ns_u64_for_http);
        let limit = json_field_alias(&v, &["limit", "maxPolicies", "max_policies"])
            .and_then(crate::wire::Json::as_u64)
            .unwrap_or(10)
            .clamp(1, 100) as usize;
        let include_disabled =
            json_bool_alias(&v, &["includeDisabled", "include_disabled"]).unwrap_or(false);
        let mut filter = crate::RetentionPolicyFilter {
            tenant_id: tenant,
            ..Default::default()
        };
        filter.policy_id = json_field_alias(&v, &["policyId", "policy_id", "id"])
            .and_then(crate::wire::Json::as_u64);
        filter.name = json_field_alias(&v, &["name", "policyName", "policy_name"])
            .and_then(crate::wire::Json::as_str)
            .map(ToString::to_string);
        let mut due = Vec::new();
        let mut skipped = 0usize;
        for policy in self.coord.retention_policies(&filter) {
            if !include_disabled && !policy.enabled {
                skipped += 1;
                continue;
            }
            if policy
                .next_run_at_ns
                .map(|next| next <= now)
                .unwrap_or(false)
            {
                due.push(policy);
            } else {
                skipped += 1;
            }
        }
        due.sort_by(|a, b| {
            a.next_run_at_ns
                .cmp(&b.next_run_at_ns)
                .then_with(|| a.policy_id.cmp(&b.policy_id))
        });
        skipped += due.len().saturating_sub(limit);

        let mut ran = 0usize;
        let mut failed = 0usize;
        let mut items = Vec::new();
        for policy in due.into_iter().take(limit) {
            match retention_policy_effective_query(&policy, now) {
                Ok(query) => {
                    let (status, result) = self.retention_plan_json(&query, tenant, true);
                    if status == 200 {
                        let policy = match self.coord.try_mark_retention_policy_ran(
                            policy.policy_id,
                            tenant,
                            now,
                        ) {
                            Ok(updated) => updated.unwrap_or(policy),
                            Err(error) => {
                                failed += 1;
                                items.push(format!(r#"{{"policy":{},"ok":false,"statusCode":500,"error":{{"error":"retention applied; policy update failed: {}"}}}}"#,json_retention_policy(&policy),error.to_string().replace('"',"'")));
                                continue;
                            }
                        };
                        ran += 1;
                        items.push(format!(
                            r#"{{"policy":{},"ok":true,"statusCode":{},"result":{}}}"#,
                            json_retention_policy(&policy),
                            status,
                            result
                        ));
                    } else {
                        failed += 1;
                        items.push(format!(
                            r#"{{"policy":{},"ok":false,"statusCode":{},"error":{}}}"#,
                            json_retention_policy(&policy),
                            status,
                            result
                        ));
                    }
                }
                Err(error) => {
                    failed += 1;
                    items.push(format!(
                        r#"{{"policy":{},"ok":false,"statusCode":400,"error":{{"error":"{}"}}}}"#,
                        json_retention_policy(&policy),
                        json_escape(&error)
                    ));
                }
            }
        }
        (
            200,
            format!(
                r#"{{"nowNs":"{}","ran":{},"failed":{},"skipped":{},"items":[{}]}}"#,
                now,
                ran,
                failed,
                skipped,
                items.join(",")
            ),
        )
    }
}
