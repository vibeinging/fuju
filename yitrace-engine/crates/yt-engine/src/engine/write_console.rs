impl WriteCoordinator {
    /// 列出 trace 摘要（web 控制台列表视图）。按 trace_id 把折叠出的 span 聚合：span 数、总/最大耗时、报错数。
    /// 输出按 trace_id 升序，确定可复算。
    pub fn list_traces(&self, snap: &Snapshot, q: &TraceQuery) -> Vec<TraceSummary> {
        // 只读 status/耗时/token —— 不碰大文本列。
        let proj = Projection::of(
            Projection::STATUS
                | Projection::DURATION_NS
                | Projection::INPUT_TOKENS
                | Projection::OUTPUT_TOKENS
                | Projection::CACHE_READ_TOKENS
                | Projection::CACHE_WRITE_TOKENS
                | Projection::MODEL
                | Projection::EXTERNAL_IDS,
        );
        let (spans, _) = self.fold_query(snap, q, None, proj);
        let mut by_trace: BTreeMap<u64, TraceSummary> = BTreeMap::new();
        let mut coverage: BTreeMap<u64, CacheTokenCoverage> = BTreeMap::new();
        for s in spans {
            let e = by_trace.entry(s.trace_id).or_insert(TraceSummary {
                trace_id: s.trace_id,
                external_trace_id: s.external_trace_id.clone(),
                span_count: 0,
                total_duration_ns: 0,
                max_duration_ns: 0,
                error_count: 0,
                total_input_tokens: 0,
                total_output_tokens: 0,
                total_cache_read_tokens: None,
                total_cache_write_tokens: None,
                cache_read_reported_spans: 0,
                cache_write_reported_spans: 0,
                total_llm_spans: 0,
            });
            if e.external_trace_id.is_none() {
                e.external_trace_id = s.external_trace_id.clone();
            }
            e.span_count += 1;
            if let Some(d) = s.duration_ns {
                e.total_duration_ns += d;
                e.max_duration_ns = e.max_duration_ns.max(d);
            }
            if matches!(s.status, Some(st) if st != 0) {
                e.error_count += 1;
            }
            e.total_input_tokens += s.input_tokens.unwrap_or(0);
            e.total_output_tokens += s.output_tokens.unwrap_or(0);
            coverage.entry(s.trace_id).or_default().observe(&s);
        }
        for (trace_id, c) in coverage {
            if let Some(summary) = by_trace.get_mut(&trace_id) {
                summary.total_cache_read_tokens = c.read_total();
                summary.total_cache_write_tokens = c.write_total();
                summary.cache_read_reported_spans = c.read_reported_spans;
                summary.cache_write_reported_spans = c.write_reported_spans;
                summary.total_llm_spans = c.total_llm_spans;
            }
        }
        by_trace.into_values().collect()
    }

    /// 列出会话摘要（多轮会话视图）：按 session_id 聚合,数 trace 数/span 数/token 汇总。升序。
    pub fn list_sessions(&self, snap: &Snapshot, q: &TraceQuery) -> Vec<SessionSummary> {
        // 按 session 聚合 token —— 只读 session_id + token,跳过文本。
        let proj = Projection::of(
            Projection::SESSION_ID
                | Projection::INPUT_TOKENS
                | Projection::OUTPUT_TOKENS
                | Projection::CACHE_READ_TOKENS
                | Projection::CACHE_WRITE_TOKENS
                | Projection::MODEL,
        );
        let (spans, _) = self.fold_query(snap, q, None, proj);
        // session_id -> (distinct traces, span_count, in_tok, out_tok)
        let mut acc: BTreeMap<
            u64,
            (
                std::collections::HashSet<u64>,
                usize,
                u64,
                u64,
                CacheTokenCoverage,
            ),
        > = BTreeMap::new();
        for s in spans {
            if let Some(sid) = s.session_id {
                let e = acc.entry(sid).or_default();
                e.0.insert(s.trace_id);
                e.1 += 1;
                e.2 += s.input_tokens.unwrap_or(0);
                e.3 += s.output_tokens.unwrap_or(0);
                e.4.observe(&s);
            }
        }
        acc.into_iter()
            .map(
                |(session_id, (traces, span_count, i, o, c))| SessionSummary {
                    session_id,
                    trace_count: traces.len(),
                    span_count,
                    total_input_tokens: i,
                    total_output_tokens: o,
                    total_cache_read_tokens: c.read_total(),
                    total_cache_write_tokens: c.write_total(),
                    cache_read_reported_spans: c.read_reported_spans,
                    cache_write_reported_spans: c.write_reported_spans,
                    total_llm_spans: c.total_llm_spans,
                },
            )
            .collect()
    }

    /// 装一个会话的**多轮对话流**：把同一 `session_id` 的多条 trace（每条=一轮）按 trace_id 升序
    /// 拼成「用户问 → agent 答」的时间线。这是多轮会话视图的渲染源，也是会话级评测的输入。
    ///
    /// 取原文要读全列。当前没有 session→trace 倒排，按 session_id **扫全量过滤**（O(全库)）——
    /// 会话视图是低频操作可接受；真要高频再加 session 边车索引。
    pub fn load_session_timeline(&self, snap: &Snapshot, session_id: u64) -> SessionTimeline {
        self.load_session_timeline_query(snap, session_id, &TraceQuery::all())
    }

    /// 带查询约束的会话时间线。控制台网关用它把 tenant 过滤压到折叠读取层。
    pub fn load_session_timeline_query(
        &self,
        snap: &Snapshot,
        session_id: u64,
        q: &TraceQuery,
    ) -> SessionTimeline {
        let (spans, _) = self.read_spans_query(snap, q);
        // 按 trace 分组本会话的 span（BTreeMap → trace_id 升序 = 轮次序）。
        let mut by_trace: BTreeMap<u64, Vec<FoldedSpan>> = BTreeMap::new();
        for s in spans {
            if s.session_id == Some(session_id) {
                by_trace.entry(s.trace_id).or_default().push(s);
            }
        }
        let mut turns = Vec::with_capacity(by_trace.len());
        let mut total_in = 0u64;
        let mut total_out = 0u64;
        let mut total_coverage = CacheTokenCoverage::default();
        for (turn_index, (trace_id, mut sps)) in by_trace.into_iter().enumerate() {
            sps.sort_by_key(|s| s.span_id);
            // 输入取最早（span_id 最小）带 input_text 的；答复取最末带 output_text 的。
            let user_input = sps
                .iter()
                .find(|s| s.input_text.is_some())
                .and_then(|s| s.input_text.clone());
            let answer = sps.iter().rev().find(|s| s.output_text.is_some());
            let agent_output = answer.and_then(|s| s.output_text.clone());
            let eval_score = answer.and_then(|s| s.eval_score);
            let mut agents: Vec<String> = sps.iter().filter_map(|s| s.agent_name.clone()).collect();
            agents.sort();
            agents.dedup();
            let input_tokens: u64 = sps.iter().map(|s| s.input_tokens.unwrap_or(0)).sum();
            let output_tokens: u64 = sps.iter().map(|s| s.output_tokens.unwrap_or(0)).sum();
            let error_count = sps.iter().filter(|s| s.status.unwrap_or(0) != 0).count();
            let mut turn_coverage = CacheTokenCoverage::default();
            for span in &sps {
                turn_coverage.observe(span);
            }
            total_in += input_tokens;
            total_out += output_tokens;
            turns.push(SessionTurn {
                trace_id,
                turn_index,
                user_input,
                agent_output,
                agents,
                span_count: sps.len(),
                input_tokens,
                output_tokens,
                cache_read_tokens: turn_coverage.read_total(),
                cache_write_tokens: turn_coverage.write_total(),
                cache_read_reported_spans: turn_coverage.read_reported_spans,
                cache_write_reported_spans: turn_coverage.write_reported_spans,
                total_llm_spans: turn_coverage.total_llm_spans,
                error_count,
                eval_score,
            });
            total_coverage.merge(&turn_coverage);
        }
        SessionTimeline {
            session_id,
            turns,
            total_input_tokens: total_in,
            total_output_tokens: total_out,
            total_cache_read_tokens: total_coverage.read_total(),
            total_cache_write_tokens: total_coverage.write_total(),
            cache_read_reported_spans: total_coverage.read_reported_spans,
            cache_write_reported_spans: total_coverage.write_reported_spans,
            total_llm_spans: total_coverage.total_llm_spans,
        }
    }

    /// 控制台用：会话行列表（标题/轮数/状态/token/首 trace），按 session_id 降序。
    /// 走**增量边车索引**：摄入时已逐事件 O(1) 维护，这里直接产出（带排序缓存）→ 写多读少也不全扫。
    /// 仅当 delete/upgrade 标脏时，才在此做一次全量重建（这两类不走 index_record）。
    pub fn console_sessions(&self, snap: &Snapshot) -> Vec<ConsoleSession> {
        // 先看是否标脏（不持锁去扫，避免 session_idx→memtable 的锁序反转死锁）。
        let dirty = self.session_idx.lock().unwrap().dirty;
        if dirty {
            let (spans, _) = self.read_spans_query(snap, &TraceQuery::all()); // 不持 session_idx 锁
            let mut idx = self.session_idx.lock().unwrap();
            if idx.dirty {
                idx.rebuild(&spans);
            }
        }
        self.session_idx.lock().unwrap().rows()
    }

    /// 控制台用：按请求租户隔离的会话行列表。
    /// 无租户时走增量边车；有租户时基于已过滤 span 临时聚合，避免全局 session_idx 泄露别的租户。
    pub fn console_sessions_for_tenant(
        &self,
        snap: &Snapshot,
        tenant: Option<u64>,
    ) -> Vec<ConsoleSession> {
        let Some(t) = tenant else {
            return self.console_sessions(snap);
        };
        let query = TraceQuery::all().for_tenant(t);
        let filter = SearchFilter {
            tenant_id: Some(t),
            ..Default::default()
        };
        self.ensure_trace_rollup_current();
        if let Some(rows) = self
            .trace_rollup
            .lock()
            .unwrap()
            .query_sessions(&query, &filter)
        {
            return rows;
        }
        let (spans, _) = self.read_spans_query(snap, &query);
        let mut idx = SessionIndex::default();
        idx.rebuild(&spans);
        idx.rows()
    }

    /// 控制台用：按租户和 attrs 过滤会话。
    ///
    /// 语义是“会话内至少一个 span 命中所有 attrs 条件”，返回该会话的完整聚合行。
    /// 先用 rollup 找出命中的 session，再聚合这些 session 的全部小字段；rollup 不可用时才回退
    /// 到原始 segment 扫描，保证 span 级 attrs 语义不被 session 级索引误判。
    pub fn console_sessions_for_tenant_and_attrs(
        &self,
        snap: &Snapshot,
        tenant: Option<u64>,
        attrs: &BTreeMap<String, String>,
    ) -> Vec<ConsoleSession> {
        if attrs.is_empty() {
            return self.console_sessions_for_tenant(snap, tenant);
        }
        let q = match tenant {
            Some(t) => TraceQuery::all().for_tenant(t),
            None => TraceQuery::all(),
        };
        let mut filter = SearchFilter {
            tenant_id: tenant,
            ..Default::default()
        };
        filter.attrs = attrs.clone();
        self.ensure_trace_rollup_current();
        if let Some(rows) = self
            .trace_rollup
            .lock()
            .unwrap()
            .query_sessions(&q, &filter)
        {
            return rows;
        }
        let (spans, _) = self.read_spans_query(snap, &q);
        let mut matching_sessions = std::collections::HashSet::new();
        for s in &spans {
            let Some(session_id) = s.session_id else {
                continue;
            };
            if attrs.iter().all(|(k, v)| s.attrs.get(k) == Some(v)) {
                matching_sessions.insert(session_id);
            }
        }
        let filtered: Vec<FoldedSpan> = spans
            .into_iter()
            .filter(|s| {
                s.session_id
                    .map(|session_id| matching_sessions.contains(&session_id))
                    .unwrap_or(false)
            })
            .collect();
        let mut idx = SessionIndex::default();
        idx.rebuild(&filtered);
        idx.rows()
    }

    /// 控制台用：一条 trace 的折叠 span（瀑布）。引擎不存 span 的 kind/name/起始时刻，这里**派生**：
    /// kind = agent>tool>model>other；name = 同源；起始时刻按 span_id 升序累加 duration 顺排（逻辑瀑布）。
    pub fn console_trace_spans(&self, snap: &Snapshot, trace_id: u64) -> Vec<ConsoleSpan> {
        self.console_trace_spans_for_tenant(snap, trace_id, None)
    }

    /// 控制台用：按请求租户隔离的一条 trace 折叠 span。
    pub fn console_trace_spans_for_tenant(
        &self,
        snap: &Snapshot,
        trace_id: u64,
        tenant: Option<u64>,
    ) -> Vec<ConsoleSpan> {
        let mut q = TraceQuery::trace(trace_id, i64::MIN, i64::MAX);
        q.tenant_id = tenant;
        let (mut spans, _) = self.read_spans_query(snap, &q);
        spans.sort_by_key(|s| s.span_id);
        let mut start = 0u64;
        spans
            .into_iter()
            .map(|s| {
                let dur = s.duration_ns.unwrap_or(0);
                let cs = Self::console_span_from_folded(s, start);
                start += dur;
                cs
            })
            .collect()
    }

    /// 单 Span 详情点读。候选 key 永远只有目标 `(trace_id, span_id)`，不会先取出同 Trace 的其它 span。
    /// 返回读计划是为了让回归测试直接约束物理读放大，而不是依赖容易抖动的耗时断言。
    pub fn console_span_for_tenant(
        &self,
        snap: &Snapshot,
        trace_id: u64,
        span_id: u64,
        tenant: Option<u64>,
    ) -> (Option<ConsoleSpan>, ReadPlanStats) {
        let keys = match tenant {
            Some(t) => HashSet::from([(Some(t), trace_id, span_id)]),
            None => self
                .filter_candidate_span_keys_for_snapshot(
                    snap,
                    &SearchFilter {
                        trace_id: Some(trace_id),
                        ..Default::default()
                    },
                )
                .into_iter()
                .filter(|key| key.2 == span_id)
                .collect(),
        };
        let mut q = TraceQuery::trace(trace_id, i64::MIN, i64::MAX);
        q.tenant_id = tenant;
        let (mut spans, scan) = self.fold_query(snap, &q, Some(&keys), Projection::ALL);
        let span = spans
            .pop()
            .filter(|span| span.span_id == span_id)
            .map(|span| Self::console_span_from_folded(span, 0));
        let matched_spans = usize::from(span.is_some());
        (
            span,
            ReadPlanStats {
                source: Some("span_point_lookup".to_string()),
                candidate_span_keys: Some(1),
                scanned_segments: scan.scanned_segments,
                point_lookup_segments: scan.point_lookup_segments,
                decoded_segment_rows: scan.decoded_segment_rows,
                decoded_memtable_rows: scan.decoded_memtable_rows,
                index_bytes_read: scan.index_bytes_read,
                data_bytes_read: scan.data_bytes_read,
                indexes_validated: scan.indexes_validated,
                indexes_rebuilt: scan.indexes_rebuilt,
                matched_spans,
                fallback_reason: scan.fallback_reason,
                ..ReadPlanStats::default()
            },
        )
    }

    fn console_span_from_folded(s: FoldedSpan, start_ns: u64) -> ConsoleSpan {
        // tool 优先：agent 上下文会被子 span 继承，工具 span 可能同时带 agent_name/tool_name。
        let (kind, name, actor_id) = if let Some(t) = &s.tool_name {
            ("tool", t.clone(), format!("tool:{t}"))
        } else if let Some(a) = &s.agent_name {
            ("agent", a.clone(), format!("agent:{a}"))
        } else if let Some(m) = &s.model {
            ("llm", m.clone(), format!("model:{m}"))
        } else {
            (
                "other",
                format!("span {}", s.span_id),
                format!("span:{}", s.span_id),
            )
        };
        ConsoleSpan {
            span_id: s.span_id,
            parent_span_id: s.parent_span_id,
            external_trace_id: s.external_trace_id,
            external_span_id: s.external_span_id,
            external_parent_span_id: s.external_parent_span_id,
            external_session_id: s.external_session_id,
            kind,
            name,
            span_name: s.span_name,
            display_name: s.display_name,
            actor_id,
            agent_name: s.agent_name,
            tool_name: s.tool_name,
            start_ns,
            duration_ns: s.duration_ns.unwrap_or(0),
            has_error: s.status.unwrap_or(0) != 0,
            has_start: s.has_start,
            has_end: s.has_end,
            input_tokens: s.input_tokens.unwrap_or(0),
            output_tokens: s.output_tokens.unwrap_or(0),
            cache_read_tokens: s.cache_read_tokens,
            cache_write_tokens: s.cache_write_tokens,
            model: s.model,
            input_text: s.input_text,
            output_text: s.output_text,
            attrs: s.attrs,
        }
    }

    /// 读取一条 trace 中可见 span 的原始日志事件。调用方传入已经过租户/删除过滤的 key 集，
    /// 这里只做原始事件扫描、event_id 去重与时间排序，保证不会把不可见 span 的日志泄漏出去。
    pub fn log_events_for_trace_keys(
        &self,
        snap: &Snapshot,
        trace_id: u64,
        keys: &std::collections::HashSet<(u64, u64)>,
    ) -> BTreeMap<u64, Vec<SpanLogEvent>> {
        self.log_events_for_trace_keys_with_stats(snap, trace_id, keys)
            .0
    }

    /// 与 `log_events_for_trace_keys` 相同，但同时返回物理读取统计，供性能回归测试约束全段回退。
    pub fn log_events_for_trace_keys_with_stats(
        &self,
        snap: &Snapshot,
        trace_id: u64,
        keys: &std::collections::HashSet<(u64, u64)>,
    ) -> (BTreeMap<u64, Vec<SpanLogEvent>>, ReadPlanStats) {
        let mut by_event: BTreeMap<u64, (u64, SpanLogEvent)> = BTreeMap::new();
        let bloom_stats = self.ensure_seg_key_bloom_for_manifest(&snap.manifest);
        let mut stats = ReadPlanStats {
            source: Some("log_event_point_lookup".to_string()),
            candidate_span_keys: Some(keys.len()),
            scanned_segments: bloom_stats.scanned_segments,
            point_lookup_segments: bloom_stats.point_lookup_segments,
            decoded_segment_rows: bloom_stats.decoded_segment_rows,
            decoded_memtable_rows: bloom_stats.decoded_memtable_rows,
            index_bytes_read: bloom_stats.index_bytes_read,
            data_bytes_read: bloom_stats.data_bytes_read,
            indexes_validated: bloom_stats.indexes_validated,
            indexes_rebuilt: bloom_stats.indexes_rebuilt,
            fallback_reason: bloom_stats.fallback_reason,
            ..ReadPlanStats::default()
        };

        for entry in snap.manifest.segments.values() {
            let bloom_skip = self
                .seg_key_bloom
                .lock()
                .unwrap()
                .get(&entry.segment_id.get())
                .map_or(false, |b| !keys.iter().any(|&key| b.maybe_contains(key)));
            if bloom_skip {
                continue;
            }
            stats.scanned_segments += 1;
            if let Some(scan) = self.segments.scan_records_for_keys(entry.segment_id, keys) {
                stats.decoded_segment_rows += scan.decoded_rows;
                stats.index_bytes_read =
                    stats.index_bytes_read.saturating_add(scan.index_bytes_read);
                stats.data_bytes_read = stats.data_bytes_read.saturating_add(scan.data_bytes_read);
                stats.indexes_validated += scan.indexes_validated;
                stats.indexes_rebuilt += scan.indexes_rebuilt;
                if scan.used_point_index {
                    stats.point_lookup_segments += 1;
                }
                for (row, rec) in scan.rows {
                    if entry.deletion_vec.is_deleted(row) {
                        continue;
                    }
                    self.collect_log_event(trace_id, keys, &rec, &mut by_event);
                }
            } else {
                let recs = self.segments.scan_records(entry.segment_id);
                stats.decoded_segment_rows += recs.len();
                stats.fallback_reason = Some("segment_store_has_no_raw_point_lookup".to_string());
                for (row, rec) in recs.iter().enumerate() {
                    if entry.deletion_vec.is_deleted(row as u32) {
                        continue;
                    }
                    self.collect_log_event(trace_id, keys, rec, &mut by_event);
                }
            }
        }

        {
            let mt = self.memtable.lock().unwrap();
            let rows = mt.read_keys_range(keys, snap.retained_watermark, snap.live_lsn);
            stats.decoded_memtable_rows += rows.len();
            for row in rows {
                let rec = WalRecord {
                    trace_id: row.trace_id,
                    span_id: row.span_id,
                    ts: row.ts,
                    identity: row.identity.clone(),
                    fields: row.fields.clone(),
                };
                self.collect_log_event(trace_id, keys, &rec, &mut by_event);
            }
        }

        let mut by_span: BTreeMap<u64, Vec<SpanLogEvent>> = BTreeMap::new();
        for (_event_id, (span_id, ev)) in by_event {
            by_span.entry(span_id).or_default().push(ev);
        }
        for events in by_span.values_mut() {
            events.sort_by_key(|ev| (ev.ts, ev.seq, ev.event_id));
        }
        stats.matched_spans = by_span.len();
        (by_span, stats)
    }

    fn collect_log_event(
        &self,
        trace_id: u64,
        keys: &std::collections::HashSet<(u64, u64)>,
        rec: &WalRecord,
        out: &mut BTreeMap<u64, (u64, SpanLogEvent)>,
    ) {
        if rec.trace_id != trace_id
            || !keys.contains(&(rec.trace_id, rec.span_id))
            || rec.fields.logs.is_empty()
        {
            return;
        }
        let event_id = rec.identity.event_id().0;
        out.entry(event_id).or_insert_with(|| {
            (
                rec.span_id,
                SpanLogEvent {
                    ts: rec.ts,
                    seq: rec.identity.seq,
                    event_type: rec.identity.event_type.tag(),
                    event_id,
                    messages: rec.fields.logs.clone(),
                    attrs: rec.fields.attrs.clone(),
                },
            )
        });
    }

    /// 按 agent 的成本归因（per-agent 成本下钻）：按 agent_name 聚合 token。按 agent 名升序。
    pub fn cost_by_agent(&self, snap: &Snapshot, q: &TraceQuery) -> Vec<AgentCost> {
        // 按 agent 归因 token —— 只读 agent_name + token,跳过文本（成本下钻是典型的"只数不读原文"）。
        let proj = Projection::of(
            Projection::AGENT_NAME
                | Projection::INPUT_TOKENS
                | Projection::OUTPUT_TOKENS
                | Projection::CACHE_READ_TOKENS
                | Projection::CACHE_WRITE_TOKENS
                | Projection::MODEL,
        );
        let (spans, _) = self.fold_query(snap, q, None, proj);
        let mut acc: BTreeMap<String, (usize, u64, u64, CacheTokenCoverage)> = BTreeMap::new();
        for s in spans {
            if let Some(a) = &s.agent_name {
                let e = acc.entry(a.clone()).or_default();
                e.0 += 1;
                e.1 += s.input_tokens.unwrap_or(0);
                e.2 += s.output_tokens.unwrap_or(0);
                e.3.observe(&s);
            }
        }
        acc.into_iter()
            .map(
                |(agent_name, (span_count, input_tokens, output_tokens, c))| AgentCost {
                    agent_name,
                    span_count,
                    input_tokens,
                    output_tokens,
                    cache_read_tokens: c.read_total(),
                    cache_write_tokens: c.write_total(),
                    cache_read_reported_spans: c.read_reported_spans,
                    cache_write_reported_spans: c.write_reported_spans,
                    total_llm_spans: c.total_llm_spans,
                },
            )
            .collect()
    }
}
