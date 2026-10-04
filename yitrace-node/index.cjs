const { NativeYiTraceDB } = require("./native.cjs");

function tenantId(options) {
  const value = options?.tenantId;
  if (value === undefined || value === null) return undefined;
  return String(value);
}

function parseJson(text) {
  return JSON.parse(text);
}

function optionValue(options, snake, camel = snake) {
  if (options?.[snake] !== undefined) return options[snake];
  if (options?.[camel] !== undefined) return options[camel];
  return undefined;
}

function defaultTimestampNs() {
  return (BigInt(Date.now()) * 1_000_000n).toString();
}

function setIfDefined(target, key, value) {
  if (value !== undefined && value !== null) target[key] = value;
}

function plainObject(value) {
  return value && typeof value === "object" && !Array.isArray(value);
}

const ATTR_ALIASES = {
  projectId: "project_id",
  callSite: "call_site",
  taskFingerprint: "task_fingerprint",
  loopId: "loop_id",
  harnessVersion: "harness_version",
  schemaFingerprint: "schema_fingerprint",
  intentSignature: "intent_signature",
  validationStatus: "validation_status",
  reviewStatus: "review_status",
  evalStatus: "eval_status",
  pathMemoryId: "path_memory_id",
  stopReason: "stop_reason",
};

const ATTR_KEYS = [
  "project_id",
  "skill",
  "mode",
  "call_site",
  "task_fingerprint",
  "loop_id",
  "harness_version",
  "schema_fingerprint",
  "intent_signature",
  "validation_status",
  "review_status",
  "eval_status",
  "path_memory_id",
  "stop_reason",
  "phase",
  "validator",
];

function normalizeSearchQuery(query = {}) {
  const out = { ...query };
  if (query.filter) {
    out.filter = { ...query.filter };
    const traceId = out.filter.traceId ?? out.filter.trace_id;
    if (traceId !== undefined && traceId !== null) {
      if (out.filter.externalTraceId === undefined && out.filter.external_trace_id === undefined) {
        out.filter.externalTraceId = String(traceId);
      }
      delete out.filter.trace_id;
    }
    if (out.filter.agentName !== undefined && out.filter.agent_name === undefined) out.filter.agent_name = out.filter.agentName;
    if (out.filter.timeFrom !== undefined && out.filter.time_from === undefined) out.filter.time_from = out.filter.timeFrom;
    if (out.filter.timeTo !== undefined && out.filter.time_to === undefined) out.filter.time_to = out.filter.timeTo;
    delete out.filter.traceId;
    delete out.filter.agentName;
    delete out.filter.timeFrom;
    delete out.filter.timeTo;
  }
  return out;
}

function normalizeVector(value, label = "embedding") {
  if (value === undefined || value === null || typeof value === "string") {
    throw new Error(`${label} must be a non-empty numeric array`);
  }
  const vector = Array.from(value, (item, i) => {
    const number = Number(item);
    if (!Number.isFinite(number)) {
      throw new Error(`${label}[${i}] must be a finite number`);
    }
    return number;
  });
  if (vector.length === 0) {
    throw new Error(`${label} must not be empty`);
  }
  return vector;
}

function normalizeEmbedder(value) {
  if (value === undefined || value === null) return undefined;
  if (typeof value === "function") {
    return {
      embedQuery: value,
      embedDocuments: async (texts) => Promise.all(texts.map((text) => value(text))),
    };
  }
  if (!plainObject(value)) {
    throw new Error("embedder must be a function or an object");
  }
  const embedQuery = value.embedQuery ?? value.embed;
  const embedDocuments = value.embedDocuments ?? value.embedBatch;
  if (typeof embedQuery !== "function" && typeof embedDocuments !== "function") {
    throw new Error("embedder requires embedQuery/embed or embedDocuments/embedBatch");
  }
  return {
    model: value.model,
    dimensions: value.dimensions ?? value.dimension ?? value.dim,
    embedQuery,
    embedDocuments,
  };
}

function searchableTextFromEvent(event) {
  if (!plainObject(event)) return "";
  const parts = [];
  for (const key of ["input_text", "inputText", "output_text", "outputText", "span_name", "spanName", "agent_name", "agentName", "tool_name", "toolName", "model"]) {
    const value = event[key];
    if (value !== undefined && value !== null && String(value).trim() !== "") parts.push(String(value));
  }
  for (const key of ["logs", "messages"]) {
    if (!Array.isArray(event[key])) continue;
    for (const item of event[key]) {
      if (item !== undefined && item !== null && String(item).trim() !== "") parts.push(String(item));
    }
  }
  return parts.join(" ");
}

function embeddingDocsFromEvents(events = []) {
  const byKey = new Map();
  for (const event of events) {
    if (!plainObject(event)) continue;
    const traceId = optionValue(event, "trace_id", "traceId");
    const spanId = optionValue(event, "span_id", "spanId");
    if (traceId === undefined || traceId === null || spanId === undefined || spanId === null) continue;
    const text = searchableTextFromEvent(event);
    if (!text) continue;
    const key = `${String(traceId)}\u0000${String(spanId)}`;
    const current = byKey.get(key);
    if (current) {
      current.text += ` ${text}`;
    } else {
      byKey.set(key, { traceId, spanId, text });
    }
  }
  return [...byKey.values()];
}

function normalizeAttrs(options = {}) {
  const attrs = {};
  const source = plainObject(options.attrs) ? options.attrs : {};
  for (const key of ATTR_KEYS) {
    if (source[key] !== undefined) attrs[key] = source[key];
    if (options[key] !== undefined) attrs[key] = options[key];
  }
  for (const [camel, snake] of Object.entries(ATTR_ALIASES)) {
    if (source[camel] !== undefined) attrs[snake] = source[camel];
    if (options[camel] !== undefined) attrs[snake] = options[camel];
  }
  return Object.keys(attrs).length > 0 ? attrs : undefined;
}

function normalizeMetadataBody(input = {}) {
  const out = { ...input };
  const attrs = { ...(plainObject(input.attrs) ? input.attrs : {}) };
  for (const key of ATTR_KEYS) {
    if (input[key] !== undefined) attrs[key] = input[key];
  }
  for (const [camel, snake] of Object.entries(ATTR_ALIASES)) {
    if (input[camel] !== undefined) attrs[snake] = input[camel];
  }
  if (Object.keys(attrs).length > 0) out.attrs = attrs;
  return out;
}

function metadataQueryString(options = {}) {
  const params = new URLSearchParams();
  for (const key of [
    "cursor",
    "offset",
    "limit",
    "target",
    "label",
    "name",
    "source",
    "status",
    "split",
    "enabled",
  ]) {
    if (options[key] !== undefined && options[key] !== null) params.set(key, String(options[key]));
  }
  const aliases = {
    traceId: "traceId",
    trace_id: "trace_id",
    spanId: "spanId",
    span_id: "span_id",
    includeDeleted: "includeDeleted",
    include_deleted: "include_deleted",
    datasetId: "datasetId",
    dataset_id: "dataset_id",
    itemId: "itemId",
    item_id: "item_id",
    datasetItemId: "datasetItemId",
    dataset_item_id: "dataset_item_id",
    evalRunId: "evalRunId",
    eval_run_id: "eval_run_id",
    auditId: "auditId",
    audit_id: "audit_id",
    policyId: "policyId",
    policy_id: "policy_id",
    createdAfterNs: "createdAfterNs",
    created_after_ns: "created_after_ns",
    createdBeforeNs: "createdBeforeNs",
    created_before_ns: "created_before_ns",
    minCreatedAtNs: "minCreatedAtNs",
    maxCreatedAtNs: "maxCreatedAtNs",
  };
  for (const [from, to] of Object.entries(aliases)) {
    if (options[from] !== undefined && options[from] !== null) params.set(to, String(options[from]));
  }
  const attrs = normalizeMetadataBody(options).attrs;
  if (attrs && Object.keys(attrs).length > 0) params.set("attrs", JSON.stringify(attrs));
  return params.toString();
}

class SpanEventBuilder {
  #defaults;
  #events = [];
  #seqBySpan = new Map();

  constructor(defaults = {}) {
    this.#defaults = { ...defaults };
  }

  startSpan(options = {}) {
    const event = this.#baseEvent(options, 1);
    this.#copyCommonFields(event, options);
    const name = optionValue(options, "name");
    setIfDefined(event, "span_name", name === undefined || name === null ? undefined : String(name));
    const displayName = optionValue(options, "display_name", "displayName");
    if (displayName !== undefined && displayName !== null && String(displayName).trim() !== "") {
      event.display_name = String(displayName).trim();
    }
    const logs = [...(Array.isArray(options.logs) ? options.logs : [])];
    if (logs.length > 0) event.logs = logs;
    this.#events.push(event);
    return event;
  }

  log(options = {}) {
    const event = this.#baseEvent(options, 4);
    this.#copyCommonFields(event, options);
    const logs = [];
    const message = optionValue(options, "message");
    const log = optionValue(options, "log");
    if (message !== undefined && message !== null) logs.push(String(message));
    if (log !== undefined && log !== null) logs.push(String(log));
    if (Array.isArray(options.logs)) logs.push(...options.logs.map(String));
    if (logs.length > 0) event.logs = logs;
    this.#events.push(event);
    return event;
  }

  endSpan(options = {}) {
    const event = this.#baseEvent(options, 2);
    this.#copyCommonFields(event, options);
    setIfDefined(event, "status", optionValue(options, "status") ?? 0);
    setIfDefined(event, "duration_ns", optionValue(options, "duration_ns", "durationNs"));
    setIfDefined(event, "output_text", optionValue(options, "output_text", "outputText"));
    this.#events.push(event);
    return event;
  }

  events() {
    return this.#events.map((event) => ({ ...event, attrs: event.attrs ? { ...event.attrs } : undefined }));
  }

  clear() {
    this.#events = [];
    this.#seqBySpan.clear();
  }

  async ingest(db, options = {}) {
    return db.ingest(this.events(), options);
  }

  #baseEvent(options, eventType) {
    const traceId = optionValue(options, "trace_id", "traceId") ?? optionValue(this.#defaults, "trace_id", "traceId");
    const spanId = optionValue(options, "span_id", "spanId");
    if (traceId === undefined || traceId === null) throw new Error("SpanEventBuilder requires traceId");
    if (spanId === undefined || spanId === null) throw new Error("SpanEventBuilder requires spanId");
    const extSpanId = optionValue(options, "ext_span_id", "extSpanId") ?? String(spanId);
    const key = `${String(traceId)}\u0000${String(extSpanId)}`;
    const seq = optionValue(options, "seq") ?? this.#nextSeq(key);
    const event = {
      trace_id: traceId,
      span_id: spanId,
      ts: optionValue(options, "ts") ?? defaultTimestampNs(),
      seq,
      event_type: eventType,
      ext_span_id: extSpanId,
    };
    const parentSpanId = optionValue(options, "parent_span_id", "parentSpanId");
    const sessionId = optionValue(options, "session_id", "sessionId") ?? optionValue(this.#defaults, "session_id", "sessionId");
    setIfDefined(event, "parent_span_id", parentSpanId);
    setIfDefined(event, "session_id", sessionId);
    setIfDefined(event, "tenant_id", optionValue(options, "tenant_id", "tenantId") ?? optionValue(this.#defaults, "tenant_id", "tenantId"));
    setIfDefined(event, "external_trace_id", optionValue(options, "external_trace_id", "externalTraceId") ?? (typeof traceId === "string" ? traceId : undefined));
    setIfDefined(event, "external_span_id", optionValue(options, "external_span_id", "externalSpanId") ?? (typeof spanId === "string" ? spanId : undefined));
    setIfDefined(event, "external_parent_span_id", optionValue(options, "external_parent_span_id", "externalParentSpanId") ?? (typeof parentSpanId === "string" ? parentSpanId : undefined));
    setIfDefined(event, "external_session_id", optionValue(options, "external_session_id", "externalSessionId") ?? (typeof sessionId === "string" ? sessionId : undefined));
    const attrs = {
      ...(plainObject(this.#defaults.attrs) ? this.#defaults.attrs : {}),
      ...(plainObject(options.attrs) ? options.attrs : {}),
    };
    if (Object.keys(attrs).length > 0) {
      event.attrs = attrs;
    }
    return event;
  }

  #copyCommonFields(event, options) {
    setIfDefined(
      event,
      "agent_name",
      optionValue(options, "agent_name", "agentName") ?? optionValue(this.#defaults, "agent_name", "agentName"),
    );
    setIfDefined(event, "tool_name", optionValue(options, "tool_name", "toolName"));
    setIfDefined(event, "model", optionValue(options, "model"));
    setIfDefined(event, "input_text", optionValue(options, "input_text", "inputText"));
    setIfDefined(event, "output_text", optionValue(options, "output_text", "outputText"));
    setIfDefined(event, "input_tokens", optionValue(options, "input_tokens", "inputTokens"));
    setIfDefined(event, "output_tokens", optionValue(options, "output_tokens", "outputTokens"));
  }

  #nextSeq(key) {
    const next = (this.#seqBySpan.get(key) ?? 0) + 1;
    this.#seqBySpan.set(key, next);
    return next;
  }
}

function createSpanEventBuilder(defaults = {}) {
  return new SpanEventBuilder(defaults);
}

class YiTraceDB {
  #native;
  #tenantId;
  #embedder;
  #embeddingDimensions;
  #autoIndexEmbeddings;
  #closed = false;

  constructor(native, options = {}) {
    this.#native = native;
    this.#tenantId = tenantId(options);
    this.#embedder = normalizeEmbedder(options.embedder ?? options.embedding);
    const dimensions = options.embeddingDimensions ?? options.dimensions ?? this.#embedder?.dimensions;
    this.#embeddingDimensions = dimensions === undefined || dimensions === null ? undefined : Number(dimensions);
    if (this.#embeddingDimensions !== undefined && (!Number.isInteger(this.#embeddingDimensions) || this.#embeddingDimensions <= 0)) {
      throw new Error("embedding dimensions must be a positive integer");
    }
    this.#autoIndexEmbeddings = options.autoIndexEmbeddings === true;
  }

  static async open(pathOrOptions) {
    const options = typeof pathOrOptions === "string" ? { dataDir: pathOrOptions } : pathOrOptions;
    if (!options?.dataDir) {
      throw new Error("YiTraceDB.open requires a dataDir");
    }
    if (Object.prototype.hasOwnProperty.call(options, "readOnly")) {
      throw new Error("OpenOptions.readOnly is not supported yet; omit it to open a writable embedded database");
    }
    return new YiTraceDB(new NativeYiTraceDB(options.dataDir), options);
  }

  async ingest(events, options = {}) {
    this.#ensureOpen();
    const response = this.#native.ingestJson(JSON.stringify(events), tenantId(options) ?? this.#tenantId);
    const result = parseJson(response);
    if (options.indexEmbeddings === true || (options.indexEmbeddings !== false && this.#autoIndexEmbeddings)) {
      await this.indexEmbeddings(embeddingDocsFromEvents(events), options);
    }
    return result;
  }

  async ingestOtlp(body, options = {}) {
    this.#ensureOpen();
    const response = this.#native.ingestOtlpJson(
      typeof body === "string" ? body : JSON.stringify(body),
      tenantId(options) ?? this.#tenantId,
    );
    return parseJson(response);
  }

  async search(query, options = {}) {
    this.#ensureOpen();
    const response = this.#native.searchJson(JSON.stringify(await this.#prepareSearchQuery(query)), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async indexEmbedding(input, options = {}) {
    this.#ensureOpen();
    const item = plainObject(input) ? input : {};
    const traceId = item.traceId ?? item.trace_id;
    const spanId = item.spanId ?? item.span_id;
    if (traceId === undefined || traceId === null) throw new Error("indexEmbedding requires traceId");
    if (spanId === undefined || spanId === null) throw new Error("indexEmbedding requires spanId");
    const vector = await this.#vectorForEmbeddingInput(item);
    this.#native.indexEmbedding(String(traceId), String(spanId), vector, tenantId(options) ?? this.#tenantId);
    return { indexed: 1 };
  }

  async indexEmbeddings(items, options = {}) {
    this.#ensureOpen();
    const list = Array.isArray(items) ? items : [];
    if (list.length === 0) return { indexed: 0 };

    const vectors = [];
    const textItems = [];
    const textPositions = [];
    for (let i = 0; i < list.length; i += 1) {
      const item = list[i];
      if (!plainObject(item)) throw new Error(`indexEmbeddings[${i}] must be an object`);
      const vector = item.vector ?? item.embedding;
      if (vector !== undefined && vector !== null && vector !== "auto") {
        vectors[i] = this.#checkEmbeddingVector(vector, `indexEmbeddings[${i}].embedding`);
      } else {
        const text = item.text ?? item.inputText ?? item.input_text ?? item.outputText ?? item.output_text;
        if (text === undefined || text === null || String(text).trim() === "") {
          throw new Error(`indexEmbeddings[${i}] requires embedding/vector or text`);
        }
        textItems.push(String(text));
        textPositions.push(i);
      }
    }
    if (textItems.length > 0) {
      const embedded = await this.#embedDocuments(textItems);
      if (!Array.isArray(embedded) || embedded.length !== textItems.length) {
        throw new Error("embedDocuments must return one embedding per input text");
      }
      for (let i = 0; i < embedded.length; i += 1) {
        vectors[textPositions[i]] = this.#checkEmbeddingVector(embedded[i], `embedding[${textPositions[i]}]`);
      }
    }

    let indexed = 0;
    for (let i = 0; i < list.length; i += 1) {
      const traceId = list[i].traceId ?? list[i].trace_id;
      const spanId = list[i].spanId ?? list[i].span_id;
      if (traceId === undefined || traceId === null) throw new Error(`indexEmbeddings[${i}] requires traceId`);
      if (spanId === undefined || spanId === null) throw new Error(`indexEmbeddings[${i}] requires spanId`);
      this.#native.indexEmbedding(String(traceId), String(spanId), vectors[i], tenantId(options) ?? this.#tenantId);
      indexed += 1;
    }
    return { indexed };
  }

  async traceSearch(query = {}, options = {}) {
    this.#ensureOpen();
    const response = this.#native.traceSearchJson(JSON.stringify(normalizeSearchQuery(query)), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async traceAggregate(query = {}, options = {}) {
    this.#ensureOpen();
    const response = this.#native.traceAggregateJson(JSON.stringify(query), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async storageStats(query = {}, options = {}) {
    this.#ensureOpen();
    const response = this.#native.storageStatsJson(JSON.stringify(query), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async retentionPlan(query = {}, options = {}) {
    this.#ensureOpen();
    const response = this.#native.retentionPlanJson(JSON.stringify(query), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async applyRetention(query = {}, options = {}) {
    this.#ensureOpen();
    const response = this.#native.applyRetentionJson(JSON.stringify(query), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async retentionAudits(options = {}) {
    this.#ensureOpen();
    const response = this.#native.retentionAuditsJson(metadataQueryString(options), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async createRetentionPolicy(policy, options = {}) {
    this.#ensureOpen();
    const response = this.#native.createRetentionPolicyJson(JSON.stringify(policy), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async retentionPolicies(options = {}) {
    this.#ensureOpen();
    const response = this.#native.retentionPoliciesJson(metadataQueryString(options), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async runRetentionPolicies(query = {}, options = {}) {
    this.#ensureOpen();
    const response = this.#native.runRetentionPoliciesJson(JSON.stringify(query), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async traceTrajectories(query = {}, options = {}) {
    this.#ensureOpen();
    const response = this.#native.traceTrajectoriesJson(JSON.stringify(normalizeSearchQuery(query)), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async trajectoryGroups(query = {}, options = {}) {
    this.#ensureOpen();
    const response = this.#native.trajectoryGroupsJson(JSON.stringify(normalizeSearchQuery(query)), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async traceDiff(leftOrQuery, rightTraceId, options = {}) {
    this.#ensureOpen();
    const query = plainObject(leftOrQuery)
      ? leftOrQuery
      : { baseTraceId: leftOrQuery, candidateTraceId: rightTraceId };
    const response = this.#native.traceDiffJson(JSON.stringify(query), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async annotate(annotation, options = {}) {
    this.#ensureOpen();
    const response = this.#native.annotateJson(JSON.stringify(normalizeMetadataBody(annotation)), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async annotations(options = {}) {
    this.#ensureOpen();
    const response = this.#native.annotationsJson(metadataQueryString(options), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async updateAnnotation(annotationId, update = {}, options = {}) {
    this.#ensureOpen();
    const response = this.#native.updateAnnotationJson(String(annotationId), JSON.stringify(normalizeMetadataBody(update)), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async deleteAnnotation(annotationId, deleteInfo = {}, options = {}) {
    this.#ensureOpen();
    const response = this.#native.deleteAnnotationJson(String(annotationId), JSON.stringify(deleteInfo ?? {}), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async linkDatasetItem(association, options = {}) {
    this.#ensureOpen();
    const response = this.#native.linkDatasetItemJson(JSON.stringify(normalizeMetadataBody(association)), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async datasetAssociations(options = {}) {
    this.#ensureOpen();
    const response = this.#native.datasetAssociationsJson(metadataQueryString(options), tenantId(options) ?? this.#tenantId);
    return parseJson(response);
  }

  async traces(options = {}) {
    this.#ensureOpen();
    return parseJson(this.#native.tracesJson(tenantId(options) ?? this.#tenantId));
  }

  async sessions(options = {}) {
    this.#ensureOpen();
    const attrs = normalizeAttrs(options);
    return parseJson(
      this.#native.sessionsJson(
        options.cursor ?? 0,
        options.limit ?? 50,
        options.filter,
        attrs ? JSON.stringify(attrs) : undefined,
        tenantId(options) ?? this.#tenantId,
      ),
    );
  }

  async loops(options = {}) {
    this.#ensureOpen();
    const attrs = normalizeAttrs(options);
    return parseJson(
      this.#native.loopsJson(
        options.cursor ?? 0,
        options.limit ?? 50,
        attrs ? JSON.stringify(attrs) : undefined,
        tenantId(options) ?? this.#tenantId,
      ),
    );
  }

  async loop(loopId, options = {}) {
    this.#ensureOpen();
    try {
      return parseJson(this.#native.loopJson(String(loopId), tenantId(options) ?? this.#tenantId));
    } catch (error) {
      if (String(error?.message ?? error).includes("status=404")) return null;
      throw error;
    }
  }

  async taskTraces(fingerprint, options = {}) {
    this.#ensureOpen();
    const attrs = normalizeAttrs(options);
    return parseJson(
      this.#native.taskTracesJson(
        String(fingerprint),
        options.cursor ?? 0,
        options.limit ?? 50,
        attrs ? JSON.stringify(attrs) : undefined,
        tenantId(options) ?? this.#tenantId,
      ),
    );
  }

  async trace(traceId, options = {}) {
    this.#ensureOpen();
    try {
      return parseJson(this.#native.traceJson(String(traceId), tenantId(options) ?? this.#tenantId));
    } catch (error) {
      if (String(error?.message ?? error).includes("status=404")) return null;
      throw error;
    }
  }

  async span(traceId, spanId, options = {}) {
    this.#ensureOpen();
    try {
      return parseJson(this.#native.spanJson(String(traceId), String(spanId), tenantId(options) ?? this.#tenantId));
    } catch (error) {
      if (String(error?.message ?? error).includes("status=404")) return null;
      throw error;
    }
  }

  async flush() {
    this.#ensureOpen();
    this.#native.flush();
  }

  async close() {
    if (!this.#closed) {
      this.#native.close();
      this.#closed = true;
    }
  }

  async #prepareSearchQuery(query = {}) {
    const out = normalizeSearchQuery(query);
    const mode = String(out.mode ?? out.searchMode ?? "").toLowerCase();
    const text = out.text ?? out.q;
    const semanticValue = String(out.semantic ?? "").toLowerCase();
    const semanticOnly = mode === "semantic" || mode === "vector" || out.semantic === true || semanticValue === "true";
    const hybrid = mode === "hybrid" || out.hybrid === true || semanticValue === "hybrid";
    const autoVector = out.vector === "auto" || out.vector === true;
    if (out.vector !== undefined && out.vector !== null && out.vector !== "auto" && out.vector !== true) {
      out.vector = this.#checkEmbeddingVector(out.vector, "query.vector");
    } else if (semanticOnly || hybrid || autoVector) {
      if (text === undefined || text === null || String(text).trim() === "") {
        throw new Error("semantic/hybrid search requires text");
      }
      out.vector = await this.#embedQuery(String(text));
      if (semanticOnly) {
        delete out.text;
        delete out.q;
      } else if (out.text === undefined && out.q !== undefined) {
        out.text = String(out.q);
      }
    }
    delete out.mode;
    delete out.searchMode;
    delete out.hybrid;
    delete out.semantic;
    return out;
  }

  async #vectorForEmbeddingInput(item) {
    const vector = item.vector ?? item.embedding;
    if (vector !== undefined && vector !== null && vector !== "auto") {
      return this.#checkEmbeddingVector(vector, "embedding");
    }
    const text = item.text ?? item.inputText ?? item.input_text ?? item.outputText ?? item.output_text;
    if (text === undefined || text === null || String(text).trim() === "") {
      throw new Error("indexEmbedding requires embedding/vector or text");
    }
    return this.#embedQuery(String(text));
  }

  async #embedQuery(text) {
    if (!this.#embedder) throw new Error("YiTraceDB.open requires embedder for semantic/hybrid search or text embedding indexing");
    if (typeof this.#embedder.embedQuery === "function") {
      return this.#checkEmbeddingVector(await this.#embedder.embedQuery(text), "query embedding");
    }
    const docs = await this.#embedDocuments([text]);
    if (!Array.isArray(docs) || docs.length !== 1) throw new Error("embedDocuments must return one embedding for query text");
    return this.#checkEmbeddingVector(docs[0], "query embedding");
  }

  async #embedDocuments(texts) {
    if (!this.#embedder) throw new Error("YiTraceDB.open requires embedder for text embedding indexing");
    if (typeof this.#embedder.embedDocuments === "function") {
      return this.#embedder.embedDocuments(texts);
    }
    if (typeof this.#embedder.embedQuery === "function") {
      return Promise.all(texts.map((text) => this.#embedder.embedQuery(text)));
    }
    throw new Error("embedder requires embedDocuments/embedBatch or embedQuery/embed");
  }

  #checkEmbeddingVector(value, label) {
    const vector = normalizeVector(value, label);
    if (this.#embeddingDimensions === undefined) {
      this.#embeddingDimensions = vector.length;
    } else if (vector.length !== this.#embeddingDimensions) {
      throw new Error(`${label} dimension ${vector.length} does not match expected ${this.#embeddingDimensions}`);
    }
    return vector;
  }

  #ensureOpen() {
    if (this.#closed) {
      throw new Error("YiTraceDB is closed");
    }
  }
}

module.exports = { YiTraceDB, SpanEventBuilder, createSpanEventBuilder };
