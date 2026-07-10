# gRPC Search Service Tracing Reference

This document is the canonical reference for every OpenTelemetry span, span event, and span
attribute emitted by `rust/search-api`. It is grounded in the source files listed in each section.
No span names or attribute keys are invented here.

---

## 1. Overview

### How tracing is set up

Source: `rust/search-api/src/telemetry/traces.rs`, `rust/search-api/src/main.rs`.

`init_tracing(telemetry_disabled, metrics)` is called once during process startup, before the gRPC
server begins accepting connections. It installs a global `tracing_subscriber` registry composed of
three layers stacked in order:

- **`EnvFilter` layer.** Reads `RUST_LOG` and applies it to all events and spans. When `RUST_LOG`
  is unset or invalid the default level is `info`. Four forced directives are always appended to
  the filter regardless of `RUST_LOG`:

  | Directive | Effect |
  |-----------|--------|
  | `lance::object_store::throttle=info` | Admits AIMD throttle events even when the Lance crate is filtered below `info` |
  | `lance::io_events=info` | Admits index-open and partition-load events |
  | `lance::dataset_events=info` | Admits dataset-lifecycle events |
  | `lance::file_audit=info` | Admits file create/delete audit events |

  These forced directives keep the `LanceEventMetricsLayer` and the OTLP export layer live for the
  four special Lance targets even when an operator sets `RUST_LOG=warn`.

- **OTLP gRPC span-exporter layer.** Present only when `telemetry_disabled` is `false`. Exports
  completed spans to the Datadog Agent via OTLP gRPC using a batch exporter. Endpoint resolution
  order: `OTEL_EXPORTER_OTLP_ENDPOINT`, then `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, then
  `http://{DD_AGENT_HOST}:4317`. Sampling is controlled by `OTEL_TRACES_SAMPLER` and
  `OTEL_TRACES_SAMPLER_ARG`. A failure during exporter construction degrades to log-only mode
  without panicking.

- **JSON stdout log layer (`DatadogJsonFormat`).** Always present. Every tracing event is written
  as a single-line JSON object containing:

  | Field | Content |
  |-------|---------|
  | `timestamp` | RFC 3339 with microsecond precision (UTC) |
  | `level` | `INFO`, `WARN`, `ERROR`, `DEBUG` |
  | `target` | The Rust tracing target string |
  | `message` | The formatted event message |
  | span fields | All fields of every active span from root to innermost, inner overrides outer |
  | event fields | The event's own fields, overriding span fields on collision |
  | `trace_id` | 32 hex digits (OTel convention), present only when a sampled span is active |
  | `span_id` | 16 hex digits (OTel convention), present only when a sampled span is active |

  The `trace_id` and `span_id` values are in the OpenTelemetry format that Datadog ingests directly
  for log-to-trace correlation.

- **`LanceEventMetricsLayer`.** Always present. Intercepts Lance span events on the four forced
  targets and forwards them to the `Metrics` DogStatsD facade. It adds no spans and no span
  attributes. It only reads event fields and emits metrics, so it never panics, never blocks a task,
  and adds no overhead for events on any other target.

### Enabling and disabling

| Mechanism | Effect |
|-----------|--------|
| `telemetry_disabled = true` (env `SEARCH_API_TELEMETRY_DISABLED=true`) | Disables OTLP span export. JSON logs and the Lance event metrics layer still run. |
| `RUST_LOG` | Controls which spans and events are emitted at all. Lance event targets are always forced to `info` regardless of this variable. |
| `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` | Controls OTel head sampling. Only sampled spans carry `trace_id`/`span_id` in logs. |
| `OTEL_SERVICE_NAME` / `DD_SERVICE` | Service name in the OTLP resource. Default: `search-api`. |
| `DD_ENV` / `DD_VERSION` | Datadog unified service tags attached as OTLP resource attributes and as constant DogStatsD tags. |

---

## 2. Span Hierarchy

The tree below shows how spans nest for each RPC type. The server span at the root is opened by
the `OtelGrpcLayer` tower middleware (`tonic_tracing_opentelemetry`), which also extracts inbound
W3C `traceparent` / `tracestate` headers from the gRPC metadata so a client-side trace is
automatically joined. Health-check RPCs are excluded from this layer by the `reject_healthcheck`
filter.

### VectorSearch

```
[server span: rpc.method=VectorSearch]   (OtelGrpcLayer)
  └─ backend.vector_search               (#[tracing::instrument])
       └─ provider.dataset               (#[tracing::instrument])
       └─ lance.vector_query             (#[tracing::instrument])
```

### TextSearch

```
[server span: rpc.method=TextSearch]     (OtelGrpcLayer)
  └─ backend.text_search                 (#[tracing::instrument])
       └─ provider.dataset               (#[tracing::instrument])
       └─ lance.text_query               (#[tracing::instrument])
```

### HybridSearch

```
[server span: rpc.method=HybridSearch]   (OtelGrpcLayer)
  └─ backend.hybrid_search               (#[tracing::instrument])
       └─ provider.dataset               (#[tracing::instrument])
       └─ lance.vector_query             (concurrent via tokio::join!)
       └─ lance.text_query               (concurrent via tokio::join!)
       └─ fusion.fuse                    (tracing::info_span!)
```

For hybrid search the two query-leg spans (`lance.vector_query` and `lance.text_query`) are
launched concurrently with `tokio::join!` inside `backend.hybrid_search`. Both are children of the
same `backend.hybrid_search` span and so appear as siblings in the trace, each carrying their own
`object_store.*` attributes independently.

### Prewarm

```
[server span: rpc.method=Prewarm]        (OtelGrpcLayer)
  └─ backend.prewarm                     (#[tracing::instrument])
       └─ provider.dataset               (#[tracing::instrument])
       └─ prewarm.index                  (tracing::info_span!, one per index, concurrent)
```

### Clusters

```
[server span: rpc.method=Clusters]       (OtelGrpcLayer)
  └─ (no child instrument span; Lance read runs inside the server span)
```

---

## 3. Span Table

Source files: `src/grpc/mod.rs`, `src/lance/backend.rs`, `src/lance/provider.rs`,
`src/lance/prewarm.rs`.

| Span name | Created by | What it covers | Key attributes | Parent |
|-----------|------------|----------------|----------------|--------|
| *(server span, name set by OtelGrpcLayer to the gRPC method path)* | `OtelGrpcLayer` tower middleware | The entire lifetime of one gRPC request, from wire receipt to response flush | `rpc.method`, `rpc.service`, `rpc.grpc.status_code` (set by `record_outcome`), `org_id`, `tenant_id`, `namespace` (set by `annotate_request_span`), `search.k` (set per RPC handler), `search.hybrid` (HybridSearch only), `prewarm.index_count` + `prewarm.resolved_version` (Prewarm only), `clusters.count` (Clusters only), `recall.*` attributes (sampled requests) | None (root of the trace or child of the inbound traceparent) |
| `backend.vector_search` | `#[tracing::instrument]` on `LanceSearchBackend::vector_search` | Lance vector ANN query lifecycle: dataset resolution plus query execution | `org_id`, `search.k` | Server span |
| `backend.text_search` | `#[tracing::instrument]` on `LanceSearchBackend::text_search` | Lance full-text query lifecycle: dataset resolution plus query execution | `org_id`, `search.k` | Server span |
| `backend.hybrid_search` | `#[tracing::instrument]` on `LanceSearchBackend::hybrid_search` | Concurrent vector and text query legs plus fusion | `org_id`, `search.k` | Server span |
| `backend.prewarm` | `#[tracing::instrument]` on `LanceSearchBackend::prewarm` | Dataset metadata open plus concurrent per-index prewarm tasks | `org_id`, `prewarm.resolved_version` (recorded after dataset open) | Server span |
| `provider.dataset` | `#[tracing::instrument]` on `CachingDatasetProvider::dataset` | One dataset handle resolution: tag resolution, cache lookup, and (on a miss) Lance dataset open | `org_id`, `tenant_id`, `namespace`, `dataset.version` (recorded on cold open only), `cache.dataset_handle_hit` (bool, always recorded) | Enclosing `backend.*` span |
| `lance.vector_query` | `#[tracing::instrument]` on `run_vector_query` | One Lance scanner scan for a nearest-neighbor query | `search.k`, `object_store.requests`, `object_store.iops`, `object_store.bytes_read`, `object_store.parts_loaded`, `object_store.indices_loaded` (all written by the scan-stats callback after the scan finishes) | `backend.vector_search` or `backend.hybrid_search` |
| `lance.text_query` | `#[tracing::instrument]` on `run_text_query` | One Lance scanner scan for a full-text query | `search.k`, `object_store.requests`, `object_store.iops`, `object_store.bytes_read`, `object_store.parts_loaded`, `object_store.indices_loaded` | `backend.text_search` or `backend.hybrid_search` |
| `fusion.fuse` | `tracing::info_span!` in `backend.hybrid_search` | Reciprocal-rank or weighted score fusion of the two leg result lists | `search.k` | `backend.hybrid_search` |
| `prewarm.index` | `tracing::info_span!` in `prewarm_indexes` | Prewarming one named index (one `dataset.prewarm_index` or `prewarm_index_with_options` call) | `index.name`, `index.kind` (`vector`, `fts`, or `scalar`) | `backend.prewarm` |

### Attribute notes

- `rpc.grpc.status_code` is written via `set_attribute` after the handler body returns, so it is
  always present on the server span regardless of whether the RPC succeeded or failed.
- `org_id`, `tenant_id`, and `namespace` are written on the server span by `annotate_request_span`
  through the OpenTelemetry layer's `set_attribute` path, which works even though the `OtelGrpcLayer`
  span does not declare those fields in its static field set.
- `dataset.version` on `provider.dataset` is recorded only on a cold open (cache miss). On a warm
  hit it is left as the empty placeholder value.
- `cache.dataset_handle_hit` is always recorded, true on a warm hit and false on a cold open.

---

## 4. Lance Span Events

Source: `rust/search-api/src/telemetry/traces.rs` (target constants and `LanceEventMetricsLayer`),
`rust/search-api/src/telemetry/metrics.rs` (tag enums with `from_lance` parsers).

Lance emits observability data on four dedicated tracing targets. The service forces each of these
targets to `info` in its `EnvFilter` regardless of `RUST_LOG`, so they always flow through both
the OTLP export layer and the `LanceEventMetricsLayer`.

These are point-in-time events. They mark that something happened (an index was opened, a file was
created) but they are not in-flight gauges. A high count of `load_vector_part` events in a query
span indicates that many partitions were loaded from storage during that scan, which is a signal to
examine the `object_store.*` attributes for byte and IOPS volume.

### 4.1 `lance::io_events`

Emitted at `info` level. The `type` field identifies the IO kind.

| `type` field value | Meaning | Typical parent span |
|--------------------|---------|---------------------|
| `open_scalar_index` | A BTree, bitmap, inverted, or ngram index was opened from the cache or storage | `lance.vector_query` or `lance.text_query` |
| `open_vector_index` | An IVF or HNSW vector index was opened | `lance.vector_query` |
| `open_frag_reuse_index` | The fragment-reuse system index was opened | `lance.vector_query` or `provider.dataset` |
| `open_mem_wal_index` | The memory-WAL system index was opened | `provider.dataset` |
| `load_vector_part` | A vector index partition (IVF centroid block) was loaded from storage | `lance.vector_query` |
| `load_scalar_part` | A scalar index partition was loaded from storage | `lance.text_query` |

Events on this target also drive the `search_api.lance.io_events` counter metric, tagged by
`io_type`. An unrecognized `type` value produces no metric.

### 4.2 `lance::dataset_events`

Emitted at `info` level. The `event` field identifies the lifecycle transition.

| `event` field value | Meaning | Typical parent span |
|---------------------|---------|---------------------|
| `loading` | A dataset version was opened (fires on every `DatasetBuilder::load()` call) | `provider.dataset` |
| `writing` | A write transaction is in progress | Not typical in the read-only search path |
| `committed` | A transaction was committed | Not typical in the read-only search path |
| `dropping_column` | A column is being dropped | Not typical in the search path |
| `deleting` | Rows are being deleted | Not typical in the search path |
| `compacting` | Fragments are being compacted | Not typical in the search path |
| `cleaning` | Old versions are being cleaned up | Not typical in the search path |

Events on this target also drive the `search_api.lance.dataset_events` counter metric, tagged by
`event`.

### 4.3 `lance::file_audit`

Emitted at `info` level. The `mode` field identifies the action and the `type` field identifies the
file kind. The actual file path appears in the event fields but is never promoted to a metric tag
to avoid unbounded tag cardinality.

| `mode` value | `type` value | Meaning |
|--------------|--------------|---------|
| `create` | `manifest` | A new manifest file was written |
| `create` | `index` | A new index file was written |
| `create` | `data` | A new data file was written |
| `create` | `deletion` | A new deletion file was written |
| `delete` | any of the above | A file was deleted after verification |
| `delete_unverified` | any of the above | A file was deleted without verification |

Events on this target also drive the `search_api.lance.file_audit` counter metric, tagged by `mode`
and `type`.

### 4.4 `lance::object_store::throttle`

Emitted at `warn` level (the `warn` level is admitted by the `info` forced directive). Lance's AIMD
rate limiter fires this event when it receives a throttling error (e.g., S3 503 SlowDown) and
reduces its fill rate.

| Field | Meaning |
|-------|---------|
| `error` | Present when the throttle event was triggered by an object-store error |
| `new_rate` | The limiter's freshly reduced fill rate, in requests per second, after the AIMD back-off |
| `previous_rate` | The rate before the back-off (informational, not used by the metrics layer) |

Events on this target drive two metrics: `search_api.throttle.errors` (counter, when `error` is
present) and `search_api.throttle.new_rate` (gauge, when `new_rate` is present). Both are
untagged: throttle pressure is a per-process signal and no org or tenant should appear on it.

---

## 5. `object_store.*` Span Attributes

Source: `rust/search-api/src/lance/backend.rs` (`ScanIoStats`, `execution_stats_callback`).

These five attributes are attached to every `lance.vector_query` and `lance.text_query` span after
the Lance scanner's execution plan finishes. They come from Lance's `ExecutionSummaryCounts`
execution-stats callback, which Lance invokes once per scan with aggregated totals. The attribute
names carry no cloud-provider prefix: the search service runs over AWS S3, Azure Blob Storage, and
GCS through Lance's provider-agnostic object-store layer, so all three providers report through
the same attributes.

| Attribute | Type | Meaning |
|-----------|------|---------|
| `object_store.requests` | `i64` | Total object-store requests made to the storage layer during this scan |
| `object_store.iops` | `i64` | I/O operations after coalescing (Lance may coalesce adjacent byte ranges into a single read before dispatching) |
| `object_store.bytes_read` | `i64` | Total bytes pulled from storage during this scan |
| `object_store.parts_loaded` | `i64` | Index partitions loaded from storage (IVF blocks, BTree pages, etc.) |
| `object_store.indices_loaded` | `i64` | Top-level index structures opened from storage |

These same values are also emitted as per-query distribution metrics:

| Attribute | Corresponding metric |
|-----------|---------------------|
| `object_store.iops` | `search_api.query.iops` (distribution, tagged by `rpc`) |
| `object_store.bytes_read` | `search_api.query.bytes_read` (distribution, tagged by `rpc`) |
| `object_store.parts_loaded` | `search_api.query.parts_loaded` (distribution, tagged by `rpc`) |

The metrics give fleet-level percentile views. The span attributes give per-request detail for
drilling into individual slow queries.

Lance exposes aggregate counts only through the callback available outside the `test-util` build.
A precise GET/HEAD/LIST breakdown is not exposed through this path.

---

## 6. `recall.*` Span Attributes

Source: `rust/search-api/src/telemetry/recall.rs`.

A deterministic, allocation-free sampler selects a fraction of `VectorSearch`, `TextSearch`, and
`HybridSearch` requests for recall capture. The fraction is fixed by the `DEFAULT_RECALL_SAMPLE_RATE`
constant in `rust/search-api/src/config.rs` (0.0, meaning disabled), no longer env-configurable. Each
query type has its own independent counter so the three streams sample independently.

When a request is sampled, the following flat set of attributes is attached to the current server
span (the `OtelGrpcLayer` span at the root of the trace) after the results are served. Attributes
that are not applicable to the query type are omitted.

### Shared attributes (all query types)

| Attribute | Type | Present | Meaning |
|-----------|------|---------|---------|
| `recall.sample` | bool | Always | Always `true`. The Datadog retention-filter key for this capture. |
| `recall.sample_id` | string | Always | UUIDv4 uniquely identifying this capture record. |
| `recall.captured_at_unix_ms` | i64 | Always | Capture wall-clock time in Unix milliseconds. |
| `recall.org_id` | string | Always | Target organization. |
| `recall.tenant_id` | string | Always | Target tenant. |
| `recall.namespace` | string | Always | Target namespace. |
| `recall.dataset_version` | i64 | When the dataset returns a version | Committed Lance dataset version that served the query. |
| `recall.k` | i64 | Always | Requested result count (`k` of the fused result for hybrid). |
| `recall.query_type` | string | Always | `vector`, `text`, or `hybrid`. |
| `recall.result_ids` | string | Always | JSON array of the served id-column values in rank order. Rows whose projection omitted the id column contribute `null` entries. |

### Vector-only attributes (present for `vector` and the vector knobs of `hybrid`)

| Attribute | Type | Present | Meaning |
|-----------|------|---------|---------|
| `recall.query_vector` | string | `vector` and `hybrid` | Full query vector as a compact JSON number array. |
| `recall.nprobes_min` | i64 | When nprobes bounds are set | Minimum probed partition count as recorded on the request. `nprobes` sets both min and max to the same value. |
| `recall.nprobes_max` | i64 | When nprobes bounds are set | Maximum probed partition count as recorded on the request. |
| `recall.refine_factor` | i64 | When set | Re-rank factor (number of extra candidates fetched for re-scoring). |
| `recall.distance_type` | string | When overriding the index default | Distance metric: `l2`, `cosine`, `dot`, or `hamming`. |
| `recall.filter` | string | Filtered vector searches only | Typed filter AST as stable JSON (the domain `Filter` struct's `serde` representation). |
| `recall.result_distances` | string | `vector` only | JSON array of the served distances in rank order. |

### Text and hybrid attributes

| Attribute | Type | Present | Meaning |
|-----------|------|---------|---------|
| `recall.text_query` | string | `text` and `hybrid` | The `TextQueryNode` AST as stable JSON. |
| `recall.text_columns` | string | `text` and `hybrid` | JSON array of the text query columns. |
| `recall.result_scores` | string | `text` and `hybrid` | JSON array of the served relevance scores (BM25 for text, fused score for hybrid) in rank order. |

### Hybrid-only attributes

| Attribute | Type | Present | Meaning |
|-----------|------|---------|---------|
| `recall.fusion` | string | `hybrid` only | Fusion strategy as JSON, for example `{"rrf":{"k":60.0}}` or `{"weighted":{"vector_weight":0.7}}`. |

### Recall capture and the offline recall audit job

The `recall.*` attributes are the wire format that feeds the Python `RecallAuditJob`
(`src/lance_etl/recall.py`). That job retrieves sampled spans from Datadog using the Spans API,
filters on `recall.sample:true`, replays the captured queries against the current dataset, and
scores recall@k, nDCG@k, and MRR. The capture must be retained beyond live query serving: configure
a Datadog retention filter on `recall.sample:true` so sampled spans are indexed and stored for
the offline job's replay window.

The `search_api.recall.samples` counter metric (tagged by `query_type` and `filtered`) tracks
capture throughput without storing any query content.

---

## 7. Traces and Metrics: When to Use Which

### Metric signals from `Metrics` (`search_api.*`)

| Metric | Signal type | Tags | Use case |
|--------|-------------|------|----------|
| `search_api.rpc.requests` | counter | `rpc`, `status` | Request rate, error rate per RPC |
| `search_api.rpc.duration_ms` | distribution | `rpc`, `status` | Latency percentiles per RPC |
| `search_api.rpc.errors` | counter | `rpc`, `status` | Non-ok error rate (subset of requests) |
| `search_api.query.iops` | distribution | `rpc` | Per-query I/O operation spread |
| `search_api.query.bytes_read` | distribution | `rpc` | Per-query bytes pulled from storage |
| `search_api.query.parts_loaded` | distribution | `rpc` | Per-query index partitions loaded |
| `search_api.throttle.errors` | counter | (none) | Object-store throttle error rate |
| `search_api.throttle.new_rate` | gauge | (none) | Current AIMD limiter fill rate after back-off |
| `search_api.lance.io_events` | counter | `io_type` | Index open and partition load event rate |
| `search_api.lance.dataset_events` | counter | `event` | Dataset lifecycle event rate (`loading` = opens) |
| `search_api.lance.file_audit` | counter | `mode`, `type` | File create/delete event rate |
| `search_api.dataset.open.duration_ms` | distribution | `cold` | Handle resolution latency, cold vs warm |
| `search_api.cache.lookup` | counter | `cache`, `tier`, `outcome` | Hit/miss rate per cache and tier |
| `search_api.cache.handles.entries` | gauge | (none) | Open handle count |
| `search_api.cache.handles.weighted_size` | gauge | (none) | Weighted handle budget used |
| `search_api.serve.cold_open` | counter | `warmed` | Serving opens that missed prewarm |
| `search_api.serve.tag_resolved` | counter | `changed` | Tag re-resolution rate, flip detection |
| `search_api.prewarm.duration_ms` | distribution | `status` | Prewarm latency and success rate |
| `search_api.prewarm.index.duration_ms` | distribution | `kind` | Per-index prewarm latency |
| `search_api.prewarm.indexes_warmed` | counter | (none) | Indexes warmed per call |
| `search_api.prewarm.warmed_bytes` | distribution | (none) | Cache bytes resident after prewarm |
| `search_api.prewarm.last_version` | gauge | (none) | Most recently prewarmed dataset version |
| `search_api.recall.samples` | counter | `query_type`, `filtered` | Recall capture throughput |
| `search_api.clusters.read.duration_ms` | distribution | (none) | Clusters centroid-read latency |
| `search_api.clusters.centroids` | distribution | (none) | Centroid count returned |

Tag policy: `org_id`, `tenant_id`, `namespace`, and dataset version never appear on any metric.
Per-tenant detail lives exclusively on traces and logs. This policy prevents timeseries explosion
at tens-of-thousands-of-orgs scale.

### When to use a trace vs a metric

Use metrics for:
- Alerting on error rate, p99 latency, throttle events, or cache hit rate across the fleet.
- SLO tracking. Metrics are pre-aggregated and cheap to query.
- Spotting regressions in query byte volume or partition-load counts by RPC type.

Use traces for:
- Diagnosing a specific slow or failed request by its `trace_id`.
- Finding which org/tenant/namespace a slow request came from (only on the span, never on metrics).
- Understanding the breakdown of a hybrid request into its two query legs and their independent
  object-store costs.
- Replaying a specific query with recall scoring (via `recall.*` attributes on sampled spans).
- Correlating a log line to its trace: every JSON log line emitted while a sampled span is active
  carries `trace_id` and `span_id`.

---

## 8. Operator How-To: Drilling a Slow Query

### Finding and following a slow query

1. In Datadog, open the **APM** service view for `search-api` (or the service name set by
   `DD_SERVICE` / `OTEL_SERVICE_NAME`).
2. Filter traces by the RPC method (`rpc.method = VectorSearch` etc.) and sort by duration
   descending.
3. Open a slow trace. The root span is the server span with `org_id`, `tenant_id`, and `namespace`.
4. Identify which child span is slow. For a vector search it will be `lance.vector_query`. For a
   hybrid search, check both `lance.vector_query` and `lance.text_query` and compare their
   durations.

### Interpreting object-store volume on a slow query leg

On the `lance.vector_query` or `lance.text_query` span, inspect the `object_store.*` attributes:

- High `object_store.bytes_read` relative to the result count suggests a wide scan with little
  index pruning. Check whether the request uses prefiltering with a high-selectivity predicate
  that defeats the vector index.
- High `object_store.parts_loaded` relative to the dataset's `num_partitions` configuration
  suggests that `nprobes` is high or that the AIMD limiter is throttling the requests and multiple
  retries are inflating the count. Cross-check `search_api.throttle.errors` in metrics.
- High `object_store.iops` with low `object_store.bytes_read` suggests many small reads, which
  may indicate cold partition metadata or fragmented index files after compaction.

### Correlating a log line to a trace

Any JSON log line emitted while a sampled span is active carries `trace_id` (32 hex digits). Take
that value and paste it into the Datadog Log Search as `trace_id:<value>` to jump directly to the
trace. This is the standard OTel log-to-trace correlation format that Datadog ingests natively.

### Checking Lance IO events on a slow span

The span events attached to a `lance.vector_query` or `lance.text_query` span from the
`lance::io_events` target show which indexes were opened or which partitions were loaded during
that scan. A large number of `load_vector_part` events on a single query leg means many IVF
partitions were fetched, likely because `nprobes` is high or the query is near cluster boundaries.
Each event is a point in time and does not convey whether the load was cache-served or
storage-served. Cross-reference the `object_store.bytes_read` attribute: a high bytes count with
many `load_vector_part` events confirms storage-bound partition loading rather than cache hits.

### Using the recall pipeline after observing a degraded recall metric

If the offline `RecallAuditJob` reports a drop in recall@k or nDCG@k:

1. Retrieve the sampled spans from Datadog with the retention filter `recall.sample:true` for the
   time window of interest.
2. The `recall.query_vector`, `recall.k`, `recall.filter`, and `recall.nprobes_min` /
   `recall.nprobes_max` attributes on each sampled span give the exact query parameters that were
   served.
3. Replay those queries against the current dataset at the version recorded in
   `recall.dataset_version` and compare the result ids in `recall.result_ids` against ground truth.
4. The `recall.fusion` and `recall.text_query` attributes on hybrid spans let the job replay the
   full fused query, not just the vector leg.
