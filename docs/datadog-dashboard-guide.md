# Datadog Dashboard Guide: lance-etl Observability

This guide describes how to build a Datadog dashboard that covers the two operational planes of
lance-etl: the Python ETL data plane (ingestion, indexing, compaction, TTL expiry, and recall
scoring) and the Rust gRPC search serving plane. It is a reference for the engineers who own these
systems. It is not a step-by-step Datadog UI tutorial. It assumes the reader knows how to create
widgets and template variables in Datadog.

---

## Table of Contents

1. [Purpose and Audience](#1-purpose-and-audience)
2. [Prerequisites and Setup](#2-prerequisites-and-setup)
3. [Template Variables](#3-template-variables)
4. [Dashboard Sections](#4-dashboard-sections)
   - 4.1 Service Health and SLOs
   - 4.2 Search Latency and Throughput
   - 4.3 Search Quality and Recall
   - 4.4 Cache Effectiveness
   - 4.5 Prewarm
   - 4.6 ETL Pipeline
   - 4.7 Compaction (Maintenance)
   - 4.8 TTL Expiry
   - 4.9 Indexing
   - 4.10 Errors and Saturation
   - 4.11 Resource and IO
5. [Widget Type Reference](#5-widget-type-reference)
6. [Monitors and Alerts](#6-monitors-and-alerts)
7. [Tips and Pitfalls](#7-tips-and-pitfalls)

---

## 1. Purpose and Audience

This dashboard is the single pane of glass for the engineers who operate the lance-etl system.
It covers two distinct planes:

- **Data plane (Python).** The Spark-based pipeline that reads Iceberg increments, upserts rows
  into Lance datasets, builds and commits indexes, compacts fragments, expires TTL rows, and scores
  recall. Metrics are emitted under the `lance.pipeline.*` namespace via DogStatsD.
- **Serving plane (Rust).** The gRPC search service (`search-api`) that handles VectorSearch,
  TextSearch, HybridSearch, Prewarm, and Clusters RPCs. Metrics are emitted under the
  `search_api.*` namespace. Traces are forwarded over OTLP to the Datadog Agent.

Primary readers are on-call engineers responding to alerts, platform engineers tuning pipeline
throughput, and ML engineers monitoring recall quality over time. The dashboard is read-only for
most users. Only on-call engineers need edit access.

---

## 2. Prerequisites and Setup

### Datadog Agent

Every host that runs a Python pipeline job or the Rust search service must have a Datadog Agent
installed and reachable.

- **DogStatsD (Python and Rust metrics).** The Agent listens on UDP port 8125 by default. The
  Python `Telemetry` class writes to `statsd_host:statsd_port` (from `TelemetryConfig`, default
  `localhost:8125`). The Rust service writes to the address resolved from
  `SEARCH_API_STATSD_ADDR`, which defaults to `DD_AGENT_HOST:8125` when `DD_AGENT_HOST` is set and
  falls back to `127.0.0.1:8125`. Both emit on the same channel.
- **OTLP traces (Rust service only).** The Rust service exports spans over OTLP gRPC. The endpoint
  is resolved from `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`) first,
  then `http://{DD_AGENT_HOST}:4317`. Enable the OTLP receiver in the Agent's `datadog.yaml`.
- **ddtrace (Python traces).** The Python jobs trace through `ddtrace`. The tracer connects to the
  Agent using `ddtrace`'s own defaults.

### Metric Namespaces

| Plane | Namespace | Sender |
|---|---|---|
| Rust search service | `search_api.*` | `Metrics::dogstatsd` in `src/telemetry/metrics.rs` |
| Python pipeline | `lance.pipeline.*` | `Telemetry.create` in `src/lance_etl/telemetry.py` |
| Lance internals (bridged) | `lance.pipeline.lance.*` | `attach_lance_event_bridge` via `capture_trace_events` |

The Lance event bridge promotes Lance's internal execution stats, throttle events, and file audit
events into `lance.pipeline.lance.execution.*`, `lance.pipeline.lance.throttle.*`, and similar
sub-namespaces. These arrive via the same DogStatsD path but are produced by Lance's callback API,
not by hand-written pipeline code.

### Standard Tags

All metrics from both planes carry the unified-service tags `env`, `service`, and `version` as
constant tags. These are configured by:

- **Rust.** `DD_ENV`, `DD_SERVICE` (or `OTEL_SERVICE_NAME`), and `DD_VERSION` environment
  variables, read at startup in `src/telemetry/metrics.rs` (`with_default_tags`) and
  `src/telemetry/traces.rs` (resource attributes on the OTLP provider).
- **Python.** `TelemetryConfig.env`, `TelemetryConfig.service`, and `TelemetryConfig.version`,
  applied as constant DogStatsD tags and as `ddtrace` service/env at `Telemetry.create` time.

The default service name for the Rust service is `search-api`. The default for the Python pipeline
is `lance-pipeline`. Use these values when filtering in Datadog.

---

## 3. Template Variables

Add the following template variables to the dashboard so every widget can be scoped without
duplicating per-widget filters.

| Variable name | Tag key | Example values | Notes |
|---|---|---|---|
| `$env` | `env` | `prod`, `staging`, `dev` | Primary scope control. |
| `$service` | `service` | `search-api`, `lance-pipeline` | Switch between planes or view both. |
| `$version` | `version` | `1.4.2`, `latest` | Helps correlate regressions to deploys. |
| `$rpc` | `rpc` | `vector_search`, `text_search`, `hybrid_search`, `prewarm`, `clusters`, `write`, `write_stream` | Scope search sections to one RPC. |
| `$cache` | `cache` | `index`, `store`, `handles` | Scope cache sections. |

Apply `$env` and `$service` as defaults on every widget. Use the others as opt-in scopes inside
their respective sections.

---

## 4. Dashboard Sections

### 4.1 Service Health and SLOs

**Goal.** Confirm the search service is running, accepting traffic, and meeting its latency and
error-rate targets. This section is the first thing an on-call engineer looks at.

**Metrics.**

| Metric | Type | Tags | What it measures |
|---|---|---|---|
| `search_api.rpc.requests` | count | `rpc`, `status` | Total request throughput by RPC and outcome. |
| `search_api.rpc.errors` | count | `rpc`, `status` | Error throughput. Only emitted on non-`ok` statuses. |
| `search_api.rpc.duration_ms` | distribution | `rpc`, `status` | Per-request end-to-end latency in milliseconds. |

**Status values.** `ok` is the success tag. Non-ok values are gRPC status codes in snake_case
(e.g. `not_found`, `internal`, `unavailable`). These come from the gRPC transport layer in
`src/grpc/mod.rs` and are passed into `Metrics::rpc` at the end of each RPC handler.

**Widgets.**

- **Request rate (timeseries).** `sum:search_api.rpc.requests{$env,$service} by {rpc}.as_rate()`.
  Good: stable or growing. Bad: sudden drop (traffic stopped reaching the service).
- **Error rate (timeseries).** `sum:search_api.rpc.errors{$env,$service} by {rpc}.as_rate()`.
  Good: zero or near-zero. Bad: any sustained non-zero error rate.
- **Error ratio (query value).** `sum:search_api.rpc.errors / sum:search_api.rpc.requests`.
  Display as a percentage. A single number gives the on-call engineer the headline ratio at a glance.
- **p50 / p95 / p99 latency (timeseries or distribution).** Use `p50:search_api.rpc.duration_ms`,
  `p95:search_api.rpc.duration_ms`, and `p99:search_api.rpc.duration_ms`, each filtered by
  `rpc:vector_search` (or the template variable). Good: p99 below your SLO threshold. Bad: p99
  climbing above it or diverging from p50.
- **SLO widget.** Create a Datadog SLO backed by the error-rate monitor (see Section 6). Display
  the 7-day and 30-day SLO budget burn on this section.

### 4.2 Search Latency and Throughput

**Goal.** Compare latency across the three search RPC types and understand the distribution shape,
not just a single percentile. Latency regressions often appear in the tail before the p50 moves.

**Metrics.** The same `search_api.rpc.duration_ms` distribution applies, scoped by `rpc`.

**Widgets.**

- **Per-RPC latency heatmap.** One heatmap widget per RPC (`vector_search`, `text_search`,
  `hybrid_search`). Heatmaps show the full distribution over time and reveal bimodal shapes (cold
  vs warm cache) that percentiles hide.
- **Per-RPC throughput (timeseries).** `sum:search_api.rpc.requests{rpc:vector_search}.as_rate()`.
  Repeat for the other two types. Stacked or overlaid.
- **Distribution widget (latency percentiles).** A top-list or distribution widget showing p50,
  p95, and p99 for each RPC type side by side. Good for quick cross-RPC comparison.
### 4.3 Search Quality and Recall

**Goal.** Track whether the search service is returning high-quality results over time, both from
the live serving capture (counters) and from the offline recall audit job (quality scores).

#### Live recall capture (Rust service)

The service samples a deterministic fraction of eligible requests at the rate fixed by the
`DEFAULT_RECALL_SAMPLE_RATE` constant in `rust/search-api/src/config.rs` (0.0, disabled by default,
no longer env-configurable). Sampled requests have `recall.*` span attributes attached to their
OTLP spans and also increment the counter below.

| Metric | Type | Tags | Notes |
|---|---|---|---|
| `search_api.recall.samples` | count | `query_type`, `filtered` | Counts samples captured. Not a quality score. |

`query_type` is one of `vector`, `text`, or `hybrid`. `filtered` is `true` or `false`.

To see recall sample throughput over time, graph `sum:search_api.recall.samples by {query_type}.as_rate()`.
A drop to zero means the sampler is disabled or the service stopped receiving traffic.

#### Recall span attributes (trace-based)

Sampled spans carry a flat set of `recall.*` attributes. The Datadog Spans API can retrieve them.
A Datadog retention filter on `@recall.sample:true` is required so sampled spans are indexed and
not discarded after 15 minutes. Once retained, use Datadog's Analytics Explorer to build funnel
views, filter by `recall.query_type`, `recall.namespace`, and `recall.dataset_version`.

Key span attributes available for analytics:

- `recall.sample` (bool) — retrieval filter.
- `recall.query_type` (string) — `vector`, `text`, or `hybrid`.
- `recall.k` (int) — requested result count.
- `recall.dataset_version` (int) — version that served the query.
- `recall.nprobes_min` / `recall.nprobes_max` (int) — probed partitions.
- `recall.result_ids` (string, JSON array) — served ids in rank order.
- `recall.result_distances` (string, JSON array) — served distances (vector only).
- `recall.result_scores` (string, JSON array) — served scores (text and hybrid).
- `recall.filter` (string, JSON) — the typed filter AST (filtered vector only).

#### Offline recall audit (Python, `lance.pipeline.*`)

The `RecallAuditJob` in `recall.py` runs as a separate job, fetches sampled spans from Datadog,
replays them against Lance, and scores recall. It emits quality scores as gauges.

| Metric | Type | Tags | What it measures |
|---|---|---|---|
| `lance.pipeline.recall.measured` | gauge | per-bucket tags | Mean recall@k for a bucket of samples. |
| `lance.pipeline.recall.ndcg` | gauge | per-bucket tags | Mean nDCG@k for a bucket. |
| `lance.pipeline.recall.mrr` | gauge | per-bucket tags | Mean MRR for a bucket. |
| `lance.pipeline.recall.samples_fetched` | gauge | none | Total samples fetched from Datadog. |
| `lance.pipeline.recall.samples_scored` | gauge | none | Samples successfully scored. |
| `lance.pipeline.recall.samples_skipped` | gauge | none | Samples skipped (missing data, version drift). |
| `lance.pipeline.recall.group_ms` | distribution | none | Time per dataset group scored. |
| `lance.pipeline.recall.score_ms` | distribution | none | Time per scoring batch. |
| `lance.pipeline.recall.small_tier_ms` | distribution | none | Small-tier total scoring time. |
| `lance.pipeline.recall.large_tier_ms` | distribution | none | Large-tier total scoring time. |
| `lance.pipeline.recall.small_groups` | gauge | none | Number of small dataset groups. |
| `lance.pipeline.recall.large_groups` | gauge | none | Number of large dataset groups. |
| `lance.pipeline.recall.large_group_fragments` | gauge | none | Fragments in large groups. |
| `lance.pipeline.recall.dataset_missing` | count | none | Groups where dataset was not found. |
| `lance.pipeline.recall.missing_columns` | count | none | Groups missing expected schema columns. |
| `lance.pipeline.recall.version_drift` | count | none | Groups scored against a drifted version. |

**Widgets.**

- **Recall@k over time (timeseries).** `avg:lance.pipeline.recall.measured`. Trend over days to
  weeks. Good: stable at or above your quality target. Bad: drifting downward, which suggests index
  staleness or a query-parameter regression.
- **nDCG@k and MRR (timeseries).** Same shape as recall. Place these on the same graph or in a
  table widget for comparison.
- **Sample coverage (query value).** `avg:lance.pipeline.recall.samples_scored /
  avg:lance.pipeline.recall.samples_fetched`. Low coverage means many samples were skipped.
- **Skip reasons (table).** `sum:lance.pipeline.recall.dataset_missing`,
  `lance.pipeline.recall.missing_columns`, `lance.pipeline.recall.version_drift`. A table widget
  with one row per reason shows where scoring is failing.

### 4.4 Cache Effectiveness

**Goal.** Understand whether the two-tier cache (in-memory Moka + on-disk) is serving index data
efficiently and whether its budgets are appropriate.

There are three logical caches, identified by the `cache` tag:

- `index` — the serialized Lance index cache (disk + memory hot tier).
- `store` — the metadata byte cache wrapping the object store.
- `handles` — the open-dataset-handle LRU (counted separately, no tier breakdown).

**Metrics.**

| Metric | Type | Tags | What it measures |
|---|---|---|---|
| `search_api.cache.lookup` | count | `cache`, `tier`, `outcome` | Lookup count by cache, tier, and hit/miss. |
| `search_api.cache.insert_bytes` | count | `cache`, `tier` | Bytes written to the disk tier per insert. |
| `search_api.cache.disk.bytes` | gauge | `cache` | Total bytes resident on disk after a sweep. |
| `search_api.cache.disk.entries` | gauge | `cache` | Total entries on disk after a sweep. |
| `search_api.cache.evictions` | count | `cache`, `reason` | Evictions by cache and reason (`ttl`, `size`, `corrupt`). |
| `search_api.cache.serialize_errors` | count | `cache` | Entries that failed to serialize (memory-only fallback). |
| `search_api.cache.handles.entries` | gauge | none | Open-dataset-handle LRU entry count. |
| `search_api.cache.handles.weighted_size` | gauge | none | Total weighted size of the handle LRU. |
| `search_api.dataset.open.duration_ms` | distribution | `cold` | Dataset open latency. `cold:true` = cache miss (actual open). |

**Derived hit rate.** Datadog does not compute hit rate natively from two raw counters, but you can
add a formula widget:

```
sum:search_api.cache.lookup{outcome:hit} /
(sum:search_api.cache.lookup{outcome:hit} + sum:search_api.cache.lookup{outcome:miss})
```

Apply `cache:index` and `tier:memory` (or `tier:disk`) as additional filters to get hit rates per
cache and tier separately.

**Widgets.**

- **Hit rate (timeseries, formula).** One line per `(cache, tier)` combination. Good: memory hit
  rate above 80%, disk hit rate above 60% for index. Bad: both tiers showing low hit rates means
  the budget is too small or the working set is too large.
- **Disk residency (timeseries).** `search_api.cache.disk.bytes` by `cache`. Compare against the
  fixed budget (`DEFAULT_DISK_INDEX_CACHE_BYTES` in `config.rs`, 8 GiB, no longer env-configurable).
  A gauge that never reaches the budget means the cache is not filling. One that is always at the
  ceiling means every insert triggers an eviction.
- **Evictions by reason (timeseries).** `sum:search_api.cache.evictions by {reason}`. Sustained
  `size` evictions mean the budget is too small. `ttl` evictions are expected (7-day TTL). `corrupt`
  evictions mean serialization is failing.
- **Cold opens (timeseries).** `sum:search_api.dataset.open.duration_ms{cold:true}` and
  `sum:search_api.dataset.open.duration_ms{cold:false}` overlaid. A surge in cold opens means the
  handle LRU is evicting too aggressively or a large fleet of new datasets was opened.
- **Handle LRU size (timeseries).** `search_api.cache.handles.weighted_size`. Compare against
  the fixed capacity (`DEFAULT_DATASET_CACHE_CAPACITY` in `config.rs`, 16384 weighted units, no
  longer env-configurable). Sustained saturation at the ceiling means the LRU is evicting cheap
  handles, which causes cold opens.

### 4.5 Prewarm

**Goal.** Confirm that the prewarm job is running before blue-green tag flips, completing
successfully, and keeping the index cache warm so serving does not pay cold-start latency.

**Metrics.**

| Metric | Type | Tags | What it measures |
|---|---|---|---|
| `search_api.prewarm.duration_ms` | distribution | `status` | Total Prewarm RPC latency. `status`: `ok`, `partial`, `error`. |
| `search_api.prewarm.index.duration_ms` | distribution | `kind` | Per-index prewarm latency. `kind`: `vector`, `fts`, `scalar`. |
| `search_api.prewarm.indexes_warmed` | count | none | Indexes successfully warmed per call. |
| `search_api.prewarm.warmed_bytes` | distribution | none | Bytes brought into cache per call. |
| `search_api.prewarm.last_version` | gauge | none | Most recently prewarmed dataset version on this process. |
| `search_api.serve.cold_open` | count | `warmed` | Cold opens at serving time. `warmed:false` = flip without prewarm. |
| `search_api.serve.tag_resolved` | count | `changed` | Serve-tag re-resolutions. `changed:true` = observed a flip. |

**Widgets.**

- **Prewarm success rate (timeseries or query value).** `sum:search_api.prewarm.duration_ms{status:ok}
  / sum:search_api.prewarm.duration_ms`. Good: 1.0. Bad: `partial` or `error` statuses appearing.
- **Prewarm latency by index kind (timeseries).** `p95:search_api.prewarm.index.duration_ms by
  {kind}`. Vector indexes typically take longer than scalar or FTS. A sudden increase suggests a
  larger index or slower object-store throughput.
- **Bytes warmed (timeseries).** `avg:search_api.prewarm.warmed_bytes`. A drop to zero means
  prewarm ran but found nothing to warm (all entries already in cache, which is good) or something
  is wrong.
- **Flip propagation (timeseries).** `sum:search_api.serve.tag_resolved{changed:true}`. Each count
  is one replica observing a tag flip. Spread across replicas over time, this shows flip
  propagation latency. `serve.cold_open{warmed:false}` immediately after a flip is the cold-start
  penalty signal.
- **Last prewarmed version (timeseries or query value).** `max:search_api.prewarm.last_version`.
  Compare against the current serving version (from traces). They should be equal or the prewarm
  version should lead.

### 4.6 ETL Pipeline

**Goal.** Monitor the incremental Iceberg-to-Lance ETL job: how many datasets were touched, how
many rows were upserted and deleted, how long merges took, and whether commit conflicts are
increasing.

All metrics are under `lance.pipeline.*` and originate from `src/lance_etl/etl.py`. Tags are
added by the calling code at emit time (e.g. a `conflicts:` bucket tag on merge latency).

**Metrics.**

| Metric | Type | Tags | What it measures |
|---|---|---|---|
| `lance.pipeline.run.datasets` | gauge | none | Datasets touched in the run. |
| `lance.pipeline.run.upserted` | gauge | none | Total upserted rows across all datasets in the run. |
| `lance.pipeline.run.deleted` | gauge | none | Total deleted rows across all datasets in the run. |
| `lance.pipeline.run.execute_ms` | distribution | none | End-to-end run wall time in milliseconds. |
| `lance.pipeline.dataset.merged` | count | none | Successful merge operations. |
| `lance.pipeline.dataset.merge_error` | count | none | Failed merge operations. |
| `lance.pipeline.dataset.merge_ms` | distribution | `conflicts` | Merge latency. `conflicts` bucket: `0`, `1`, `2+`. |
| `lance.pipeline.dataset.merge_conflict_retries` | count | `conflicts` | Conflict retries observed per merge. |
| `lance.pipeline.dataset.upserted` | distribution | none | Upserted row count per dataset. |
| `lance.pipeline.dataset.deleted` | distribution | none | Deleted row count per dataset. |
| `lance.pipeline.dataset.delete_ms` | distribution | none | Delete operation latency per dataset. |
| `lance.pipeline.errors` | count | none | ETL partition errors (incremented by `Telemetry.error`). |

**Widgets.**

- **Run summary (query values).** `avg:lance.pipeline.run.datasets`, `avg:lance.pipeline.run.upserted`,
  `avg:lance.pipeline.run.deleted`. One query value widget each for the most recent run.
- **Merge throughput (timeseries).** `sum:lance.pipeline.dataset.merged.as_rate()` and
  `sum:lance.pipeline.dataset.merge_error.as_rate()` overlaid.
- **Merge latency by conflict bucket (timeseries).** `p95:lance.pipeline.dataset.merge_ms by
  {conflicts}`. Queries with `conflicts:2+` should be a minority. A growing `2+` share means write
  contention is increasing.
- **Conflict retries (timeseries).** `sum:lance.pipeline.dataset.merge_conflict_retries`. An
  upward trend means the dataset is hot and the retry budget is being consumed regularly.
- **Row throughput (timeseries).** `avg:lance.pipeline.dataset.upserted` and
  `avg:lance.pipeline.dataset.deleted` overlaid.

### 4.7 Compaction (Maintenance)

**Goal.** Confirm that the compaction job is keeping fragment counts under control and that its
commit conflicts and errors are within tolerance.

The compaction job runs in two tiers: small datasets are compacted in Spark executors, large
datasets use a distributed plan. Both emit into the `lance.pipeline.*` namespace via
`src/lance_etl/compaction.py`. The note in the task description about renaming to "maintenance" is
noted: the current code uses "compaction" for the fragment-merge step and "ttl" for the expiry
step. They may share an Airflow DAG, but their metrics remain distinct. Read the current metric
names below as they appear in the code.

**Metrics.**

| Metric | Type | Tags | What it measures |
|---|---|---|---|
| `lance.pipeline.run.datasets` | gauge | none | Datasets processed in the compaction run. |
| `lance.pipeline.run.bytes_removed` | gauge | none | Total bytes freed across all datasets in the run. |
| `lance.pipeline.run.fragments_removed` | gauge | none | Total fragments removed in the run. |
| `lance.pipeline.run.small_tier_ms` | distribution | none | Small-tier wall time. |
| `lance.pipeline.run.large_tier_ms` | distribution | none | Large-tier wall time. |
| `lance.pipeline.run.tags_flipped` | gauge | none | Datasets whose serving tag was updated after compaction. |
| `lance.pipeline.run.serving_tag_ms` | distribution | none | Time to update serving tags. |
| `lance.pipeline.run.manifests_migrated` | gauge | none | Datasets migrated to V2 manifest paths. |
| `lance.pipeline.dataset.compacted` | count | none | Datasets successfully compacted. |
| `lance.pipeline.dataset.committed` | count | none | Compaction commits (small and large tiers). |
| `lance.pipeline.dataset.commit_conflict` | count | none | Commit conflicts during compaction. |
| `lance.pipeline.dataset.total_ms` | distribution | `uri` | Per-dataset compaction wall time. |
| `lance.pipeline.dataset.bytes_removed` | distribution | none | Bytes removed per dataset per compaction. |
| `lance.pipeline.dataset.cleanup_ms` | distribution | none | Cleanup (old version removal) latency per dataset. |
| `lance.pipeline.dataset.deferred_to_large_tier` | count | none | Small-tier datasets escalated to the large tier. |
| `lance.pipeline.dataset.hot_skipped` | count | `uri` | Datasets skipped because they were too recently touched. |
| `lance.pipeline.dataset.replanned` | count | `uri` | Large-tier datasets whose compaction plan was regenerated. |
| `lance.pipeline.dataset.rewrite_ms` | distribution | none | Large-tier rewrite phase latency. |
| `lance.pipeline.dataset.commit_ms` | distribution | none | Large-tier commit phase latency. |
| `lance.pipeline.dataset.tag_update_ms` | distribution | `tag` | Time to write one dataset's serving tag. |
| `lance.pipeline.dataset.tag_created` | count | `tag` | Tags created for the first time. |
| `lance.pipeline.dataset.tag_updated` | count | `tag` | Tags updated (version changed). |
| `lance.pipeline.dataset.manifest_migrated` | count | none | Individual dataset manifests migrated. |
| `lance.pipeline.dataset.migrate_manifest_ms` | distribution | `uri` | Per-dataset manifest migration time. |
| `lance.pipeline.run.migrate_manifest_ms` | distribution | none | Total manifest migration run time. |

**Widgets.**

- **Bytes freed per run (timeseries or query value).** `avg:lance.pipeline.run.bytes_removed`.
  Good: consistently non-zero, confirming compaction is removing fragments. A long stretch of zero
  means the pipeline is not writing enough fragments to trigger compaction, or the job is failing.
- **Fragment removal rate (timeseries).** `avg:lance.pipeline.run.fragments_removed`. Watch for
  sudden drops.
- **Compaction conflicts (timeseries).** `sum:lance.pipeline.dataset.commit_conflict`. Should be
  low. A surge means the dataset is being written concurrently during compaction.
- **Tier timing (timeseries).** `p95:lance.pipeline.run.small_tier_ms` and
  `p95:lance.pipeline.run.large_tier_ms`. Large-tier compaction is expected to be slower. Sudden
  increases in either suggest I/O pressure or a large dataset being compacted.
- **Tag flips (timeseries).** `sum:lance.pipeline.run.tags_flipped`. When using blue-green
  deployment, this shows how many datasets were promoted to the new version after compaction.

### 4.8 TTL Expiry

**Goal.** Confirm the TTL delete step is expiring rows on schedule and that it is not silently
skipping datasets or piling up commit conflicts.

There is no standalone TTL module or CLI command. Per-row TTL expiration is one phase of the
unified fleet job: `run_ttl_on_open_dataset` in `src/lance_etl/maintenance/job.py`, driven by
`MaintenanceJob` and, in production, by the code-owned reconciler release profile (the
Airflow `lance_etl_pipeline` DAG's `lance_etl_ttl_column` Variable). Leaving that column unset
turns TTL off and the job is compaction plus cleanup only. TTL emits under the same
`lance.pipeline.dataset.*` namespace as compaction (4.8), not a separate `ttl.*` namespace, because
it is one phase of the same per-dataset task.

**Metrics.**

| Metric | Type | Tags | What it measures |
|---|---|---|---|
| `lance.pipeline.dataset.ttl_delete_ms` | distribution | none | Per-dataset TTL delete latency, timed around the retrying delete action. |
| `lance.pipeline.dataset.ttl_rows_deleted` | distribution | none | Rows deleted per dataset by the TTL predicate, including zero when nothing expired. |
| `lance.pipeline.dataset.ttl_expired` | count | none | Emitted once per dataset where the TTL delete removed at least one row. |
| `lance.pipeline.dataset.ttl_commit_conflict` | count | none | TTL delete commit conflicts observed by `commit_with_retries` before it succeeded or exhausted its budget. |
| `lance.pipeline.dataset.ttl_column_missing` | count | none | Datasets skipped because the configured TTL column or timestamp column is absent from the schema. |

**Widgets.**

- **TTL row throughput (timeseries).** `avg:lance.pipeline.dataset.ttl_rows_deleted`. Rises when
  more data is aging out. A sudden spike may indicate a misconfigured expiry window.
- **Datasets actively expiring (timeseries).** `sum:lance.pipeline.dataset.ttl_expired`. Shows how
  many datasets in the fleet had at least one row deleted this run.
- **TTL delete latency (timeseries).** `p95:lance.pipeline.dataset.ttl_delete_ms`. Sudden increases
  suggest a large expired-row set or write contention on the dataset.
- **TTL conflicts and skips (timeseries).** `sum:lance.pipeline.dataset.ttl_commit_conflict`
  overlaid with `sum:lance.pipeline.dataset.ttl_column_missing`. A rising conflict count means the
  TTL delete is racing with concurrent ingestion on the same dataset. A nonzero
  `ttl_column_missing` count means `--ttl-column` (or the Datadog-configured timestamp column) does
  not match the dataset's actual schema and should be corrected.

### 4.9 Indexing

**Goal.** Track how long index builds take, whether training artifacts are being reused or
retrained, and whether index commits are succeeding.

These metrics originate from `src/lance_etl/indexing.py` and cover all index types: vector
(IVF_RQ), scalar (BTREE, BITMAP), and FTS (INVERTED).

**Metrics.**

| Metric | Type | Tags | What it measures |
|---|---|---|---|
| `lance.pipeline.index.build_ms` | distribution | `index` | Per-index shard build latency. |
| `lance.pipeline.index.merge_ms` | distribution | `index` | Per-index segment merge latency. |
| `lance.pipeline.index.commit_ms` | distribution | `index` | Per-index commit latency. |
| `lance.pipeline.index.delta_merge_ms` | distribution | `index` | Delta merge latency. |
| `lance.pipeline.index.committed` | count | `index` | Successful index commits. |
| `lance.pipeline.index.commit_conflict` | count | none | Index commit conflicts. |
| `lance.pipeline.index.optimized` | count | none | Index optimization completions. |
| `lance.pipeline.index.deltas_merged` | count | `index` | Delta merge completions. |
| `lance.pipeline.index.skipped` | count | `index` | Index builds skipped (already up to date). |
| `lance.pipeline.index.stale_segments_dropped` | count | none | Stale segments dropped before commit. |
| `lance.pipeline.index.stale_fragment_replan` | count | none | Replans triggered by stale fragments. |
| `lance.pipeline.index.dropped_stale` | count | none | Stale index entries dropped. |
| `lance.pipeline.artifacts.trained` | count | none | IVF/RaBitQ artifact training completions. |
| `lance.pipeline.artifacts.retrained_for_growth` | count | none | Retraining triggered by dataset growth. |
| `lance.pipeline.artifacts.reused` | count | none | Existing artifacts reused without retraining. |
| `lance.pipeline.artifacts.partitions_degraded` | count | none | Partition count lowered to fit data. |
| `lance.pipeline.artifacts.train_ms` | distribution | none | Artifact training latency. |
| `lance.pipeline.tier.small_ms` | distribution | none | Indexing small-tier wall time. |
| `lance.pipeline.tier.large_ms` | distribution | none | Indexing large-tier wall time. |
| `lance.pipeline.tier.small_datasets` | gauge | none | Datasets in the small-tier indexing run. |
| `lance.pipeline.tier.large_datasets` | gauge | none | Datasets in the large-tier indexing run. |
| `lance.pipeline.dataset.segments` | gauge | `uri` | Index segment count per dataset after the run. |
| `lance.pipeline.dataset.total_ms` | distribution | `uri` | Per-dataset indexing wall time. |
| `lance.pipeline.run.datasets` | gauge | none | Datasets indexed in the run. |
| `lance.pipeline.errors` | count | `uri` | Indexing job errors per dataset. |

**Widgets.**

- **Build latency by index (top list).** `p95:lance.pipeline.index.build_ms by {index}`. Which
  indexes take the longest to build on this dataset fleet.
- **Commit conflicts (timeseries).** `sum:lance.pipeline.index.commit_conflict`. Should be low. A
  surge means index commits are colliding with concurrent ETL writes.
- **Artifact reuse ratio (timeseries).** `sum:lance.pipeline.artifacts.reused /
  (sum:lance.pipeline.artifacts.reused + sum:lance.pipeline.artifacts.trained)`. Good: close to 1.0,
  meaning training is not repeated unnecessarily. A sudden drop means the dataset grew beyond the
  retrain threshold.
- **Stale replans (timeseries).** `sum:lance.pipeline.index.stale_fragment_replan`. Consistently
  non-zero means the dataset is changing between index build and commit, forcing replans.

### 4.10 Errors and Saturation

**Goal.** Aggregate all error signals in one place so on-call engineers can identify whether an
alert is from the search serving plane, the ETL, the indexes, or the cache.

**Widgets.**

- **Error rate by source (timeseries, stacked).** Combine:
  - `sum:search_api.rpc.errors.as_rate()` (serving)
  - `sum:lance.pipeline.errors.as_rate()` (ETL, indexing)
- **Throttle events (timeseries).** `sum:search_api.throttle.errors` and
  `avg:search_api.throttle.new_rate`. The throttle layer is Lance's AIMD object-store rate limiter.
  `throttle.errors` counts throttle rejections. `throttle.new_rate` shows the limiter's current
  fill rate (requests per second). A rate that keeps decreasing means the object store is
  consistently returning 503/429. The Python pipeline emits equivalent signals via the Lance event
  bridge as `lance.pipeline.lance.throttle.previous_rate` and `lance.pipeline.lance.throttle.new_rate`
  gauges and `lance.pipeline.lance.throttle.error` counts.
- **Cache serialize errors (timeseries).** `sum:search_api.cache.serialize_errors by {cache}`. Each
  count means one entry could not be persisted to disk and is memory-only. A surge means index data
  is not being cached to disk, increasing cold-open risk.
- **Commit conflict summary (table).** Side-by-side view of
  `lance.pipeline.dataset.merge_conflict_retries` (ETL), `lance.pipeline.index.commit_conflict`
  (indexing), `lance.pipeline.dataset.commit_conflict` (compaction), and
  `lance.pipeline.ttl.commit_conflict` (TTL). This shows which job is under the most write contention.

### 4.11 Resource and IO

**Goal.** Understand how much data and how many object-store operations each query or pipeline run
generates, and whether the object store is becoming a bottleneck.

#### Rust service (per query)

| Metric | Type | Tags | What it measures |
|---|---|---|---|
| `search_api.query.iops` | distribution | `rpc` | Object-store I/O operations per query after coalescing. |
| `search_api.query.bytes_read` | distribution | `rpc` | Bytes read from storage per query. |
| `search_api.query.parts_loaded` | distribution | `rpc` | Index partitions loaded per query. |
| `search_api.throttle.errors` | count | none | Object-store throttle rejections per event. |
| `search_api.throttle.new_rate` | gauge | none | AIMD rate-limiter fill rate after a reduction. |

These metrics have no `org` or `tenant` tags by design (cardinality policy, see Section 7). Per-
dataset detail lives on traces. The per-query stats also land on the per-query-leg span as
provider-neutral `object_store.*` attributes (`object_store.requests`, `object_store.iops`,
`object_store.bytes_read`, `object_store.parts_loaded`, `object_store.indices_loaded`). The names
carry no provider prefix because the same path serves S3, Azure Blob, and GCS.

**Widgets.**

- **IOPS per query (distribution).** `avg:search_api.query.iops by {rpc}`. High IOPS per query
  means queries are not hitting the in-memory cache and are scanning many index partitions.
- **Bytes read per query (distribution).** `avg:search_api.query.bytes_read by {rpc}`. Compare
  against bandwidth limits.
- **Parts loaded per query (distribution).** `avg:search_api.query.parts_loaded by {rpc}`. A
  high parts-loaded count with high IOPS means `nprobes` is set too high or the index is too
  granular.
- **Throttle events (timeseries).** `sum:search_api.throttle.errors` and
  `avg:search_api.throttle.new_rate`. A declining rate gauge alongside rising errors means the AIMD
  controller is backing off.

#### Rust service (Lance event bridge)

The `LanceEventMetricsLayer` turns Lance's own tracing events into low-cardinality counters. The
same events also appear as span events on the open and per-query-leg spans (see Section 4.1 traces).

| Metric | Type | Tags | What it measures |
|---|---|---|---|
| `search_api.lance.io_events` | count | `io_type` | An index open or partition load. `io_type` is one of open_scalar_index, open_vector_index, open_frag_reuse_index, open_mem_wal_index, load_vector_part, load_scalar_part. |
| `search_api.lance.dataset_events` | count | `event` | A dataset-lifecycle transition. `event` is one of loading, writing, committed, dropping_column, deleting, compacting, cleaning. `event:loading` counts a dataset open. |
| `search_api.lance.file_audit` | count | `mode`, `type` | A file create or delete. `mode` is create, delete, or delete_unverified. `type` is manifest, index, data, or deletion. |

**Widgets.**

- **Index opens and part loads (timeseries).** `sum:search_api.lance.io_events by {io_type}`. A
  spike in `open_*` events after a version flip means serving is opening indexes cold. Sustained
  `load_*_part` volume means queries miss the index cache and fetch partitions from storage.
- **Dataset opens (timeseries).** `sum:search_api.lance.dataset_events{event:loading}`. Tracks how
  often replicas open dataset versions. Read against prewarm to confirm warm flips.
- **File audit (timeseries).** `sum:search_api.lance.file_audit by {mode,type}`. Watch
  `mode:delete` on `type:manifest` and `type:data` to confirm cleanup is running and to spot
  unexpected deletes.

These counters are discrete point events, not durations, so they show how often something happened
and what fired within a span. They do not give an in-flight concurrency gauge.

#### Python pipeline (bridged Lance events)

The Lance event bridge emits `lance.pipeline.lance.execution.iops`,
`lance.pipeline.lance.execution.bytes_read`, and `lance.pipeline.lance.execution.indices_loaded`
as distributions tagged by `event:execution`. These parallel the Rust-side metrics and cover all
Lance operations on the driver and executors.

---

## 5. Widget Type Reference

| Question | Best widget |
|---|---|
| Is the service healthy right now? | Query value (single number, colored by threshold) |
| How has latency trended over the past day? | Timeseries |
| How is latency distributed (bimodal, long tail)? | Heatmap or distribution |
| What is the p50/p95/p99 for each RPC? | Distribution or timeseries with percentile queries |
| Which index type takes longest to build? | Top list |
| Are we meeting our error-rate SLO? | SLO widget (error budget burn) |
| How much disk does the cache consume? | Timeseries (gauge) |
| How many errors occurred by type? | Table |
| How has recall drifted over the past month? | Timeseries |
| What fraction of samples were scored vs skipped? | Query value (formula: scored/fetched) |
| Is cache hit rate degrading? | Timeseries (formula: hits/(hits+misses)) |
| Are throttle events increasing? | Timeseries (count + gauge overlaid) |

---

## 6. Monitors and Alerts

Each monitor below should be scoped with `env:prod` and the appropriate `service` tag. Use
multi-alert by `rpc` where noted so a single monitor covers all RPC types independently.

### 6.1 Search Latency SLO Breach

- **Metric.** `p99:search_api.rpc.duration_ms` (multi-alert by `rpc`).
- **Condition shape.** Alert when the 5-minute rolling p99 exceeds your latency budget for three
  consecutive evaluation windows. Warning at 75% of the budget, critical at 100%.
- **Why three windows.** A single window spike is often a cold cache open. Three consecutive windows
  indicate a sustained regression.

### 6.2 Error Rate Spike

- **Metric.** `sum:search_api.rpc.errors / sum:search_api.rpc.requests` (multi-alert by `rpc`).
- **Condition shape.** Alert when the 10-minute rolling error ratio exceeds a threshold you derive
  from your SLO (e.g. if the SLO is 99.9% availability, alert at 0.5% error rate to give budget
  headroom). Use anomaly detection if baseline traffic is too variable for a fixed threshold.

### 6.3 Cache Hit-Rate Drop

- **Metric.** Formula: `sum:search_api.cache.lookup{cache:index,tier:memory,outcome:hit} /
  sum:search_api.cache.lookup{cache:index,tier:memory}`.
- **Condition shape.** Alert when the 15-minute rolling in-memory index hit rate drops below your
  expected floor. The floor depends on your working-set size relative to your memory budget. Start
  with a value derived from the steady-state hit rate observed in the first two weeks of operation.

### 6.4 Commit Conflict Surge

- **Metric.** `sum:lance.pipeline.dataset.merge_conflict_retries`.
- **Condition shape.** Alert when the hourly conflict count exceeds a threshold that represents
  normal concurrency (e.g. two standard deviations above the baseline weekly average). A sudden
  surge means multiple jobs are competing on the same dataset.

### 6.5 Recall Regression

- **Metric.** `avg:lance.pipeline.recall.measured`.
- **Condition shape.** Alert when the 24-hour rolling average recall@k drops below your quality
  floor (set this from the baseline established during the initial recall audit run). Use a long
  evaluation window because the recall job runs periodically, not continuously.
- **Caveat.** If no samples were scored (`lance.pipeline.recall.samples_scored` is zero), suppress
  the alert or treat zero as a data-quality issue rather than a recall regression.

### 6.6 Prewarm Failure

- **Metric.** `sum:search_api.prewarm.duration_ms{status:error}` or
  `sum:search_api.prewarm.duration_ms{status:partial}`.
- **Condition shape.** Alert on any non-zero count in a 5-minute window. Prewarm failures
  before a blue-green tag flip mean the next serving open will be cold.

### 6.7 Object-Store Throttle Sustained

- **Metric.** `avg:search_api.throttle.new_rate`.
- **Condition shape.** Alert when the rate drops below a threshold that represents healthy
  throughput. The AIMD controller reduces the rate in response to 429/503 errors from the object
  store. A rate that never recovers above the threshold means the object store is persistently
  throttling this service.

---

## 7. Tips and Pitfalls

### Low-Cardinality Tagging Policy

The `Metrics` struct in `src/telemetry/metrics.rs` explicitly documents that `org_id`, `tenant_id`,
and `version` never appear as metric tags. With 30k+ tenants, adding a tenant tag would create
30k timeseries per metric, which would exceed Datadog's custom metric limits and inflate costs
dramatically. Per-tenant detail lives on traces and logs, where the `org_id`, `tenant_id`, and
`namespace` fields are present on every span. The same policy applies to the Python pipeline: the
`TelemetryConfig.constant_tags` list should contain only low-cardinality tags like `region` or
`team`.

If you need to investigate a specific tenant's latency, use the Datadog Trace Explorer filtered
by `@org_id:<value>` and `@service:search-api`. Do not add tenant-level metric tags.

### OTLP Traces and Metrics Together

The Rust service emits both DogStatsD metrics and OTLP traces. Use them together:

- Metrics give fleet-wide aggregates (p99 across all requests).
- Traces give per-request detail (which dataset, which version, which filter, which result set).

When a metric alert fires, drill down by clicking into a trace from the same time window. The span
attributes on a `search_api.VectorSearch` span include `org_id`, `tenant_id`, `namespace`,
`recall.dataset_version`, and the query parameters, which the metric alone does not carry.

### Log Correlation via Trace IDs

Every log line from the Rust service includes `trace_id` and `span_id` fields (32 and 16 hex
characters) when a sampled OTLP span is active. The Datadog log pipeline ingests these from the
JSON stdout format defined in `src/telemetry/traces.rs`. Configure a Datadog log pipeline rule to
extract `trace_id` and `span_id` into their reserved fields so the Trace-to-Log correlation link
works in the UI. The Python pipeline uses `ddtrace` and the `TraceContextFilter` logging filter
to inject `dd.trace_id` and `dd.span_id` into every log record.

### Sampled Recall Caveats

The recall sampler is deterministic and counter-based (no RNG). At rate 0.1 and 1000 eligible
requests, exactly 100 are sampled. This means the sampled set is the same across restarts if the
service restarts at the same counter offset. For operational purposes this is fine, but it means
two replicas with identical counters capture the same requests. When comparing recall scores across
replicas, note that the sample set may not be independent.

The recall audit job scores against the live dataset, not the version that served the original
query. If the dataset was compacted or reindexed between capture and scoring, the scored recall may
differ from what the user experienced. The `recall.version_drift` counter tracks how many groups
were scored against a drifted version. A high drift count means the audit job is running too
infrequently relative to the index update cadence.

### Recall Span Retention

Sampled recall spans are retained only if a Datadog retention filter is configured for
`@recall.sample:true`. Without this filter, sampled spans are discarded after 15 minutes and the
recall audit job cannot retrieve them. Create the retention filter before enabling the sampler in
production.

### Dashboard Refresh and Staleness

The Python pipeline emits gauges at the end of each run, not continuously. Run-summary gauges like
`lance.pipeline.run.datasets` reflect the most recent completed run. Set the dashboard time window
to at least the pipeline run interval (typically 1 hour) to avoid seeing stale-looking zeroes between
runs. For continuous serving metrics from the Rust service, a 15-minute window is appropriate.
