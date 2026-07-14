# lance-etl Architecture Overview

*This document predates the unified architecture (ADR 0028) and needs a refresh.*

> A single reference page for the lance-etl system. It pulls together the architecture decision records (ADRs), the findings narrative, the gRPC contracts, and the code so that product, leadership, operations, and engineering all have one place to look.

This page is written for a mixed audience. Each major section is labeled with who it is for. Non-technical readers can stay in the sections marked "everyone" and skip the deep engineering ones without losing the thread.

---

## Table of contents

1. [Executive summary](#1-executive-summary)
2. [Glossary](#2-glossary)
3. [System overview and high-level architecture](#3-system-overview-and-high-level-architecture)
4. [Components and frameworks](#4-components-and-frameworks)
5. [Data model and schemata](#5-data-model-and-schemata)
6. [gRPC endpoints and contracts](#6-grpc-endpoints-and-contracts)
7. [Data flow](#7-data-flow)
8. [Architectural choices and decision making](#8-architectural-choices-and-decision-making)
9. [Scale and performance](#9-scale-and-performance)
10. [Observability](#10-observability)
11. [Operations and runbook](#11-operations-and-runbook)
12. [Open issues, limitations, and future work](#12-open-issues-limitations-and-future-work)

---

## 1. Executive summary

*Audience: everyone*

lance-etl turns raw source data into fast, searchable, per-customer datasets.

- It reads source records out of **Apache Iceberg** (a large analytics table format) and writes them into **Lance** datasets, one dataset per customer slice.
- Each Lance dataset is then indexed so it can answer two kinds of questions quickly:
  - **Vector search**: "find the items most similar to this one" (powered by machine-learning embeddings).
  - **Full-text search**: "find the items that mention these words" (classic keyword search).
  - **Hybrid search**: both at once, intelligently blended into one ranked list.
- A separate, fast **gRPC search service** (written in Rust) serves those queries to applications in real time.

### The scale it is built for

- Up to **1 billion vectors** in total.
- Spread across roughly **30,000 organizations** (tenants).
- The size distribution follows a **power law**: a small number of very large organizations, and a very long tail of tiny ones. The whole system is engineered around that shape.

### Business value, in plain terms

- **One pipeline, many tenants.** Thirty thousand customer datasets are built and maintained by the same automated jobs, not by hand.
- **Relevant results.** Vector, keyword, and hybrid search give applications modern, high-quality retrieval out of the box.
- **Fast and cheap to serve.** Aggressive caching and prewarming keep query latency low and object-store costs down.
- **Safe to operate.** Ingestion, maintenance, and serving run concurrently with proven zero data loss, and promotions between dataset versions are designed to be instant and reversible.
- **Cost control built in.** Per-row TTL can expire data when each row's own lifetime elapses, and compaction reclaims the freed storage automatically.
- **Decisions are documented.** Every load-bearing choice has a written ADR with the reasoning and the evidence, so the system stays understandable as it grows.

---

## 2. Glossary

*Audience: everyone*

| Term | Plain-language meaning |
|---|---|
| **Lance** | A modern columnar data format optimized for machine-learning and vector workloads. The pipeline writes one Lance dataset per customer slice. |
| **Apache Iceberg** | A large-scale analytics table format. It is the upstream source the pipeline reads from. |
| **Embedding / vector** | A list of numbers that represents the meaning of an item (a document, image, product). Similar items have nearby vectors. |
| **Vector search** | Finding the items whose vectors are closest to a query vector. This is "find me things like this." |
| **Full-text search (FTS)** | Classic keyword search over text fields, ranked by relevance (BM25 scoring). |
| **Hybrid search** | Running a vector leg and a text leg, then blending the two ranked lists into one. |
| **RRF (Reciprocal Rank Fusion)** | A simple, robust way to merge two ranked lists by rewarding items that rank highly in either list. The default blending method for hybrid search. |
| **Recall** | A quality measure. Of the truly best results, what fraction did the search actually return? Higher is better. |
| **Org / tenant / namespace** | The three identifiers that address a dataset. Think organization, a sub-account inside it, and a logical grouping of data. |
| **Blue-green serving** | Building a new version of a dataset off to the side ("green"), then flipping traffic to it in one atomic step, with an easy rollback to the old one ("blue"). |
| **TTL (time to live)** | A per-row expiry mechanism. Each row carries its own lifetime in a dedicated Duration column. The maintenance job deletes rows where the row's event timestamp plus its lifetime is before the current instant. |
| **Compaction** | Housekeeping that merges many small data files into fewer large ones so reads stay fast and storage stays tidy. |
| **Index** | A precomputed structure that makes a certain kind of lookup fast (vector, range, category, or text). |

---

## 3. System overview and high-level architecture

*Audience: everyone (with an engineer-facing subsection)*

The system has two halves that meet at the Lance datasets in object storage.

- **The Python data plane** builds and maintains the datasets. It runs as scheduled batch jobs on Apache Spark. It reads from Iceberg, writes to Lance, builds indexes, compacts, expires old data, and migrates datasets.
- **The Rust serving plane** answers live queries. It is a small, fast gRPC service that reads the same Lance datasets.

The data plane is about throughput over thousands of datasets. The serving plane is about low-latency responses to individual requests. Keeping them separate lets each be tuned for its own job.

### Architecture diagram

```
                          SOURCE
                  +----------------------+
                  |  Apache Iceberg      |
                  |  (events / records)  |
                  +----------+-----------+
                             | incremental read (snapshot-id bounds)
                             v
   ============== PYTHON DATA PLANE (Apache Spark) ==============
   |                                                             |
   |   etl  -->  maintenance  -->  index                         |
   |   (merge_insert)  (TTL+compact+cleanup)  (segment API)      |
   |                                                             |
   |   maintenance: recall audit | migrate-manifests |          |
   |                migrate-namespace | tag (blue-green)         |
   ============================|=================================
                               | writes / reads
                               v
                 +-----------------------------+
                 |   Object storage (S3/GCS/   |
                 |   Azure): per-org Lance      |
                 |   datasets                   |
                 |   base/<org>/<tenant>/       |
                 |        <namespace>.lance     |
                 +--------------+--------------+
                       reads ^
                             |
   ============== RUST SERVING PLANE (tonic gRPC) ===============
   |                                                             |
   |   SearchService                                             |
   |   - VectorSearch                                            |
   |   - TextSearch                                              |
   |   - HybridSearch                                            |
   |   - Prewarm            +-- two-tier disk + memory cache     |
   |   - Clusters           +-- typed filter AST (no raw SQL)    |
   ============================|=================================
                               | OTLP traces + DogStatsD metrics
                               v
                        +-------------+
                        |   Datadog   |
                        +-------------+
```

### Split of responsibilities

- **Driver vs executors (Spark).** The Spark driver only plans, broadcasts small read-only artifacts (such as trained centroids and version pins), and commits. All heavy reads, writes, index builds, and compaction run inside executor tasks. The driver never opens a dataset for row-level work.
- **Read-only serving.** The SearchService reads versioned Lance datasets. Durable writes enter through Iceberg and the Spark ETL path.
- **Engine vs transport (Rust).** The Rust crate is layered so that the query engine, the wire protocol, and the caching layer never leak into each other.

### Engineer-facing detail

*Audience: engineers*

- The Spark jobs are orchestrated by an **Airflow DAG** (`etl >> maintenance >> index`). Maintenance runs before indexing on purpose so fresh fragments are merged before any index covers them, which avoids paying inline index-remap cost on the large tier.
- The Rust crate enforces hard layering boundaries: `domain` (engine- and transport-agnostic types and traits), `lance` (the only place Lance types appear), `grpc` (the only place proto and tonic types appear), `cache`, and `telemetry`. `lib.rs` re-exports a clean surface.
- Every request resolves to exactly **one** dataset at `base/<org>/<tenant>/<namespace>.lance`. There is no cross-dataset fan-out in the server (see ADR 0014).

---

## 4. Components and frameworks

*Audience: engineers*

### Python package (`src/lance_etl/`)

| Module | Responsibility |
|---|---|
| `etl.py` | `IcebergToLanceETL`: incremental Iceberg read, pivot named vectors and texts out of their maps into concrete indexable columns, flatten the metadata map, last-write-wins collapse, repartition by routing key, `merge_insert` upsert plus `when_matched_delete` into per-key Lance datasets. |
| `indexing.py` | `LanceIndexer` plus per-type handlers (`VectorIndexHandler`, `BTreeIndexHandler`, `BitmapIndexHandler`, `FtsIndexHandler`). Builds indexes via the Lance segment API. |
| `maintenance.py` | `MaintenanceJob`: per-row TTL expiration (opt-in), two-tier (small and large) compaction, version cleanup — applied in that order per dataset. Also contains blue-green tag helpers and manifest migration. |
| `recall.py` | `RecallAuditJob`: replays Datadog-sampled queries as exact brute-force scans, scores recall@k, nDCG@k, MRR. |
| `migrate_namespace.py` | `NamespaceMigrator`: copy-plus-optimize a whole namespace to a new name, source kept for rollback. |
| `iceberg_optimize.py` | `IcebergOptimizer`: optimizes the upstream Iceberg source table via `CALL <catalog>.system.<procedure>` statements (`rewrite_data_files`, `rewrite_manifests`, `expire_snapshots`, opt-in `remove_orphan_files`). Distinct from `MaintenanceJob`, which optimizes Lance datasets. |
| `telemetry.py` | `Telemetry`, `TelemetryConfig`, `LanceRuntimeConfig`, `commit_with_retries`, the Lance event bridge, and the shared retry-budget constants. |
| `cloud_storage.py` | `resolve_filesystem` plus `discover_datasets` for cloud-agnostic pyarrow filesystem I/O (S3, GCS, Azure). |
| `arrow_types.py` | `resolve_arrow_type` / `resolve_type_map` for CLI type specs. |
| `cli.py` | Entry point: `etl`, `maintenance`, `index`, `recall`, `tag`, `migrate-manifests`, `migrate-namespace`, `optimize-iceberg`. |

### Benchmark package (`bench/`)

The `bench` package (`python -m bench`) drives the real pipeline and the live server end to end over SIFT1M and a synthetic dataset, through a dataset-adapter registry. Subcommands: `download`, `prepare`, `ingest`, `index`, `compact`, `search`, `report`, `all`. It produces recall, results, and Pareto-plot artifacts.

### Rust crate (`rust/search-api/`)

| Layer | Responsibility |
|---|---|
| `domain/` | Engine- and transport-agnostic types and traits: `DatasetTarget`, query types, the typed `Filter` AST, `SearchBackend`, `DatasetProvider`, fusion, prewarm, clusters, and the single `SearchError`. References neither proto, tonic, nor Lance. |
| `cache/` | Persistent two-tier caching: a disk-backed index cache (`disk_cache.rs`), a path-filtered metadata byte cache (`store_cache.rs`), shared on-disk layout (`layout.rs`), and a background janitor (`janitor.rs`). Caches index and metadata only, never raw data. |
| `lance/` | The only layer that touches Lance, Arrow, and DataFusion. `provider.rs` (resolution, shared session, handle LRU), `backend.rs` (the `SearchBackend`), `filter.rs` (AST to DataFusion expr), `text.rs` (FTS translation), `rows.rs` (Arrow to JSON), `prewarm.rs`, `index_reader.rs` (IVF centroid extraction). |
| `grpc/` | Thin tonic transport. `mod.rs` contains `SearchGrpc<B>` and pure proto-to-domain conversions. This is the only place proto and tonic types appear. |
| `telemetry/` | Datadog observability: OTLP trace export, JSON logs with trace correlation, a typed DogStatsD metrics facade, and sampled-query recall capture. Every emitter is infallible. |
| `config.rs` | Environment-driven runtime configuration. |

### Airflow (`airflow/lance_etl_dag.py`)

A configurable-schedule DAG that runs `etl >> maintenance >> index`. An optional `optimize-iceberg` task, gated by the `lance_etl_optimize_iceberg_enabled` Variable (default off), runs before `etl` and optimizes the upstream Iceberg source table. Each stage maps to a `SparkSubmitOperator` that calls `python -m lance_etl.cli <subcommand>`. The `maintenance` task runs per-row TTL expiration (when `lance_etl_ttl_column` is set), two-tier compaction, and version cleanup in a single Spark job.

### Frameworks used

| Framework | Used for |
|---|---|
| **PySpark** | Distributed orchestration of the data-plane jobs. |
| **pylance / Lance** | The dataset format, the index APIs, compaction, and the object-store layer. |
| **Apache Iceberg** | The upstream source table (read via Spark, Iceberg 1.10). |
| **tonic + tonic-build + prost** | The Rust gRPC server and protobuf code generation. |
| **DataFusion** | Query expression evaluation behind the typed filter AST. |
| **Moka** | In-memory cache used together with the disk cache and for the handle LRU. |
| **Datadog (OTLP + DogStatsD)** | Traces, metrics, and trace-correlated logs on both planes. |

---

## 5. Data model and schemata

*Audience: engineers (with a plain intro for everyone)*

### Plain intro

Every customer slice gets its own dataset file on object storage. The location of that file is derived purely from the customer's identity (organization, tenant, namespace). Inside, each record has an id, a timestamp, some labels (metadata), one or more vectors, and one or more text fields.

### Dataset addressing and path layout

- A dataset lives at: `base_uri/<org>/<tenant>/<namespace>.lance`
- More generally, the path is built from the configured `partition_cols` list (default `org_id`, `tenant_id`, `namespace`), so the path is `base_uri/<val1>/<val2>/.../<valN>.lance` in that order.
- Every path component is validated against a strict allowlist (`PATH_COMPONENT_PATTERN`), so a value can never inject a path traversal or collide a route.
- Each routing key lives in **exactly one** dataset. The per-dataset `merge_insert` keyed on the id column is therefore the sole deduplication mechanism. No cross-dataset reader dedup is needed (ADR 0014).

### Record schema

| Field | Type | Notes |
|---|---|---|
| **vector id** | string | Client-assigned unique id (`vector_id`). The collapse and `merge_insert` key. |
| **event timestamp** | timestamp | The single canonical clock (`ts_col`, default `timestamp`). Drives last-write-wins collapse and time-range queries. **There is no `_ingested_at`.** (ADR 0016) |
| **metadata** | map<string, string> | Free-form labels. Lands as an Arrow `Map<Utf8, Utf8>` column downstream. |
| **vectors** | map<string, vector> | Optional. A map of named fixed-dimension vectors, each keyed by its column name. One record can carry several named vectors. |
| **texts** | map<string, string> | Optional. A map of named text fields. Each key is the FTS column name. A text field under key `body` lands in the `body` column, the same column a `TextSearch` names. |

Key points:

- **Event time is authoritative.** It is used for ordering, collapse, and time-bounded serving. Date-range queries are expressed as scalar range filters on this column, pruned efficiently by a BTREE index.
- **No ingest-time column.** This was deliberately removed (ADR 0016 supersedes 0011). The consequence is that receipt-based (ingest-age) retention is not expressible. Retention is by event age only.
- **Maps, not structs.** Lance has no map type and structs are not used downstream, so the maps are unpacked during ETL. Named vectors and texts are pivoted into concrete indexable columns (one column per declared field, the vector column cast to a fixed-size-list), while the metadata map stays stored-only payload flattened into parallel `metadata_keys` / `metadata_values` list columns.

### Index types

| Index | Built over | Purpose |
|---|---|---|
| **IVF_RQ** (vector) | a vector column | Approximate nearest-neighbor search. Uses IVF partitioning plus RaBitQ quantization. |
| **BTREE** (scalar) | any orderable column | Efficient range pruning, for example time-range queries on the event timestamp. |
| **BITMAP** (scalar) | low-cardinality columns | Efficient equality and category filtering. |
| **INVERTED** (FTS) | text columns | Full-text BM25 search, optionally with positions for phrase queries. |

## 6. gRPC endpoints and contracts

*Audience: engineers*

The search service runs as one binary with one port, one health endpoint, and one telemetry pipeline. Its proto is `proto/lance_etl/v1/lance_etl.proto` in package `lance_etl.v1`.

### SearchService

| RPC | Purpose | Request / response shape (high level) |
|---|---|---|
| **VectorSearch** | Nearest-neighbor search on a vector column. | Request: target, `VectorQuery`, optional `Rerank`, optional `TimeRange`. Response: hits ordered nearest-first, each with a projected row and a distance. |
| **TextSearch** | Full-text search via the INVERTED index. | Request: target, `TextQuery`, optional `Rerank`, optional `TimeRange`. Response: hits ordered best-first, each with a row and a BM25 score. |
| **HybridSearch** | Runs a vector leg and a text leg, then fuses them. | Request: target, `VectorQuery`, `TextQuery`, fused `k`, `Fusion`, optional `Rerank`, optional `TimeRange`, optional request-level `Filter` and `FilterMode` applied to both legs. Response: fused hits, each with a row and a fused score. |
| **Prewarm** | Pulls one dataset's metadata and index structures into local caches before traffic arrives. | Request: target, what to warm, and an optional explicit version or tag. Response: per-index outcomes, durations, cache size, and the resolved version warmed. |
| **Clusters** | Reads the IVF centroids of a vector index. | Request: target, optional index name. Response: centroids in partition order, dimension, index name, partition count. |

### Notable contract rules

- **Typed filter AST, no raw SQL.** Filters are a typed predicate tree (`Comparison`, `InList`, `IsNull`, `IsNotNull`, `Between`, `and`, `or`, `not`). Column names are validated against the dataset schema and an identifier allowlist. Literals become typed DataFusion `lit` expressions. Clients can never inject expression text (ADR 0005). An injection attempt such as a column named `id; DROP TABLE users` is rejected at the allowlist. String equality (`column = "value"`) is fully supported on all search RPCs. The string literal is transported verbatim through `LiteralValue.string_value` and becomes a typed DataFusion expression, never raw SQL.
- **Hybrid request-level filter.** `HybridSearchRequest` accepts a `Filter filter = 8` and `FilterMode filter_mode = 9` at the request level. The filter is ANDed into both the vector leg and the text leg independently. When a leg already carries its own filter the two predicates are combined with a typed `AND` node. The request-level `filter_mode` overrides both legs' modes when the request-level filter is present.
- **Event-time windowing via TimeRange.** The three search RPCs accept an optional `TimeRange { optional int64 start_ms; optional int64 end_ms }` (epoch milliseconds, start inclusive, end exclusive, either bound optional). The window always applies to the event-timestamp column, fixed to the `DEFAULT_EVENT_TIMESTAMP_COLUMN` constant in `config.rs` (`event_timestamp`), no longer env-configurable. The range is translated into a typed predicate ANDed with any caller-provided `Filter`, never as raw SQL. A BTREE or zone-map on that column prunes the scan. An absent `TimeRange` leaves every search path behaving exactly as before (ADR 0021).
- **Fusion specs.** Hybrid fusion offers two strategies. **RRF** sums `1 / (rrf_k + rank)` across legs (default `rrf_k = 60`). **Weighted** min-max normalizes each leg into `[0, 1]` and combines them with a vector weight (default `0.7`). RRF is the default when no fusion message is set.
- **Rerank field.** Every search RPC accepts an optional `Rerank`. The only strategy is `IdentityRerank` (keep order, optionally truncate to `top_n`), implemented as a plain `truncate_to_top_n` helper in `grpc/mod.rs`. The earlier async `Reranker` trait and `IdentityReranker` seam (`domain/rerank.rs`) were removed since truncation was the only effect that trait ever had in production.
- **Pre-release proto.** The proto carries no backward-compatibility guarantee. Breaking reshapes have been taken freely where warranted.

---

## 7. Data flow

*Audience: everyone (with engineer detail)*

### End-to-end build and serve, in plain terms

0. (Optional) The Iceberg source table is optimized via Iceberg's own maintenance procedures to bin-pack small files, rewrite manifests, and expire stale snapshots before the ETL reads it.
1. New and changed records land in the Iceberg source table.
2. The ETL job reads just the new window of changes, not the whole table.
3. It collapses each id down to its latest state and routes it to the one dataset that owns it.
4. It merges those records into the per-customer Lance datasets.
5. Indexes are built so search is fast.
6. Compaction tidies the data files.
7. A tag flip promotes the freshly built version to live serving.
8. The Rust service answers queries against the live version, with caches kept warm.

### End-to-end, engineer detail

1. **Incremental Iceberg read.** The wall-clock window is resolved to `start-snapshot-id` / `end-snapshot-id` by querying the `{table}.snapshots` metadata table, because Iceberg 1.10 rejects `start-timestamp` / `end-timestamp` on batch scans. On first run with no prior snapshot, it falls back to a full batch scan pinned at the end bound (ADR 0003).
2. **Pivot, flatten, and collapse.** The named vectors and texts are pivoted out of their map columns into concrete indexable columns and the metadata map is flattened into parallel list columns. Rows are collapsed to the last-write-wins terminal state per id using the event timestamp.
3. **Repartition and route.** Rows are shuffled by routing key so each row can only reach its own dataset. The dataset URI is a validated pure function of the routing columns.
4. **merge_insert.** Each routing key's rows are applied to exactly one Lance dataset with a `merge_insert` upsert plus `when_matched_delete`. Replayed or retried windows converge rather than duplicate, so backfills are just catch-up replays of the same job.
5. **Distributed index build via the segment API.** The driver trains IVF centroids and one shared RaBitQ model, broadcasts them, executors build one index segment per fragment shard, and the driver merges and commits. Scalar and FTS indexes follow their own segment flows (ADR 0001).
6. **Maintenance: TTL, two-tier compaction, version cleanup.** When a per-row TTL column is configured, expired rows are deleted first so the compaction that follows reclaims their storage. Small datasets are then compacted whole-dataset-per-task in one batched job, including version cleanup. Large datasets use the distributed plan/execute/commit fan-out driven concurrently (ADR 0002). Version cleanup runs after each dataset's compaction.
7. **Blue-green tag flip.** A `prod` tag is moved to the new version for an O(1) cutover. The safe sequence is build green, prewarm green by explicit version, then flip the tag (ADR 0013).
8. **Serving.** The Rust service resolves the target to one dataset, applies the typed filter, runs the query, and returns ranked hits, served out of warm disk and memory caches.

### Maintenance flows

- **Source Iceberg table optimization.** The upstream Iceberg table that the ETL reads accumulates many small data files, a growing manifest list, and unbounded snapshot history when appended to on every run. The optional `optimize-iceberg` step runs Iceberg's own `CALL <catalog>.system.<procedure>` maintenance before `etl`: `rewrite_data_files` bin-packs small files, `rewrite_manifests` realigns manifests, `expire_snapshots` prunes history beyond a configurable retention horizon, and the opt-in `remove_orphan_files` deletes unreferenced files. This is distinct from the Lance maintenance job described below. (ADR 0023)
- **Per-row TTL expiration.** Each row carries its own lifetime in a dedicated Arrow `Duration` column. The maintenance job deletes rows where `ts_column + ttl_column < now`, which Lance evaluates natively as timestamp-plus-duration column arithmetic. TTL is off by default. It runs only when `MaintenanceConfig.ttl_column` names a column present in the dataset schema. There are no global retention or enabled knobs. The delete runs before compaction so the compaction reclaims the vacated storage. (ADR 0018)
- **Namespace migrate.** Every dataset in a source namespace is copied to a new namespace name, then optimized (write, recompact, reindex) in production order. The source is kept for rollback. Two-tier scale (ADR 0019).
- **Recall audit.** A fraction of vector, text, and hybrid queries are sampled onto Datadog spans, including the dataset version that served them. An offline Spark job replays each query as an exact brute-force scan against that pinned version and scores recall@k, nDCG@k, and MRR (ADR 0008).

---

## 8. Architectural choices and decision making

*Audience: engineers*

Each decision below cites its ADR. Accepted unless noted.

### Distributed indexing via the segment API (ADR 0001)
- Every index is built through Lance's uncommitted-segment APIs, with three distinct flows for vector, scalar, and FTS.
- Vector (IVF_RQ) requires a shared broadcast RaBitQ model. Without it each shard derives its own random rotation and merged segments are inconsistent.
- BTREE and BITMAP go per-shard then straight to commit with no merge step (Lance main rejects `merge_index_metadata` for these). FTS uses a shared `index_uuid` and a create-index commit.
- All heavy I/O runs in executors. The driver only plans, broadcasts, and commits.

### Maintenance as a single ordered job: TTL, two-tier compaction, version cleanup (ADR 0002)
- The 30k-org power law means a sequential per-dataset loop would launch on the order of 120k blocking Spark jobs.
- Maintenance runs three steps per dataset in order: per-row TTL expiration (opt-in), two-tier compaction, and version cleanup. Tier A batches many small datasets into one job using non-distributed calls that honor every option. Tier B keeps the distributed fan-out for large datasets, driven concurrently with FAIR scheduler pools.
- A tier-B commit conflict triggers a re-plan and re-execute, never a blind re-commit, because the commit pins its conflict scan to the plan version.

### Incremental read via snapshot bounds (ADR 0003)
- Iceberg 1.10 rejects timestamp options on batch scans, verified against the runtime jar.
- The window is resolved to snapshot ids first, then read as an incremental append scan, with a full-scan fallback on first run.

### Generic partition routing, by-date dropped (ADR 0004, amended by 0014)
- A single `partition_cols` list drives routing end to end (default `org_id, tenant_id, namespace`).
- The by-date partition target, the strftime-to-Spark translation, and `--partition-derive` were removed (0014). Each key now lives in exactly one dataset, so the per-dataset `merge_insert` is the sole dedup. Generic stable-identity routing is retained.

### Rust gRPC layering and the typed filter AST (ADR 0005)
- Hard layer boundaries: `domain` references neither proto, tonic, nor Lance. `grpc` is a thin adapter. `lance` is the only place Lance types appear.
- Filtering is a typed AST validated against the schema and an identifier allowlist. No raw SQL ever crosses the boundary.

### Disk cache and prewarm (ADR 0007)
- A hybrid disk-plus-memory cache is injected into the shared Lance session index cache, plus a metadata byte cache that excludes raw `data/` reads.
- Cache keys are URI plus index-UUID plus version-manifest-path, so entries are version-correct. The latest-version pointer is never cached, or a flip would be invisible.
- A Prewarm RPC warms a dataset (and optionally a specific version) before traffic.

### Observability and recall audit (ADR 0008)
- Both planes instrument Datadog. The Rust service emits per-RPC OTLP traces and DogStatsD metrics, taps two Lance trace surfaces, and keeps tag cardinality low (rpc and status only, never org or tenant).
- A deterministic fraction of queries is sampled with the served dataset version recorded, so the offline recall score is exact rather than approximate-under-churn.

### Compaction and index coexistence race fix (ADR 0009)
- Ingestion, compaction, and indexing run concurrently with zero data loss and convergence.
- A genuine race (an index build orphaning fragments compaction removed) is guarded by a shared stale-fragment predicate that re-reads, re-resolves the live fragment set, rebuilds, and re-commits within a budget.

### Stable row IDs rejected (ADR 0010)
- Move-stable row IDs were implemented and the structural upside proven, then **rejected**.
- Under the production pattern (`merge_insert` + `delete` + concurrent compaction) they trip an upstream `RowIdIndex` overlapping-chunk invariant: a panic in debug, and silent data corruption risk in release. This is a rejection, not a deferral. Revisiting requires a fresh ADR.

### Event-time canonical clock, `_ingested_at` removed (ADR 0011 superseded by 0016)
- `_ingested_at` was introduced (0011) and then removed entirely (0016).
- Two clocks for one concept created drift on retries and backfills. The source event timestamp is now the single canonical clock. The tradeoff: receipt-based (ingest-age) retention is no longer expressible.

### V2 manifest paths (ADR 0012)
- Every dataset is created with V2 manifest paths so an open costs a single object-store request regardless of version-history depth.
- This is a creation-time naming choice with no concurrency caveat, so it defaults on. Existing datasets migrate one-shot.

### Blue-green serving (ADR 0013, Proposed)
- A `prod` tag gives O(1) cutover. The correct sequence is build green, prewarm green by explicit version, then flip the tag.
- Status is **Proposed**: the Rust serving-side implementation (tag resolution and prewarm-before-flip safety) is not yet done. The proto changes are additive and non-breaking.

### Knob reduction (ADR 0015)
- Roughly 30 rarely-varied knobs were removed. Universally-correct constants (for example `num_bits=1`, V2 manifest paths, compaction mode) became module constants. Schema column names and fine-grained tokenizer toggles became code-level dataclass fields, not CLI flags. Retry budgets were consolidated into single named constants in `telemetry.py`.

### Intake service removed (ADR 0017 superseded)
- The placeholder service could acknowledge writes without a durable destination. It was removed before release.
- Iceberg is the only durable ingestion source. Any future online-write API requires a fresh ADR with durable transport and replay semantics.

### Per-row TTL folded into maintenance (ADR 0018)
- Each row carries its own lifetime in a dedicated Arrow `Duration` column. The delete predicate is `ts_column + ttl_column < TIMESTAMP 'now'`, evaluated natively by Lance/DataFusion as timestamp-plus-duration column arithmetic.
- TTL is off by default. It runs only when `MaintenanceConfig.ttl_column` names a column present in the dataset schema. There are no global retention or enabled knobs: passing the column name is the opt-in.
- Both column names are validated against the dataset schema and the identifier allowlist before any predicate is constructed. Receipt-based retention is explicitly not supported.

### Namespace migrate (ADR 0019)
- A whole namespace is copied to a new name, then optimized in production order (write, recompact, reindex), reusing `MaintenanceJob` and `LanceIndexer`.
- The source is kept for a free blue-green rollback story. Reindex is skipped when no index columns are supplied because the columns cannot be guessed. Two-tier scale.

### gRPC event-time range on search RPCs (ADR 0021)
- An optional `TimeRange { optional int64 start_ms; optional int64 end_ms }` field is added to `VectorSearchRequest`, `TextSearchRequest`, and `HybridSearchRequest`.
- The window always applies to the event-timestamp column, fixed to the `DEFAULT_EVENT_TIMESTAMP_COLUMN` constant in `config.rs` (`event_timestamp`), no longer env-configurable.
- The bound is a typed DataFusion literal matched to the column's Arrow type (timestamp or integer). The range predicate is ANDed with any caller-provided `Filter`, so the two compose.
- A BTREE or zone-map on the event-timestamp column prunes the scan. Existing clients that never set `time_range` are unaffected.

### Lance trace-event bridge: object-store stats, IO/dataset/file events (ADR 0022)
- The execution-stats callback that already emits `query.*` DogStatsD metrics also attaches provider-neutral `object_store.*` attributes to the per-query-leg span: `object_store.requests`, `object_store.iops`, `object_store.bytes_read`, `object_store.parts_loaded`, `object_store.indices_loaded`. The names carry no provider prefix because the same path serves S3, Azure Blob, and GCS.
- Three more Lance tracing targets are force-admitted at `info` through the `EnvFilter` so they survive a narrowing `RUST_LOG`. The OTLP layer records each as a span event on the active span, so a dataset open shows its `loading` dataset event and index opens as span events on the `provider.dataset` span, and the per-query-leg spans carry the IO and file events of a scan.
- A single `LanceEventMetricsLayer` (which subsumes the throttle tap) also turns each event into a low-cardinality counter: `lance.io_events` tagged `io_type`, `lance.dataset_events` tagged `event` (`event:loading` counts a dataset open), and `lance.file_audit` tagged `mode` and `type`. Tags are the fixed Lance enums, never a uri or path.
- Attributes and tags are counts only, with no org, tenant, or version identifier, so cardinality stays low. A GET / HEAD / LIST breakdown is not emitted: Lance does not expose that in production builds. The bridged events are point events, so they count occurrences but do not give an in-flight concurrency gauge.
- The capture is infallible, matching ADR 0008. An unreachable Datadog Agent never panics and never fails a request.

### Iceberg source-table optimization job (ADR 0023)
- The pipeline now maintains two distinct stores: the per-tenant Lance datasets (via `MaintenanceJob`) and the upstream Iceberg source table (via `IcebergOptimizer`).
- `IcebergOptimizer` issues `CALL <catalog>.system.<procedure>` statements via the Iceberg Spark session extensions. Steps run in a fixed safe order: `rewrite_data_files`, `rewrite_manifests`, `expire_snapshots`, and the opt-in `remove_orphan_files`.
- Table identifiers are validated against a bare-identifier allowlist before any statement is constructed. Only validated names and typed `TIMESTAMP` literals reach the SQL, so no raw string injection is possible.
- The job is exposed as the `optimize-iceberg` CLI subcommand and an optional Airflow task gated by `lance_etl_optimize_iceberg_enabled`. It runs before `etl` because it maintains the table the ETL reads.

---

## 9. Scale and performance

*Audience: engineers*

### The shape of the problem

- Up to **1 billion vectors** across roughly **30,000 organizations**.
- A **power-law** distribution: averaging 1B rows over 30k orgs is about 33k rows each, but a small head holds most of the data and a long tail holds almost nothing.
- The central design consequence: never run a sequential per-dataset driver loop. It would launch on the order of 120k blocking Spark jobs.

### The two-tier small/big strategy, applied everywhere

The same idea recurs across the data plane: classify each dataset by fragment count, batch the long tail, and fan the head out.

| Job | Small tier (tail) | Large tier (head) |
|---|---|---|
| **ETL** | Power-law-aware merge sizing, idempotent merge per key. | Distributed merge across executors. |
| **Indexing** | Whole-dataset-per-task in one batched job, incremental `optimize_indices`. | Per-dataset segment fan-out, concurrent with FAIR pools. |
| **Compaction** | `Compaction.execute` in process (honors every option). | Distributed plan/execute/commit, concurrent, re-plan on conflict. |
| **Recall** | Group by `(uri, version)` and fan out. | Same fan-out path. |
| **TTL** | Single delete call in process. | Same delete, isolated partition budget. |
| **Migrate** | Copy whole dataset per task, batched. | Distributed per-dataset sharded copy. |

### Vector indexing: IVF_RQ with RaBitQ

- IVF partitions data into clusters. RaBitQ quantizes vectors so the index is compact and fast.
- The IVF partition count follows a size-aware policy: `clamp(rows // 8192, 16, 32768)` unless configured.
- Vector indexing is skipped entirely below a row floor where a flat KNN scan is sufficient.
- Centroids are retrained (through the full rebuild path) once a dataset grows past `RETRAIN_GROWTH_FACTOR` (4x) its trained row count, so reused centroids cannot go stale forever.

### Serving performance

- A **disk-backed index and metadata cache** keeps cold opens off the object store. Raw data is provably excluded by a path classifier.
- The **Prewarm RPC** warms a dataset before traffic. Measured on the synthetic end-to-end benchmark, Prewarm roughly halved cold first-query latency (about 17.8 ms to 8.0 ms, and 10.6 ms to 4.7 ms, across two orgs).
- One shared Lance session safely spans all datasets because cache keys are URI-scoped.

### Object-store and I/O tuning

- **V2 manifest paths** make every dataset open a single object-store request regardless of version history (ADR 0012).
- Process-global Lance I/O knobs (IO thread count, client retry timeout) are stamped into the environment before the tokio runtime starts, so every thread sees a consistent value from the service config.
- The metadata byte cache has a bounded max range size, and the janitor sweeps both cache tiers on a fixed interval enforcing TTL and disk budgets.

---

## 10. Observability

*Audience: engineers*

Both planes report to Datadog. Every emitter on the Rust side is infallible by construction: an unreachable Datadog Agent never panics and never fails a request.

### Traces

- The Rust service emits **per-RPC OTLP spans**. A tower layer opens the server span, and handlers annotate it with the dataset target and the gRPC status.
- The Python jobs use ddtrace and bridge Lance's own structured trace events into Datadog.

### Metrics (DogStatsD)

- A typed `Metrics` facade emits `search_api.*` metrics.
- **Tag cardinality is kept deliberately low: rpc and status only, never org or tenant.** This keeps the metrics bill and cardinality bounded across 30k tenants.
- Several Lance trace surfaces are tapped by the `LanceEventMetricsLayer`: per-query execution stats (`query.iops`, `query.bytes_read`, `query.parts_loaded`), the object-store throttle target (`throttle.errors`, `throttle.new_rate`), and the `lance::io_events`, `lance::dataset_events`, and `lance::file_audit` targets (`lance.io_events` tagged `io_type`, `lance.dataset_events` tagged `event`, `lance.file_audit` tagged `mode` and `type`).

### Span attributes and span events for object-store IO

- Per-query-leg spans (`lance.vector_query` / `lance.text_query`) carry provider-neutral `object_store.*` attributes sourced from the Lance execution-stats callback: `object_store.requests`, `object_store.iops`, `object_store.bytes_read`, `object_store.parts_loaded`, `object_store.indices_loaded`. The names carry no provider prefix because the same path serves S3, Azure Blob, and GCS.
- The bridged Lance events also surface as span events. A dataset open shows its `loading` dataset event and the index `open_*` IO events on the `provider.dataset` span, and a scan's IO and file events appear on the per-query-leg span.
- These attributes are aggregate counts only. A GET / HEAD / LIST breakdown is not available in production Lance builds and is intentionally not emitted. The point events count occurrences but do not give an in-flight concurrency gauge.
- A hybrid request shows the object-store volume of each leg separately under the one RPC trace. A single-leg request has exactly one such child span.
- No org, tenant, or version identifier is attached, so span cardinality remains low (ADR 0022).

### Logs

- **JSON logs with trace correlation** on both sides, so a log line can be tied back to the span that produced it.

### The Lance event bridge

- On the first `Telemetry.create` per process, the bridge attaches automatically and turns Lance's file-audit, dataset, object-store-throttle, index-I/O, and execution-stats events into Datadog counters, gauges, and distributions, including the raw object-store `requests` field distinct from coalesced `iops`.

### Recall audit

- The service samples a deterministic fraction of vector, text, and hybrid queries (`floor(N * rate)`, lock-free, no RNG) onto the request span, recording the query, the params, the typed filter AST, the served result ids, and crucially the dataset version that served the query.
- The offline `recall` Spark job pulls those spans, opens each dataset pinned at the recorded version, brute-forces exact top-k, and reports **recall@k**, **nDCG@k**, and **MRR** per org and per params.
- Recording the version makes the score exact rather than approximate-under-churn. The audit must run inside the version-cleanup retention horizon, or the pinned version may be gone.

---

## 11. Operations and runbook

*Audience: ops and engineers*

### CLI subcommands (`python -m lance_etl.cli <subcommand>`)

| Subcommand | Purpose |
|---|---|
| `etl` | Read a time window from an Iceberg table and upsert/delete into per-tenant Lance datasets. |
| `maintenance` | Per-dataset maintenance in order: per-row TTL expiration (when `--ttl-column` is set, deleting rows where `ts_column + ttl_column < now`), two-tier distributed compaction, and version cleanup. TTL is off by default. Pass `--ttl-column` to opt in. Pass `--ts-column` to name the event timestamp column (default `timestamp`). |
| `index` | Build IVF_RQ vector, BTREE scalar, BITMAP, and full-text BM25 indexes over a set of datasets. |
| `recall` | Replay Datadog-sampled queries as exact brute-force scans and report recall@k, nDCG@k, MRR. |
| `tag` | Flip `HEAD` to an explicit target dataset version for blue-green promotion. |
| `migrate-manifests` | Migrate dataset manifest paths to the V2 naming scheme. |
| `migrate-namespace` | Copy a whole namespace to a new namespace name. One-off operator tool, not scheduled. |
| `optimize-iceberg` | Optimize the upstream Iceberg source table via Iceberg's own `CALL` maintenance procedures. Distinct from `maintenance`, which optimizes Lance datasets. Data-file and manifest rewrites default on. Snapshot expiration and orphan removal require explicit retention-gated opt-in. |

### The Airflow DAG

- The base pipeline is `etl >> maintenance >> index`.
- An optional `optimize-iceberg` task, gated by the `lance_etl_optimize_iceberg_enabled` Variable (default off), runs before `etl`. It optimizes the upstream Iceberg source table (bin-packs small files, rewrites manifests, expires old snapshots). The destructive `remove_orphan_files` step is further gated by `lance_etl_optimize_remove_orphan_files` (default off).
- The `maintenance` task runs TTL expiration (when `lance_etl_ttl_column` is set), two-tier compaction, and version cleanup in a single Spark job. Setting `lance_etl_ttl_column` to the name of a per-row Duration column enables TTL. Leaving it empty makes the task compaction plus cleanup only.
- **Maintenance runs before indexing on purpose.** It merges fresh uncovered fragments before any index covers them, so the large-tier inline index remap cost for fresh data disappears, and it serializes compaction and index commits per dataset within a run.
- `max_active_runs=1` extends that serialization across runs so overlapping runs cannot race same-name index maintenance commits.
- The schedule is driven by the `lance_etl_schedule` Variable (default `@daily`). The window comes from the Airflow data interval, or from explicit `start` / `end` keys in the trigger config.
- Backfills are native Airflow backfills. The idempotent `merge_insert` ensures a replayed slot converges rather than duplicates.

### Blue-green flip procedure

The order matters. **Always build, prewarm, then flip. Never flip, then warm.**

1. Build the new (green) dataset version offline (for example via `index` or `migrate-namespace`).
2. Tag green before any cleanup runs, so cleanup cannot delete it (`error_if_tagged_old_versions`).
3. **Prewarm green by explicit version** on every serving replica (the Prewarm RPC accepts an explicit version or tag and returns the resolved version).
4. Flip the `HEAD` tag to green with the `tag` subcommand and required `--tag-version`.
5. Watch telemetry for a flip-without-prewarm signal (served version not equal to the most-recently-prewarmed version).
6. To roll back, flip the tag back to the prior version.

The Python `tag` helper only writes `HEAD` at an explicit version and logs the safe sequence. It never assumes the serving layer auto-refreshes.

### Running a namespace migration

1. Run `migrate-namespace --source-namespace <old> --target-namespace <new> --base-uri <root>` with index column flags so the copy is reindexed (without them, reindex is skipped).
2. The job copies every dataset in the namespace, then recompacts and reindexes (skip with `--no-recompact` / `--no-reindex`).
3. The source is kept. Verify the new namespace and its rebuilt indexes (for example with a recall audit).
4. Repoint serving by moving the `prod` tag to the migrated datasets (prewarm first, per the flip procedure).
5. Delete the source only after a confirmed cutover. Cleanup is left to the operator on purpose, so it can never race an in-progress verification.

`migrate-manifests` (V2 manifest migration) is **not transactional**: run it only with the targeted datasets quiesced (no concurrent ingestion, compaction, or indexing).

---

## 12. Open issues, limitations, and future work

*Audience: everyone*

These are honest, verified against the ADRs.

- **Blue-green serving is still Proposed, not implemented.** The Python tag helper and the design are ready, but the Rust serving-side tag resolution and prewarm-before-flip safety are not done. A prior attempt was interrupted and backed out to keep the crate compiling (ADR 0013).
- **Reindex during migrate needs explicit index columns.** A namespace copy carries no indexes, and the columns to rebuild cannot be guessed, so reindex is skipped with a warning unless index column flags are supplied (ADR 0019).
- **No receipt-based (ingest-age) TTL.** There is no ingest-time column by design, so TTL is driven by each row's own lifetime column relative to its event timestamp. Adding ingest-age TTL would require a fresh ADR with an explicit ingest-time design, not a revival of the removed `_ingested_at` column (ADR 0016, ADR 0018).
- **Physically separate per-date datasets are no longer a built-in.** By-date partitioning and cross-date fan-out were removed. Time-bounded queries are now scalar range filters on the event timestamp. Routing each date to its own dataset is possible only as a deliberate orchestrator-layer choice, which reintroduces multi-dataset serving and is documented as a how-to, not a default (ADR 0014).
- **Stable row IDs remain rejected.** They are unsafe under the production concurrent workload (silent corruption risk on release builds). They will not be re-added, even opt-in, unless the upstream `RowIdIndex` defect is fixed, and only then via a fresh ADR (ADR 0010).
- **Large-tier index remap is paid inline.** The Python `Compaction.commit` binding hard-codes default options, so `defer_index_remap` cannot take effect on the large tier. Every covering index is remapped inline at commit. This is a binding gap, not a format limitation, and the DAG ordering (compact before index) keeps the cost low for fresh data (ADR 0002).
- **Insert-only fast path is not built yet.** A bulk first-write fast path (fragment writes plus a batched commit) is an identified follow-up for first-load bulk ingestion.

---

*This page is generated from the ADRs in `docs/adr/`, the findings narrative in `docs/FINDINGS.md`, the gRPC contracts under `rust/search-api/proto/`, the CLI in `src/lance_etl/cli.py`, the Airflow DAG, and the module documentation across the Python package and the Rust crate. When a decision changes, add or update an ADR first, then refresh this page.*
