# lance-etl

PySpark ETL pipeline that ingests embeddings and text from an Apache Iceberg table into per-org
Lance vector datasets, builds distributed IVF_RQ / scalar / FTS indices, and compacts them with
version cleanup. A companion Rust gRPC service (tonic) serves vector, full-text, and hybrid search
over the same datasets.

Scale target: up to 1 billion vectors spread across up to 30,000 organisations with a power-law
size distribution.

---

## Architecture overview

### ETL tier (Python / PySpark)

```
Iceberg table
  └─ IcebergToLanceETL.run()               driver: read + collapse + repartition
       └─ mapInArrow(merge_partition)       executor: merge_insert upsert/delete per org dataset
LanceIndexer.run()
  └─ VectorIndexHandler.build()            driver: train IVF centroids + build RaBitQ model
       └─ parallelize(shards).map(...)     executor: create_index_uncommitted per fragment shard
       └─ merge_existing_index_segments    driver: merge segments
       └─ commit_existing_index_segments   driver: commit
  └─ BTreeIndexHandler / BitmapIndexHandler  same shard/commit flow, no shared uuid
  └─ FtsIndexHandler.build()              driver: shared index_uuid
       └─ create_scalar_index             executor: build per-fragment INVERTED segment
       └─ merge_index_metadata            driver: merge INVERTED metadata
       └─ LanceDataset.commit             driver: publish with LanceOperation.CreateIndex
LanceCompactor.run()
  └─ Tier A (small datasets)              executor: Compaction.execute per dataset
  └─ Tier B (large datasets)
       └─ Compaction.plan()               driver: build rewrite-task list
       └─ parallelize(tasks).map(...)     executor: CompactionTask.execute
       └─ Compaction.commit               driver: commit rewrites
  └─ cleanup_old_versions                 driver: prune old versions (tagged versions exempt)
```

All heavy I/O and compute runs in executors. The driver plans, broadcasts shared artifacts
(IVF centroids, RaBitQ model), and commits. Move-stable row IDs are rejected (see
`docs/adr/0010-stable-row-ids-rejected.md`). V2 manifest paths are on by default, making each of
the 30k dataset opens a single object-store request.

### Rust gRPC search service (`rust/search-api`)

Five-layer design over tonic:

| Layer | Crate path | Responsibility |
|---|---|---|
| `domain` | `crate::domain` | Engine-agnostic types: `Filter` AST, query types, rerank seam, traits |
| `cache` | `crate::cache` | Disk + Moka index cache and metadata byte cache, plugged into Lance seams |
| `lance` | `crate::lance` | `LanceSearchBackend`, `CachingDatasetProvider`, typed AST -> DataFusion `Expr` |
| `grpc` | `crate::grpc` | Thin tonic adapter: proto <-> domain conversion, `SearchGrpc<B>` over any backend |
| `telemetry` | `crate::telemetry` | OTLP traces, DogStatsD metrics, JSON logs with trace correlation |

The typed filter AST (`Filter` with `Compare`, `InList`, `IsNull`, `IsNotNull`, `Between`, `And`,
`Or`, `Not`) prevents SQL injection: column names are validated against the dataset schema and the
identifier allowlist `[A-Za-z_][A-Za-z0-9_]*`. Literals are typed `datafusion::lit` calls, never
parsed as expressions.

`CachingDatasetProvider` holds one shared Lance `Session` plus a Moka LRU of open `Dataset`
handles. Concurrent requests for the same org coalesce via the async cache. A hybrid disk-tier
cache (`DiskIndexCacheBackend` + `MetadataByteCache`) extends the session caches to a local
directory (default `/tmp/rust-search/cache`), so cold restarts skip the network for recently read
indexes and metadata. Raw data bytes are never cached.

Date-range fan-out: when `DatasetTarget` carries a `DateRange`, each day resolves to one dataset at
`{base}/{org_id}/{tenant_id}/{namespace}/{YYYY-MM-DD}.lance`. Days whose dataset does not exist are
skipped. Concurrent day-legs are bounded by `SEARCH_API_FANOUT_CONCURRENCY`. Results are deduped by
`SEARCH_API_ID_COLUMN` (keeping the best per-leg score) then re-fused.

Blue-green serving: a `prod` tag (or any named tag) is updated atomically with `tags.update`. When
`SEARCH_API_SERVE_BY_TAG=true`, the provider resolves the tag to a concrete version, keys its LRU
and caches on that version, and re-reads the tag after `SEARCH_API_SERVE_TAG_TTL_SECS` seconds so
a flip propagates within the TTL. The correct operational sequence is: build the green version,
prewarm every replica against the green version explicitly (use the `version` or `tag` field in
`PrewarmRequest`), then flip the tag. Never flip then warm.

---

## Module map

### Python (`src/lance_etl/`)

| Module | Key types | Purpose |
|---|---|---|
| `etl.py` | `ETLConfig`, `IcebergToLanceETL` | Iceberg read, collapse, repartition, merge_insert |
| `indexing.py` | `LanceIndexer`, `*IndexHandler` | Distributed index builds via the segment API |
| `compaction.py` | `CompactionConfig`, `LanceCompactor` | Two-tier compaction (small/large), blue-green tag, migrate |
| `recall.py` | `RecallAuditJob`, `RecallJobConfig` | Offline recall@k / nDCG@k / MRR audit from Datadog spans |
| `telemetry.py` | `Telemetry`, `TelemetryConfig` | ddtrace spans, DogStatsD, Lance event bridge |
| `cloud_storage.py` | `resolve_filesystem`, `discover_datasets` | pyarrow filesystem + recursive dataset discovery |
| `arrow_types.py` | `resolve_arrow_type`, `resolve_type_map` | Arrow type specs (`fixed_size_list<float32,768>`) |
| `cli.py` | `main`, `build_parser` | Six subcommands: `etl`, `compact`, `index`, `recall`, `tag`, `migrate-manifests` |

### Rust (`rust/search-api/src/`)

| Path | Purpose |
|---|---|
| `domain/filter.rs` | Typed filter AST — no raw SQL accepted anywhere |
| `domain/query.rs` | `VectorQuery`, `TextQuery`, `HybridQuery`, `Hit`, `FusedHit` |
| `domain/backend.rs` | `SearchBackend` trait |
| `domain/prewarm.rs` | `PrewarmSpec`, `PrewarmReport`, `Prewarmer` trait |
| `domain/clusters.rs` | `ClusterSpec`, `ClusterReport`, `ClusterReader` trait |
| `domain/fusion.rs` | `FusionSpec` (Rrf and Weighted variants) and fusion logic |
| `domain/rerank.rs` | `Reranker` seam, `IdentityReranker` (no-op default) |
| `domain/merge.rs` | Dedup-by-id fan-out merge, keep-best policy |
| `cache/disk_cache.rs` | Hybrid disk + Moka `CacheBackend` for the Lance index cache |
| `cache/store_cache.rs` | Read-through byte cache for immutable metadata |
| `cache/layout.rs` | Versioned stamp dir, key hashing, atomic writes, TTL/budget sweep |
| `cache/janitor.rs` | Periodic TTL + byte-budget sweep loop |
| `lance/backend.rs` | `LanceSearchBackend<P>` — date-range fan-out, dedup, post-fusion rerank |
| `lance/provider.rs` | `DatasetProvider` trait, `CachingDatasetProvider`, tag-version TTL cache |
| `lance/filter.rs` | `filter_to_expr`: domain filter -> DataFusion `Expr` |
| `lance/text.rs` | FTS query node tree -> Lance FTS parameters |
| `lance/prewarm.rs` | `Prewarmer` impl over Lance prewarm APIs |
| `lance/index_reader.rs` | IVF centroid extraction, `ClusterReader` impl |
| `grpc/mod.rs` | `SearchGrpc<B>`: tonic service adapter |
| `grpc/convert.rs` | Proto <-> domain conversion |
| `telemetry/traces.rs` | OTLP span export, JSON stdout logs with trace correlation |
| `telemetry/metrics.rs` | Typed DogStatsD facade (`search_api.*` prefix) |
| `telemetry/recall.rs` | Deterministic sampled-query capture into `recall.*` span attributes |
| `config.rs` | `Config` from environment variables |

### Airflow (`airflow/lance_etl_dag.py`)

DAG `lance_etl_pipeline` running `etl -> compact -> index` as `SparkSubmitOperator` tasks with
`max_active_runs=1`. Compaction runs before indexing so fresh uncovered fragments are merged into
large fragments before the index covers them, avoiding inline remap cost on every index commit.
Schedule is driven by the Airflow Variable `lance_etl_schedule` (default `@daily`). Data-interval
windowing and `dag_run.conf` overrides are described in the module docstring.

### Benchmark package (`bench/`)

End-to-end benchmark driving the real ETL, indexer, compactor, and gRPC server. Install with:

```bash
uv pip install --group bench
```

Run with `python -m bench <subcommand>`. Subcommands: `download`, `prepare`, `ingest`, `index`,
`compact`, `search`, `report`, `all`. Each subcommand accepts the full flag set, so one flag vector
can drive the entire `all` chain.

### Documentation (`docs/`)

- `docs/adr/` — 13 Architecture Decision Records (0001 through 0013) covering distributed indexing,
  two-tier compaction, snapshot-id bounds, dynamic partition routing, gRPC layering, date-range
  fan-out, disk cache and prewarm, observability and recall audit, compaction/index coexistence,
  stable-row-id rejection, ingested-at column, V2 manifest paths, and blue-green serving.
- `docs/FINDINGS.md` — narrative companion to the ADRs: verified APIs, production patterns,
  scale design, coexistence results, and open items.
- `market-research/` — detailed evaluation notes, plans, and evidence underlying the ADRs.

---

## Getting started

### Python environment

```bash
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
uv pip install --group bench
```

The project requires `pylance>=8.0.0b6`, which must currently be built from the lance checkout:

```bash
cd /Users/gstamatakis/IdeaProjects/lance
maturin develop --release -m python/Cargo.toml
```

Once `pylance>=8.0.0b6` is published to PyPI, a plain `uv pip install -e ".[dev]"` will suffice.

### CLI overview

The CLI is deliberately small and opinionated. It exposes only the arguments that are genuinely
per-deployment: the data and identity contract (which table, which window, where datasets live,
Datadog service) and what to build (key/vector/metadata columns, partition routing, which index
types and their tokenizer knobs). Every tuning knob — shuffle partitions, retry budgets, compaction
fragment sizing, IVF training parameters, two-tier thresholds — is set to an opinionated default in
the configuration dataclasses (`ETLConfig`, `IndexJobConfig`, `CompactionConfig`) and stays tunable
in code, not from the command line.

The entry point is installed as `lance-etl`.

#### `etl` — read a snapshot window from Iceberg and upsert/delete into Lance datasets

```bash
lance-etl etl \
  --table prod.vectors.events \
  --start 2024-01-15T00:00:00 \
  --end 2024-01-16T00:00:00 \
  --base-uri s3://my-bucket/lance \
  --key-col vector_id \
  --vectors-col vectors \
  --column-type vectors=fixed_size_list<float16,768> \
  --dd-service lance-pipeline --dd-env prod
```

`--start` / `--end` accept ISO 8601 strings or epoch milliseconds and resolve to Iceberg
snapshot-id bounds. `--column-type` can be repeated for each column needing a type cast.

Partition routing and derived columns:

```bash
lance-etl etl ... \
  --partition-by org_id,tenant_id,namespace \
  --partition-derive event_date=processing_timestamp:%Y-%m-%d
```

`--partition-by` (default `org_id,tenant_id,namespace`) sets the columns that build the dataset
path `base_uri/<val1>/.../<valN>.lance`. `--partition-derive NAME=SOURCE:FORMAT` derives a column
from a source timestamp column using a Python strftime pattern before routing.

Window pushdown filter (applied after the Iceberg read):

```bash
lance-etl etl ... \
  --window-start 2024-01-15T06:00:00 \
  --window-end 2024-01-15T12:00:00 \
  --window-column updated_at
```

Full `etl` flag reference:

| Flag | Default | Purpose |
|---|---|---|
| `--table` | (required) | Fully-qualified Iceberg table name |
| `--start` | (required) | Iceberg snapshot window start (ISO 8601 or epoch ms) |
| `--end` | (required) | Iceberg snapshot window end (ISO 8601 or epoch ms) |
| `--base-uri` | (required) | Root URI for per-tenant Lance datasets |
| `--key-col` | `vector_id` | Unique row key for merge_insert dedup |
| `--partition-by` | `org_id,tenant_id,namespace` | Comma-separated routing columns |
| `--partition-derive` | none | Repeatable `NAME=SOURCE:FORMAT` derived column |
| `--vectors-col` | `vectors` | Vector column name |
| `--metadata-col` | `metadata` | Metadata column name |
| `--ts-col` | `timestamp` | Timestamp column name |
| `--op-col` | `op` | CDC operation column name |
| `--delete-op-value` | `delete,DELETE,d` | Repeatable delete sentinel values |
| `--column-type` | none | Repeatable `name=arrow_type` cast |
| `--iceberg-option` | none | Repeatable `key=value` Iceberg read option |
| `--window-start` | none | Inclusive lower bound for the window pushdown filter |
| `--window-end` | none | Exclusive upper bound for the window pushdown filter |
| `--window-column` | `updated_at` | Column used for the window pushdown filter |
| `--ingested-at-col` | `_ingested_at` | Ingestion-timestamp column stamped on every row |
| `--storage-option` | none | Repeatable `key=value` passed to pylance |
| `--dd-service` | `lance-pipeline` | Datadog service tag |
| `--dd-env` | `prod` | Datadog env tag |
| `--dd-version` | empty | Datadog version tag |
| `--dd-tag` | none | Repeatable constant `key=value` Datadog tag |

#### `compact` — distributed compaction

```bash
lance-etl compact \
  --base-uri s3://my-bucket/lance \
  --dd-service lance-pipeline --dd-env prod
```

Dataset selection: `--dataset-uri` (repeatable), `--datasets-file`, or `--base-uri` (discovers all
`*.lance` paths recursively). All tuning knobs use opinionated defaults from `CompactionConfig`.

#### `index` — build or incrementally maintain indices

```bash
lance-etl index \
  --base-uri s3://my-bucket/lance \
  --vector-column vector \
  --metric cosine \
  --scalar-column updated_at \
  --bitmap-column category \
  --text-column text \
  --fts-with-position \
  --dd-service lance-pipeline --dd-env prod
```

Full `index` flag reference (data-shape flags only — tuning knobs use `IndexJobConfig` defaults):

| Flag | Default | Purpose |
|---|---|---|
| `--vector-column` | none | Vector column for IVF_RQ index |
| `--metric` | `L2` | Distance metric: `L2`, `cosine`, or `dot` |
| `--scalar-column` | none | Repeatable column for a BTREE index |
| `--bitmap-column` | none | Repeatable column for a BITMAP index |
| `--text-column` | none | Repeatable column for an INVERTED (BM25) index |
| `--fts-with-position` | off | Store token positions for phrase queries |
| `--fts-base-tokenizer` | none | FTS base tokenizer name |
| `--fts-language` | none | Stemming and stop-word language |
| `--fts-lower-case` | none | Enable lowercase normalisation |
| `--fts-stem` | none | Enable stemming |
| `--fts-remove-stop-words` | none | Enable stop-word removal |
| `--fts-ascii-folding` | none | Enable ASCII folding |
| `--rebuild` | off | Reindex every fragment (use after tokenizer or parameter changes) |

#### `recall` — offline recall audit

Fetches Datadog-sampled vector, text, and hybrid search spans, replays each query as an exact
brute-force or exact BM25 scan against the dataset version that served it, and reports
recall@k, nDCG@k, and MRR per RPC-parameter bucket and per organisation.

```bash
lance-etl recall \
  --from 2024-01-15T00:00:00 \
  --to 2024-01-16T00:00:00 \
  --base-uri s3://my-bucket/lance \
  --dd-service lance-pipeline --dd-env prod
```

`DD_API_KEY` and `DD_APP_KEY` must be set in the environment.

| Flag | Default | Purpose |
|---|---|---|
| `--from` | (required) | Window start (ISO 8601 or epoch ms) |
| `--to` | (required) | Window end (ISO 8601 or epoch ms) |
| `--base-uri` | (required) | Root URI for per-tenant Lance datasets |
| `--dd-site` | `datadoghq.com` | Datadog site domain for the Spans search API |
| `--max-samples` | `10000` | Cap on sampled spans fetched |
| `--id-column` | `vector_id` | Unique id column matched against served result ids |
| `--vector-column` | `vector` | Fixed-size-list vector column for brute-force distances |
| `--batch-size` | `8192` | Scanner batch size for the brute-force scan |

What it measures:

- **Vector queries**: exact brute-force nearest-neighbor scan at the pinned dataset version.
  Reports recall@k, nDCG@k, and MRR against the true distance-ordered ranking.
- **Text queries** (`recall.query_type=text`): exact Okapi BM25 ranking at the pinned version.
  Text recall is primarily a staleness and version-correctness signal since FTS returns exact results.
- **Hybrid queries** (`recall.query_type=hybrid`): recomputes exact vector and exact BM25 top-k,
  fuses with the recorded fusion strategy (RRF or weighted), and grades the served ids.

#### `tag` — blue-green serving-tag flip

Updates a serving tag (default `prod`) to a target dataset version. Tagged versions are exempt from
version cleanup.

```bash
lance-etl tag \
  --base-uri s3://my-bucket/lance \
  --tag prod \
  --tag-version 42 \
  --dd-service lance-pipeline --dd-env prod
```

Safe operational sequence:

1. Build the green version (ETL + index + compact run).
2. Prewarm every replica against the green version using the `Prewarm` RPC with an explicit
   `version` (or `tag`) field. Confirm `resolved_version` in the response matches the green version.
3. Flip the tag with `lance-etl tag --tag-version <green>`. Never flip then warm.

| Flag | Default | Purpose |
|---|---|---|
| `--tag` | `prod` | Serving tag name to update |
| `--tag-version` | none | Target version. Omit to point the tag at each dataset's latest. |

#### `migrate-manifests` — migrate to V2 manifest paths

```bash
lance-etl migrate-manifests \
  --base-uri s3://my-bucket/lance \
  --dd-service lance-pipeline --dd-env prod
```

Migrates each dataset's manifest paths to the V2 naming scheme, turning every subsequent dataset
open into a single object-store request. Not transactional: run only with the targeted datasets
quiesced (no concurrent ingestion, compaction, or indexing).

### Running tests

```bash
# Python tests
.venv/bin/pytest

# Rust tests
cd rust/search-api
cargo test
```

### Running the gRPC search service

```bash
cd rust/search-api
cargo build --release
LANCE_ETL_BASE_URI=s3://my-bucket/lance \
  SEARCH_API_PORT=8080 \
  ./target/release/search-api
```

Environment variables (`LANCE_ETL_BASE_URI` is required. All others are optional):

| Variable | Default | Purpose |
|---|---|---|
| `LANCE_ETL_BASE_URI` | (required) | Base URI all dataset paths are resolved under |
| `SEARCH_API_PORT` | `8080` | TCP port |
| `SEARCH_API_DATASET_CACHE_CAPACITY` | `1024` | Max open dataset handles in the LRU |
| `SEARCH_API_INDEX_CACHE_BYTES` | `1073741824` (1 GiB) | In-memory index cache budget |
| `SEARCH_API_METADATA_CACHE_BYTES` | `268435456` (256 MiB) | In-memory metadata cache budget |
| `SEARCH_API_CACHE_DIR` | `/tmp/rust-search/cache` | Root directory for persistent disk caches |
| `SEARCH_API_DISK_INDEX_CACHE_BYTES` | `8589934592` (8 GiB) | Disk budget for the index cache tier |
| `SEARCH_API_DISK_STORE_CACHE_BYTES` | `2147483648` (2 GiB) | Disk budget for the metadata byte cache |
| `SEARCH_API_DISK_CACHE_TTL_SECS` | `604800` (7 days) | TTL for disk cache entries |
| `SEARCH_API_STORE_CACHE_MAX_RANGE_BYTES` | `4194304` (4 MiB) | Max byte-range cached per metadata read |
| `SEARCH_API_DISK_CACHE_SWEEP_SECS` | `300` | Janitor sweep interval in seconds |
| `SEARCH_API_DISK_CACHE_DISABLED` | `false` | Set to `true` for pure in-memory fallback |
| `SEARCH_API_PREWARM_CONCURRENCY` | `4` | Indexes warmed concurrently per Prewarm RPC |
| `SEARCH_API_FANOUT_CONCURRENCY` | `8` | Per-day datasets queried concurrently per fan-out |
| `SEARCH_API_ID_COLUMN` | `vector_id` | Logical id column for deduplication across date legs |
| `SEARCH_API_IO_CONCURRENCY` | `256` | Parallel in-flight object-store requests per dataset |
| `SEARCH_API_IO_BLOCK_SIZE_BYTES` | `262144` (256 KiB) | Minimum object-store request size |
| `SEARCH_API_OBJECT_STORE_TIMEOUT_SECS` | `120` | Total retry-window timeout per cloud request |
| `SEARCH_API_RECALL_SAMPLE_RATE` | `0.0` (off) | Fraction of requests sampled for offline recall |
| `SEARCH_API_SERVE_BY_TAG` | `false` | Resolve the serve tag instead of opening latest |
| `SEARCH_API_SERVE_TAG` | `prod` | Tag name resolved when `SEARCH_API_SERVE_BY_TAG=true` |
| `SEARCH_API_SERVE_TAG_TTL_SECS` | `10` | Seconds a resolved tag version is trusted |
| `SEARCH_API_STATSD_ADDR` | `127.0.0.1:8125` | DogStatsD UDP address (honors `DD_AGENT_HOST`) |
| `SEARCH_API_TELEMETRY_DISABLED` | `false` | Disable trace export and DogStatsD (JSON logs only) |

`DD_AGENT_HOST` is read by the default statsd address resolver: when set, the default becomes
`${DD_AGENT_HOST}:8125`. `SEARCH_API_STATSD_ADDR` overrides it unconditionally.

Proto RPCs on `lance_etl.search.v1.SearchService`:

| RPC | Key request fields | Purpose |
|---|---|---|
| `VectorSearch` | `target`, `query`, `rerank` | Nearest-neighbor search with optional rerank |
| `TextSearch` | `target`, `query`, `rerank` | BM25 full-text search with optional rerank |
| `HybridSearch` | `target`, `vector`, `text`, `k`, `fusion`, `rerank` | Fused vector + text (RRF or weighted) |
| `Prewarm` | `target`, `metadata`, `all_indexes`, `index_names`, `version`/`tag` | Pull caches at a version or tag |
| `Clusters` | `target`, `index_name` | Read IVF centroid vectors of the vector index |

All requests carry a `DatasetTarget` (`org_id`, `tenant_id`, `namespace`, optional `DateRange`).
Without a `DateRange` the target resolves to `{base}/{org}/{tenant}/{namespace}.lance`. With one it
fans out over one dataset per calendar day, skipping missing days. Filters are typed AST nodes
(`Filter` oneof) — raw SQL strings are never accepted.

The `Prewarm` RPC accepts `version` (explicit committed version id) or `tag` (resolves the named
tag at call time) and returns `resolved_version`, enabling the safe green-before-flip workflow.

Fusion: `RrfFusion` (default, reciprocal-rank fusion with configurable `rrf_k`) or `WeightedFusion`
(min-max normalized legs combined by `vector_weight`). Post-fusion reranking: `IdentityRerank`
(no-op identity, with optional `top_n` truncation) is the only shipped strategy and is the seam
where a cross-encoder or LLM reranker slots in without changing the request shape.

### Airflow DAG deployment

Deploy `airflow/lance_etl_dag.py` to your Airflow DAGs folder. Set the Airflow Connection
`spark_default` to point at your Spark cluster. Pipeline order is `etl >> compact >> index` with
`max_active_runs=1`.

Configure via Airflow Variables:

| Variable | Default | Purpose |
|---|---|---|
| `lance_etl_schedule` | `@daily` | Airflow schedule expression |
| `lance_etl_iceberg_table` | `prod.vectors.events` | Fully-qualified Iceberg table name |
| `lance_etl_lance_base_uri` | `s3://my-bucket/lance` | Base URI for Lance datasets |
| `lance_etl_datasets_file` | `/opt/lance/datasets.txt` | File listing dataset URIs for `index` and `compact` |
| `lance_etl_index_flags` | empty | Shell-tokenized index column-selection flags for the `index` step |
| `lance_etl_spark_conn_id` | `spark_default` | Airflow Spark connection id |
| `lance_etl_executor_instances` | `8` | `spark.executor.instances` |
| `lance_etl_executor_memory` | `8g` | `spark.executor.memory` |
| `lance_etl_driver_memory` | `4g` | `spark.driver.memory` |
| `lance_etl_spark_conf_overrides` | `{}` | JSON object of extra Spark conf key/value pairs |
| `lance_etl_dd_service` | `lance-pipeline` | Datadog service tag |
| `lance_etl_dd_env` | `prod` | Datadog env tag |
| `lance_etl_dd_tags` | empty | Comma-separated `key:value` constant tags |
| `lance_etl_window_column` | `updated_at` | Iceberg timestamp column for window pushdown |
| `lance_etl_partition_by` | empty | Comma-separated partition columns for `--partition-by` |
| `lance_etl_partition_derive` | empty | Comma-separated `NAME=SOURCE:FORMAT` derivation specs |

`lance_etl_index_flags` is required when index maintenance is desired. Without it the `index` step
configures zero handlers and is a silent no-op. Example value:
`--vector-column vector --metric cosine --scalar-column updated_at --text-column text`.

Manual triggers can supply `{"start": "<ISO-8601>", "end": "<ISO-8601>"}` in `dag_run.conf` to
override the window bounds. Backfill with `airflow dags backfill lance_etl_pipeline`.

The `lance-etl` wheel must be installed on every executor. Either bake it into the cluster image
or ship it via `spark.submit.pyFiles` (see the module docstring in `airflow/lance_etl_dag.py`).

### Benchmark (`bench/`)

Run `python -m bench <subcommand>` from the repo root after installing the bench group.

Key flags shared by all subcommands:

| Flag | Default | Purpose |
|---|---|---|
| `--dataset` | `sift1m` | Registered dataset adapter: `sift1m` or `synthetic` |
| `--limit` | `1000000` | Base vectors to benchmark |
| `--tenants` | `1` | Round-robin split into this many org datasets |
| `--batches` | `1` | Sequential ETL merge batches (values above 1 create fragments for compaction) |
| `--warmup-queries` | `100` | Queries issued before the timed recall sweep |
| `--prewarm` | off | Call the Prewarm RPC before the first timed query per org |
| `--endpoint` | `localhost:50051` | gRPC server address |
| `--nprobes` | `1,10,25,50,100` | Probed-partition sweep values |
| `--refine-factors` | `none,5,10` | Re-ranking factor sweep |

Results land in `bench/results/<run-id>/`. Each run writes `summary.md`, `recall.csv`,
`results.csv`, and (when the recall sweep ran) `pareto.png`.

The `search` subcommand runs recall (nprobes x refine_factor sweep), FTS (BM25 latency), hybrid
(vector + text fused with RRF), and load (sustained QPS via `ghz` when on PATH) legs. The
`all` chain skips `search` with a recorded reason when the gRPC server is unreachable.

A new corpus plugs in by implementing `DatasetAdapter` and calling `register_adapter`. The
`synthetic` adapter generates a deterministic in-memory Gaussian corpus with no download and backs
the offline integration tests.
