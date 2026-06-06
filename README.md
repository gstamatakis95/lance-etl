# lance-etl

PySpark ETL pipeline that ingests embeddings and text from an Apache Iceberg table into per-org
Lance vector datasets, builds distributed IVF_RQ / scalar / FTS indices over those datasets, and
compacts them with version cleanup. A companion Rust gRPC service (tonic) serves vector, full-text,
and hybrid search over the same datasets.

Scale target: up to 1 billion vectors spread across up to 30,000 organisations.

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
  └─ BTreeIndexHandler / BitmapIndexHandler  same shard/commit flow, no merge
  └─ FtsIndexHandler.build()              driver: shared index_uuid
       └─ executor: create_scalar_index    executor: build per-fragment INVERTED segment
       └─ merge_index_metadata            driver: merge per-fragment INVERTED metadata
       └─ LanceDataset.commit             driver: publish with LanceOperation.CreateIndex
LanceCompactor.run()
  └─ Compaction.plan()                    driver: build rewrite-task list
       └─ parallelize(tasks).map(...)     executor: CompactionTask.execute
       └─ Compaction.commit               driver: commit rewrites
       └─ cleanup_old_versions            driver: prune old versions
```

The index segment API is the correct path for all index types. Spark heavy work runs in executors
only: the driver plans, broadcasts shared artifacts, and commits.

### Rust gRPC search service (`rust/search-api`)

Five-layer design over tonic:

| Layer | Crate path | Responsibility |
|---|---|---|
| `domain` | `crate::domain` | Engine-agnostic types: `Filter` AST, query types, `SearchBackend`, traits |
| `cache` | `crate::cache` | Disk + Moka index cache and metadata byte cache, plugged into Lance seams |
| `lance` | `crate::lance` | `LanceSearchBackend`, `CachingDatasetProvider`, typed AST -> DataFusion `Expr` |
| `grpc` | `crate::grpc` | Thin tonic adapter: proto <-> domain conversion, `SearchGrpc<B>` over any backend |
| `telemetry` | `crate::telemetry` | OTLP traces, DogStatsD metrics, JSON logs with trace correlation |

The typed filter AST (`Filter` enum with `Compare`, `InList`, `IsNull`, `IsNotNull`, `Between`,
`And`, `Or`, `Not`) prevents SQL injection: column names are validated against the dataset schema
and the identifier allowlist `[A-Za-z_][A-Za-z0-9_]*`. Literals are typed `datafusion::lit` calls,
never parsed as expressions.

`CachingDatasetProvider` holds one shared Lance `Session` (in-memory index + metadata caches) plus
a Moka LRU of open `Dataset` handles. Concurrent requests for the same org coalesce via the async
cache. A hybrid disk-tier cache (`DiskIndexCacheBackend` + `MetadataByteCache`) extends the session
caches to a local directory (default `/tmp/rust-search/cache`), so cold restarts skip the network
for recently read indexes and metadata.

Date-range fan-out: when `DatasetTarget` carries a `DateRange`, each day resolves to one dataset at
`{base}/{org_id}/{tenant_id}/{namespace}/{YYYY-MM-DD}.lance`. Days whose dataset does not exist are
skipped. Concurrent day-legs are bounded by `SEARCH_API_FANOUT_CONCURRENCY`. Results are
deduped by `SEARCH_API_ID_COLUMN` (keeping the best per-leg score) then fused with global RRF.

---

## Module map

### Python (`src/lance_etl/`)

| Module | Key types | Purpose |
|---|---|---|
| `etl.py` | `ETLConfig`, `IcebergToLanceETL` | Iceberg read, collapse, repartition, merge_insert |
| `indexing.py` | `LanceIndexer`, `*IndexHandler` | Distributed index builds via the segment API |
| `compaction.py` | `CompactionConfig`, `LanceCompactor` | Distributed compaction (plan / execute / commit) |
| `telemetry.py` | `Telemetry`, `TelemetryConfig`, `LanceRuntimeConfig` | ddtrace spans, DogStatsD, Lance events |
| `cloud_storage.py` | `resolve_filesystem`, `discover_datasets` | pyarrow filesystem + recursive dataset discovery |
| `arrow_types.py` | `resolve_arrow_type`, `resolve_type_map` | Arrow type specs (`fixed_size_list<float32,768>`) |
| `cli.py` | `main`, `build_parser` | Entry point: `etl`, `compact`, `index` subcommands |

### Rust (`rust/search-api/src/`)

| Path | Purpose |
|---|---|
| `domain/filter.rs` | Typed filter AST |
| `domain/query.rs` | `VectorQuery`, `TextQuery`, `HybridQuery`, `Hit`, `FusedHit` |
| `domain/backend.rs` | `SearchBackend` trait |
| `domain/prewarm.rs` | `PrewarmSpec`, `PrewarmReport`, `Prewarmer` trait |
| `domain/clusters.rs` | `ClusterSpec`, `ClusterReport`, `ClusterReader` trait |
| `domain/fusion.rs` | `FusionSpec`, `RrfFusion` |
| `domain/merge.rs` | Dedup-by-id fan-out merge |
| `cache/disk_cache.rs` | Hybrid disk + Moka `CacheBackend` for the Lance index cache |
| `cache/store_cache.rs` | Read-through byte cache for immutable metadata |
| `cache/layout.rs` | Versioned stamp dir, key hashing, atomic writes, TTL/budget sweep |
| `cache/janitor.rs` | Periodic TTL + byte-budget sweep loop |
| `lance/backend.rs` | `LanceSearchBackend<P>` — fan-out, dedup, RRF |
| `lance/provider.rs` | `DatasetProvider` trait, `CachingDatasetProvider` |
| `lance/filter.rs` | `filter_to_expr`: domain filter -> DataFusion `Expr` |
| `lance/text.rs` | FTS query node tree -> Lance FTS parameters |
| `lance/prewarm.rs` | `Prewarmer` impl over Lance prewarm APIs |
| `lance/index_reader.rs` | IVF centroid extraction, `ClusterReader` impl |
| `grpc/mod.rs` | `SearchGrpc<B>`: tonic service adapter |
| `grpc/convert.rs` | Proto <-> domain conversion |
| `telemetry/traces.rs` | OTLP span export, JSON stdout logs |
| `telemetry/metrics.rs` | Typed DogStatsD facade |
| `config.rs` | `Config` from environment variables |

### Airflow (`airflow/lance_etl_dag.py`)

DAG `lance_etl_pipeline` running `etl -> index -> compact` as `SparkSubmitOperator` tasks.
Schedule is driven by the Airflow Variable `lance_etl_schedule` (default `@daily`). Each task calls
`lance-etl <subcommand>` via `LANCE_ETL_CLI`. Data-interval windowing and `dag_run.conf` override
are described in the module docstring.

### Benchmark package (`bench/`)

End-to-end benchmark driving the real ETL, indexer, compactor, and gRPC server. Install with:

```bash
uv pip install --group bench
```

Run with `python -m bench <subcommand>`. Subcommands: `download`, `prepare`, `ingest`, `index`,
`compact`, `search`, `report`, `all`. Each subcommand accepts the full flag set, so one flag vector
can drive the entire `all` chain.

---

## Getting started

### Python environment

```bash
# Create and activate a virtual environment
uv venv
source .venv/bin/activate

# Install the package and dev dependencies
uv pip install -e ".[dev]"

# Install bench extras
uv pip install --group bench
```

**Note on pylance.** The project requires `pylance>=8.0.0b6`, which at the time of writing must be
built from the lance checkout at `/Users/gstamatakis/IdeaProjects/lance`:

```bash
cd /Users/gstamatakis/IdeaProjects/lance
maturin develop --release -m python/Cargo.toml
```

Once `pylance>=8.0.0b6` is published to PyPI, a plain `uv pip install -e ".[dev]"` will suffice.

### CLI usage

The entry point is installed as `lance-etl` (see `[project.scripts]` in `pyproject.toml`).

**ETL** — read one 24-hour window from an Iceberg table and upsert/delete into Lance datasets:

```bash
lance-etl etl \
  --table prod.vectors.events \
  --start 2024-01-15T00:00:00 \
  --end 2024-01-16T00:00:00 \
  --base-uri s3://my-bucket/lance \
  --num-partitions 512 \
  --column-type vectors_values=fixed_size_list<float16,768> \
  --dd-service lance-pipeline --dd-env prod
```

`--start` and `--end` accept ISO 8601 strings or epoch milliseconds. `--column-type` can be
repeated for each column that needs a type cast (e.g. `float16` for half-precision vectors). All
`--storage-option key=value` pairs are forwarded to pylance as object-store credentials.

**Dynamic routing** — partition columns and derived columns:

```bash
lance-etl etl ... \
  --partition-by org_id,tenant_id,namespace \
  --partition-derive event_date=processing_timestamp:%Y-%m-%d
```

`--partition-by` (default `org_id,tenant_id,namespace`) sets the columns that build the dataset
path `base_uri/<val1>/.../<valN>.lance`. `--partition-derive NAME=SOURCE:FORMAT` derives a column
from a source timestamp column using a Python strftime pattern before routing. Both flags are
repeatable. A key whose routing value changes between runs leaves a stale copy in the old dataset.
Readers and serving layers deduplicate.

**Window filter** — narrow the rows that reach the merge step:

```bash
lance-etl etl ... \
  --window-start 2024-01-15T06:00:00 \
  --window-end 2024-01-15T12:00:00 \
  --window-column updated_at
```

The window filter is applied as a Spark `DataFrame.filter` immediately after the Iceberg read. The
Iceberg read itself uses snapshot-id bounds resolved from the `{table}.snapshots` metadata table
(`start-snapshot-id` / `end-snapshot-id` for incremental scans, `snapshot-id` for the first run).

**Index** — build IVF_RQ, btree, bitmap, and FTS indices:

```bash
lance-etl index \
  --base-uri s3://my-bucket/lance \
  --vector-column vector \
  --num-partitions 256 \
  --scalar-column category \
  --text-column text \
  --fts-with-position \
  --num-shards 64
```

`--dataset-uri` is repeatable. `--datasets-file` reads one URI per line. `--base-uri` discovers
all `*.lance` datasets recursively at any depth so custom `--partition-by` hierarchies are picked
up alongside the default three-level layout.

**Compact** — run distributed compaction:

```bash
lance-etl compact \
  --base-uri s3://my-bucket/lance \
  --target-rows-per-fragment 1000000 \
  --max-tasks 256
```

`--defer-index-remap` opts in to deferred index remap (off by default). On the current lance build
a deferred remap leaves indexed vector queries failing with a missing fragment-id error until the
remap runs. Use it only when a remap step runs before queries resume. Deferred remap only takes
effect on the small-dataset tier. The large-dataset tier always remaps inline.

Common flags (all subcommands):

| Flag | Default | Purpose |
|---|---|---|
| `--dd-service` | `lance-pipeline` | Datadog service tag |
| `--dd-env` | `prod` | Datadog env tag |
| `--statsd-host` | `localhost` | DogStatsD host |
| `--statsd-port` | `8125` | DogStatsD port |
| `--lance-io-threads` | unset | `LANCE_IO_THREADS` — set to 128-256 for cloud stores |
| `--lance-cpu-threads` | unset | `LANCE_CPU_THREADS` — set below executor core count |
| `--storage-option` | none | Repeatable `key=value` passed to pylance as `storage_options` |

### Running tests

```bash
# Python tests (from repo root)
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

Environment variables:

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `LANCE_ETL_BASE_URI` | yes | — | Base URI all dataset paths are resolved under |
| `SEARCH_API_PORT` | no | `8080` | TCP port |
| `SEARCH_API_DATASET_CACHE_CAPACITY` | no | `1024` | Max open dataset handles in the LRU |
| `SEARCH_API_INDEX_CACHE_BYTES` | no | `1073741824` (1 GiB) | In-memory index cache budget |
| `SEARCH_API_METADATA_CACHE_BYTES` | no | `268435456` (256 MiB) | In-memory metadata cache budget |
| `SEARCH_API_CACHE_DIR` | no | `/tmp/rust-search/cache` | Root directory for persistent disk caches |
| `SEARCH_API_DISK_INDEX_CACHE_BYTES` | no | `8589934592` (8 GiB) | Disk budget for the index cache tier |
| `SEARCH_API_DISK_STORE_CACHE_BYTES` | no | `2147483648` (2 GiB) | Disk budget for the metadata byte cache |
| `SEARCH_API_DISK_CACHE_TTL_SECS` | no | `604800` (7 days) | TTL for disk cache entries |
| `SEARCH_API_STORE_CACHE_MAX_RANGE_BYTES` | no | `4194304` (4 MiB) | Max byte-range cached per metadata read |
| `SEARCH_API_DISK_CACHE_SWEEP_SECS` | no | `300` | Janitor sweep interval |
| `SEARCH_API_DISK_CACHE_DISABLED` | no | `false` | Set to `true` for pure in-memory fallback |
| `SEARCH_API_PREWARM_CONCURRENCY` | no | `4` | Indexes warmed concurrently per Prewarm RPC |
| `SEARCH_API_FANOUT_CONCURRENCY` | no | `8` | Per-day datasets queried concurrently per fan-out |
| `SEARCH_API_ID_COLUMN` | no | `vector_id` | Logical id column for deduplication across date legs |
| `SEARCH_API_STATSD_ADDR` | no | `127.0.0.1:8125` | DogStatsD UDP address (honors `DD_AGENT_HOST`) |
| `SEARCH_API_TELEMETRY_DISABLED` | no | `false` | Disable trace export and DogStatsD (JSON logs only) |

Proto RPCs on `lance_etl.search.v1.SearchService`:

| RPC | Request key fields | Purpose |
|---|---|---|
| `VectorSearch` | `target`, `query` | Nearest-neighbor search |
| `TextSearch` | `target`, `query` | BM25 full-text search |
| `HybridSearch` | `target`, `vector`, `text`, `k` | Vector + text fused with RRF |
| `Prewarm` | `target`, `metadata`, `all_indexes`, `index_names` | Pull caches for one dataset |
| `Clusters` | `target`, `index_name` | Read IVF centroid vectors of the vector index |

All requests carry a `DatasetTarget` with `org_id`, `tenant_id`, `namespace`, and an optional
`DateRange`. Without a `DateRange` the target resolves to the single dataset at
`{base}/{org_id}/{tenant_id}/{namespace}.lance`. With one it fans out over one dataset per day at
`{base}/{org_id}/{tenant_id}/{namespace}/{YYYY-MM-DD}.lance`, skipping missing days. Filters are
typed AST nodes (`Filter` oneof) — raw SQL strings are never accepted.

### Airflow DAG deployment

Deploy `airflow/lance_etl_dag.py` to your Airflow DAGs folder. Set the Airflow Connection
`spark_default` to point at your Spark cluster. Configure the pipeline via Airflow Variables:

| Variable | Default | Purpose |
|---|---|---|
| `lance_etl_schedule` | `@daily` | Airflow schedule expression |
| `lance_etl_iceberg_table` | `prod.vectors.events` | Fully-qualified Iceberg table name |
| `lance_etl_lance_base_uri` | `s3://my-bucket/lance` | Base URI for Lance datasets |
| `lance_etl_datasets_file` | `/opt/lance/datasets.txt` | File listing dataset URIs for index and compact |
| `lance_etl_spark_conn_id` | `spark_default` | Airflow Spark connection id |
| `lance_etl_executor_instances` | `8` | `spark.executor.instances` |
| `lance_etl_executor_memory` | `8g` | `spark.executor.memory` |
| `lance_etl_dd_service` | `lance-pipeline` | Datadog service tag |
| `lance_etl_dd_tags` | empty | Comma-separated `key:value` constant tags |
| `lance_etl_window_column` | `updated_at` | Iceberg timestamp column for window pushdown |
| `lance_etl_partition_by` | empty | Comma-separated partition columns for `--partition-by` |
| `lance_etl_partition_derive` | empty | Comma-separated `NAME=SOURCE:FORMAT` derivation specs |

The schedule is controlled by `lance_etl_schedule`. Each run processes the Airflow data interval.
Manual triggers can supply `{"start": "<ISO-8601>", "end": "<ISO-8601>"}` in `dag_run.conf` to
override the window bounds. Backfill with `airflow dags backfill lance_etl_pipeline`.

The `lance-etl` wheel must be installed on every executor. Either bake it into the cluster image
or ship it via `spark.submit.pyFiles` (see the module docstring in `airflow/lance_etl_dag.py`).

### Benchmark (`bench/`)

Run `python -m bench <subcommand>` from the repo root after installing the bench group.

Key flags shared by all subcommands:

| Flag | Default | Purpose |
|---|---|---|
| `--dataset` | `sift1m` | Registered dataset adapter (`sift1m` or `synthetic`) |
| `--limit` | `1000000` | Base vectors to benchmark |
| `--tenants` | `1` | Round-robin split into this many org datasets |
| `--batches` | `1` | Sequential ETL merge batches |
| `--warmup-queries` | `100` | Queries issued before the timed recall sweep |
| `--prewarm` | off | Call the Prewarm RPC before the first timed query per org |
| `--endpoint` | `localhost:50051` | gRPC server address |
| `--nprobes` | `1,10,25,50,100` | Probed-partition sweep values |
| `--refine-factors` | `none,5,10` | Re-ranking factor sweep |

Results land in `bench/results/<run-id>/`. Each run writes `summary.md`, `recall.csv`,
`results.csv`, and (when the recall sweep ran) `pareto.png`.

The `search` subcommand runs four legs: recall (nprobes x refine_factor sweep with Recall@1/10/100),
FTS (BM25 latency and cluster-consistency hit rate), hybrid (vector + text fused with RRF), and load
(sustained QPS via `ghz` when it is on PATH). The `clusters` probe validates the Clusters RPC
geometry per org. The `all` chain skips `search` with a recorded reason when the gRPC server is
unreachable, allowing offline runs.
