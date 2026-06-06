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
  └─ FtsIndexHandler.build()              driver: shared index_uuid; executor: create_scalar_index
       └─ merge_index_metadata            driver: merge per-fragment INVERTED metadata
       └─ LanceDataset.commit             driver: publish with LanceOperation.CreateIndex
LanceCompactor.run()
  └─ Compaction.plan()                    driver: build rewrite-task list
       └─ parallelize(tasks).map(...)     executor: CompactionTask.execute
       └─ Compaction.commit               driver: commit rewrites (defer_index_remap=True by default)
       └─ cleanup_old_versions            driver: prune old versions
```

The index segment API is the correct path for all index types. Spark heavy work runs in executors
only: the driver plans, broadcasts shared artifacts, and commits.

### Rust gRPC search service (`rust/search-api`)

Three-layer design over tonic:

| Layer | Package | Responsibility |
|---|---|---|
| `domain` | `crate::domain` | Engine-agnostic types: `Filter` AST, query types, `SearchBackend`, `DatasetProvider` |
| `lance` | `crate::lance` | `LanceSearchBackend`, `CachingDatasetProvider`, typed AST -> DataFusion `Expr` |
| `grpc` | `crate::grpc` | Thin tonic adapter: proto <-> domain conversion, `SearchGrpc<B>` over any `SearchBackend` |

The typed filter AST (`Filter` enum with `Compare`, `InList`, `IsNull`, `IsNotNull`, `Between`,
`And`, `Or`, `Not`) prevents SQL injection: column names are validated against the dataset schema
and the identifier allowlist `[A-Za-z_][A-Za-z0-9_]*`; literals are typed `datafusion::lit` calls,
never parsed as expressions.

The `CachingDatasetProvider` holds one shared Lance `Session` (index + metadata caches) and a Moka
LRU of open `Dataset` handles. Concurrent requests for the same org coalesce via the async cache.

---

## Module map

### Python (`src/lance_etl/`)

| Module | Key types | Purpose |
|---|---|---|
| `etl.py` | `ETLConfig`, `IcebergToLanceETL` | Iceberg read, collapse, repartition, merge_insert |
| `indexing.py` | `LanceIndexer`, `*IndexHandler` | Distributed index builds via the segment API |
| `compaction.py` | `CompactionConfig`, `LanceCompactor` | Distributed compaction (plan / execute / commit) |
| `telemetry.py` | `Telemetry`, `TelemetryConfig`, `LanceRuntimeConfig` | ddtrace spans, DogStatsD, Lance events |
| `cloud_storage.py` | `resolve_filesystem`, `CloudProvider` | pyarrow filesystem for IVF_RQ sidecars |
| `arrow_types.py` | `resolve_arrow_type`, `resolve_type_map` | Arrow type specs (`fixed_size_list<float32,768>`) |
| `cli.py` | `main`, `build_parser` | Entry point: `etl`, `compact`, `index` subcommands |

### Rust (`rust/search-api/src/`)

| Path | Purpose |
|---|---|
| `domain/filter.rs` | Typed filter AST |
| `domain/query.rs` | `VectorQuery`, `TextQuery`, `HybridQuery`, `Hit`, `FusedHit` |
| `domain/backend.rs` | `SearchBackend` trait |
| `domain/fusion.rs` | `FusionSpec`, `RrfFusion` |
| `lance/backend.rs` | `LanceSearchBackend<P>` implementing `SearchBackend` |
| `lance/provider.rs` | `DatasetProvider` trait, `CachingDatasetProvider` |
| `lance/filter.rs` | `filter_to_expr`: domain filter -> DataFusion `Expr` |
| `lance/text.rs` | FTS query node tree -> Lance FTS parameters |
| `grpc/mod.rs` | `SearchGrpc<B>`: tonic service adapter |
| `grpc/convert.rs` | Proto <-> domain conversion |
| `config.rs` | `Config` from environment variables |

### Airflow (`airflow/lance_etl_dag.py`)

Daily DAG (`lance_etl_daily`) running `etl -> index -> compact` as `SparkSubmitOperator` tasks.
Schedule is `@daily`; each task calls `lance-etl <subcommand>` via `LANCE_ETL_CLI`.

---

## Getting started

### Python environment

```bash
# Create and activate a virtual environment
uv venv
source .venv/bin/activate

# Install the package and dev dependencies
uv pip install -e ".[dev]"
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

**Index** — build IVF_RQ, btree, bitmap, and FTS indices:

```bash
lance-etl index \
  --datasets-file /opt/lance/datasets.txt \
  --vector-column vector \
  --num-partitions 256 \
  --scalar-column category \
  --text-column text \
  --fts-with-position \
  --num-shards 64
```

`--dataset-uri` is repeatable; `--datasets-file` reads one URI per line. `--rebuild` reindexes
every fragment; without it only uncovered fragments are indexed (incremental).

**Compact** — run distributed compaction with deferred index remap:

```bash
lance-etl compact \
  --datasets-file /opt/lance/datasets.txt \
  --target-rows-per-fragment 1000000 \
  --max-tasks 256
```

`--no-defer-index-remap` disables the Fragment Reuse Index and pays inline remap cost.
`--no-cleanup` skips version pruning.

Common flags (all subcommands):

| Flag | Default | Purpose |
|---|---|---|
| `--dd-service` | `lance-pipeline` | Datadog service tag |
| `--dd-env` | `prod` | Datadog env tag |
| `--statsd-host` | `localhost` | DogStatsD host |
| `--statsd-port` | `8125` | DogStatsD port |
| `--lance-io-threads` | unset | `LANCE_IO_THREADS`; set to 128-256 for cloud stores |
| `--lance-cpu-threads` | unset | `LANCE_CPU_THREADS`; set below executor core count |
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
LANCE_ETL_BASE_URI="s3://my-bucket/lance/{org_id}.lance" \
  SEARCH_API_PORT=8080 \
  ./target/release/search-api
```

Environment variables:

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `LANCE_ETL_BASE_URI` | yes | — | URI template with `{org_id}` placeholder |
| `SEARCH_API_PORT` | no | `8080` | TCP port |
| `SEARCH_API_DATASET_CACHE_CAPACITY` | no | `1024` | Max open dataset handles |
| `SEARCH_API_INDEX_CACHE_BYTES` | no | `1073741824` | Session index cache budget |
| `SEARCH_API_METADATA_CACHE_BYTES` | no | `268435456` | Session metadata cache budget |

Proto RPCs: `VectorSearch`, `TextSearch`, `HybridSearch` on `lance_etl.search.v1.SearchService`.
Filters are typed AST nodes (`Filter` oneof) — raw SQL strings are never accepted.

### Airflow DAG deployment

Deploy `airflow/lance_etl_dag.py` to your Airflow DAGs folder. Set the Airflow Connection
`spark_default` to point at your Spark cluster. Configure the pipeline via Airflow Variables:

| Variable | Default | Purpose |
|---|---|---|
| `lance_etl_iceberg_table` | `prod.vectors.events` | Fully-qualified Iceberg table name |
| `lance_etl_lance_base_uri` | `s3://my-bucket/lance` | Base URI for Lance datasets |
| `lance_etl_datasets_file` | `/opt/lance/datasets.txt` | File listing dataset URIs for index and compact |
| `lance_etl_spark_conn_id` | `spark_default` | Airflow Spark connection id |
| `lance_etl_executor_instances` | `8` | `spark.executor.instances` |
| `lance_etl_executor_memory` | `8g` | `spark.executor.memory` |
| `lance_etl_dd_service` | `lance-pipeline` | Datadog service tag |
| `lance_etl_dd_tags` | empty | Comma-separated `key:value` constant tags |

The `lance-etl` wheel must be installed on every executor. Either bake it into the cluster image
or ship it via `spark.submit.pyFiles` (see the module docstring in `airflow/lance_etl_dag.py`).
