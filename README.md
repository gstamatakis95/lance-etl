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
MaintenanceJob.run()
  └─ TTL delete (optional)               executor: delete rows where ts + ttl_col < now
  └─ Tier A (small datasets)             executor: Compaction.execute per dataset + cleanup
  └─ Tier B (large datasets)
       └─ Compaction.plan()              driver: build rewrite-task list
       └─ parallelize(tasks).map(...)    executor: CompactionTask.execute
       └─ Compaction.commit              driver: commit rewrites
       └─ cleanup_old_versions           driver: prune old versions (tagged versions exempt)
```

All heavy I/O and compute runs in executors. The driver plans, broadcasts shared artifacts
(IVF centroids, RaBitQ model), and commits. Move-stable row IDs are rejected (see
`docs/adr/0010-stable-row-ids-rejected.md`). V2 manifest paths are on by default, making each of
the 30k dataset opens a single object-store request.

### Rust gRPC search service (`rust/search-api`)

Five-layer design over tonic:

| Layer | Crate path | Responsibility |
|---|---|---|
| `domain` | `crate::domain` | Engine-agnostic types: `Filter` AST, query types, rerank seam, intake record/sink, traits |
| `cache` | `crate::cache` | Disk + Moka index cache and metadata byte cache, plugged into Lance seams |
| `lance` | `crate::lance` | `LanceSearchBackend`, `CachingDatasetProvider`, typed AST -> DataFusion `Expr` |
| `grpc` | `crate::grpc` | Tonic adapters: `SearchGrpc<B>` (search) and `IntakeGrpc<S>` (intake) |
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

Blue-green serving: a `HEAD` tag (or any named tag) is updated atomically with `tags.update`. When
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
| `maintenance.py` | `MaintenanceConfig`, `MaintenanceJob` | Per-row TTL expiration (opt-in), two-tier compaction (small/large), and version cleanup — run in that order per dataset |
| `recall.py` | `RecallAuditJob`, `RecallJobConfig` | Offline recall@k / nDCG@k / MRR audit from Datadog spans |
| `telemetry.py` | `Telemetry`, `TelemetryConfig` | ddtrace spans, DogStatsD, Lance event bridge |
| `cloud_storage.py` | `resolve_filesystem`, `discover_datasets` | pyarrow filesystem + recursive dataset discovery |
| `arrow_types.py` | `resolve_arrow_type`, `resolve_type_map` | Arrow type specs (`fixed_size_list<float32,768>`) |
| `iceberg_optimize.py` | `IcebergOptimizer`, `IcebergOptimizeConfig`, `IcebergOptimizeReport` | Source Iceberg table maintenance via `CALL` procedures (`rewrite_data_files`, `rewrite_manifests`, `expire_snapshots`, opt-in `remove_orphan_files`). Distinct from the Lance maintenance job. |
| `cli.py` | `main`, `build_parser` | Eight subcommands: `etl`, `maintenance`, `index`, `recall`, `tag`, `migrate-manifests`, `migrate-namespace`, `optimize-iceberg` |
| `migrate_namespace.py` | `NamespaceMigrator`, `MigrateConfig` | One-off operator utility to copy a whole namespace to a new namespace name |

### Rust (`rust/search-api/src/`)

| Path | Purpose |
|---|---|
| `domain/filter.rs` | Typed filter AST — no raw SQL accepted anywhere |
| `domain/query.rs` | `VectorQuery`, `TextQuery`, `HybridQuery`, `Hit`, `FusedHit` |
| `domain/backend.rs` | `SearchBackend` trait |
| `domain/prewarm.rs` | `PrewarmSpec`, `PrewarmReport`, `Prewarmer` trait |
| `domain/clusters.rs` | `ClusterSpec`, `ClusterReport`, `ClusterReader` trait |
| `domain/fusion.rs` | `FusionSpec` (Rrf and Weighted variants) and within-dataset fusion logic |
| `domain/rerank.rs` | `Reranker` seam, `IdentityReranker` (no-op default) |
| `domain/intake.rs` | `IntakeBatch`, `Record`, `RecordWrite`, `WriteOp`, `RecordSink` trait, `StdoutSink` placeholder |
| `cache/disk_cache.rs` | Hybrid disk + Moka `CacheBackend` for the Lance index cache |
| `cache/store_cache.rs` | Read-through byte cache for immutable metadata |
| `cache/layout.rs` | Versioned stamp dir, key hashing, atomic writes, TTL/budget sweep |
| `cache/janitor.rs` | Periodic TTL + byte-budget sweep loop |
| `lance/backend.rs` | `LanceSearchBackend<P>` — single-dataset dispatch, post-fusion rerank |
| `lance/provider.rs` | `DatasetProvider` trait, `CachingDatasetProvider`, tag-version TTL cache |
| `lance/filter.rs` | `filter_to_expr`: domain filter -> DataFusion `Expr` |
| `lance/text.rs` | FTS query node tree -> Lance FTS parameters |
| `lance/prewarm.rs` | `Prewarmer` impl over Lance prewarm APIs |
| `lance/index_reader.rs` | IVF centroid extraction, `ClusterReader` impl |
| `grpc/mod.rs` | `SearchGrpc<B>`: tonic search service adapter |
| `grpc/convert.rs` | Proto <-> domain conversion for the search service |
| `grpc/intake.rs` | `IntakeGrpc<S>`: tonic adapter over any `RecordSink` |
| `grpc/intake_convert.rs` | Proto <-> domain conversion for the intake service |
| `telemetry/traces.rs` | OTLP span export, JSON stdout logs with trace correlation |
| `telemetry/metrics.rs` | Typed DogStatsD facade (`search_api.*` prefix, `Rpc` + `IntakeRpc` tag enums) |
| `telemetry/recall.rs` | Deterministic sampled-query capture into `recall.*` span attributes |
| `config.rs` | `Config` from environment variables |

### Airflow (`airflow/lance_etl_dag.py`)

DAG `lance_etl_pipeline` running `etl -> maintenance -> index` as `SparkSubmitOperator` tasks with
`max_active_runs=1`. An optional `optimize-iceberg` task, gated by the `lance_etl_optimize_iceberg_enabled`
Variable (default off), runs before `etl` to maintain the upstream Iceberg source table. Maintenance runs
before indexing so fresh uncovered fragments are merged into large fragments before the index covers them,
avoiding inline remap cost on every index commit.
Schedule is driven by the Airflow Variable `lance_etl_schedule` (default `@daily`).
Data-interval windowing and `dag_run.conf` overrides are described in the module docstring. The
`migrate-namespace` subcommand is a one-off operator tool run manually via the CLI and is not
scheduled here.

### Benchmark package (`bench/`)

End-to-end benchmark driving the real ETL, indexer, compactor, and gRPC server. Install with:

```bash
uv pip install --group bench
```

Run with `python -m bench <subcommand>`. Subcommands: `download`, `prepare`, `ingest`, `index`,
`compact`, `search`, `report`, `all`. Each subcommand accepts the full flag set, so one flag vector
can drive the entire `all` chain.

### Documentation (`docs/`)

- `docs/adr/` — 23 Architecture Decision Records (0001 through 0023) covering distributed indexing,
  two-tier compaction, snapshot-id bounds, dynamic partition routing, gRPC layering, disk cache and
  prewarm, observability and recall audit, compaction/index coexistence, stable-row-id rejection,
  ingested-at column, V2 manifest paths, blue-green serving, by-date partitioning removal,
  CLI and config knob reduction, event-time canonical clock, Rust intake service, TTL expiration,
  namespace migrate utility, map pivot to concrete columns, gRPC event-time range search,
  object-store request tracing, and Iceberg source-table optimization.
- `docs/FINDINGS.md` — narrative companion to the ADRs: verified APIs, production patterns,
  scale design, coexistence results, and open items.
- `docs/datadog-dashboard-guide.md` — guide to the Datadog dashboards shipped with the pipeline.
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
Datadog service) and what to build (partition routing, which index types, the distance metric, and
the FTS base tokenizer and language). Every tuning knob — the schema column names, shuffle
partitions, retry budgets, compaction fragment sizing, IVF training parameters, fine-grained FTS
tokenizer toggles, and two-tier thresholds — is set to an opinionated default in the configuration
dataclasses (`ETLConfig`, `IndexJobConfig`, `MaintenanceConfig`) and stays tunable in
code, not from the command line.

The entry point is installed as `lance-etl`. Subcommands: `etl`, `maintenance`, `index`, `recall`,
`tag`, `migrate-manifests`, `migrate-namespace`, `optimize-iceberg`.

#### `etl` — read a snapshot window from Iceberg and upsert/delete into Lance datasets

```bash
lance-etl etl \
  --table prod.vectors.events \
  --start 2024-01-15T00:00:00 \
  --end 2024-01-16T00:00:00 \
  --base-uri s3://my-bucket/lance \
  --vector-field embedding \
  --text-field body \
  --column-type embedding=fixed_size_list<float16,768> \
  --dd-service lance-pipeline --dd-env prod
```

`--start` / `--end` accept ISO 8601 strings or epoch milliseconds and resolve to Iceberg
snapshot-id bounds. `--column-type` can be repeated for each column needing a type cast. The named
vectors and texts in the source `vectors` / `texts` map columns are pivoted into concrete indexable
columns: each `--vector-field` becomes a fixed-size-list column the IVF_RQ index can target (pair it
with a matching `--column-type` cast) and each `--text-field` becomes a string column the INVERTED
index can target. Undeclared map keys are dropped, a key absent from a row yields NULL, and the
`metadata` map stays stored-only payload flattened into `metadata_keys` / `metadata_values` arrays.

Partition routing:

```bash
lance-etl etl ... \
  --partition-by org_id,tenant_id,namespace
```

`--partition-by` (default `org_id,tenant_id,namespace`) sets the columns that build the dataset
path `base_uri/<val1>/.../<valN>.lance`. Every column must exist in the source table. Each key
lives in exactly one dataset, so the per-dataset `merge_insert` is the sole dedup mechanism.

Window pushdown filter (applied after the Iceberg read):

```bash
lance-etl etl ... \
  --window-start 2024-01-15T06:00:00 \
  --window-end 2024-01-15T12:00:00
```

Full `etl` flag reference:

| Flag | Default | Purpose |
|---|---|---|
| `--table` | (required) | Fully-qualified Iceberg table name |
| `--start` | (required) | Iceberg snapshot window start (ISO 8601 or epoch ms) |
| `--end` | (required) | Iceberg snapshot window end (ISO 8601 or epoch ms) |
| `--base-uri` | (required) | Root URI for per-tenant Lance datasets |
| `--partition-by` | `org_id,tenant_id,namespace` | Comma-separated routing columns |
| `--column-type` | none | Repeatable `name=arrow_type` cast |
| `--iceberg-option` | none | Repeatable `key=value` Iceberg read option |
| `--window-start` | none | Inclusive lower bound for the window pushdown filter |
| `--window-end` | none | Exclusive upper bound for the window pushdown filter |
| `--storage-option` | none | Repeatable `key=value` passed to pylance |
| `--dd-service` | `lance-pipeline` | Datadog service tag |
| `--dd-env` | `prod` | Datadog env tag |
| `--dd-version` | empty | Datadog version tag |
| `--dd-tag` | none | Repeatable constant `key=value` Datadog tag |

#### `maintenance` — TTL expiration, distributed compaction, and version cleanup

```bash
lance-etl maintenance \
  --base-uri s3://my-bucket/lance \
  --dd-service lance-pipeline --dd-env prod
```

Runs four ordered steps per dataset: a cheap single-org data-quality guard (when `--base-uri` is set),
per-row TTL expiration (when `--ttl-column` is set), two-tier distributed compaction, and version cleanup.
The DQ guard calls `dataset.count_rows(filter=predicate)` over only the partition columns to confirm each
dataset contains rows for only its own routing key. TTL deletes expired rows before compaction so the
compaction reclaims that storage.

Dataset selection: `--dataset-uri` (repeatable), `--datasets-file`, or `--base-uri` (discovers all
`*.lance` paths recursively). All tuning knobs use opinionated defaults from `MaintenanceConfig`.

DQ guard flags (require `--base-uri`):

| Flag | Default | Purpose |
|---|---|---|
| `--no-verify-single-org` | off (guard on) | Disable the single-org DQ guard. The guard requires `--base-uri` to derive expected routing values from each dataset URI. |
| `--raise-on-contamination` | off | Raise an error when contamination is found instead of logging and continuing. |

TTL flags:

| Flag | Default | Purpose |
|---|---|---|
| `--ttl-column` | none (TTL off) | Per-row TTL column holding each row's lifetime as an Arrow `Duration`. When set, rows are deleted before compaction by the predicate `ts_column + ttl_column < now`. Absent means TTL is off. |
| `--ts-column` | `timestamp` | Event timestamp column used as the TTL clock. Must match `ETLConfig.ts_col`. Only consulted when `--ttl-column` is set. |

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

Updates a serving tag (default `HEAD`) to a target dataset version. Tagged versions are exempt from
version cleanup.

```bash
lance-etl tag \
  --base-uri s3://my-bucket/lance \
  --tag HEAD \
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
| `--tag` | `HEAD` | Serving tag name to update |
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

#### `migrate-namespace` — copy a whole namespace to a new name

```bash
lance-etl migrate-namespace \
  --source-namespace legacy \
  --target-namespace v2 \
  --base-uri s3://my-bucket/lance \
  --vector-column vector --metric cosine \
  --dd-service lance-pipeline --dd-env prod
```

Copies every dataset whose namespace component equals `--source-namespace` to the same address with
the namespace component replaced by `--target-namespace`. Source datasets are never deleted, so an
operator can verify the new namespace and flip serving through the blue-green tag helpers before
removing the source. Each target is recompacted and reindexed in production pipeline order after
copying. This is a one-off operator tool and is not scheduled in the Airflow DAG.

| Flag | Default | Purpose |
|---|---|---|
| `--source-namespace` | (required) | Namespace value to copy from |
| `--target-namespace` | (required) | Namespace value to copy to |
| `--base-uri` | (required) | Root URI under which per-tenant datasets live |
| `--partition-by` | `org_id,tenant_id,namespace` | Partition columns building the dataset path |
| `--no-recompact` | off | Skip compaction of target datasets after copying |
| `--no-reindex` | off | Skip index rebuild on target datasets after copying |
| `--overwrite-target` | off | Allow overwriting target datasets that already exist |

Index column flags (`--vector-column`, `--scalar-column`, `--bitmap-column`, `--text-column`,
`--metric`, `--fts-with-position`, `--fts-base-tokenizer`, `--fts-language`) are shared with the
`index` subcommand and are optional. When none are given, reindexing is skipped with a warning.

#### `optimize-iceberg` — optimize the upstream Iceberg source table

Runs Iceberg's own table maintenance procedures on the source Iceberg table, which is a separate
store from the Lance datasets maintained by the `maintenance` subcommand.

```bash
lance-etl optimize-iceberg \
  --table prod.vectors.events \
  --dd-service lance-pipeline --dd-env prod
```

Four steps run in a fixed safe order. `rewrite_data_files` bin-packs small data files into larger
ones (default on). `rewrite_manifests` rewrites the manifest list to align with the new file layout
(default on, runs after rewrite to be consistent). `expire_snapshots` prunes snapshot history
beyond a retention horizon — at least the last 5 snapshots are always kept regardless of age, and
snapshots older than 7 days beyond that count are expired (default on). `remove_orphan_files`
deletes files no live snapshot references — opt-in because it is the only step that can delete data
files outright. Iceberg's own three-day safety horizon is respected so an in-flight write is never
mistaken for an orphan.

Each step is wrapped with telemetry timing and a metric. Heavy work runs distributed in Spark.

| Flag | Default | Purpose |
|---|---|---|
| `--table` | (required) | Fully-qualified Iceberg source table: `catalog.namespace.table` |
| `--no-rewrite-data-files` | off | Skip the bin-pack rewrite of small data files |
| `--no-rewrite-manifests` | off | Skip the manifest rewrite |
| `--no-expire-snapshots` | off | Skip snapshot-history expiration |
| `--remove-orphan-files` | off (opt-in) | Delete files no live snapshot references |
| `--expire-retain-last` | `5` | Snapshots always retained regardless of age |
| `--expire-older-than-days` | `7` | Age horizon in days for snapshot expiration |

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
| `SEARCH_API_DISK_CACHE_DISABLED` | `false` | Set to `true` for pure in-memory fallback |
| `SEARCH_API_PREWARM_CONCURRENCY` | `4` | Indexes warmed concurrently per Prewarm RPC |
| `SEARCH_API_IO_CONCURRENCY` | `256` | Parallel in-flight object-store requests per dataset |
| `SEARCH_API_RECALL_SAMPLE_RATE` | `0.0` (off) | Fraction of requests sampled for offline recall |
| `SEARCH_API_SERVE_BY_TAG` | `false` | Resolve the serve tag instead of opening latest |
| `SEARCH_API_SERVE_TAG` | `HEAD` | Tag name resolved when `SEARCH_API_SERVE_BY_TAG=true` |
| `SEARCH_API_SERVE_TAG_TTL_SECS` | `10` | Seconds a resolved tag version is trusted |
| `SEARCH_API_EVENT_TIMESTAMP_COLUMN` | `event_timestamp` | Column that request `TimeRange` filters are applied to |
| `SEARCH_API_STATSD_ADDR` | `127.0.0.1:8125` | DogStatsD UDP address (honors `DD_AGENT_HOST`) |
| `SEARCH_API_TELEMETRY_DISABLED` | `false` | Disable trace export and DogStatsD (JSON logs only) |

`DD_AGENT_HOST` is read by the default statsd address resolver: when set, the default becomes
`${DD_AGENT_HOST}:8125`. `SEARCH_API_STATSD_ADDR` overrides it unconditionally.

Both services live in one proto file, `proto/lance_etl/v1/lance_etl.proto` (package `lance_etl.v1`),
and share the `DatasetTarget` message.

Proto RPCs on `lance_etl.v1.SearchService`:

| RPC | Key request fields | Purpose |
|---|---|---|
| `VectorSearch` | `target`, `query`, `rerank`, `time_range` | Nearest-neighbor search with optional rerank and optional event-time window |
| `TextSearch` | `target`, `query`, `rerank`, `time_range` | BM25 full-text search with optional rerank and optional event-time window |
| `HybridSearch` | `target`, `vector`, `text`, `k`, `fusion`, `rerank`, `time_range` | Fused vector + text (RRF or weighted) with optional event-time window |
| `Prewarm` | `target`, `metadata`, `all_indexes`, `index_names`, `version`/`tag` | Pull caches at a version or tag |
| `Clusters` | `target`, `index_name` | Read IVF centroid vectors of the vector index |

All requests carry a `DatasetTarget` (`org_id`, `tenant_id`, `namespace`), which resolves to the
single dataset at `{base}/{org}/{tenant}/{namespace}.lance`. Filters are typed AST nodes (`Filter`
oneof) — raw SQL strings are never accepted. Event-time windowing is expressed as an optional
`TimeRange { optional int64 start_ms; optional int64 end_ms }` (epoch milliseconds, start
inclusive, end exclusive, either bound optional). The window always applies to the event-timestamp
column (name from `SEARCH_API_EVENT_TIMESTAMP_COLUMN`, default `event_timestamp`) and is
translated to a typed range predicate ANDed with any `Filter`, pruned by a BTREE or zone-map on
that column. A `TimeRange` absent from the request leaves every search path behaving exactly as
before.

Proto RPCs on `lance_etl.v1.IntakeService`:

| RPC | Streaming | Key request fields | Purpose |
|---|---|---|---|
| `Write` | unary | `target`, `writes[]` | Apply one batch of record writes (UPSERT or DELETE) to a single dataset |
| `WriteStream` | client-streaming | `target`, `writes[]` per message | High-throughput stream of record-write batches. Returns one aggregated response on half-close. |

Each `RecordWrite` carries an `op` (`WriteOp`: UPSERT or DELETE) and a `Record`. A `Record` contains
a string `id`, an `event_timestamp_ms` (epoch milliseconds — the canonical ETL clock, no separate
ingestion timestamp), a `metadata` string map, a `vectors` map of named fixed-dimension float arrays
(one per vector column), and a `texts` map of named text fields (one per FTS column). The dataset
`target` on the request names `org_id`, `tenant_id`, and `namespace` and is never duplicated onto
individual records. The `WriteRecordsResponse` returns only record ids: `succeeded_ids` for records
the sink accepted and `failed_ids` for records that failed validation or sink acceptance. A record
whose id is itself empty or invalid cannot be reported by id and is omitted from `failed_ids`.
Validated batches are handed to a `RecordSink`. The only shipped sink is `StdoutSink` (a
structured-print placeholder). A future `KafkaSink` implements the same `RecordSink` trait and
replaces it at the construction site in `main` without changing the proto, transport, or domain
types.

The `Prewarm` RPC accepts `version` (explicit committed version id) or `tag` (resolves the named
tag at call time) and returns `resolved_version`, enabling the safe green-before-flip workflow.

Fusion: `RrfFusion` (default, reciprocal-rank fusion with configurable `rrf_k`) or `WeightedFusion`
(min-max normalized legs combined by `vector_weight`). Post-fusion reranking: `IdentityRerank`
(no-op identity, with optional `top_n` truncation) is the only shipped strategy and is the seam
where a cross-encoder or LLM reranker slots in without changing the request shape.

### Airflow DAG deployment

Deploy `airflow/lance_etl_dag.py` to your Airflow DAGs folder. Set the Airflow Connection
`spark_default` to point at your Spark cluster. Pipeline order is `etl >> maintenance >> index` with
`max_active_runs=1`.

Configure via Airflow Variables:

| Variable | Default | Purpose |
|---|---|---|
| `lance_etl_schedule` | `@daily` | Airflow schedule expression |
| `lance_etl_iceberg_table` | `prod.vectors.events` | Fully-qualified Iceberg table name |
| `lance_etl_lance_base_uri` | `s3://my-bucket/lance` | Base URI for Lance datasets |
| `lance_etl_datasets_file` | `/opt/lance/datasets.txt` | File listing dataset URIs for `index` and `maintenance` |
| `lance_etl_index_flags` | empty | Shell-tokenized index column-selection flags for the `index` step |
| `lance_etl_spark_conn_id` | `spark_default` | Airflow Spark connection id |
| `lance_etl_executor_instances` | `8` | `spark.executor.instances` |
| `lance_etl_executor_memory` | `8g` | `spark.executor.memory` |
| `lance_etl_driver_memory` | `4g` | `spark.driver.memory` |
| `lance_etl_spark_conf_overrides` | `{}` | JSON object of extra Spark conf key/value pairs |
| `lance_etl_dd_service` | `lance-pipeline` | Datadog service tag |
| `lance_etl_dd_env` | `prod` | Datadog env tag |
| `lance_etl_dd_tags` | empty | Comma-separated `key:value` constant tags |
| `lance_etl_partition_by` | empty | Comma-separated partition columns for `--partition-by` |
| `lance_etl_ttl_column` | empty (TTL off) | Per-row TTL column name forwarded to the `maintenance` step as `--ttl-column`. When set, the column must hold each row's lifetime as an Arrow `Duration`. Rows are expired before compaction by `ts_column + ttl_column < now`. Absent means TTL is off. |
| `lance_etl_optimize_iceberg_enabled` | `false` | When truthy (`true`/`1`/`yes`), adds an optional `optimize-iceberg` task before `etl` that runs Iceberg's own source-table maintenance procedures (`rewrite_data_files`, `rewrite_manifests`, `expire_snapshots`). This is source-table maintenance and is distinct from the Lance `maintenance` task. |
| `lance_etl_optimize_remove_orphan_files` | `false` | When truthy, the `optimize-iceberg` task also runs the destructive `remove_orphan_files` procedure. Only files older than Iceberg's three-day safety horizon are removed. Opt-in because this step can delete data files outright. |

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
