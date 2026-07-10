# lance-etl

PySpark ETL pipeline that ingests embeddings and text from an Apache Iceberg table into per-org
Lance vector datasets, builds distributed IVF_RQ / scalar / FTS indices, and compacts them with
version cleanup. A companion Rust gRPC service (tonic) serves vector, full-text, and hybrid search
over the same datasets.

Scale target: up to 1 billion vectors spread across up to 30,000 organisations with a power-law
size distribution. One Lance dataset per `org_id/tenant_id/namespace`. There is no cross-org or
cross-dataset query surface anywhere in the system.

This top-level README is the map. The detail lives in the per-directory guides linked below.

---

## Architecture overview

### ETL tier (Python / PySpark)

The Spark jobs under `src/lance_etl/` produce the Lance datasets and their indices. Every job has
the same shape: the driver plans the rounds and commits, and all heavy I/O and compute runs in
executors. Every dataset size follows the same task shape — a small dataset is simply the one-task
case.

```
Iceberg table
  └─ IcebergToLanceETL.run()              driver: resolve snapshot bounds, split key-hash batches
       └─ mapInArrow(merge_partition)     executor: pivot + merge_insert upsert/delete per dataset
       └─ stamp_interval_tags             executors: hour tag on every written dataset
LanceIndexer.run()                        rounds of plan -> artifacts -> build -> commit
  └─ plan_dataset_indexes                 executor fan-out: role discovery + shard specs
  └─ bootstrap_vector_index               executor: committed create_index, streaming k-means
  └─ build_one_shard                      ONE flat Spark job: vector/scalar/FTS segments
  └─ commit_one_index                     executor fan-out: merge (vector) + publish
MaintenanceJob.run()                      rounds of plan -> execute -> commit
  └─ plan_one_dataset                     executor fan-out: TTL delete + Compaction.plan
  └─ execute_rewrite_task                 ONE flat Spark job: CompactionTask.execute
  └─ commit_one_dataset                   executor fan-out: Compaction.commit + cleanup
```

The unified pipeline job serializes the fleet phases `prune -> maintenance -> index -> stamp`.
Move-stable row IDs are rejected (`docs/adr/rejected-and-operator-tools.md`, ADR 0010). V2 manifest
paths are on by default, making each of the 30k dataset opens a single object-store request.
Serving is tag-based: a `HEAD` (or named) tag points at a concrete version, and the ETL stamps
hourly interval tags that a query can pin. The full job-by-job guide is in
`src/lance_etl/README.md`.

### Rust gRPC search service (`rust/search-api`)

Five-layer design over tonic, each layer with a one-directional dependency on the layer below:

| Layer | Responsibility |
|---|---|
| `domain` | Engine-agnostic types: `Filter` AST, query types, intake record/sink, traits |
| `cache` | Hybrid Moka + pluggable persistent (disk/redis) caches, plugged into Lance seams |
| `lance` | `LanceSearchBackend`, `CachingDatasetProvider`, typed AST -> DataFusion `Expr` |
| `grpc` | Tonic adapters: `SearchGrpc<B>` (search) and `IntakeGrpc<S>` (intake) |
| `telemetry` | OTLP traces, DogStatsD metrics, JSON logs with trace correlation |

Filters are a typed AST — raw SQL is never accepted or constructed. Every request carries exactly
one `DatasetTarget` (`org_id`, `tenant_id`, `namespace`). A hybrid persistent cache extends the
in-process session caches beyond the process (local disk by default or shared Redis) and never
caches raw row data. Blue-green flips go through a named tag, warmed before the flip. The full
service guide — RPCs, environment variables, invariants, and observability — is in
`rust/search-api/README.md`.

---

## Repository layout

```
lance-etl/
  src/lance_etl/     Python package: Spark ETL, indexing, maintenance, pipeline, tools, recall   (README.md, AGENTS.md)
  rust/search-api/   Rust gRPC search service: tonic transport over the Lance crate               (README.md, AGENTS.md)
  bench/             End-to-end benchmark driving the real ETL, indexer, compactor, and server    (bench/README.md)
  airflow/           Two Airflow DAGs: the ETL DAG and the unified pipeline DAG
  tests/             pytest suite
  docs/adr/          Architecture decisions, six thematic documents plus a numbered index          (docs/adr/README.md)
  market-research/   Evaluation notes, plans, and evidence underlying the ADRs
  pyproject.toml     Build, dependencies, ruff config
```

---

## Getting started

### Python environment

```bash
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
uv pip install --group bench
```

The project requires `pylance>=8.0.0`, which installs from PyPI, so a plain
`uv pip install -e ".[dev]"` suffices. The Rust service sources the lance crates from crates.io at
the same version.

### Running the jobs

Five installed entry points, one per job: `lance-etl-etl`, `lance-etl-index`,
`lance-etl-maintenance`, `lance-etl-pipeline`, and `lance-etl-tools`. Each is also runnable as
`python -m lance_etl.<pkg>`. The command reference (CLI flags, the recall/tag/migrate operator
tools, and the Airflow deployment variables) is in `src/lance_etl/README.md`.

### Running the gRPC search service

```bash
cd rust/search-api
cargo build --release
LANCE_ETL_BASE_URI=s3://my-bucket/lance \
  SEARCH_API_PORT=8080 \
  ./target/release/search-api
```

`LANCE_ETL_BASE_URI` is the only required variable. The full environment-variable table, the proto
RPC surface, and the caching and observability behavior are in `rust/search-api/README.md`.

### Running the benchmark

```bash
uv pip install --group bench
python -m bench e2e --dataset sift1m
```

The benchmark drives the real ETL, indexer, compactor, and gRPC server end to end. Subcommands,
flags, and the agent-driveable `experiment` iteration are documented in `bench/README.md`.

### Running the tests

```bash
# Python tests
.venv/bin/pytest

# Rust tests
cd rust/search-api
cargo test
```

---

## Documentation

- `docs/adr/README.md` — the architecture decisions, consolidated into six thematic documents (ETL
  and data model, fleet orchestration and maintenance, indexing, serving and tags, caching and
  observability, rejected decisions and operator tools). Every original ADR number resolves through
  the index.
- `docs/datadog-dashboard-guide.md` — guide to the Datadog dashboards shipped with the pipeline.
- `market-research/` — detailed evaluation notes, plans, and evidence underlying the ADRs.

For contributors and coding agents, the repo-wide rules are in `AGENTS.md`, with package specifics
in `src/lance_etl/AGENTS.md` and `rust/search-api/AGENTS.md`.
