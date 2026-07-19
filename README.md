# lance-etl

`lance-etl` synchronizes one local Iceberg table into independently published Lance datasets.
PostgreSQL is the durable control plane. One Python reconciler process plans exact Iceberg snapshot
transitions, runs bounded work through a local Spark session, and publishes exact Lance versions.
The optional Rust gRPC process reads the same PostgreSQL catalog for vector, full-text, and hybrid
search.

Each validated `(tenant_id, namespace, org_id)` route owns one Lance dataset. Search never fans a
request across datasets or organizations.

## Local architecture

```text
Iceberg table
  -> local reconciler and local Spark executors
  -> replay-safe Lance mutation, compaction, and indexing
  -> PostgreSQL publication evidence and active pointer
  -> optional local search-api process
```

PostgreSQL stores the source registration, immutable dataset specifications, exact snapshot
lineage, durable dataset work, publication evidence, and serving pointer. A work row freezes the
specification revision used for the run. Dataset specifications contain the field projection plus
all ingestion, compaction, indexing, prewarm, and retention options. Environment variables are
limited to process bootstrap, local paths, PostgreSQL connectivity, Spark startup, and telemetry.

Spark is an execution dependency created and stopped by the local process. There is no external
scheduler or remote Spark submission layer.

Specification changes use one repository lifecycle: create a named spec, persist a complete
normalized `DRAFT`, activate it, select that spec as a source default, and assign its active
revision to existing datasets. Activation retires the former active revision. PostgreSQL prevents
changes to every parent, field, index, and option row after activation. Assigning a revision to an
already materialized dataset creates one deterministic replay-safe `REBUILD` work item.

The claim records a `launcher_kind` audit label on `dataset_work`. It does not affect ordering,
eligibility, retries, or fencing.

## Repository layout

```text
lance-etl/
  src/lance_etl/     Python reconciler, ETL, indexing, maintenance, and control-plane code
  rust/search-api/   Optional Rust gRPC search service
  migrations/        Alembic migrations for the local PostgreSQL control plane
  tests/             Python unit and integration tests
  bench/             Local end-to-end benchmark package
  docs/adr/          Architecture decisions
  compose.yaml       Disposable local PostgreSQL service
```

## Start locally

Create the locked Python environment:

```bash
uv sync --locked --group dev --python 3.14.0
```

Start the included local PostgreSQL service:

```bash
docker compose up -d postgres
export LANCE_ETL_DATABASE_URL='postgresql+psycopg://lance_etl:lance_etl@localhost:5432/lance_etl'
uv run lance-etl-reconcile migrate
```

Or use an existing local PostgreSQL installation:

```bash
createdb lance_etl
export LANCE_ETL_DATABASE_URL='postgresql+psycopg://localhost/lance_etl'
uv run lance-etl-reconcile migrate
```

Choose local storage and the catalog-qualified Iceberg table:

```bash
mkdir -p var/lance
export LANCE_ETL_LANCE_BASE_URI="${PWD}/var/lance"
export LANCE_ETL_SOURCE_TABLE='local.vectors.events'
export LANCE_ETL_SPARK_WAREHOUSE="${PWD}/var/iceberg"
```

The Iceberg table must already exist with the route, mutation, payload-map, `ts`, and partition
contract shown in [the local runbook](docs/production-release.md#prepare-the-iceberg-source).

The defaults use a Hadoop Iceberg catalog named `local`, a warehouse under `.lance-etl/iceberg`,
and Spark `local[*]`. Set `LANCE_ETL_CANONICAL_BASELINE_SNAPSHOT_ID` when adopting an existing
Iceberg table. On the first run, the process registers the source and binds it to the bundled active
dataset specification. Later runs load that registration from PostgreSQL and reject bootstrap
values that drift from it.

Run one complete bounded pass:

```bash
uv run lance-etl-reconcile run-once
```

Run the same pass continuously with a local polling interval:

```bash
uv run lance-etl-reconcile run
```

The process reads loop, lease, retry, SLO, and cleanup bounds from environment variables into
`ReconcilerSettings` once at startup. Restart it after changing any of those environment values.

Inspect the durable queue and retention state:

```bash
uv run lance-etl-reconcile status
```

The single Alembic baseline creates exactly 9 application tables. Their responsibilities and
every stored option are documented in
[docs/adr/postgresql-dataset-control-plane.md](docs/adr/postgresql-dataset-control-plane.md).
See [docs/production-release.md](docs/production-release.md) for the local runbook.

## Search service

The Rust service is optional. The reconciler verifies a candidate Lance version locally before
publication. Build and test the search process independently:

```bash
cd rust/search-api
cargo build --release --locked
cargo test --locked
```

Its PostgreSQL catalog, local cache, and RPC configuration are documented in
[rust/search-api/README.md](rust/search-api/README.md). It is not required to run the reconciler
locally.

## Benchmarks

```bash
uv sync --locked --group dev --group bench --python 3.14.0
uv run python -m bench e2e --dataset sift1m
```

The benchmark uses local Spark and file-backed datasets by default. Its subcommands and artifacts
are documented in [bench/README.md](bench/README.md).

## Tests

Run formatting, linting, and the regular Python suite:

```bash
uvx ruff format src/ tests/ bench/ migrations/
uvx ruff check src/ tests/ bench/ migrations/
.venv/bin/pytest -m "not integration"
```

PostgreSQL tests create and remove isolated schemas. Point them at a disposable local database:

```bash
createdb lance_etl_test
LANCE_ETL_TEST_DATABASE_URL='postgresql+psycopg://localhost/lance_etl_test' \
  .venv/bin/pytest tests/test_state_postgres.py tests/test_postgres_queue_load.py
```

Run the complete local Iceberg to PostgreSQL to Lance path in its own process so Spark can load the
Iceberg runtime at JVM startup:

```bash
LANCE_ETL_TEST_DATABASE_URL='postgresql+psycopg://localhost/lance_etl_test' \
  .venv/bin/pytest tests/test_local_e2e.py -x -q -m integration
```

Run the standalone Iceberg maintenance integration in a separate process for the same reason:

```bash
.venv/bin/pytest tests/test_iceberg_optimize.py -x -q -m integration
```

For contributor rules, read [AGENTS.md](AGENTS.md) and the package-specific guides before changing
code.
