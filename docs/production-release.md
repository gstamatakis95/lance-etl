# Local operation and release runbook

This project runs as local processes. PostgreSQL is the durable control plane. The Python
reconciler creates and stops a local Spark session. The Rust search service is optional and starts
directly when local search is needed.

## Prerequisites

- Python 3.14 and `uv`
- Java compatible with the pinned PySpark release
- Docker Compose or local PostgreSQL with `createdb`, `psql`, and `pg_isready`
- `protoc` and Rust when running the optional search process
- enough local storage for Iceberg, Lance, Spark scratch data, and index artifacts

Install the locked Python environment:

```bash
uv sync --locked --group dev --python 3.14.0
```

## Create PostgreSQL

The included Compose file starts only PostgreSQL:

```bash
docker compose up -d postgres
export LANCE_ETL_DATABASE_URL='postgresql+psycopg://lance_etl:lance_etl@localhost:5432/lance_etl'
export PGHOST='localhost' PGPORT='5432' PGUSER='lance_etl' PGPASSWORD='lance_etl' PGDATABASE='lance_etl'
docker compose ps
```

Or use an existing local installation:

```bash
createdb lance_etl
export LANCE_ETL_DATABASE_URL='postgresql+psycopg://localhost/lance_etl'
pg_isready
```

Apply the single Alembic baseline through the installed command:

```bash
uv run lance-etl-reconcile migrate
```

The migration creates exactly 14 application tables and seeds the singleton reconciler settings
plus the bundled active dataset specification.

Verify the table set:

```bash
psql lance_etl -Atc \
  "SELECT tablename FROM pg_tables WHERE schemaname = current_schema() ORDER BY tablename"
```

Expected application tables:

```text
dataset_fields
dataset_publications
dataset_spec_revisions
dataset_specs
dataset_state
dataset_work
datasets
fts_index_options
iceberg_sources
index_definitions
publication_indexes
reconciler_settings
source_snapshots
vector_index_options
```

`alembic_version` is Alembic metadata and is not one of the 14 application entities.

## Configure local paths

```bash
mkdir -p .lance-etl/lance .lance-etl/iceberg
export LANCE_ETL_LANCE_BASE_URI="$PWD/.lance-etl/lance"
export LANCE_ETL_SOURCE_TABLE='local.db.events'
export LANCE_ETL_SPARK_WAREHOUSE="$PWD/.lance-etl/iceberg"
export LANCE_ETL_SPARK_MASTER='local[*]'
```

The first run reads the actual Iceberg table UUID and creates the `iceberg_sources` registration.
If the table already contains history, also set the exact retained baseline:

```bash
export LANCE_ETL_CANONICAL_BASELINE_SNAPSHOT_ID='123456789'
```

PostgreSQL becomes authoritative after registration. A later change to the table identity, source
mapping, storage namespace, or baseline is rejected instead of silently changing an existing
source.

## Prepare the Iceberg source

The source table must exist before the first reconciliation cycle. The bundled specification uses
this local Iceberg contract:

```sql
CREATE NAMESPACE IF NOT EXISTS local.db;

CREATE TABLE local.db.events (
    tenant_id STRING NOT NULL,
    namespace STRING NOT NULL,
    org_id STRING NOT NULL,
    vector_id STRING NOT NULL,
    op STRING NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    processing_timestamp TIMESTAMP NOT NULL,
    vectors MAP<STRING, ARRAY<FLOAT>> NOT NULL,
    texts MAP<STRING, STRING> NOT NULL,
    metadata MAP<STRING, STRING> NOT NULL,
    ttl BIGINT
) USING iceberg
PARTITIONED BY (tenant_id, namespace, org_id, hours(processing_timestamp))
TBLPROPERTIES ('format-version' = '2');
```

Mutation values are `insert`, `update`, `upsert`, `delete`, `i`, `u`, or `d`, matched without case
sensitivity. Map keys must match the active `dataset_fields` contract. Every non-delete row must
carry each configured vector at its exact dimension. `ttl` is seconds and is required as a source
column while the active specification includes the TTL field.

## Inspect the dataset specification

The bundled active revision is intentionally relational. Inspect it before the first substantial
run:

```bash
psql lance_etl -c \
  "SELECT s.name, r.revision_number, r.state, encode(r.configuration_digest, 'hex') AS digest
   FROM dataset_specs AS s
   JOIN dataset_spec_revisions AS r USING (spec_id)
   ORDER BY s.name, r.revision_number"
```

Inspect ingestion, compaction, index maintenance, and publication policy:

```bash
psql lance_etl -x -c \
  "SELECT ingest_shuffle_partitions, merge_rows_per_chunk, merge_batch_bytes,
          write_rows_per_fragment, compaction_enabled, compaction_mode,
          target_rows_per_fragment, max_source_fragments, compaction_threads,
          defer_index_remap, materialize_deletions, materialize_deletions_threshold,
          cleanup_older_than_seconds, retain_versions, fragments_per_index_task,
          max_index_deltas, max_stale_replans, prewarm_required,
          retained_publications, artifact_retention_seconds
   FROM dataset_spec_revisions WHERE state = 'ACTIVE'"
```

Field and index configuration is under `dataset_fields`, `index_definitions`,
`vector_index_options`, and `fts_index_options`. Do not edit an active revision in place. Insert and
validate a complete DRAFT through `ControlPlaneRepository.create_draft_spec_revision`, promote it
with `activate_spec_revision`, and assign existing datasets with `assign_dataset_spec_revision`.
Use `set_source_default_spec` to select the named spec for newly discovered datasets. PostgreSQL
freezes ACTIVE and RETIRED graphs. Assignment creates one deterministic REBUILD when the existing
materialization carries another revision.

Airflow is optional. If a local launch supplies `AIRFLOW_CTX_DAG_ID`, `AIRFLOW_CTX_DAG_RUN_ID`,
`AIRFLOW_CTX_TASK_ID`, `AIRFLOW_CTX_MAP_INDEX`, and `AIRFLOW_CTX_TRY_NUMBER`, claims persist them on
`dataset_work` as audit provenance. They do not affect work order or eligibility.

## Run one cycle

```bash
uv run lance-etl-reconcile run-once
```

The command:

1. validates the registered source and next direct-parent snapshot
2. persists source evidence and deterministic dataset work
3. claims bounded work with dataset fencing
4. runs ingestion, compaction, indexing, validation, and local prewarm
5. appends qualified publications and moves active pointers atomically
6. performs bounded retention

Run continuously when desired:

```bash
uv run lance-etl-reconcile run
```

Override only the local sleep interval for an interactive run:

```bash
uv run lance-etl-reconcile run --poll-seconds 10
```

The durable polling, claim, lease, retry, SLO, and cleanup defaults remain in
`reconciler_settings`.

## Status checks

```bash
uv run lance-etl-reconcile status
```

Inspect open work directly:

```bash
psql lance_etl -x -c \
  "SELECT work_id, dataset_id, kind, phase, state, attempt_count,
          launcher_kind, airflow_ctx_dag_id, airflow_ctx_dag_run_id,
          airflow_ctx_task_id, airflow_ctx_map_index, airflow_ctx_try_number,
          next_attempt_at, lease_expires_at, error_code
   FROM dataset_work
   WHERE state <> 'SUCCEEDED'
   ORDER BY next_attempt_at, created_at"
```

Inspect source progress:

```bash
psql lance_etl -x -c \
  "SELECT source_snapshot_seq, snapshot_id, parent_snapshot_id,
          iceberg_sequence_number, kind, state, error_code, error_message
   FROM source_snapshots
   ORDER BY source_snapshot_seq DESC LIMIT 20"
```

Inspect serving routes:

```bash
psql lance_etl -x -c \
  "SELECT d.tenant_id, d.namespace, d.org_id,
          p.lance_uri, p.lance_version, p.published_at
   FROM datasets AS d
   JOIN dataset_state AS st USING (dataset_id)
   JOIN dataset_publications AS p
     ON p.dataset_id = st.dataset_id
    AND p.publication_id = st.active_publication_id
   ORDER BY d.tenant_id, d.namespace, d.org_id"
```

## Failure recovery

### Retryable work

Retryable failures enter `RETRY_WAIT` with bounded exponential backoff. The same work ID is reused
and `attempt_count` increases. Expired running work becomes eligible for a fresh token and higher
fence. Stale executors cannot complete after losing their token or dataset fence.

### Blocked work

Diagnose the stored error before reopening one explicit work row:

```bash
uv run lance-etl-reconcile repair \
  --action retry-blocked \
  --work-id WORK_UUID \
  --dry-run

uv run lance-etl-reconcile repair \
  --action retry-blocked \
  --work-id WORK_UUID
```

### Rebuild

Enqueue a deterministic rebuild for one route. `request-id` makes repeated operator invocation
idempotent:

```bash
uv run lance-etl-reconcile repair \
  --action rebuild \
  --tenant-id TENANT \
  --namespace NAMESPACE \
  --org-id ORG \
  --request-id REQUEST_UUID \
  --dry-run
```

Remove `--dry-run` after confirming the route and request identity. Repair never edits the active
publication pointer directly.

### Crash after a Lance commit

The reconciler compares PostgreSQL expected state with the Lance completion marker. A matching
marker reconciles the result without replaying rows. A mismatched digest blocks rather than
guessing.

## Start local search

Search is optional. Build it once:

```bash
cd rust/search-api
cargo build --locked
```

Run it against the same database and Lance namespace:

```bash
SEARCH_API_LOCAL_MODE=true \
LANCE_ETL_BASE_URI="$PWD/../../.lance-etl/lance" \
LANCE_ETL_DATABASE_URL='postgresql://lance_etl:lance_etl@localhost/lance_etl' \
SEARCH_API_TELEMETRY_DISABLED=true \
cargo run --locked
```

The service resolves the active URI and exact version from PostgreSQL. Search listens only on
loopback in local mode.

## Retention and cleanup

Retention is conservative. It protects:

- every active publication
- candidates referenced by open work
- the source replay floor required by unfinished snapshots
- the historical publication count required by the frozen dataset specification
- evidence younger than artifact and audit horizons

Lance version cleanup must never use a zero horizon or unverified deletion while concurrent writers
may exist. Do not reuse a retired dataset URI for unrelated content without clearing every URI-keyed
cache generation.

## Validation before release

```bash
uvx ruff format src/ tests/ bench/ migrations/
uvx ruff check src/ tests/ bench/ migrations/
.venv/bin/pytest -m "not integration"
```

Run PostgreSQL tests against a disposable local database:

```bash
createdb lance_etl_test
LANCE_ETL_TEST_DATABASE_URL='postgresql+psycopg://localhost/lance_etl_test' \
  .venv/bin/pytest tests/test_state_postgres.py tests/test_postgres_queue_load.py
```

Run the full local data path in a fresh process so Spark loads the Iceberg runtime at JVM startup:

```bash
LANCE_ETL_TEST_DATABASE_URL='postgresql+psycopg://localhost/lance_etl_test' \
  .venv/bin/pytest tests/test_local_e2e.py -x -q -m integration
```

Validate Rust independently:

```bash
cd rust/search-api
cargo fmt --check
cargo clippy --locked -- -D warnings
cargo test --locked
```

## Backup boundary

Back up PostgreSQL and the Iceberg and Lance storage directories together when a reproducible local
snapshot is needed. PostgreSQL is serving truth, but immutable publication rows refer to exact Lance
objects. Restoring only one side does not recreate a consistent publication.
