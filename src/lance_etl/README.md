# `lance_etl` Python package

The Python package owns the local Iceberg-to-Lance write path. PostgreSQL holds durable intent and
serving truth. A local Spark session supplies Iceberg access and executor-side Lance work.

See the repository [README](../../README.md) for setup and [AGENTS.md](AGENTS.md) for Python rules.

## Reconciliation cycle

`lance-etl-reconcile run-once` performs one bounded cycle:

1. Load the registered Iceberg source and loop settings from PostgreSQL.
2. Validate the next direct-parent Iceberg snapshot transition.
3. Persist exact snapshot evidence and deterministic per-dataset ingest work.
4. Claim dataset-disjoint work with `FOR UPDATE SKIP LOCKED`, a lease token, and a fence epoch.
5. Run replay-safe ingestion, compaction, indexing, validation, and local exact-version prewarm.
6. Append an immutable publication with row, fragment, schema, manifest, and index evidence.
7. Atomically move `dataset_state.active_publication_id` to the qualified publication.
8. Advance the source snapshot only after all related ingest work succeeds.

`lance-etl-reconcile run` repeats the same cycle using the PostgreSQL polling interval. It does not
introduce another scheduler or retry database.

Loop settings are loaded once when the process starts. Restart the local reconciler after changing
`reconciler_settings`.

## PostgreSQL entities

The application schema contains exactly 14 normalized tables:

| Table | Responsibility |
|---|---|
| `reconciler_settings` | Singleton claim, lease, polling, retry, SLO, and audit cleanup bounds |
| `dataset_specs` | Stable names for dataset contracts |
| `dataset_spec_revisions` | Immutable schema and ingestion, compaction, indexing, publication, and retention policy |
| `dataset_fields` | Ordered target fields, semantic roles, physical types, and Iceberg projections |
| `index_definitions` | Ordered required Lance indexes and their target fields |
| `vector_index_options` | Typed IVF_RQ metric, partition sizing, training, and retraining options |
| `fts_index_options` | Typed INVERTED tokenizer, positions, language, and backlog options |
| `iceberg_sources` | Source identity, local Lance base URI, baseline, replay horizon, and source-column mapping |
| `datasets` | First-class `(tenant_id, namespace, org_id)` route, source, lifecycle, and desired spec revision |
| `source_snapshots` | Exact Iceberg lineage and accepted or blocked transition evidence |
| `dataset_work` | Durable work, lease, retry state, and latest bounded error under the dataset fence |
| `dataset_publications` | Immutable exact Lance version plus schema, row, fragment, and manifest evidence |
| `publication_indexes` | Per-publication evidence for every required index definition |
| `dataset_state` | Mutable ingest cursor, materialized spec, fence epoch, and active publication pointer |

`state/repository.py` is the only owner of transactions and state transitions. Each work item and
publication freezes one `spec_revision_id`. Retries increment `dataset_work.attempt_count` while
retaining the same deterministic work identity. A fresh lease token and higher dataset fence reject
stale executors without a parallel attempt-history entity.

Configuration changes follow the repository lifecycle API:

1. `create_spec` creates or replay-validates a stable named identity.
2. `create_draft_spec_revision` recomputes the digest and atomically stores the complete typed
   parent, fields, indexes, and family options.
3. `activate_spec_revision` validates the graph, retires the former ACTIVE revision, and activates
   the DRAFT.
4. `set_source_default_spec` selects a spec that has an ACTIVE revision for newly discovered
   datasets.
5. `assign_dataset_spec_revision` changes an existing dataset's desired ACTIVE revision and creates
   one deterministic REBUILD when its materialized revision differs.

PostgreSQL triggers allow content changes only while a revision is DRAFT. ACTIVE and RETIRED graphs
cannot be updated or deleted, while historical work and publications continue to reference retired
revisions. A blocked `source_snapshots` row persists both its bounded classification code and
diagnostic message so a replay must present identical evidence.

`dataset_work` also stores launcher kind and optional standard `AIRFLOW_CTX_*` fields. The local
runtime reads them once when present. This is audit provenance only and does not introduce an
Airflow service, DAG, attempt table, or alternate scheduler.

The complete column and transaction design is in
[ADR 0042](../../docs/adr/postgresql-dataset-control-plane.md).

## Immutable dataset specifications

An active revision owns all behavior that must be reproducible later:

- ordered target fields, roles, physical types, nullability, source columns, map keys, and vector
  dimensions
- ingestion shuffle width, merge chunk rows, merge batch bytes, and write rows per fragment
- compaction enablement, mode, target fragment size, source-fragment and thread limits, index-remap
  behavior, deletion materialization, cleanup horizon, and retained Lance versions
- index task width, delta bound, and stale-replan bound
- every index name, family, field, and order
- IVF_RQ metric, explicit or adaptive partition range, target rows per partition, minimum row floor,
  RaBitQ bits, streaming sample rate, refine passes, and retraining growth factor
- INVERTED position storage, tokenizer, language, and maximum unindexed fragments
- local prewarm requirement, publication history count, and artifact retention horizon

A deterministic SHA-256 digest covers semantic configuration. Changing any setting requires a new
revision. Existing work continues with its frozen revision while newly planned work adopts the
dataset's desired revision.

## First-run source registration

Process bootstrap values identify the local Iceberg table and Lance root. On the first run,
`ensure_source_registration` creates an active `iceberg_sources` row and binds it to the bundled
active spec. It records the table UUID, catalog and table name, canonical baseline, replay horizon,
Lance storage namespace, and source-column mapping. Later runs treat the PostgreSQL row as
authoritative and reject drift. `dataset_state.ingest_lance_uri` and `ingest_lance_version` are the
current physical materialization cursor for each dataset.

The source contract requires direct snapshot lineage and the route partition fields configured in
the source row. An existing table needs a canonical baseline. Unsupported deletes and untrusted
rewrites are recorded as blocked snapshots instead of being guessed through.

## Local Spark boundary

The process creates and stops one Spark session. The driver plans and owns PostgreSQL transactions.
Heavy Iceberg and Lance reads, writes, index builds, and compaction tasks run in executor closures.
This rule still applies under `local[*]` because local mode uses real executor tasks.

## Replay-safe ingestion and publication

`etl/replay_sink.py` collapses terminal mutations per record and persists a source sequence plus
event digest. A later sequence replaces an earlier mutation. An exact replay is a no-op. Equal
sequences with different digests are rejected.

The completion marker makes a crash between a Lance commit and its PostgreSQL transition
recoverable. All merge, compaction, and index commits use `commit_with_retries`. Move-stable row IDs
remain prohibited.

Publication succeeds only after validation produces complete typed evidence for the frozen spec.
The Rust search service resolves only the active publication's URI and exact Lance version through
`datasets -> dataset_state -> dataset_publications`.

## Local configuration

| Variable | Purpose |
|---|---|
| `LANCE_ETL_DATABASE_URL` | PostgreSQL URL using the psycopg 3 driver |
| `LANCE_ETL_LANCE_BASE_URI` | Local directory or object-store root registered for the source |
| `LANCE_ETL_SOURCE_TABLE` | Catalog-qualified Iceberg table |
| `LANCE_ETL_LOCAL_ROOT` | Default root for local Lance and Iceberg data |
| `LANCE_ETL_SPARK_MASTER` | Spark master, default `local[*]` |
| `LANCE_ETL_SPARK_CATALOG` | Hadoop Iceberg catalog name, default `local` |
| `LANCE_ETL_SPARK_WAREHOUSE` | Local Iceberg warehouse path |
| `LANCE_ETL_SPARK_ICEBERG_PACKAGE` | Iceberg runtime Maven coordinate |
| `LANCE_ETL_CANONICAL_BASELINE_SNAPSHOT_ID` | Optional initial source snapshot |
| `DD_SERVICE`, `DD_ENV` | Optional low-cardinality telemetry identity |

Data-path options belong in PostgreSQL, not environment variables.

## Commands

```bash
export LANCE_ETL_DATABASE_URL='postgresql+psycopg://lance_etl:lance_etl@localhost/lance_etl'
uv run lance-etl-reconcile migrate
uv run lance-etl-reconcile run-once
uv run lance-etl-reconcile run
uv run lance-etl-reconcile status
```

Restricted repairs preserve repository transitions:

```bash
uv run lance-etl-reconcile repair --action retry-blocked --work-id WORK_UUID
uv run lance-etl-reconcile repair --action rebuild \
  --tenant-id TENANT --namespace NAMESPACE --org-id ORG --request-id REQUEST_UUID
```

## Development

```bash
uv sync --locked --group dev --python 3.14.0
uvx ruff format src/ tests/ bench/ migrations/
uvx ruff check src/ tests/ bench/ migrations/
.venv/bin/pytest -m "not integration"
```

Set `LANCE_ETL_TEST_DATABASE_URL` to a disposable local PostgreSQL database for the real control
plane and end-to-end tests.
