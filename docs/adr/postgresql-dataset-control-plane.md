# ADR 0042 — First-class PostgreSQL dataset control plane

Status: Accepted

## Decision

PostgreSQL is the only durable control plane for the local Iceberg-to-Lance process. The schema is
normalized into exactly 9 application tables. Dataset is a first-class entity. Immutable dataset
specification revisions own every reproducible schema, ingestion, compaction, indexing,
qualification, and retention option.

The local reconciler is the sole transition owner. Spark executes bounded data work locally. The
optional Rust search process reads only the serving catalog join. No scheduler-owned eligibility
state, untyped configuration document, or parallel control database is part of the design.

## Entity model

```text
dataset_spec_revisions
     -> dataset_fields
     -> index_definitions

iceberg_sources -> source_snapshots
       |               |
       v               v
    datasets ------> dataset_work
       |               |
       v               v
       +-----------> dataset_publications -> publication_indexes

ReconcilerSettings is loaded from environment variables at process startup, not a table
```

### `ReconcilerSettings` (not a table)

Local control-loop bounds are process bootstrap configuration built by
`ReconcilerSettings.from_environment()`, not a PostgreSQL row:

- polling interval, claim batch size, drain batch limit, and maximum snapshots per plan
- lease duration and heartbeat interval
- retry base delay, retry maximum delay, and maximum attempts
- maximum due work and maximum open-work age SLOs
- maximum retention age, audit retention, and cleanup batch size

The process loads these values once at startup from environment variables. Restart the local
reconciler after changing them. They do not alter the meaning of an existing Lance artifact.

### `dataset_spec_revisions`

A specification exists only as its revisions: there is no separate spec-identity table. Each
revision carries its own `spec_id` (the stable grouping identity shared by all revisions of one
named spec), `name`, and optional `description`.

A revision is the immutable root of one complete data contract. Lifecycle is `DRAFT`, `ACTIVE`, or
`RETIRED`. At most one revision per spec is active. `configuration_digest` is a deterministic
SHA-256 digest of all semantic configuration, including child field and index definitions.

Ingestion columns:

- `ingest_shuffle_partitions`
- `merge_rows_per_chunk`
- `merge_batch_bytes`
- `write_rows_per_fragment`

Compaction and cleanup columns:

- `compaction_enabled`
- `compaction_mode`, either `try_binary_copy` or `reencode`
- `target_rows_per_fragment`
- nullable `max_source_fragments`
- nullable `compaction_threads`
- `defer_index_remap`
- `materialize_deletions`
- `materialize_deletions_threshold`
- nullable `cleanup_older_than_seconds`
- nullable `retain_versions`

Index maintenance columns:

- `fragments_per_index_task`
- `max_index_deltas`
- `max_stale_replans`

Publication and artifact columns:

- `prewarm_required`
- `retained_publications`
- `artifact_retention_seconds`

A semantic change creates a new revision. Existing work continues with its frozen revision.

The repository owns the complete authoring lifecycle. A specification exists only as its revisions.
`create_draft_spec_revision` recomputes the semantic digest and inserts the revision under a
caller-supplied name, its fields, indexes, and typed family options in one transaction.
`activate_spec_revision` validates the complete graph,
retires the former ACTIVE revision, and promotes the DRAFT. Database triggers permit DRAFT content
changes and the single DRAFT to ACTIVE transition. They reject updates or deletion of ACTIVE and
RETIRED parents and children. Historical work and publications may continue to reference a RETIRED
revision.

### `dataset_fields`

Fields define the ordered Lance schema and its projection from Iceberg. Each row stores ordinal,
target name, semantic role, source kind, source column, optional map key, physical data type,
nullability, upsert requirement, and optional vector dimension.

Roles are `KEY`, `EVENT_TIME`, `VECTOR`, `TEXT`, `METADATA`, `TOMBSTONE`, and
`LINEAGE`. Source kinds are `DIRECT`, `MAP_KEY`, and `DERIVED`. The schema requires exactly one key,
one event time, one tombstone, and the three canonical lineage fields
`lance_etl_window_seq`, `lance_etl_source_sequence`, and `lance_etl_event_digest`. The single
`EVENT_TIME` field is the canonical `ts` column. Fields must use canonical execution order: key,
event time, vectors, texts, metadata, the three lineage fields, and tombstone. Ordinals are
contiguous from zero.

The key, lineage, and tombstone fields are non-nullable. Every other target field is nullable
because tombstone rows do not carry their payload. Key, event-time, and vector fields are required
on upsert. Text, metadata, lineage, and tombstone fields are not. Canonical names,
physical types, legal source mappings, and vector dimensions are validated before a revision can
become active.

### `index_definitions`

Each row declares one ordered required Lance index with a stable name, family, and field reference.
Families are `IVF_RQ`, `BTREE`, `BITMAP`, `ZONEMAP`, and `INVERTED`. Scalar families (`BTREE`,
`BITMAP`, `ZONEMAP`) need no typed options because their supported behavior is completely described
by the definition and revision-level maintenance policy. `IVF_RQ` and `INVERTED` carry their typed
options as nullable columns on this same row, gated by a per-index-type CHECK constraint rather than
a child table:

IVF_RQ columns:

- metric `l2`, `cosine`, or `dot`
- nullable explicit `num_partitions`
- adaptive `minimum_partitions` and `maximum_partitions`
- `target_rows_per_partition` and `minimum_rows`
- RaBitQ `num_bits`
- `streaming_sample_rate` and `streaming_refine_passes`
- `retrain_growth_factor`

The explicit partition count, when present, must fall inside the configured adaptive range.

INVERTED columns: `with_position`, optional `base_tokenizer`, optional `language`, and
`max_unindexed_fragments`.

### `iceberg_sources`

A source row records stable source identity and first-run bootstrap truth:

- source name, Spark catalog, table namespace, table name, and immutable table UUID
- local Lance base URI and lifecycle state
- default dataset spec and optional canonical baseline snapshot
- replay horizon
- monotonic planning epoch that fences stale Iceberg observations across source gates and repairs

The first local run inserts the source registration when absent. Later runs load it from PostgreSQL
and reject a changed table UUID, table name, Lance root, or baseline. Physical source names are a
fixed code-owned contract and are not duplicated as database configuration.

### `datasets`

Dataset is the first-class logical unit and the only mutable row in the control plane. The globally
unique route is `(tenant_id, namespace, org_id)`. A dataset references its source, lifecycle, and
desired spec revision, and carries its own physical materialization state directly: materialized
spec revision, last applied source snapshot, ingest Lance URI and version, active publication
pointer, and monotonically increasing fence epoch. The Rust search catalog resolves this same
route, so source-scoped route duplicates are prohibited.

The active publication pointer references an immutable publication of the same dataset. Search
resolves:

```text
datasets -> datasets.active_publication_id -> dataset_publications
```

Only the resulting allowlisted Lance URI and exact version leave the catalog layer.

### `source_snapshots`

Rows retain exact Iceberg snapshot ID, parent ID, Iceberg sequence number, partition spec ID,
commit time, operation, classification, and state. Classifications are `BASELINE`, `APPEND`,
`TRUSTED_MAINTENANCE`, and `REJECTED`. States are `SEALED`, `COMPLETE`, and `BLOCKED`.

This table is the source chronology and retention evidence. Snapshot IDs are identities, not an
ordering clock. Direct parent lineage and Iceberg sequence numbers decide order.

Blocked and rejected rows persist a bounded `error_code` and `error_message`. Replaying the same
snapshot must reproduce both values exactly. This prevents a weaker replay from erasing the reason
that source history was rejected.

### `dataset_work`

One row is one deterministic unit of `INGEST`, `PUBLISH`, or `REBUILD`. Phases are `INGEST`,
`COMPACT`, `INDEX`, `VALIDATE`, `PREWARM`, and `PUBLISH`. State is `PENDING`, `RUNNING`,
`RETRY_WAIT`, `SUCCEEDED`, or `BLOCKED`.

The row freezes:

- dataset, owning source, source snapshot, and spec revision
- expected ingest URI and version plus expected active publication
- current lease token, lease expiry, attempt count, next attempt time, and latest error evidence
- candidate Lance URI and version
- applied source time, source row count, and source digest
- optional artifact manifest URI and digest
- launcher kind audit label

A retry increments `attempt_count`, obtains a fresh lease, and advances the dataset fence on the
same deterministic row. PostgreSQL constraints allow at most one running row per dataset and one
open publication row per dataset. Claim order prevents a later ingest snapshot from passing an
earlier unfinished one. The current lease token plus monotonic dataset fence reject superseded
workers without a parallel attempt-history table. Composite foreign keys require the dataset and
snapshot to belong to the same source.

The `launcher_kind` label is optional claim provenance recorded on the deterministic work row. It
does not affect eligibility, ordering, retries, leases, or fencing.

### `dataset_publications`

A publication is immutable serving evidence created from successful publication work. Its
composite foreign key binds the dataset, work, frozen spec revision, and source snapshot to the
exact work lineage that produced it. It records the exact Lance URI and version, schema digest,
total and distinct row counts, live and distinct-live row counts, fragment count, manifest URI and
digest, publication time, and optional retirement time.

Database constraints require total rows to equal distinct keys and live rows to equal distinct live
keys. One dataset cannot publish the same exact URI and version twice.

### `publication_indexes`

Each required index has one evidence row per publication. A composite foreign key binds the
observed index family to the exact configured definition. The row records indexed fragment count,
requires zero unindexed fragments, and carries an optional artifact-generation digest. Publication
validation requires the evidence set to match the frozen revision exactly.

## Transaction boundaries

### Plan source work

One transaction inserts or reuses the exact source snapshot, creates newly discovered `datasets`
rows, and inserts deterministic ingest work. Unsupported source history is persisted as a blocked
snapshot with an error code.

### Claim and execute

Claiming locks due rows with `FOR UPDATE SKIP LOCKED`, checks dataset order, increments
`attempt_count`, advances `fence_epoch`, and writes a fresh lease on the deterministic work row.
Executor work occurs outside the transaction. Heartbeats and every completion transition compare
the work identity, lease token, and fence epoch.

### Complete ingestion

Successful ingestion compare-and-swaps the expected ingest tuple, records the candidate and source
evidence, advances the dataset row, and schedules publication. The source snapshot becomes complete
only after all of its ingest work succeeds.

### Publish

Publication validates the candidate against its frozen revision, appends publication and index
evidence, retires the previous active publication, and changes the dataset active pointer in one
transaction. Failed prewarm or validation leaves the former active publication unchanged.

### Retry and recovery

Retryable failures retain the same work ID and enter `RETRY_WAIT` with bounded exponential backoff.
The maximum-attempt setting moves exhausted work to `BLOCKED`. Expired leases are reclaimable under
a new token and higher fence. Lance completion markers reconcile crashes after a Lance commit but
before the PostgreSQL commit.

### Retention

Cleanup protects the active publication, open-work candidates, source replay floor, and the number
of historical publications required by the frozen spec. It removes bounded batches only after the
configured artifact and audit horizons.

## Configuration boundary

PostgreSQL stores all dataset behavior: schema, ingestion, compaction, indexing, prewarm, and
retention policy. Local reconciler loop policy (polling, claim, lease, retry, SLO, and cleanup
bounds) is process bootstrap configuration loaded once at startup by
`ReconcilerSettings.from_environment()`, not a database row. The environment otherwise contains only
bootstrap and secret-bearing process concerns:

- PostgreSQL connection URL
- first-run Iceberg table and Lance root
- local Spark master, catalog, warehouse, and package coordinate
- optional first baseline snapshot
- telemetry connection and identity

Do not add environment variables for dataset-scoped ingestion, compaction, indexing, prewarm, or
retention behavior. Add a typed column with validation to the appropriate normalized entity and
include it in the revision digest.

## Migration strategy

Breaking changes are allowed. `migrations/versions/0001_control_plane.py` is the single Alembic
baseline and seeds the bundled active dataset specification revision. Schema changes rewrite that
baseline. Existing control-plane data is dropped and recreated instead of being carried through
forward compatibility migrations. Fresh local databases upgrade directly to the current 9-table
schema.
