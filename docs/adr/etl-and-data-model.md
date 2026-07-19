# ETL and data-model decisions

The source is one registered Iceberg table. Each validated `(tenant_id, namespace, org_id)` route
maps to one first-class PostgreSQL dataset and one Lance dataset. The active dataset specification
revision defines its complete target schema and processing behavior.

## ADR 0003 — Incremental Iceberg reads use snapshot identities

Status: Accepted

Incremental planning uses exact Iceberg snapshot IDs and direct parent lineage. Snapshot IDs are
opaque identities and must not be sorted numerically. `source_snapshots` records the table sequence
number, parent, partition spec, operation, commit time, classification, and state.

Spark scans use snapshot ID bounds. A canonical baseline is required when adopting an existing
table. Unsupported deletes, lineage gaps, and untrusted rewrites produce durable blocked evidence.

## ADR 0004 — Dataset routing and duplicate semantics

Status: Accepted, with dataset made first-class by ADR 0042

The globally unique logical route is `(tenant_id, namespace, org_id)`. Route components are
validated bounded path segments. One request, work item, and publication always addresses one
dataset. Cross-organization sharing and cross-dataset search are rejected.

Within one source transition, ingestion collapses mutations by record key and source sequence. The
highest sequence wins. Repeating the same sequence and digest is a no-op. Repeating the same
sequence with different content is an error. Deletes become tombstones when the active schema has
the tombstone role.

## ADR 0011 — Ingestion timestamp

Status: Superseded by ADR 0016

A processing-time `_ingested_at` column is not the data clock. Exact source snapshot identity and
the source event time provide reproducible ordering.

## ADR 0016 — Event-time canonical clock

Status: Accepted

The active spec requires exactly one `EVENT_TIME` field. Source mapping selects its Iceberg column.
The value drives time-range filters and TTL expiration. It is not replaced by process start time,
file modification time, or snapshot commit time.

## ADR 0020 — Static map projection

Status: Superseded by ADR 0024

A fixed code-owned list of map keys cannot represent different dataset schemas. The projection now
comes from immutable `dataset_fields` rows.

## ADR 0024 — Specification-driven map pivot

Status: Accepted

Each field declares a source kind:

- `DIRECT` selects a typed Iceberg column
- `MAP_KEY` extracts one declared key from the configured vectors, texts, or metadata map
- `DERIVED` produces lineage or tombstone values owned by the ETL

The revision validates field names, roles, types, nullability, upsert requirements, vector
dimensions, source columns, and map keys before work starts. The vector dimension must be divisible
by eight for RaBitQ. Projection is deterministic and no runtime discovery changes the target
schema.

## ADR 0032 — Hour-based source identity

Status: Superseded

Hourly tags and schedule windows are not source identities. Exact Iceberg snapshots and per-dataset
source cursors replace them. Event-time filtering remains a scalar search predicate on the
specification's event-time field.

## ADR 0034 — Adaptive routing and streaming merge

Status: Accepted with bounds stored in the specification revision

Spark reads only the exact qualified snapshot transition and route partitions. Streaming routing
groups rows without collecting the source on the driver. Ingestion partitions, merge rows per
chunk, merge byte budget, and rows per output fragment are frozen PostgreSQL values.

The driver may plan and broadcast small read-only artifacts. Lance row reads and writes run in
executor closures. Completion is recorded only after all executor results validate.

## Target schema contract

The role set is `KEY`, `EVENT_TIME`, `VECTOR`, `TEXT`, `METADATA`, `TTL`, `TOMBSTONE`,
and `LINEAGE`. One key and one event time are mandatory. TTL and tombstone are optional singletons.
Any number of vectors, texts, metadata fields, scalar fields, and lineage fields may be declared
when their source mapping remains unique.

Index definitions reference field identities in the same revision:

| Index family | Compatible field purpose |
|---|---|
| `IVF_RQ` | Fixed-size float vector |
| `INVERTED` | Text |
| `BTREE` | Key, event time, or supported scalar |
| `BITMAP` | Low-cardinality scalar or tombstone |
| `ZONEMAP` | Ordered event time or scalar |

The active revision is seeded with a representative vector, text, scalar, event-time, and tombstone
schema plus all five supported index families. New revisions may change the schema and options, but
existing work and publications retain their original revision identity and digest.

## First-run bootstrap

The first local run registers the Iceberg table in `iceberg_sources`, including its immutable UUID,
catalog-qualified name, Lance storage namespace, baseline, replay horizon, and source-column
mapping. PostgreSQL becomes authoritative after registration. A later process cannot silently point
the same source name at a different table or storage root.

New routes discovered in qualified manifests create `datasets` and `dataset_state` rows in the same
planning transaction as deterministic ingest work. A route is never invented by enumerating all
possible organizations.
