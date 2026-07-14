# Iceberg time partitioning and the reconciler

Production does not create one Lance dataset per date and does not pass date windows through
Airflow. Iceberg owns time partitioning. Lance owns one durable dataset per logical target.

## Source layout

The accepted Iceberg partition order is:

1. `tenant_id`
2. `namespace`
3. `org_id`
4. `hours(processing_timestamp)`

The hour transform is a pruning key. `event_timestamp` remains the query and TTL clock. The
reconciler reads manifest partition values to discover touched logical targets and hours, then
executes a scan pinned to an exact Iceberg snapshot or direct parent-to-child snapshot pair.

## Why Airflow has no date parameters

Wall-clock intervals cannot prove which Iceberg files or deletes belong to an immutable source
generation. They also make manual retries dependent on the time at which a task is rerun. The
single production DAG therefore exposes only five closed actions and has no `start`, `end`,
`partition-by`, dataset-list, or backfill parameters.

Catch-up is durable and cursor-free. `plan_and_enqueue_window` compares the newest recorded source
window with the current pinned Iceberg head. It enqueues a bounded prefix of direct descendants.
Repeated runs enqueue the same immutable identities, then continue until the durable tip reaches
the pinned head.

## Retention

`gate_source_retention` reports the oldest unfinished source window. For an append window the
retained floor is its direct parent snapshot, because that parent is required by the exact
incremental scan. A blocked INGEST keeps the floor. A serving retry does not keep source retention
after source application has completed.

Iceberg snapshot expiration must consume this decision. It must not infer a cutoff from Airflow's
execution date. Destructive orphan removal remains a separately authorized maintenance operation.

## Query-time date filtering

Date and time ranges remain logical predicates over `event_timestamp`. The release profile builds
the required scalar and zone-map time indexes. The search service combines its typed time range
with any typed filter expression. Physical Iceberg hour partitions do not leak into the search
API.

## Backfill and replay

Use a retained canonical Iceberg snapshot for first startup. The baseline qualifier scans that
exact snapshot on executors, validates the release schema, computes canonical mutation digests,
and rejects more than one distinct mutation per target and `vector_id`.

After startup, do not launch date-specific DAGs. Restore the required Iceberg ancestry when it was
expired, then let the reconciler advance direct snapshot lineage. Target work is idempotent under
its deterministic identity, lease token, target fence, source digest, and Lance completion marker.
