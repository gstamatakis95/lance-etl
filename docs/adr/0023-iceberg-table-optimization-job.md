# 0023. Iceberg source-table optimization job

Status: Accepted

## Context

The pipeline maintains two very different stores. The per-tenant Lance datasets are kept healthy by the Lance
maintenance job in `src/lance_etl/maintenance.py`, which runs TTL expiration, two-tier compaction, and version cleanup
(see [ADR 0002](0002-two-tier-compaction-orchestration.md)). The upstream Iceberg source table that the ETL reads each
run (see [ADR 0003](0003-read-increment-snapshot-bounds.md)) has its own, separate maintenance needs that the Lance job
does not and should not touch.

An Iceberg table that is appended to on every ETL window accumulates many small data files, a growing manifest list,
and an unbounded snapshot history. Left alone, small files slow every scan, deep snapshot history grows table metadata,
and unreferenced files waste object storage. Iceberg ships its own table maintenance as Spark SQL stored procedures, so
the right move is to call those procedures rather than reimplement compaction on the source side.

## Decision

Add `src/lance_etl/iceberg_optimize.py` with an `IcebergOptimizeConfig`, an `IcebergOptimizer`, and an
`IcebergOptimizeReport`. The job optimizes the source Iceberg table by issuing
`CALL <catalog>.system.<procedure>(...)` statements through the Iceberg Spark session extensions. It is a distinct job
from the Lance maintenance job and runs against a different store.

Four steps run in a fixed safe order, each enabled by an opinionated default toggle.

1. `rewrite_data_files` (default on). Bin-packs small data files into larger ones. The target file size defaults to 512
   MiB, matching Iceberg's own write default, and the minimum input-file count per bin-pack group defaults to 5, also
   the Iceberg default.
2. `rewrite_manifests` (default on). Rewrites the manifest list so manifests align with the new file layout produced by
   the data-file rewrite. It runs after `rewrite_data_files` for that reason.
3. `expire_snapshots` (default on). Prunes snapshot history beyond a retention horizon. It retains at least the last 5
   snapshots regardless of age and expires snapshots older than 7 days beyond that count. This is the Iceberg analog of
   Lance version cleanup.
4. `remove_orphan_files` (default off). Deletes files no live snapshot references. It is opt-in because it is the only
   step that can delete data files outright, and it respects Iceberg's own three-day `older_than` safety horizon so an
   in-flight write is never mistaken for an orphan.

Each step is wrapped with telemetry timing and a metric, and the per-step outcome (the procedure's integer result
columns, such as `rewritten_data_files_count` or the deleted-snapshot counts, plus durations) is collected into the
report.

Heavy work runs in Spark. Each `CALL` plans and executes as a normal distributed Spark job across executors. The driver
only issues the statements, which satisfies the executor rule the same way the ETL does. The table identifier is
validated before it ever reaches a statement: it must be a dotted `catalog.namespace.table` identifier and every
component must match the bare-identifier allowlist. The catalog prefix selects the procedure namespace and the remaining
components form the validated `table` argument. Only the validated table name and numeric or typed `TIMESTAMP` literals
are interpolated, so no arbitrary string reaches the SQL.

The job is exposed as the CLI subcommand `optimize-iceberg` with flags for the table, the shared telemetry identity, the
step toggles, and the expire retention. The Iceberg catalog is supplied through the Spark configuration at submit time
exactly like the ETL's reads, so no per-job catalog or warehouse flag is added. An optional Airflow task, gated off by
default behind the `lance_etl_optimize_iceberg_enabled` Variable, runs the job before `etl` because it maintains the
source the ETL reads.

## Consequences

Source-table maintenance becomes a first-class, scheduled-but-optional job that is cleanly separated from Lance dataset
maintenance. Operators get small-file compaction, manifest rewrite, and snapshot expiration on the source with sane
defaults and few knobs, and the destructive orphan-file removal stays opt-in. Because the job delegates to Iceberg's own
procedures, it tracks Iceberg's correctness and concurrency guarantees rather than duplicating them. The cost is a
dependency on the Iceberg Spark session extensions being configured on the cluster, which the ETL already requires, so
no new deployment surface is introduced.
