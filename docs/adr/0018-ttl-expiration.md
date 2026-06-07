# 0018. TTL data-expiration by event age

Status: Accepted

## Context

Datasets written by the ETL pipeline accumulate rows indefinitely. Operators need a way to expire rows that are
older than a configured retention window, for cost control, compliance, or capacity management. Two possible
expiration clocks exist in the system:

1. **Event age** -- the source event timestamp column (`ETLConfig.ts_col`, default `"timestamp"`), which is the
   single canonical clock established by [ADR 0016](0016-event-time-canonical-clock.md).
2. **Ingest age** -- the wall-clock time a row was written into Lance. There is no ingest-time column in the
   schema. [ADR 0016](0016-event-time-canonical-clock.md) removed `_ingested_at` and [ADR
   0011](0011-ingested-at-column.md) is superseded. Receipt-based retention (ingest age) is not expressible
   through this pipeline.

Lance's `LanceDataset.delete(predicate: str)` writes a deletion vector marking rows as logically deleted without
rewriting fragment files. The deletion vector keeps existing indexes valid, so no index rebuild is required after
a TTL delete. A follow-up `Compaction.execute` materialises the deletion vectors and physically removes the rows,
reclaiming storage. This is the same compaction path used by [ADR 0002](0002-two-tier-compaction-orchestration.md).

The pipeline operates at fleet scale (tens of thousands of datasets per namespace), so any expiration job must
distribute work across executors rather than running per-dataset deletes on the driver.

## Decision

Implement TTL expiration as `TTLJob` in `src/lance_etl/ttl.py`, controlled by `TTLConfig`.

**TTL is default off.** `TTLConfig.enabled` defaults to `False`. Operators opt in by setting `enabled=True`.
A zero or negative retention raises `ValueError` at run time to prevent accidental bulk deletion.

**The canonical clock is the source event timestamp column.** The expiration predicate is
`{ts_col} < TIMESTAMP '{cutoff}'` where `cutoff = now(UTC) - retention`. The column name is validated against
the dataset schema and the identifier allowlist `[A-Za-z_][A-Za-z0-9_]*` before the predicate is constructed.
The cutoff is formatted as an unambiguous ISO-8601 UTC literal. No user-supplied string reaches the Lance SQL
engine without validation.

**Two-tier execution** mirrors the compaction orchestration from [ADR 0002](0002-two-tier-compaction-orchestration.md).
A tier-A Spark job fans all dataset URIs out across executors. Each executor probes its dataset's fragment count.
Datasets at or below `small_tier_fragment_threshold` (default 128, matching the compaction default) are expired
in process on the executor. Datasets above the threshold are returned to the driver and fanned out in a dedicated
tier-B pass with a separate Spark partition budget. For TTL the per-dataset delete is a single Lance call
regardless of dataset size, so the tier split primarily prevents large datasets from sharing partitions with many
small ones in the first pass. The driver only plans and collects results. All Lance I/O runs on executors, per the
rule that heavy work runs in executors.

**Compaction after deletion.** `TTLConfig.compact_after_delete` defaults to `True`. When set, each dataset that
had rows deleted is compacted in process with `Compaction.execute(materialize_deletions=True)` and then pruned
with `cleanup_old_versions`. This reclaims storage immediately. Operators who run a separate compaction job in
the same pipeline may set `compact_after_delete=False` to avoid double-compaction.

**All commits go through `commit_with_retries`.** The delete action re-opens the dataset before each attempt so
each retry operates against the latest version. The retry budget defaults to `DEFAULT_COMMIT_RETRIES` (20).

**Receipt-based retention is not supported.** There is no ingest-time column. Retention is by event age only. If
ingest-age retention is needed in future, a new ADR must introduce an explicit ingest-time column design rather
than restoring the rejected `_ingested_at` column.

## Consequences

`TTLJob.run(spark, dataset_uris=..., base_uri=...)` is the entry point. The orchestrator wires it as a CLI
subcommand (`ttl`) and as an Airflow task that runs after compaction in the pipeline DAG. `TTLReport` carries
`datasets_scanned`, `datasets_expired`, `total_rows_deleted`, `datasets_compacted`, `datasets_skipped`, and
`enabled` for CLI output and telemetry.

The predicate safety approach matches the stance taken in [ADR 0005](0005-rust-grpc-layering-typed-filter.md)
for the gRPC filter API: column names pass an identifier allowlist and a schema-membership check, and the cutoff
literal is a typed value formatted internally. No user-supplied string reaches the SQL engine unvalidated.

Existing datasets that carry the timestamp column are compatible with no migration. Datasets that do not carry
the configured timestamp column are skipped with a warning rather than failing the run, so a mixed fleet
(some datasets pre-dating the column or using a different column name) does not block expiration of the rest.

Receipt-based (ingest-age) retention is explicitly not supported. Cross-reference: [ADR
0016](0016-event-time-canonical-clock.md) explains why there is no ingest-time column.

The `lance.LanceDataset.delete` call writes deletion vectors and returns a `DeleteResult` dict with
`num_deleted_rows`. Existing indexes remain valid after the delete because deletion vectors are applied lazily
at read time. Physical row removal requires the follow-up `Compaction.execute` step, which is why
`compact_after_delete` defaults to `True`.
