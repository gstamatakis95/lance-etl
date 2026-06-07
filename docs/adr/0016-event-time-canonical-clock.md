# 0016. Event-time canonical clock: remove `_ingested_at`, use source event timestamp

Status: Accepted

## Context

[ADR 0011](0011-ingested-at-column.md) introduced an `_ingested_at` column stamped by `F.current_timestamp()`
at ingest time. The column was a payload column only: excluded from collapse keys and routing, refreshed by
`when_matched_update_all` on updates, and intended as a per-row ingestion-time provenance signal.

Two problems emerged from maintaining two time columns:

1. Two clocks for one concept. The source event timestamp (`ETLConfig.ts_col`, default `"timestamp"`) already
   drives last-write-wins collapse. The ingestion timestamp added a second, derived time axis that drifted
   from the event axis on retries and backfills, creating ambiguity about which timestamp represented the
   authoritative event time.

2. Date-range queries required a fan-out or a filter on the ingest time rather than on the event time. Serving
   queries that say "return results from events before date X" is naturally expressed as a scalar range filter on
   the event timestamp, not on the ingestion timestamp. A BTREE scalar index on the event timestamp column gives
   efficient pruning for exactly this query shape. There is no equivalent efficient path on `_ingested_at` for
   event-time queries.

The CLI flag `--ingested-at-col` and the `ETLConfig.ingested_at_col` field were already removed in
[ADR 0015](0015-cli-and-config-knob-reduction.md). This ADR removes the column itself.

## Decision

Remove `_ingested_at` entirely. The source event timestamp column (`ETLConfig.ts_col`, default `"timestamp"`)
is the single canonical time clock.

Concretely:

- Remove `INGESTED_AT_COLUMN` from `etl.py`.
- Remove `ensure_ingested_at_column` and the call that evolved pre-existing datasets.
- Remove `stamp_ingested_at` and the step that added the column before collapse.
- Remove any `merge_insert` special-casing that refreshed `_ingested_at` on updates.
- Date-range queries are expressed as scalar range filters on the event timestamp column, pruned by a BTREE
  scalar index. Callers configure a BTREE index on `ts_col` in `IndexJobConfig.scalar_columns` when they want
  efficient time-range pruning. The indexer already accepts any column for BTREE. No auto-building of this
  index is added to defaults.

## Consequences

The written dataset schema no longer carries `_ingested_at`. Existing datasets that have the column continue
to work: the column is simply not written or updated by new ETL runs. Operators who want to remove it from
existing datasets can call `drop_columns` out of band.

One tradeoff: receipt-based (ingest-age) retention is no longer expressible through this pipeline because there
is no ingest-time column. Retention is by event age only (scalar range filter on `ts_col`). If ingest-age
retention is needed in future, a forthcoming TTL ADR must address it with an explicit design rather than by
restoring `_ingested_at`.

Date-range queries are now unambiguous: a filter such as `timestamp >= T` on the event timestamp column with a
BTREE index gives efficient, semantically correct results for event-time queries. Backfills and retries do not
produce misleading ingest timestamps because no ingest timestamp is recorded.

This supersedes [ADR 0011](0011-ingested-at-column.md).
