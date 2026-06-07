# 0011. The `_ingested_at` ingestion-timestamp column

Status: Superseded by [0016](0016-event-time-canonical-clock.md)

## Context

We want a per-row ingestion-time signal for provenance and incremental bookkeeping. Accuracy can be relaxed: one
value per run is acceptable and the same value across many rows is fine.

This ADR was superseded when the column was removed entirely. The source event timestamp column (`ETLConfig.ts_col`,
default `"timestamp"`) is now the single canonical clock. See [ADR 0016](0016-event-time-canonical-clock.md) for the
full rationale, the tradeoff on ingest-age retention, and the new scalar-range-filter query model.

## Decision

Stamp an `_ingested_at` timestamp column (configurable name, default `_ingested_at`, CLI `--ingested-at-col`)
via `F.current_timestamp()` before collapse and repartition, so it lands in every routed dataset and flows
through merge_insert. `when_matched_update_all` refreshes it on updates. It is a payload column only, excluded
from collapse keys, partition routing, and partition validation. The leading underscore is a data-column string
value (filterable through the gRPC allowlist), not a Python identifier, so the no-leading-underscore code rule
does not apply.

## Consequences

`F.current_timestamp()` is fixed per Spark query, which matches the relaxed accuracy requirement exactly. For
datasets created before the column existed, `merge_insert` rejects a source carrying a column the target lacks,
so an explicit metadata-only `add_columns` runs before the merge. The stable-row-id provenance columns
(`_row_created_at_version`, `_row_last_updated_at_version`) are not a substitute: they are version integers not
timestamps, only populated when stable row IDs are on (rejected in [0010](0010-stable-row-ids-rejected.md)), and
not in the gRPC filter allowlist. So `_ingested_at` is the durable ingestion-time signal.

This decision was reversed in [ADR 0016](0016-event-time-canonical-clock.md). The column is no longer stamped.
All code paths that stamped, evolved, or referenced `_ingested_at` were removed.
