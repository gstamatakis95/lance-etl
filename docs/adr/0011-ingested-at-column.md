# 0011. The `_ingested_at` ingestion-timestamp column

Status: Accepted

## Context

We want a per-row ingestion-time signal for provenance and incremental bookkeeping. Accuracy can be relaxed: one
value per run is acceptable and the same value across many rows is fine.

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
