# ETL and data model — architecture decisions

This document consolidates the architecture decisions governing the Iceberg-to-Lance ETL and
the per-dataset data model. Each section keeps its original ADR number so references like
"ADR 0016" in code and docs resolve here. Superseded decisions are summarized at the end.

## ADR 0003 — Incremental Iceberg reads via snapshot-id bounds

Status: Accepted

Iceberg 1.10 categorically rejects `start-timestamp` / `end-timestamp` read options on batch
scans (they are valid only for changelog scans, verified against the resolved runtime jar). The
ETL therefore resolves each wall-clock window to snapshot ids before reading: a helper
(`snapshot_id_bounds` in `etl/job.py`) queries the `{table}.snapshots` metadata table for the
last snapshot strictly before the window start (exclusive lower bound) and the last snapshot at
or before the window end (inclusive upper bound), then reads with `start-snapshot-id` /
`end-snapshot-id` as an incremental append scan. The first run with no prior snapshot falls back
to a full batch scan pinned with `snapshot-id` at the end bound. A window that resolves to no new
snapshots returns an empty frame, gated by a `has_new_snapshots` flag so a narrow window whose
bounds resolve to the same snapshot does not silently skip data. The orthogonal
`--window-start` / `--window-end` / `--window-column` pushdown filter composes on top.

## ADR 0004 — Routing targets and duplicate semantics

Status: Accepted (dynamic partition targets later fixed to the trio)

Routing is the fixed trio `org_id/tenant_id/namespace`: every row maps to exactly one dataset at
`{base}/{org_id}/{tenant_id}/{namespace}.lance` with no cross-org sharing. `dataset_uri` (in
`etl/sink.py`) validates every path component, and dataset discovery for indexing and
maintenance is a recursive `*.lance` glob. Because each key lives in exactly one dataset, the
per-dataset `merge_insert` keyed on `key_col` is the sole dedup mechanism.

Two pieces of the original decision were later removed. The `partition_derivations` /
`--partition-derive` strftime machinery went away with by-date partitioning (ADR 0014), and the
configurable `partition_cols` knob on the ETL was fixed to the trio (a `--partition-by` flag
survives only on the `migrate-namespace` operator tool). The single-org contamination guard that
once ran in maintenance was also removed in the unified-orchestration rework.

## ADR 0016 — Event-time canonical clock

Status: Accepted (supersedes ADR 0011)

The source event timestamp column (`ETLConfig.ts_col`, now defaulting to `"event_timestamp"`
per ADR 0024) is the single canonical clock. The `_ingested_at` ingestion-timestamp column was
removed entirely: maintaining two time columns created a second, derived time axis that drifted
from the event axis on retries and backfills, and event-time range queries are naturally scalar
range filters on the event timestamp (pruned by a BTREE index) rather than filters on ingest
time. Existing datasets carrying the column keep working, the column simply is not written or
updated. The accepted tradeoff: ingest-age retention is not expressible, retention is by event
age only (the per-row TTL design measures against the event clock).

## ADR 0024 — Dynamic per-dataset map pivot: every key becomes a column

Status: Accepted (supersedes ADR 0020)

Every distinct key present in a routing group's `vectors`, `texts`, and `metadata` maps becomes
a concrete column in that group's dataset. The pivot (`pivot_map_columns` in `etl/pivot.py`)
runs per dataset on the executor after the shuffle collocates rows, so each org's schema
contains only the keys that org actually uses — no cross-org schema pollution across a
power-law fleet of 30k+ orgs, and no operator enumeration of searchable fields at deploy time.

Mechanics that still govern the code:

- Keys colliding with an existing or reserved column are skipped and metered
  (`dataset.invalid_map_keys`) rather than failing the job.
- Vector map values arrive as `list<float32>` (contract `ARRAY<FLOAT>`), the fixed-size-list
  dimension is inferred from the first non-null entry, float64 is normalized to float32, and an
  all-null vector column stays a nullable list with no FSL cast.
- Text and metadata keys become `string` columns — real, filter-eligible, scalar-index-ready.
- New keys in later windows are absorbed by grow-only `add_columns` schema evolution. No code
  path ever removes a dataset column.
- The input schema carries no type uncertainty: `docs/iceberg-source-table.sql` is the single
  typed contract, `validate_schema` verifies it fully, and all casts are contract-driven. The
  string-spec type parsing (`arrow_types.py`, `ETLConfig.column_types`) was deleted.
- The TTL column (`ttl`, `BIGINT` seconds) is cast automatically to `pa.duration("s")` so the
  maintenance delete predicate `event_timestamp + ttl < now` evaluates natively.
- Defaults align with the SQL contract: `ts_col="event_timestamp"`,
  `window_column="processing_timestamp"`.

## ADR 0032 (ETL half) — Hourly interval tags at write time

Status: Accepted

Every ETL run stamps each dataset it wrote with a Lance tag named after the truncated UTC hour
it was produced (`%Y%m%dT%H%M%SZ`, via `cliutil.parse_hour_tag`, wired as `--tag-stamp` and
passed `{{ data_interval_end }}` by the Airflow DAG). The stamp runs once on the driver after
all batches commit (`IcebergToLanceETL.stamp_interval_tags`) and is create-or-move: a later run
within the same hour advances that hour's tag to the newest version, so a tag always marks the
latest version produced in its hour. Tagged versions are exempt from version cleanup until the
pipeline prunes old interval tags. The query-pinning half of ADR 0032 lives in
`serving-filters-and-tags.md`.

## Superseded decisions

- **ADR 0011 — `_ingested_at` ingestion-timestamp column.** Superseded by ADR 0016. The column
  and its stamping machinery were removed.
- **ADR 0020 — Static declared-field map pivot.** Superseded by ADR 0024. The
  `vector_fields` / `text_fields` declarations, the `--vector-field` / `--text-field` /
  `--column-type` flags, and the positional `metadata_keys` / `metadata_values` arrays were all
  removed.
