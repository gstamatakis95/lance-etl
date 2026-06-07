# 0003. Incremental Iceberg reads via snapshot-id bounds

Status: Accepted

## Context

The ETL reads a bounded window from an Iceberg table each run. The first implementation passed
`start-timestamp` and `end-timestamp` read options to a plain Iceberg batch scan. Iceberg 1.10 categorically
rejects those options for batch scans: they are valid only for changelog scans. This was verified by inspecting
the resolved runtime jar (`iceberg-spark-runtime-4.0_2.13-1.10.0`) with `javap`, which carries the precondition
string "Cannot set ... for incremental scans and batch scan."

## Decision

Resolve the wall-clock window to snapshot ids before reading. A helper queries the `{table}.snapshots` metadata
table to find the last snapshot committed strictly before the window start (the exclusive lower bound, the state
the previous run already processed) and the last snapshot at or before the window end (the inclusive upper
bound). When a prior snapshot exists, read with `start-snapshot-id` and `end-snapshot-id` as an incremental
append scan. On first run with no snapshot before the window start, fall back to a full batch scan pinned with
`snapshot-id` at the end bound. When the window resolves to no new snapshots, return an empty frame.

## Consequences

Option names were verified against the actual runtime jar (`SparkReadOptions.START_SNAPSHOT_ID` etc.), not
guessed. The orthogonal `--window-start` / `--window-end` / `--window-column` pushdown filter is unchanged and
composes on top. The bench harness drives the real production read path through Iceberg 1.10 end to end. The
edge case where the start and end bounds resolve to the same snapshot is handled by a `has_new_snapshots` flag
so a narrow window does not silently skip data.
