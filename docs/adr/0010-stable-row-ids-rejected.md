# 0010. Move-stable row IDs

Status: Rejected (2026-06-07)

## Context

Lance offers move-stable row IDs (`enable_stable_row_ids` at dataset creation). With them, a row keeps a fixed
logical row id across compaction and only its physical address moves, so `needs_remapping` is derived from the
manifest as false and compaction skips inline index remap entirely. That promised to delete a whole class of
workarounds: the IVF_RQ coverage-tracking mitigation, the dead-fragment handling, and the cost of inline remap
on every compaction commit. We implemented it as an opt-in flag and proved the upside.

## Decision

Reject move-stable row IDs. Remove the flag and all wiring. Do not offer it even as an option.

## Consequences

The structural win was real and verified: on a stable-row-id dataset, both the distributed tier-B
`Compaction.commit` path and the small-tier `Compaction.execute` path preserved IVF_RQ, BTREE, and FTS search
with no index rebuild, unchanged index segment UUIDs, and no Fragment Reuse Index. Recall even improved as
sharded segments merged. A negative control confirmed that without the flag, remap genuinely runs (UUIDs
change).

It is nonetheless unsafe on the pinned Lance build under our actual workload. The production pattern
(`merge_insert` + `delete` + concurrent compaction) trips the stable-row-id `RowIdIndex` overlapping-chunk
invariant at `rust/lance-table/src/rowids/index.rs`. In a debug build it panics. In a release build
(`debug-assertions=false`) the guard is compiled out and the index is built from overlapping chunks anyway,
risking a silently wrong row-id to address mapping, which is worse than a crash. This was reproduced both in
`test_concurrent_coexistence` with the flag on and in a minimal standalone reproducer. Lance's own tests do not
exercise concurrent merge plus compaction on stable-row-id datasets, which points to an unexercised upstream gap.

Because the remap problem is otherwise handled (see [0009](0009-compaction-index-coexistence.md)) and the risk
is silent data corruption on release builds, the trade is not worth it. The decision is rejection, not
deferral. If the upstream `RowIdIndex` defect is ever fixed, revisiting this is a fresh ADR, not a revival of
this one.

The evaluation evidence is preserved in `market-research/stable-row-ids-plan.md` and
`market-research/stable-row-version-columns.md`. The latter also records why the stable-row-id provenance
columns (`_row_created_at_version`, `_row_last_updated_at_version`) do not substitute for
[0011](0011-ingested-at-column.md): they are version integers not timestamps, only exist when the flag is on,
and are not in the gRPC filter allowlist.
