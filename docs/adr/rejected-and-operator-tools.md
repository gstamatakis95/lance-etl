# Rejected decisions and operator tools — architecture decisions

This document consolidates the standing rejections and the operator-facing utilities. Each
section keeps its original ADR number so references like "ADR 0010" resolve here. The stable
row ID rejection comes first because it is an active prohibition (AGENTS.md hard rule 8), kept
at close to full length.

## ADR 0010 — Move-stable row IDs (REJECTED)

Status: Rejected (2026-06-07)

Lance offers move-stable row IDs (`enable_stable_row_ids` at dataset creation). With them a row
keeps a fixed logical row id across compaction, `needs_remapping` derives to false, and
compaction skips inline index remap entirely — deleting a whole class of workarounds. The flag
was implemented as an opt-in and the upside was proven: on a stable-row-id dataset both
compaction paths preserved IVF_RQ, BTREE, and FTS search with no index rebuild and unchanged
index segment UUIDs, with a negative control confirming that without the flag the remap
genuinely runs.

It is nonetheless REJECTED, not deferred. The production pattern — `merge_insert` plus `delete`
plus concurrent compaction — trips the stable-row-id `RowIdIndex` overlapping-chunk invariant
(`rust/lance-table/src/rowids/index.rs`). In a debug build it panics. In a release build the
guard is compiled out and the index is built from overlapping chunks anyway, risking a SILENTLY
WRONG row-id-to-address mapping, which is worse than a crash. The failure reproduced both in
the coexistence stress test with the flag on and in a minimal standalone reproducer. Lance's
own tests do not exercise concurrent merge plus compaction on stable-row-id datasets, which
points to an unexercised upstream gap.

Because the remap problem is otherwise handled (the orphan-race guard, see
`fleet-orchestration-and-maintenance.md`, ADR 0009) and the risk is silent data corruption on
release builds, the trade is not worth it. Do not add `enable_stable_row_ids=True` to any
dataset creation or compaction path, and do not offer it as an option. If the upstream
`RowIdIndex` defect is ever fixed, revisiting this requires a fresh decision record, not a
revival of this one.

## ADR 0012 — V2 manifest paths fleet-wide

Status: Accepted

V1 names manifests so that finding the latest version costs a directory LIST that grows with
version count — a large avoidable cost across 30k datasets opened repeatedly. Every dataset is
therefore created with `enable_v2_manifest_paths=True`, making every open one object-store
request regardless of history depth. The flag is honored only at bootstrap and is mandatory.
Legacy V1 datasets are rebuild-only because durable source state can reproduce them. The former
non-transactional in-place migration command was removed. The serving-side consequence is that the
byte cache must never cache the V2 latest-version hint file (see `caching-and-observability.md`).

## ADR 0015 — CLI and config knob reduction: opinionated defaults

Status: Accepted (specific constant lists have since evolved with the code)

Every knob is a surface-area cost: it appears in help text, needs documentation and tests, and
creates an implicit operator contract. Knobs no deployment varies are a maintenance liability.
The standing policy:

- Universally correct values are module-level constants with docstrings explaining why they are
  baked, not config fields or env vars. Still-live examples: `num_bits=1` (the only value
  IVF_RQ accepts), `compaction_mode="try_binary_copy"`, the path-component allowlist (a
  security invariant that must not be weakenable by config), and the Rust constants for the
  cache TTL, sweep interval, byte-cache max range, IO block size, and object-store timeout.
- Schema column names and tokenizer toggles are dataclass fields tunable in code, never CLI
  flags — wrong per-invocation values would break ingestion semantics or tokenizer consistency.
- Retry budgets have one canonical home: `DEFAULT_CONFLICT_RETRIES`, `DEFAULT_COMMIT_RETRIES`,
  and `DEFAULT_LARGE_COMMIT_RETRIES` in `telemetry.py`.

The original tier-1 list included training constants (`TRAIN_SAMPLE_RATE`, `TRAIN_MAX_ITERS`)
that were later deleted with the in-heap trainer (ADR 0030 replaced them with streaming
parameters on `IndexJobConfig`), and `INGESTED_AT_COLUMN` was removed with the column
(ADR 0016). The policy outlived the specific lists.

## ADR 0017 — Rust intake service with a pluggable record sink

Status: Superseded (2026-07-13)

The placeholder-only `IntakeService`, its `RecordSink` abstraction, and `StdoutSink` were removed
before release. Accepting a write over gRPC without a durable destination could report success
without creating replayable source state. Iceberg is now the only durable ingestion source. A
future online-write design requires a fresh ADR with a real durable transport, idempotency keys,
and reconciliation semantics before any public write RPC is added.

## ADR 0019 — Namespace copy/migrate utility

Status: Superseded (2026-07-22)

The copy-plus-optimize namespace migration library and CLI were removed. Namespace changes now
create a new immutable specification revision and rebuild targets from the durable Iceberg source.
This keeps one ingestion, compaction, and indexing path and avoids a second data-copy engine with
its own overwrite, sharding, and partial-failure semantics.
