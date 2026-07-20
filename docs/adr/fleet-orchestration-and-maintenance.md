# Local reconciliation and maintenance decisions

This document preserves the accepted execution and maintenance decisions while recording that the
current runtime is one local PostgreSQL-backed reconciler. Historical scheduler-specific state and
remote submission layers are superseded by ADR 0042.

## Current execution model

The local process owns one bounded cycle:

1. Read the registered source and settings from PostgreSQL.
2. Plan exact Iceberg snapshot transitions and persist dataset work.
3. Claim dataset-disjoint work with a lease and fence.
4. Run ingestion, compaction, indexing, qualification, and exact-version prewarm through local
   Spark executors.
5. Publish immutable evidence and atomically switch the active pointer.
6. Sweep retained publications and source audit evidence in bounded batches.

PostgreSQL is the only scheduler state. Spark is created and stopped by the process. One open lane
per dataset prevents conflicting mutations while allowing independent datasets to progress.

## ADR 0002 — Two-tier compaction orchestration

Status: Superseded by ADR 0028 and ADR 0042

The useful result survives. Small compactions may use the direct Lance optimization path. Large
compactions may plan tasks, execute them in parallel, and commit the result. Both now run as the
`COMPACT` phase of frozen dataset work rather than as separate scheduled jobs.

## ADR 0009 — Stale index-plan safety

Status: Accepted

Index segments built against fragments that a rewrite removes cannot be committed blindly. Index
commit code must detect stale plans and rebuild from current fragments. The specification revision
sets `max_stale_replans`. Dataset fencing makes concurrent ingest, compaction, and indexing on the
same dataset unsupported by construction. Commit-conflict handling still protects against an
unexpected external Lance writer.

BTREE and BITMAP segment results commit unmerged. ZONEMAP segments merge before commit. IVF_RQ
segments reuse preserved centroids and their RaBitQ model. INVERTED shards use one shared index UUID
and an atomic metadata swap. These rules are normative in the root `AGENTS.md`.

## ADR 0018 — Retention-window expiry

Status: Accepted (supersedes the original per-row TTL field decision)

Retention is a window on the immutable dataset specification revision (`record_retention_seconds`),
not a per-row column. When the window is set, maintenance derives expiry from the `ts` column plus
the window and applies tombstones before compaction, deleting every row whose `ts` is before now
minus the window. `materialize_deletions` and `materialize_deletions_threshold` control when
physical deletion materialization occurs. The data model keeps source truth and physical cleanup
separate.

Tombstone rows carry the delete mutation's event `ts` and expire on a separate clock from live
rows. A live row is deleted at `ts < now - record_retention_seconds` as above. A tombstone is
deleted only once its `ts` is past both the record-retention window and the source replay horizon,
that is `ts < now - max(record_retention_seconds, replay_horizon_seconds)`. A window that could
resurrect a deleted row carries an older-or-equal source sequence, so keeping the tombstone until it
is past the replay horizon preserves its source-sequence anti-resurrection watermark for every
window a replay could still re-apply. This makes a tombstone strictly longer-lived than the live row
it shadowed. The bound is exact only under the assumption that event `ts` tracks ingest time, since
the replay horizon is enforced on the control-plane snapshot ingest time while retention runs on
event `ts`. That divergence is a pre-existing property of event-`ts` retention and is not introduced
here. When the replay horizon is unbounded the predicate never matches a tombstone, so tombstones
are retained forever rather than risk a premature GC. Before this decision tombstones carried a null
`ts` and never expired at all, so `record_retention_seconds` silently failed to bound storage on
delete-heavy datasets.

## ADR 0023 — Iceberg source-table maintenance

Status: Accepted as a reusable local library

Iceberg file rewrite and metadata cleanup remain useful, but they are not part of the Lance serving
transaction. Operators may run the local `iceberg_optimize.py` library separately. Source
qualification trusts only exact snapshot lineage and explicitly accepted maintenance operations.

An Iceberg cleanup must retain the oldest snapshot required by unfinished `source_snapshots` and
`dataset_work`. Wall-clock schedule intervals do not define source progress.

## ADR 0026 — Job isolation

Status: Superseded for invocation

The ETL, indexing, maintenance, and recall packages remain separable libraries with focused tests.
The installed write-path command is now only `lance-etl-reconcile`. Legacy library CLIs do not own
durable progress and are not scheduled independently.

## ADR 0027 — Unified pipeline

Status: Superseded by local reconciliation

The useful phase ordering is retained inside one work row:

```text
INGEST -> COMPACT -> INDEX -> VALIDATE -> PREWARM -> PUBLISH
```

A phase transition is durable and fenced. A retry resumes the same deterministic work identity.
Qualification or prewarm failure never changes the active publication.

## ADR 0028 — Task-based execution and column roles

Status: Execution model accepted, control schema superseded by ADR 0042

Heavy operations use task planning and executor-side work so driver memory scales with snapshot
metadata and touched datasets rather than source rows. The immutable spec retains semantic field
roles and typed per-family index definitions. The former fleet queue model is replaced by
`source_snapshots`, first-class `datasets`, and `dataset_work`.

## ADR 0035 — Per-dataset failure isolation

Status: Accepted

A failed dataset enters retry wait or blocked state without discarding successful work for another
dataset. Source completion waits for every dataset affected by that source snapshot. Status reports
due work, old open work, blocked snapshots, and retention age from PostgreSQL.

## ADR 0036 — Idle-dataset cleanup rotation

Status: Accepted through bounded retention

Cleanup is work-conserving and bounded. The local cycle retains the active publication, open-work
candidates, required historical publications, and source replay floor. `retained_publications`,
`artifact_retention_seconds`, `cleanup_older_than_seconds`, `retain_versions`,
`audit_retention_seconds`, and `cleanup_batch_size` are the governing PostgreSQL values.

## ADR 0037 — Discoverable parallelism

Status: Accepted with PostgreSQL-owned bounds

Parallelism is explicit and bounded. Ingestion uses `ingest_shuffle_partitions`. Index fan-out uses
`fragments_per_index_task`. IVF_RQ partition count is explicit or derived from row count and clamped
between `minimum_partitions` and `maximum_partitions`. Compaction may set a thread limit. Spark
starts with a small code-owned local default. Frozen work uses the specification revision selected
during planning for data-path parallelism.

## ADR 0038 — Ingestion and maintenance must not overlap per dataset

Status: Superseded by PostgreSQL dataset lanes

The invariant remains. A unique partial index permits at most one `RUNNING` row per dataset.
Claiming advances the dataset fence, and all heartbeats and completion transitions compare the
lease token plus fence epoch. Separate lock files and timing conventions are no longer used.

## ADR 0039 — Commit retry and idempotency hardening

Status: Accepted

Every Lance merge, index commit, and compaction commit uses `commit_with_retries`, which reopens the
latest dataset version before each attempt. Retryable PostgreSQL work uses bounded backoff and a
maximum attempt count. Replayed source mutations carry a source sequence and event digest. The
completion marker resolves a crash after a Lance commit and before a PostgreSQL transition.

Large compaction conflicts require re-planning and re-execution. Repeating a stale compaction
commit cannot make the plan current.

## ADR 0041 — Clustered rewrite

Status: Accepted

A clustered rewrite may reorganize fragments by IVF assignment for locality. It is an explicit
maintenance operation inside a rebuild work item. The rewrite preserves trained centroids and the
RaBitQ model, drops the invalidated IVF_RQ index through the overwrite, then rebuilds segments from
the preserved artifacts without training again.

The candidate remains private until schema, rows, fragments, and every required index validate.
Local exact-version prewarm must succeed when the frozen revision requires it. Publication then
appends immutable evidence and changes the active pointer atomically.

## Operational invariants

- Never infer source progress from timestamps or numeric snapshot ordering.
- Never open Lance on the driver for row-level work.
- Never run two mutating phases for the same dataset concurrently.
- Never publish a candidate without exact spec and index evidence.
- Never let cleanup remove an active publication, open-work candidate, or source replay anchor.
- Never enable move-stable row IDs.
- Never reuse a retired Lance URI for an unrelated new dataset while URI-keyed caches may exist.
