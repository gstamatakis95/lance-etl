# `lance_etl.maintenance`

`maintenance/` owns retention expiry, unified compaction, Lance version cleanup, the opt-in
clustered rewrite, and the fleet upkeep libraries (blue-green serving tags and interval-tag pruning)
that back the `COMPACT` phase of the reconciler's per-dataset work item. There
is one code path for every dataset size — `MaintenanceJob` plans, executes, and commits the same way
for a one-fragment dataset and a thousand-fragment one. For where compaction sits in the
reconciliation cycle (`INGEST -> COMPACT -> INDEX -> VALIDATE -> PREWARM -> PUBLISH`), see the
package [README](../README.md). Vector/scalar/FTS index builds themselves — including the index
rebuild that clustered rewrite triggers — live in the sibling [`indexing/`](../indexing/README.md)
package. This README covers only what `maintenance/` does with fragments, deletions, and versions.

## Module-by-module

| File | Responsibility |
|---|---|
| `job.py` | `MaintenanceConfig`, `MaintenanceJob`: retention expiry, the plan/execute/commit compaction split, version cleanup, and the commit-conflict replan loop |
| `cluster.py` | Clustered rewrite: IVF-centroid-locality fragment reorganization and the preserved-centroid vector index rebuild ([ADR 0041](../../../docs/adr/fleet-orchestration-and-maintenance.md)) |
| `tools.py` | Fleet-wide blue-green serving-tag flips and interval-tag pruning |
| `__init__.py` | Re-exports the consumer surface from `job.py`, `cluster.py`, and `tools.py` |

## Retention expiry

Retention is a window on the frozen dataset specification revision
(`record_retention_seconds`), not a per-row column — [ADR 0018](../../../docs/adr/fleet-orchestration-and-maintenance.md).
`MaintenanceConfig.retention_seconds: int | None` carries that window into this package. `None`
(the default) disables expiry entirely.

`retention_predicate(config, cutoff)` (`job.py`) builds the delete predicate:

- With no `deleted_column` configured, the predicate is the plain `ts_column < cutoff`.
- With `deleted_column` set, live rows and tombstones expire on separate clocks. A live row is
  deleted at `ts < now - retention_seconds` as before. A tombstone — a row whose `deleted_column` is
  true — is deleted only once its `ts` is past both `record_retention_seconds` and
  `replay_horizon_seconds`, i.e. `ts < now - max(retention_seconds, replay_horizon_seconds)`. A
  tombstone carries the delete mutation's own event `ts` precisely so this predicate has something
  to expire it on. When `replay_horizon_seconds` is `None` the tombstone clause never matches, so
  tombstones are retained forever rather than risk resurrecting a row a late replay could still
  legitimately re-apply. `MaintenanceConfig.deleted_column` is the opt-in switch. Standalone
  operator callers leave it unset and keep the plain predicate.

`compute_cutoff` derives the fixed cutoff timestamp once per run. `run_retention_on_open_dataset`
validates the `ts` (and `deleted_column`, if set) column exists, then runs
`dataset.delete(predicate, conflict_retries=...)` through `commit_with_retries` with
`config.commit_retries` (default `DEFAULT_COMMIT_RETRIES`, 20, from `telemetry.py`). `plan_one_dataset`
calls this only in round 0 of a run — later replan rounds pass no cutoff, so retention applies
exactly once per fleet run regardless of how many compaction replan rounds follow.

## Unified compaction: plan / execute / commit

`MaintenanceJob` runs the same three phases for every dataset, driver-scheduled and
executor-executed:

| Phase | Function | Where it runs |
|---|---|---|
| **Plan** | `plan_one_dataset` | Executor, one task per dataset, via the per-dataset fan-out helper |
| **Execute** | `execute_rewrite_task` (scheduled by `MaintenanceJob.execute_fleet_tasks`) | Executor, one flat Spark job across every dataset's tasks together |
| **Commit** | `commit_one_dataset` (scheduled by `MaintenanceJob.commit_fleet`) | Executor, `mapPartitions` per dataset |

`plan_one_dataset` opens the dataset once, runs retention (round 0 only), skips planning when the
derived-state check finds nothing to do (`compaction_skip_reason`: a single fragment with zero
deletions never needs a plan), then calls `Compaction.plan(dataset, options=config.execute_options())`
and serializes every `task.json()` for the execute phase. `execute_rewrite_task` reopens the dataset
at the plan's pinned `read_version` and runs `CompactionTask.from_json(task_json).execute(dataset)` —
every dataset's tasks are flattened into one Spark job rather than one job per dataset, so a small
dataset's task doesn't wait behind a large dataset's whole plan. `commit_one_dataset` runs
`Compaction.commit(dataset, rewrites, options=config.execute_options())` inside `commit_with_retries`
using the small `large_commit_retries` budget, and on success runs version cleanup inline.

`MaintenanceJob.run` (driver) and `run_round` (driver) hold no dataset handles themselves — every
`lance.dataset(...)` open happens inside an executor closure, per root `AGENTS.md` hard rule 5.

`MaintenanceConfig.execute_options()` builds the Lance compaction options dict from these fields:

| Field | Default | Meaning |
|---|---|---|
| `target_rows_per_fragment` | `1_048_576` | Matches lance's own `CompactionOptions` default explicitly, so the pin survives an upstream default change |
| `materialize_deletions` | `True` | Whether compaction physically drops deleted rows |
| `materialize_deletions_threshold` | `MATERIALIZE_DELETIONS_THRESHOLD` (lance default, 0.1) | Deleted-row fraction that makes a fragment eligible for rewrite |
| `compaction_mode` | `COMPACTION_MODE` (`"try_binary_copy"`) | Falls back to reencode per task rather than erroring on a deletion-bearing fragment, unlike `force_binary_copy` |
| `max_source_fragments` | `256` | Cap on fragments consumed per incremental compaction run. `None` is unbounded, `0` is rejected |
| `num_threads` | `None` | Worker threads inside one rewrite task |
| `defer_index_remap` | `False` | When `True`, added to the options dict passed to `Compaction.commit`, which builds the `__lance_frag_reuse` system index at commit time instead of remapping every index immediately |

## The two-layer commit-conflict handling

**Layer 1 — inside `commit_with_retries` (`telemetry.py`).** Lance's own inner
`execute_with_retry` already retries a `RetryableCommitConflict` internally before ever surfacing an
exception, and converts exhaustion into `TooMuchWriteContention` (which `is_commit_conflict_error`
does not treat as a conflict marker). `commit_with_retries` is strictly complementary: it catches the
non-retryable `Error::CommitConflict` variant that Lance's inner loop returns straight through, and
it covers commit types with no inner Lance retry loop at all — `Compaction.commit` and every
segment-index commit in `indexing/` — by re-opening and re-reading the dataset before each of its
own attempts.

**Layer 2 — the maintenance-specific replan (`commit_one_dataset`, `job.py`).**
`Compaction.commit`'s conflict scan is pinned to the plan's `read_version`, so a *semantic* conflict
(another writer touched the same fragments) fails deterministically on every retry — only the raw
manifest-write race benefits from retrying at all, which is why `large_commit_retries` is kept small
(`DEFAULT_LARGE_COMMIT_RETRIES`, 2). When `commit_with_retries` exhausts and
`is_commit_conflict_error` still holds, `commit_one_dataset` catches it and returns
`{"uri": uri, "conflict": True}` instead of raising. `MaintenanceJob.run_round` collects these into a
`conflicted` list and `MaintenanceJob.run` feeds it back into the next round's plan phase — up to
`REPLAN_BUDGET` rounds (3) — so phase P re-plans against the version the conflicting writer left
behind. A dataset still conflicted after every round is deferred to the next scheduled run as "hot"
(the `dataset.hot_skipped` metric fires), receiving only an idle-version cleanup pass
(`cleanup_hot_dataset`) rather than a forced compaction commit.

## Version cleanup

`cleanup_dataset` calls `dataset.cleanup_old_versions(older_than=timedelta(seconds=
config.cleanup_older_than_seconds), retain_versions=config.retain_versions,
error_if_tagged_old_versions=False)`. The default `cleanup_older_than_seconds` (`216_000`, 60 hours)
deliberately exceeds the interval-tag retention window (hourly tags plus slack) so a run that prunes
the oldest interval tag never also reclaims the exact version that tag pinned while a replica is
still reading it — `None` defers to lance's own 14-day default.
`MIN_CLEANUP_HORIZON_SECONDS` (6 hours) is a hard floor: `validate_cleanup_horizon` rejects any
configured value below it eagerly, at the top of `MaintenanceJob.run`, failing the whole run fast
rather than corrupting one dataset silently — cleanup is not transactional, so the floor must exceed
the longest concurrent job. `error_if_tagged_old_versions=False` means a tagged version (the
blue-green `HEAD` tag, or an interval tag) is silently skipped from cleanup regardless of age.

Idle-dataset cleanup is additionally rotation-gated
([ADR 0036](../../../docs/adr/fleet-orchestration-and-maintenance.md)): `dataset_cleanup_slot` hashes
each dataset URI with SHA-256 (not the builtin `hash()`, which is per-process salted and would make
the slot assignment unstable across runs) into a fixed slot in `range(cleanup_rotation_slots)`
(default 8), and `active_cleanup_slot`/`should_clean_idle` advance the active slot every
`cleanup_rotation_cadence_hours` (default 1). A dataset that did real work this run — retention
deleted rows, or compaction ran — is always cleaned regardless of its rotation slot. Only a
genuinely idle dataset waits for its slot to come around.

## Clustered rewrite (`cluster.py`, ADR 0041)

Clustered rewrite is an opt-in (`MaintenanceConfig.cluster_rewrite`, default `False`),
non-transactional, maintenance-window full dataset reorganization: it reorders rows so rows sharing
the same IVF centroid land contiguously in the same fragment, which Lance has no built-in compaction
mode for. It requires the targeted datasets to be quiesced for its duration and runs before normal
compaction — a dataset that gets clustered this run skips normal compaction in the same run.

`run_cluster_rewrites`, orchestrated by `ClusterRunState`, runs the full pipeline per eligible
dataset:

1. **Plan** (`plan_cluster_rewrite`, executor fan-out): resolves the single vector-role column (or
   the explicit `cluster_column` override), checks eligibility via `cluster_guard_reason` — a
   committed vector index, a stored vector config carrying `rows_at_train` and `rabitq_model`, and a
   non-empty dataset are all required — and skips a dataset that is already clustered and unwritten
   since its last rewrite (`cluster_generation_skip_reason`, keyed on a `lance-etl.cluster_generation`
   config-KV fingerprint of sorted fragment ids plus row count). Retention runs here too, and
   centroids are resolved sidecar-first via `resolve_cluster_centroids`, falling back to
   `dataset.get_ivf_model(index_name)` with a sidecar backfill — the same centroid-resolution pattern
   `indexing/optimize.py` uses.
2. **Histogram** (`partition_histogram`, one flat job per bounded dataset batch): assigns each row's nearest centroid
   (`assign_partition_ids`, replicating Lance's own IVF nearest-centroid assignment for l2, cosine,
   and dot) in blocks of `ASSIGN_BLOCK_ROWS` (65536), then reduces shard histograms by dataset on
   executors before collecting one bounded count vector per dataset.
3. **Bucket derivation** (`derive_buckets`/`derive_global_buckets`, driver): packs the histogram into
   contiguous write buckets capped at `target_rows_per_fragment` rows, salting an oversized single
   partition into multiple sub-buckets.
4. **Rewrite shuffle** (`run_rewrite_shuffle`, one flat job per bounded dataset batch): reads full rows per shard, tags each
   with a temporary partition column, streams IPC chunks under one combined per-shard buffer cap,
   and a `partitionBy` shuffle co-locates rows by contiguous partition-range bucket. `write_bucket`
   decodes one chunk at a time directly into `write_fragments(mode="overwrite", ...)`, producing
   uncommitted fragment metadata without bucket-wide concatenation or sorting. Nothing is committed yet.
5. **Commit overwrite** (`commit_cluster_overwrite`, per-dataset executor fan-out):
   `LanceDataset.commit(uri, LanceOperation.Overwrite(schema, fragments), read_version=..., ...)`
   through `commit_with_retries` with `large_commit_retries`. **This commit preserves version
   history, tags, and the dataset config KV (column roles and the stored vector config) but drops
   every index** — Overwrite always does. The committed fragment and row fingerprint is retained
   for the finalisation phase.
6. **Vector index rebuild** (`rebuild_indexes` -> `build_cluster_index_segment`, one flat job plus
   per-dataset finalize fan-out): rebuilds ONLY the IVF_RQ vector index, reusing the exact
   preserved-artifact tuple (`centroids`, `rabitq_model`, `num_bits`, `num_partitions`) that
   `VectorIndexHandler.prepare` in `indexing/handlers.py` produces — no retraining — via the same
   `build_vector_segment` / `commit_segments` calls documented in the
   [indexing README](../indexing/README.md#vector-ivf_rq), i.e.
   `create_index_uncommitted` per shard, `merge_existing_index_segments`, then
   `commit_existing_index_segments`. Only after that commit succeeds is the preserved data
   fingerprint stamped as the current clustered generation. A rebuild failure therefore cannot
   leave an unindexed generation marked current.

BTREE, BITMAP, ZONEMAP, and FTS indexes are **not** touched by clustered rewrite — they are dropped
by the Overwrite in step 5 and left for the next scheduled `LanceIndexer` run in `indexing/` to
rebuild. `cluster.py` never flips a serving tag itself. That stays with the reconciler's own
publish/prewarm phase, so a text query never sees a clustered-but-unindexed generation served before
every required index (not just vector) has been rebuilt and validated.

Failure isolation applies at every phase (plan, histogram, rewrite, commit, rebuild) per dataset. A
failed index rebuild specifically is non-fatal to the rewrite itself: data stays committed and
clustered, just temporarily unindexed, marked with `{"error", "phase": "cluster_index"}` for the next
run to pick up.

## `tools.py`: serving tags and interval tags

| Function | Purpose |
|---|---|
| `update_serving_tag` / `update_serving_tags` | Flips one or more named tags (default `("HEAD",)`) to an explicit target version via `flip_one_tag` -> `resolve_serving_tag`, a create-or-update with a single-level lost-race fallback (`TAG_EXISTS_MARKER`/`TAG_MISSING_MARKER` string matching on the `ValueError` lance raises, since tag mutation is a plain object-store put/delete rather than an optimistic-concurrency manifest commit, so `commit_with_retries` does not apply here) |
| `prune_interval_tags` | Classifies tags by `datetime.strptime(name, "%Y%m%dT%H%M%SZ")`, leaves non-matching tags (like `HEAD`) untouched, and deletes everything but the newest `tag_keep_last` interval tags, idempotent against a tag already missing |

`update_serving_tag` requires an explicit `target_version` for `HEAD` (it raises otherwise) and its
docstring is explicit about the mandatory blue-green sequence: build the green version, prewarm the
serving layer against that explicit version, only then flip the tag — a tag move alone never
refreshes a running serving process's cache.

## Invocation

The package has no standalone write CLI. `ConfiguredPublicationRunner.run` in
`reconciler/workers.py` instantiates `MaintenanceJob(MaintenanceConfig(...))` directly whenever
`spec.compaction_enabled`, driven by the durable PostgreSQL work item — see the
[publication README](../publication/README.md) for how that fits into the publish workflow.
`cluster.py`'s `run_cluster_rewrites` is reached only through `MaintenanceJob.run` when
`MaintenanceConfig.cluster_rewrite` is set. Nothing in the reconciler imports `cluster.py` directly.

## Forbidden operations and invariants

- **No stable row IDs.** No function in this package passes `enable_stable_row_ids`. See root
  [`AGENTS.md`](../../../AGENTS.md) hard rule 8 and
  [ADR 0010](../../../docs/adr/rejected-and-operator-tools.md). ADR 0041 restates it explicitly as an
  operational invariant for clustered rewrite specifically.
- **Version cleanup must respect `MIN_CLEANUP_HORIZON_SECONDS`.** Do not lower the floor below what
  the longest concurrent job needs — cleanup is not transactional.
- **A semantic compaction conflict must re-plan, not blindly retry.** Do not raise
  `large_commit_retries` as a substitute for the replan loop — `Compaction.commit`'s conflict scan is
  pinned to the plan version, so retrying the same stale plan can never converge.
- **Clustered rewrite must reuse preserved centroids and the stored RaBitQ model, never retrain.**
  Retraining during a clustered rewrite would silently produce a different rotation than the one
  other shards or a concurrent incremental build assume.
- **Clustered rewrite must not flip a serving tag or claim other indexes are rebuilt.** Only the
  vector index is rebuilt in `cluster.py`. Promotion is the reconciler's publish/prewarm
  responsibility.
- **Driver plans, executors mutate Lance.** No function in this package opens a dataset outside an
  executor closure — root `AGENTS.md` hard rule 5.

## Testing pointers

| Test file | Covers |
|---|---|
| `tests/test_maintenance.py` | Core `MaintenanceJob` plan/execute/commit flow |
| `tests/test_maintenance_fri.py` | `defer_index_remap` / frag-reuse behavior |
| `tests/test_maintenance_replan.py` | The commit-conflict replan loop and `REPLAN_BUDGET` |
| `tests/test_compaction_deletion_skip.py` | `compaction_skip_reason` derived-state skip logic |
| `tests/test_index_maintenance.py` | Interaction between compaction and index maintenance |
| `tests/test_index_replan_guard.py` | Stale-fragment interaction between compaction and indexing |
| `tests/test_cleanup_rotation.py` | The idle-dataset rotation-slot cleanup gate |
| `tests/test_validate_cleanup_horizon.py` | `MIN_CLEANUP_HORIZON_SECONDS` enforcement |
| `tests/test_cluster_assignment.py` | `assign_partition_ids` nearest-centroid assignment |
| `tests/test_cluster_rewrite.py` | The full clustered-rewrite pipeline end to end |
| `tests/test_serving_tag.py`, `tests/test_serving_tag_idempotency.py` | `update_serving_tag`/`flip_one_tag` including the lost-race fallback |
| `tests/test_prune_interval_tags.py` | Interval-tag classification and pruning |
| `tests/test_fleet_orchestration.py` | Fleet-level per-dataset failure isolation |
