# Concurrency and coexistence: ingest + compact + index on the same datasets

How the three jobs (merge_insert ingestion, compaction, index build and maintenance) interact at the commit
layer, with line-cited evidence from the pinned lance checkout at `/Users/gstamatakis/IdeaProjects/lance`, and
the concrete strategy for running them concurrently across 30,000 power-law-distributed orgs.

All `path:line` references below were spot-verified against the checkout during this research pass.

## The authoritative conflict matrix

The live conflict matrix is `TransactionRebase::check_txn` in
`rust/lance/src/io/commit/conflict_resolver.rs:190-223`, which dispatches to per-operation `check_*_txn`
methods. The older `Operation::conflicts_with` table in `rust/lance/src/dataset/transaction.rs` is legacy and
coarse. `commit_transaction` (`rust/lance/src/io/commit.rs:975-982`) consults only `TransactionRebase`.

Three outcomes exist:

- `Ok(())` — compatible, the transactions commute (possibly after auto-rebase).
- `retryable_conflict_err` — `Error::RetryableCommitConflict` (`conflict_resolver.rs:157-167`,
  `lance-core/src/error.rs:110`). Retry can succeed.
- `incompatible_conflict_err` — `Error::IncompatibleTransaction` (`conflict_resolver.rs:169-182`). Hard
  conflict, never retry.

### Matrix summary

| This op \ other op | Append | Update/Delete | Rewrite (compaction) | CreateIndex | Overwrite/Restore |
|---|---|---|---|---|---|
| Append | OK | OK | OK | OK | HARD |
| Update/Delete | OK | OK if disjoint, rebase del-files, else RETRY | RETRY iff frag overlap | OK | HARD |
| Rewrite | OK | RETRY iff frag overlap | RETRY on overlap, RETRY if both emit FRI | see below | HARD |
| CreateIndex | OK | OK (optimistic) | depends on defer_index_remap | RETRY iff same name | HARD |

### Evidence by pair

- Append vs everything except Overwrite/Restore/UpdateMemWalState: compatible. Append never reads existing
  fragments (`check_append_txn`, `conflict_resolver.rs:870-896`, hard-conflict arms at 878-882).
- Update/Delete vs Update/Delete: compatible when touched fragment sets are disjoint (`conflict_resolver.rs:271-278`
  for Delete, 427-434 for Update). Overlapping fragments where the other transaction only changed deletion files
  are auto-rebased by re-merging deletion files in `finish_delete_update` (`conflict_resolver.rs:1316` onward),
  provided `affected_rows` is available. Retryable when the other transaction rewrote data files of a shared
  fragment (291-295, 447-451), removed a shared fragment (302-308, 458-464), or `affected_rows` is `None` on the
  full-fragment update path (280-284, 436-440).
- Update with `inserted_rows_filter` (primary-key merge_insert) vs Update: retryable on bloom-filter key
  intersection or config mismatch (343-376). Vs Append: always retryable since appended keys are unknown
  (386-394). Our `etl.py` merge_insert does not enable PK filters, so Update vs Append stays compatible.
- Update/Delete vs Rewrite: retryable iff the rewrite's `old_fragments` intersect the update's modified
  fragments, else compatible (`check_delete_txn` 239-249, `check_update_txn` 395-405, symmetric direction in
  `check_rewrite_txn` 661-681). This is THE contention pair for ingest plus compact: merge_insert mutates the
  deletion files of exactly the fragments holding matched keys, and compaction targets those same
  small or deletion-heavy fragments.
- Update/Delete vs CreateIndex, ReserveFragments, Project: compatible (232-238, 380-385). Vs DataReplacement:
  retryable on fragment overlap (250-260, 406-416). Vs Merge: always retryable (311-313, 467-469). Vs
  Overwrite/Restore: hard conflict (314-318, 470-472).
- CreateIndex vs Append: compatible (`check_create_index_txn`, 499-501). Vs Delete/Update: compatible, committed
  optimistically because row addresses stay valid and deleted rows are filtered at query time (537-539). Vs
  Merge/ReserveFragments/Project/UpdateConfig: compatible (540-543, 598). Index builds and incremental
  maintenance fully commute with ingestion.
- CreateIndex vs CreateIndex: retryable iff same index name, or both committing the frag-reuse or mem-wal system
  index, otherwise compatible (502-536). Two concurrent maintenance runs of the same index name collide, while
  different indexes on one dataset commute.
- CreateIndex vs Rewrite, the `defer_index_remap` pivot:
  - Rewrite committed WITH a frag-reuse index (`defer_index_remap=true`): compatible, no conflict for normal
    column indexes (552-574, explanatory comment at 552-556).
  - Rewrite WITHOUT a frag-reuse index (eager remap): retryable iff the rewrite's `old_fragments` intersect the
    new index's `fragment_bitmap`, and retryable whenever a new index lacks a fragment bitmap (576-596).
  - Reverse direction (`check_rewrite_txn` 720-811): with deferred remap, compatible unless a rewrite group
    straddles a new index's bitmap (752-777). With eager remap, retryable on bitmap overlap (791-810).
    Both-sides frag-reuse commits are auto-rebased by merging FRI versions (734-745, `finish_create_index`
    1478-1518).
- Rewrite vs Append/ReserveFragments/Project: compatible (654-660). Rewrite vs Rewrite: retryable on
  old-fragment overlap, and retryable when both produce a frag-reuse index even if disjoint (682-702, TODO at
  696-697). Vs DataReplacement: retryable on fragment overlap (703-716). Vs Merge: retryable (717-719).
- DataReplacement vs Append/Delete/Update/Merge/ReserveFragments/Project/UpdateConfig: compatible (905-913).
  Vs CreateIndex: retryable iff a replaced field is among newly indexed fields (914-936). Vs Rewrite: retryable
  on fragment overlap (937-951). Vs DataReplacement: retryable on (fragment id AND field) overlap (952-971).
- ReserveFragments vs everything except Overwrite/Restore: compatible (1038-1061). Used internally by
  `commit_compaction` to reserve ids (`rust/lance/src/dataset/optimize.rs:1916-1925`), never a contention source.
- Project vs most ops: compatible (1063-1079). Vs Merge/Project: retryable (1080-1083).

### Cleanup is not a transaction

`cleanup_old_versions` never enters the conflict matrix. It physically deletes unreferenced files
(`rust/lance/src/dataset/cleanup.rs`). Files newer than `UNVERIFIED_THRESHOLD_DAYS = 7` (`cleanup.rs:131,
324-325`) are kept unless provably referenced only by old versions, controlled by `delete_unverified`
(`cleanup.rs:357-358`, default false at 928). Auto-cleanup also fires inside every successful commit unless
`skip_auto_cleanup` (`io/commit.rs:1084-1093`). The hazard is with long-running committers pinned to old
versions: `commit_transaction` may `checkout_version(read_version)` (`io/commit.rs:936-940`) and rebase scans
the transaction files of all versions since `read_version` (`io/commit.rs:968`), so cleanup horizons must
exceed the longest-running job.

## Retry semantics: two layers, and where they do not reach

### Inner layer: the manifest-write race

The `commit_transaction` loop (`io/commit.rs:959-1128`) retries only the raw manifest-write race
(`CommitError::CommitConflict`, two writers picking the same version number). Each iteration reloads all
transactions since `read_version` and auto-rebases compatible ones via `TransactionRebase`. Budget is
`CommitConfig.num_retries`, default 20 (`lance-table/src/io/commit.rs:1521-1533`). A semantic
`RetryableCommitConflict` from `check_txn` propagates OUT immediately (the `?` at `io/commit.rs:979`). The
inner loop does not absorb it.

### Outer layer: data operations only

`execute_with_retry` (`rust/lance/src/dataset/write/retry.rs:74-130`, verified) catches
`Error::RetryableCommitConflict`, checks out latest, and re-plans plus re-executes the whole
update/delete/merge_insert. Defaults: `max_retries=10`, `retry_timeout=30s` (`retry.rs:23-30`, verified, and
`merge_insert.rs:464`). On exhaustion it raises `TooMuchWriteContention` (`retry.rs:126-129`), a different
error string than commit conflicts.

Backoff is `SlotBackoff` (`lance-core/src/utils/backoff.rs:111-146`): the unit is 110% of the first attempt's
wall time (`retry.rs:105-111`, verified, and `io/commit.rs:1099-1105`), and attempt `i` sleeps a random slot in
`[0, 2^(i+2))` units. Backoff therefore scales with actual operation cost and randomizes writers apart. With K
concurrent committers on one dataset, expected retries per committer is O(K). Our worst case per dataset is 3-4
committer classes (merge_insert, delete, index commit, compaction commit), so lance's 10/20 defaults are
comfortable and raising them further mostly wastes time on deterministic conflicts.

### What has NO outer retry inside lance

CreateIndex and Rewrite commits: `commit_existing_index_segments` builds the transaction at the handle's current
version and calls `apply_commit` (`rust/lance/src/index.rs:1248-1258`), and `commit_compaction` does the same
(`optimize.rs:2036-2042`). A `RetryableCommitConflict` surfaces to Python. Our `commit_with_retries`
(`src/lance_etl/telemetry.py:143-180`) matches the "Retryable commit conflict" / "Commit conflict" display
strings (`lance-core/src/error.rs:109-115`) and re-runs the action.

### The critical asymmetry: index retry works, compaction commit retry does not

Retrying an index commit IS productive: the action re-opens the dataset at latest (`indexing.py:385-392,
959-971`), so `check_txn` no longer sees the conflicting transaction.

Retrying a distributed compaction commit is NOT productive. `commit_compaction` pins `read_version` to
`min(RewriteResult.read_version)` precisely so the conflict checker re-scans `[plan_version, head]`
(`optimize.rs:1895-1912` comment and `tasks_read_version` at 1908-1912, both verified). The same conflicting
Delete/Update is found on every attempt, so `commit_with_retries` around `Compaction.commit`
(`compaction.py:289-305`, verified) deterministically burns all retries with backoff and then fails. Compaction
conflicts require RE-PLAN plus re-execute, not re-commit. The small tier already gets this right: its retry
action re-runs `Compaction.execute` end to end (`compaction.py:213-230`, verified), which re-plans at the
latest version.

### Hard conflicts and contention exhaustion

`Error::IncompatibleTransaction` (anything vs Overwrite/Restore) must never be retried, and our string match
correctly excludes it ("Incompatible transaction", `error.rs:103-108`). `TooMuchWriteContention` ("Too many
concurrent writers", `error.rs:116-121`) is also not matched by our wrapper, so merge_insert exhaustion fails
the Spark task rather than double-retrying, which is correct.

merge_insert-specific: each lance-internal retry re-executes the full merge plan against the latest version
(`retry.rs:84-95`), so correctness is preserved through rewrites and deletes that landed in between. Our
`etl.py` raises `retry_timeout` to 120 s (`etl.py:226`) because the 30 s default bounds total time across ALL
attempts and a head-org merge attempt can exceed 30 s by itself.

## Coexistence strategy

### Already safe, leave as is

- Ingestion vs indexing: CreateIndex commutes with Append/Update/Delete (`conflict_resolver.rs:499, 537-539`),
  and our index commits re-open latest per attempt (`indexing.py:385-400, 959-979`). Segment-API builds and
  incremental maintenance run fully concurrent with merge_insert and delete on the same dataset with no
  ordering requirement.
- Ingestion vs ingestion on one dataset auto-rebases deletion-file-only overlaps and re-plans on real conflicts
  via lance's internal `conflict_retries` (`etl.py:396-427`). Keep exactly one merge_insert task per routing key
  per ETL run (our `apply_merge` already groups by key) so intra-run self-contention is zero.

### Fix: distributed compaction retry must re-plan

In `compaction.py` `LanceCompactor` (tier B), treat a `RetryableCommitConflict` from `Compaction.commit` as
"rewrite results are stale". Loop back to `Compaction.plan` plus executor re-execute instead of re-calling
`Compaction.commit`, which re-fails deterministically (`optimize.rs:1908-1912, 2018-2034`). Budget 2-3
re-plans, then skip the dataset and defer to the next cycle, emitting a hot-dataset metric. Drop
`commit_retries=20` around `Compaction.commit` to ~2 since it only helps the rare raw manifest race, which the
inner `num_retries=20` already covers.

### Shrink the compaction conflict window on head orgs

1. Keep `max_source_fragments` small so each run rewrites a bounded slice of the OLDEST fragments. Old cold
   fragments are least likely to intersect merge_insert's touched fragments, turning Rewrite vs Update from
   retryable into compatible via the disjoint-fragments check (`conflict_resolver.rs:661-681`).
2. Schedule tier-B compaction for head orgs in the gap after the ETL window commit. The DAG already orders
   etl, index, compact at the DAG level, but keep per-dataset (not just per-DAG) ordering for the head.
3. Tail orgs need nothing. Their datasets see at most one writer at a time and the small tier's
   re-plan-on-retry action (`compaction.py:213-230`) is already correct.

### defer_index_remap: small tier only, for now

Use `defer_index_remap=true` on the small tier, where it is honored (`compaction.py:200-202`), making Rewrite
vs CreateIndex compatible (`conflict_resolver.rs:552-574`, verified) and keeping commits short. Tier B cannot
defer remap today because the Python `Compaction.commit` binding hard-codes `CompactionOptions::default()`
(`lance/python/src/dataset/optimize.rs:567-568` TODO, verified). Therefore on the large tier, NEVER run an
index build or maintenance commit for a dataset while its compaction commit is in flight. Serialize "compact"
and "index" per dataset in the orchestrator. This is per-dataset ordering, not a data-path lock, and both
sides survive a violation via retryable conflicts, but the loser wastes a full segment build or forces a
compaction re-plan. Once the binding gap is fixed upstream, deferred remap makes index-vs-compact concurrency
free on tier B too and the ordering can be dropped. Caveat: the repo records an observed vector-query failure
under deferral on the pinned build (`compaction.py:66-69`), so deferral stays opt-in until verified.

### Per-(dataset, index-name) mutual exclusion for index jobs

CreateIndex vs CreateIndex with the same name is retryable (`conflict_resolver.rs:502-536`, verified) and the
retry publishes whichever build finished last, silently discarding the other build's work. Enforce in the
orchestrator that only one build or maintenance run per index name is active. Different index names on the same
dataset may run in parallel safely.

### Index maintenance after a conflicting rewrite must rebuild, not blind-commit

If `commit_existing_index_segments` gets a Rewrite conflict, a naive retry at the new head version can succeed
while the segments still reference pre-compaction row addresses. The checker only sees transactions after
`read_version`, and the orphan check at `index.rs:1227-1239` protects existing coverage, not new-segment
staleness. On index commit conflict, re-read the dataset, recompute covered fragments
(`indexing.py` `covered_fragments`), and rebuild segments for any fragment ids that no longer exist before
recommitting. Tier-B inline remap partially compensates (compaction remaps committed indexes), but only for
indexes committed BEFORE the rewrite.

### Version cleanup placement and horizons

Keep cleanup driver-side, after the compaction commit, one runner per dataset (`compaction.py:176-194, 231,
367`). Keep `delete_unverified` at the default false so the 7-day unverified threshold (`cleanup.rs:131,
324-325, 357-358`) protects in-flight executor-written files, since compaction rewrite outputs and index
segment files are unreferenced until their driver commit. Set `cleanup_older_than_seconds` (and any
`lance.auto_cleanup` dataset config) to comfortably exceed the longest possible job on a head org, meaning
plan-to-commit for tier-B compaction plus retries, so `commit_transaction` can still checkout `read_version`
and read the transaction files it needs for rebase (`io/commit.rs:936-940, 968`). A floor of several hours is
enough for manifests, and unreferenced data files are already 7-day protected.

## Per-tier guidance

### Tail orgs (the vast majority of 30k)

- Effectively zero contention. At most one writer at a time per dataset.
- Lance defaults (merge_insert `conflict_retries=10`, inner `num_retries=20`) are over-provisioned and harmless.
- Small-tier compaction with `defer_index_remap` honored and re-plan-on-retry already correct.
- Maintenance sweeps should rely on no-op gates (see optimization-recommendations.md) so touching an unchanged
  org costs near zero.

### Head orgs (the small set holding most rows)

- At most ~3 concurrent committer classes when all three jobs overlap. `SlotBackoff` scales sleep with actual
  commit latency, so budgets need not grow.
- ETL: `conflict_retries=10` with `retry_timeout=120s` (already set, `etl.py:225-226`).
- Index: `commit_retries` can stay 20 since each retry is metadata-cheap, but add the stale-segment rebuild
  check above.
- Compaction: the re-plan loop above replaces a large commit retry budget. Bound each run with
  `max_source_fragments` and schedule in the post-ETL gap.
- FTS: the INVERTED commit path re-reads `current.version` per attempt (`indexing.py:959-971`), safe against
  ingestion, but a full FTS rebuild overlapping a tier-B compaction is the most expensive retryable collision
  (a whole rebuild lost). Schedule FTS rebuilds inside the same per-dataset serialization slot as compaction.
- Alert on the existing `index.commit_conflict` / `dataset.commit_conflict` counters per URI. A sustained
  nonzero rate identifies head-org datasets whose compaction slot must move off the ingest window.

## Known risks

1. Deterministic tier-B commit-retry spin until the re-plan loop ships (`compaction.py:289-305` vs
   `optimize.rs:1908-1912`). Wastes up to 20 backoff slots on a head org, then fails the run.
2. Stale index publish after a rewrite conflict (blind retry of `commit_existing_index_segments`). Severity:
   wrong or missing search results for affected rows until the next maintenance run.
3. Inline index remap lengthens the tier-B commit window (binding hard-codes default options), growing the
   window in which incoming merge_inserts force the compaction into conflict. Bounded by
   `max_source_fragments`, fixed properly only upstream.
4. Cleanup horizon vs long-running jobs: an aggressive `cleanup_older_than_seconds` below a tier-B
   plan-to-commit cycle can break conflict-resolution reads (`io/commit.rs:936-940, 968`). Never combine with
   `delete_unverified=true` while any job runs.
5. `commit_with_retries` matches error display strings (`telemetry.py:172-174`) rather than typed exceptions. A
   future lance wording change silently turns retryable conflicts into job failures. Pin the lance version or
   add a test asserting the strings against the vendored checkout (`lance-core/src/error.rs:109`).
6. PK-mode caveat: switching merge_insert to `inserted_rows_filter` mode flips Update vs Append from compatible
   to always-retryable (`conflict_resolver.rs:386-394`) and adds bloom-filter false-positive conflicts between
   updates (343-376). Re-evaluate head-org contention before enabling.
7. Two concurrent same-name index maintenance runs (overlapping DAG runs) silently discard one build's work.
   Nothing in lance or our code prevents this today, hence the orchestrator exclusion.

## Verification results

`tests/test_concurrent_coexistence.py` runs the three jobs as concurrent threads against one head dataset
(~191,000 final rows, dim 16, 16 merge_insert rounds mixing inserts, updates, and deletes) plus four tail
datasets, using the production helpers directly: `apply_merge`, the tier-B plan/execute/commit triad with the
re-plan loop, `compact_small_dataset`, the segment-API and FTS metadata-merge index paths, and
`commit_with_retries` everywhere. Measured on the passing run (full suite 307 passed):

- Runtime 37.4 s standalone, 68.3 s for the entire suite including it.
- Zero data loss: every dataset's final content matched the tracked expected state exactly (key set, payload
  checksums, and row counts), with deleted keys absent.
- 10 commit conflicts retried successfully, 2 tier-B re-plan cycles, 0 hot-skips, 1 stale segment dropped by the
  index commit guard, 3 delta merges. No actor died.
- Convergence: head at 2 fragments (band 3 for 191k rows at 100k target), every tail at 1 fragment, every index
  at 0 unindexed fragments, and exact vector, FTS, and scalar probes all correct.

Two real defects were found and fixed during this proof:

1. `create_index_uncommitted` requires `replace=True` for incremental coverage. The uncommitted build path
   applies the same-name existence guard (`rust/lance/src/index/create.rs:200-205`), so the first maintenance
   pass after an index was committed raised "Index name already exists" and the segment path could never extend
   an existing index. `replace` is consumed only by the committed `execute` removal logic (`create.rs:497-510`),
   making the flag a pure guard bypass on the uncommitted path. Fixed in all three segment builders in
   `indexing.py`.
2. Compaction's inline eager remap silently corrupts IVF_RQ indexes on the pinned build. Measured: 40/40 exact
   top-1 recall before, 22/40 after one `Compaction.execute` rewrote the covered fragments, while
   `num_unindexed_fragments` stayed 0 and `num_indexed_rows` stayed exact, so no existing maintenance trigger
   could ever fire. The corruption reproduces identically for `create_index`-built indexes, deferred remap still
   fails vector queries loudly (the recorded caveat), and BTREE plus FTS remaps measured sound (40/40 after the
   same rewrite). Mitigation in `VectorIndexHandler`: the artifact sidecar now records the live fragment ids
   covered at each build, and `remap_requires_rebuild` forces a full segment rebuild (reusing trained centroids
   and rotation) whenever any recorded fragment no longer exists. This supersedes the milder risk wording above
   for vector indexes: on this build, a remapped IVF_RQ index must never be trusted, and per-dataset
   compact-then-index ordering is mandatory rather than advisory until the upstream remap is fixed.

One convergence behavior was confirmed as Lance design rather than a defect: the compaction planner refuses to
bin fragments whose covering index sets differ (`optimize.rs:662-694`) and every index delta carries its own
fragment bitmap (`load_index_fragmaps`, `optimize.rs:1378-1391`), so unmerged deltas fence compaction bins and
isolated sub-target fragments stay uncompactable until deltas are merged. The bounded `merge_index_deltas`
cadence (default cap 4) is therefore load-bearing for fragment convergence, not just query latency.
