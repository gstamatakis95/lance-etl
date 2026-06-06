# Market research: operating Lance at 30k-org power-law scale

Research basis for evolving the lance-etl pipeline, where ingestion (merge_insert upserts and deletes),
compaction (plan, execute, commit, version cleanup), and indexing (segment-API builds plus incremental
maintenance) must coexist against the same datasets. Each namespace holds up to 30,000 orgs and up to 1 billion
rows, power-law distributed: most orgs are tiny while a small head holds most of the data.

All lance `path:line` citations were spot-verified against the read-only checkout at
`/Users/gstamatakis/IdeaProjects/lance` and found accurate.

## Contents

- [production-patterns.md](production-patterns.md) — public production case studies (700 M to petabyte
  scale), upstream performance-guide facts, and the patterns they imply for our power-law multi-tenant shape.
- [concurrency-and-coexistence.md](concurrency-and-coexistence.md) — the authoritative operation conflict
  matrix with line-cited evidence, the two retry layers and their blind spots, and the concrete per-tier
  coexistence strategy for ingest plus compact plus index.
- [optimization-recommendations.md](optimization-recommendations.md) — every compaction, indexing, and
  maintenance optimization with evidence, risk, and verdict, split into apply-now versus deferred.

## Top 10 takeaways

1. Index builds fully commute with ingestion. CreateIndex is compatible with Append, Update, and Delete in the
   live conflict checker (`conflict_resolver.rs:499, 537-539`), so segment-API builds and incremental
   maintenance can run concurrently with merge_insert on the same dataset with no ordering requirement.

2. Retrying a distributed compaction commit is deterministic failure. `commit_compaction` pins `read_version`
   to plan time (`optimize.rs:1908-1912`), so the same conflicting ingest transaction is found on every retry.
   Tier B must re-plan and re-execute on conflict, not re-commit. The small tier already does this correctly.

3. The tier-B Python binding ignores `defer_index_remap` (`python/src/dataset/optimize.rs:567-568` hard-codes
   `CompactionOptions::default()`), so every distributed compaction commit remaps covering indexes inline.
   Until fixed upstream, serialize compact and index per dataset on the large tier.

4. Reordering the DAG to etl >> compact >> index removes inline remap cost for fresh data, because the planner
   flushes a bin whenever the covering-index set changes (`optimize.rs:676-695`) and uncovered fragments merge
   freely. Verify against lancedb issue #2751 (which recommends the opposite order for the OSS client) in e2e.

5. Stop rebuilding every index every run. `optimize_indices()` appends only unindexed fragments and no-ops
   cheaply when nothing changed (`index.rs:1349-1361`, `append.rs:393-404`), which is the difference between
   30,000 daily full rebuilds and a near-free maintenance sweep across the power-law tail.

6. Bound delta accumulation and retrain on growth. Incremental maintenance accumulates one delta per run per
   index (every query consults all of them), so merge with `num_indices_to_merge=N` on an `index_statistics`
   threshold, and persist `rows_at_train` to force IVF retraining when an org has grown several-fold past its
   centroids.

7. The per-dataset commit ceiling on object storage is roughly 1-4 transactions per second (sequential manifest
   writes), and merge_insert with updates conflicts under concurrency while appends never do. Batch client-side
   per routing key, keep one merge_insert per key per run, and reserve update-heavy operations for
   low-concurrency windows.

8. Version cleanup is not a transaction and never conflicts, but it can break in-flight committers by deleting
   the transaction files rebase needs (`io/commit.rs:936-940, 968`). Keep the horizon longer than the longest
   head-org job, never use `cleanup_older_than=0` or `delete_unverified=true` with concurrent writers, and tag
   versions needed for reproducibility. Unmanaged versions reach terabytes in production.

9. Enforce per-(dataset, index-name) mutual exclusion in the orchestrator. Same-name CreateIndex commits are
   retryable conflicts (`conflict_resolver.rs:502-536`) where the loser's entire build is silently discarded,
   and a blind index-commit retry after a rewrite conflict can publish segments pointing at compacted-away
   fragments. Rebuild stale segments before recommitting.

10. Move-stable row ids are the structural endgame. With them compaction needs no index remapping at all
    (`needs_remapping = !uses_stable_row_ids && !defer_index_remap`, `optimize.rs:1893`), removing the
    remap problem, the binding gap, and most compact-versus-index contention. Deferred only because it
    requires migrating 30k existing datasets and a verification pass on the pinned build.
