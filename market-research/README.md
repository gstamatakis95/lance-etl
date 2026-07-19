# Market research: operating Lance at 30k-org power-law scale

Research basis for the local PostgreSQL-backed lance-etl pipeline, where ingestion (`merge_insert`
upserts and deletes), compaction, and segment-API indexing coexist against the same datasets. The
research models up to 30,000 organizations and one billion rows under a power-law distribution.
Most datasets are tiny while a small head contains most rows.

ADR 0042 is the current control-plane decision. Older research references to scheduler queues or
fixed code configuration are historical evidence only. The implementation now stores first-class
datasets, exact source snapshots, durable work, immutable publications, and normalized dataset
specification revisions in PostgreSQL. The local reconciler creates and stops Spark directly.

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
- [trace-events-and-distributed-writes.md](trace-events-and-distributed-writes.md) — commit and observability
  internals behind the distributed write and index paths.
- [use-cases.md](use-cases.md) — round-2 survey of who runs Lance in production and for what, organized by
  use-case family, each entry with sources, verification status, and a relevance-to-our-shape line.
- [production-techniques.md](production-techniques.md) — round-2 techniques catalog (object store and
  multi-writer, lifecycle, ecosystem) with guide URL plus checkout path:line evidence, a maturity verdict, and
  an apply-to-lance-etl recommendation per technique, plus an appendix of unverified claims.
- [rag-usecases.md](rag-usecases.md) — RAG-focused survey of how teams run Lance/LanceDB as the retrieval store
  in production RAG and agent-memory systems, organized by five angles (architecture and freshness, retrieval
  quality with hybrid plus rerank plus multivector, eval and observability, scale and multitenancy, named cases),
  each finding tagged VERIFIED-CHECKOUT / VERIFIED-DOCS / BLOG with a relevance-to-lance-etl line, a ranked
  gaps-and-candidate-features section (rerank hook, weighted fusion, nDCG in the recall job), and an
  unverified-claims appendix.

## Round 2 takeaways

Production-ready techniques surfaced in round 2, ranked by what we should adopt. See production-techniques.md
for evidence and the full verdict per item.

1. Enable V2 manifest paths fleet-wide (yes-now). For 30,000 datasets in one bucket, `enable_v2_manifest_paths`
   turns every dataset open from O(version_count) LIST requests into one LIST (or a HEAD with the version-hint
   file). UUID fragment names already self-distribute across S3 partitions, so no prefix sharding is needed.

2. Publish exact versions through PostgreSQL (implemented). Validate the freshly ingested and indexed
   candidate, append immutable publication evidence, then compare-and-swap the dataset state's active-publication
   pointer. The gRPC service opens that exact URI and version. Lance tags remain retention pins and compatibility
   mirrors, not serving authority.

3. Correct the cleanup horizon understanding (yes-now). The low-level `cleanup_old_versions` default is 14
   days, not 7 (`dataset.py:2934-2936`). Keep the horizon longer than the longest head-org job, set
   `delete_rate_limit` on the fleet to dodge S3 503s, never use older_than=0 or delete_unverified with
   concurrent writers, and tag reproducibility versions.

4. Drive incremental reconciliation from durable source identity (implemented). PostgreSQL stores exact Iceberg
   source snapshots and per-dataset cursors. The local process materializes only dataset work discovered from the
   pinned snapshot manifests instead of deriving progress from wall-clock scheduler intervals.

5. Tune the search service for S3 throughput (yes-now, benchmark first). Raise LANCE_IO_THREADS toward 128-256
   with proportionally larger io_buffer_size in the Rust prewarm and disk_cache paths, and adopt
   `timeout: 120s` for large-row reads. The AIMD limiter caps at 5000 req/s per process, validate under Datadog
   for a fleet running ingest plus compaction plus indexing simultaneously.

6. Conditional-put is the default S3 commit handler (yes-now, posture confirmation). On modern S3 our three
   concurrent jobs need no DynamoDB. Reserve `s3+ddb://` only for older S3-compatible stores lacking conditional
   put, and remember S3 CRR does not replicate the DynamoDB commit store.

7. Stable row ids were investigated and rejected (ADR 0010). The tested combination of merge, delete, and
   concurrent compaction violates the row-id index invariant. Do not enable them without a fresh ADR and new
   upstream evidence.

8. WeRide and Harvey are our two closest production analogues (validation). WeRide's weekly/monthly scheduled
   re-indexing over growing sensor embeddings maps to our reconciler's maintenance cadence, and Harvey's
   sub-2-second P50 metadata-filtered search on 15 M rows validates the typed Filter AST direction (rule #7).

## Top 10 takeaways (round 1)

1. Index builds fully commute with ingestion. CreateIndex is compatible with Append, Update, and Delete in the
   live conflict checker (`conflict_resolver.rs:499, 537-539`), so segment-API builds and incremental
   maintenance can run concurrently with merge_insert on the same dataset with no ordering requirement.

2. Retrying a distributed compaction commit is deterministic failure. `commit_compaction` pins `read_version`
   to plan time (`optimize.rs:1908-1912`), so the same conflicting ingest transaction is found on every retry.
   Tier B must re-plan and re-execute on conflict, not re-commit. The small tier already does this correctly.

3. The tier-B Python binding ignores `defer_index_remap` (`python/src/dataset/optimize.rs:567-568` hard-codes
   `CompactionOptions::default()`), so every distributed compaction commit remaps covering indexes inline.
   Until fixed upstream, serialize compact and index per dataset on the large tier.

4. Ordering dataset work as ingest, compact, then index removes inline remap cost for fresh data, because the planner
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

9. Enforce per-(dataset, index-name) mutual exclusion in the PostgreSQL dataset lane. Same-name CreateIndex commits are
   retryable conflicts (`conflict_resolver.rs:502-536`) where the loser's entire build is silently discarded,
   and a blind index-commit retry after a rewrite conflict can publish segments pointing at compacted-away
   fragments. Rebuild stale segments before recommitting.

10. Move-stable row ids looked attractive in the initial research because they remove compaction remapping.
    Subsequent qualification rejected them after finding an overlapping-chunk invariant failure under merge,
    delete, and concurrent compaction. ADR 0010 is authoritative.
