# ADR 0028: Unified task-based fleet orchestration, Lance format 2.1, and column-role metadata

## Status

Accepted. Supersedes the two-tier compaction orchestration of ADR 0002 and the small-tier plain
``create_index`` path of ADR 0001. Builds on the sidecar-free vector artifacts of ADR 0025.

## Context

Compaction and indexing previously classified every dataset into a small or a large tier.
The tiers used different Lance APIs. Small-tier compaction ran ``Compaction.execute`` inside one
executor task while the large tier ran plan, execute, and commit from driver thread pools on a
FAIR scheduler pool. Small-tier indexing ran plain ``create_index`` and ``create_scalar_index``
builds that store no artifact config, while the large tier used the segment API with stored
artifacts. The divergence duplicated logic, produced different on-disk artifacts per tier
(forcing the promotion full-rebuild self-heal), hid bugs at the tier boundary, and left Spark
underused because fleet parallelism came from driver threads instead of Spark scheduling.

## Decision

Every heavy job follows one task-based shape, and a small dataset is simply the case with one
task:

1. Phase P (plan) fans out per dataset on executors and returns independent task specs.
2. Phase E (execute) runs ALL datasets' tasks in one flat Spark job.
3. Phase C (commit) fans out per dataset (or per dataset and index) on executors.
4. Conflicted or stale datasets re-enter the next round, bounded by the existing budgets
   (``replan_budget`` for compaction, ``max_stale_replans`` for indexing), then defer to the
   next scheduled run.

Compaction plans with ``Compaction.plan``, executes serialized ``CompactionTask`` items in the
flat job, and commits per dataset with ``Compaction.commit``. Indexing resolves IVF_RQ
artifacts in a flat executor job (reuse reads centroids from the committed index, training runs
in process on the executor under the train semaphore), builds vector, scalar, and FTS shards in
one flat job sized by ``fragments_per_index_task``, and commits per index on executors so the
vector segment merge never runs on the driver. Driver thread pools, FAIR scheduler pools, and
every tier threshold are deleted.

New datasets are created with Lance data storage version 2.1 at every creation site (the ETL
sink bootstrap and the migrate-namespace copies). Existing datasets keep their stored format
and lance reads both transparently.

The ETL pivot records each created column's role in the dataset config KV under
``lance-etl.columns``. Keys pivoted from the ``vectors`` map are ``vector`` columns, keys from
``texts`` are ``text`` columns, and keys from ``metadata`` are ``scalar`` columns. The mapping
is grow-only and idempotent. When no explicit index columns are configured, the indexing plan
phase derives per-dataset targets from these roles: vector roles get IVF_RQ and text roles get
BM25 INVERTED. Scalar roles build nothing unless explicitly configured, per the current scope.

The Lance write seam is formalized in ``etl/sink.py``. A Spark DataSourceV2 connector was
evaluated and rejected because a DSv2 write targets one table per write, while this sink routes
rows by content to thousands of per-tenant datasets with per-dataset schema evolution and
last-write-wins merge conditions. The executor-side sink remains the correct pattern, and the
official ``lance-spark`` connector stays an option for plain single-dataset reads.

Spark receives memory-safe defaults. ``build_spark`` enables adaptive query execution and caps
Arrow batches at 4096 rows unless the operator set the keys explicitly, and the Airflow base
conf defaults ``spark.executor.memoryOverheadFactor`` to 0.3 because Lance native reads and
PySpark workers live outside the JVM heap.

## Consequences

- One code path per job means one set of invariants to test and no tier-boundary bugs. Fleet
  parallelism comes from Spark scheduling one flat job instead of driver threads.
- Tiny datasets pay segment-API overhead (artifact resolve, segment build, commit) instead of a
  single ``create_index`` call. The ``vector_min_rows`` floor still skips the smallest.
- Every new vector index stores its artifact config, so the promotion full-rebuild divergence
  disappears over time. The self-heal for pre-unification artifact-less indexes remains.
- ``defer_index_remap`` now takes effect on every compaction commit because the options-carrying
  commit path is the only path.
- Replan rounds retry conflicted datasets at fleet cadence rather than per-dataset loops, with
  the same bounded budgets and the same defer-to-next-run terminal behavior.
- The query API keeps its persistent two-tier on-disk cache from ADR 0007 unchanged. This ADR
  required no Rust changes.
