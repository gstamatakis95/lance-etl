# Fleet orchestration, maintenance, and pipeline — architecture decisions

This document consolidates the decisions behind the job structure, the unified task-based fleet
orchestration, maintenance (TTL, compaction, cleanup), and source-table upkeep. Each section
keeps its original ADR number so references like "ADR 0028" resolve here. Superseded decisions
are summarized at the end, and the closing section preserves the historical verification runs.

## ADR 0026 — Job isolation: separate packages and CLIs

Status: Accepted (the three-separate-DAGs portion superseded by ADR 0027)

Each operational job is its own Python package with its own `cli.py` and `__main__.py`:
`lance_etl.etl` (`lance-etl-etl`), `lance_etl.indexing` (`lance-etl-index`),
`lance_etl.maintenance` (`lance-etl-maintenance`), plus `lance_etl.tools` (`lance-etl-tools`)
for the unscheduled operator subcommands and `lance_etl.pipeline` (`lance-etl-pipeline`, added
by ADR 0027). There is no aggregate `lance-etl` binary. Shared argument helpers live in
`lance_etl.cliutil`. The jobs share no runtime coordination beyond the Lance version history and
the commit-conflict retry protocol, and each performs a zero-write derived-state skip check from
the open manifest (`index_skip_reason`, `compaction_skip_reason`) before doing any work.

## ADR 0027 — Unified pipeline: prune, maintenance, index, stamp in one DAG

Status: Accepted

The separate maintenance and index DAGs enforced their compact-before-index ordering only by
schedule stagger, paid two cluster spin-ups per hour, and left interval tagging homeless. One
package, `lance_etl.pipeline`, now runs four phases in series over the fleet inside one DAG
(`lance_etl_pipeline`, `max_active_runs=1`):

1. **Prune** (skipped when `tag_keep_last` is `None`): delete interval tags beyond the keep-last
   window (default 48), first, so the same run's cleanup reclaims the newly unpinned versions.
2. **Maintenance**: `MaintenanceJob.run` — TTL expiration, unified compaction, version cleanup.
3. **Index**: `LanceIndexer.run` with derived-state skip.
4. **Stamp**: when `tag_stamp` is set, write the `%Y%m%dT%H%M%SZ` interval tag via
   `update_serving_tags`, optionally advancing `HEAD` when `serve_tag` is on.

Compact-before-index is structural rather than conventional, one cluster spin-up serves all
phases, and the standalone maintenance/index CLIs remain as unscheduled operator tools. The ETL
DAG additionally stamps the truncated-hour tag at write time (ADR 0032), and the pipeline's
post-index stamp converges onto the same tag name.

## ADR 0028 — Unified task-based fleet orchestration, format 2.1, column roles

Status: Accepted (supersedes ADR 0002's tiers and ADR 0001's small-tier index path)

Compaction and indexing previously split every dataset into a small or a large tier with
different Lance APIs per tier, duplicating logic and hiding bugs at the boundary. Now every
heavy job follows one task-based shape, and a small dataset is simply the one-task case:

1. Phase P (plan) fans out per dataset on executors and returns independent task specs.
2. Phase E (execute) runs ALL datasets' tasks in ONE flat Spark job.
3. Phase C (commit) fans out per dataset (or per dataset and index) on executors.
4. Conflicted or stale datasets re-enter the next round under bounded budgets
   (`replan_budget` for compaction, `max_stale_replans` for indexing), then defer to the next
   scheduled run.

Compaction plans with `Compaction.plan`, executes serialized `CompactionTask` items in the flat
job, and commits per dataset with `Compaction.commit`. Indexing reads reusable IVF_RQ artifacts
back in a flat executor job and builds vector, scalar, and FTS shards in one flat job sized by
`fragments_per_index_task` (vector training itself moved to the committed streaming bootstrap,
see ADR 0030 in `indexing.md`). Driver thread pools, FAIR scheduler pools, and every tier
threshold were deleted — fleet parallelism comes from Spark scheduling.

Two data-model decisions ride with the unification. New datasets are created with Lance data
storage version 2.1 at every creation site (existing datasets keep their stored format, lance
reads both). And the ETL pivot records each created column's role in the dataset config KV under
`lance-etl.columns` (`vector`, `text`, or `scalar` by source map), which drives automatic index
target discovery (ADR 0029). The Lance write seam is formalized in `etl/sink.py` — a Spark
DataSourceV2 connector was evaluated and rejected because DSv2 targets one table per write while
this sink content-routes to thousands of datasets. Spark gets memory-safe defaults
(AQE, Arrow batches capped at 4096 rows, `memoryOverheadFactor` 0.3 in the Airflow base conf).

## ADR 0018 — Per-row TTL expiration inside maintenance

Status: Accepted (the original standalone TTL job design is superseded by its own amendment)

TTL is per-row: each row carries its own lifetime in a TTL column holding an Arrow `Duration`,
and the delete predicate `{ts_column} + {ttl_column} < TIMESTAMP '{now}'` evaluates natively in
Lance's DataFusion planner (interval-literal arithmetic is rejected, `Duration` column
arithmetic verified working — that empirical result chose the column shape). Naming the column
(`MaintenanceConfig.ttl_column`) is the opt-in, there is no retention window or enabled flag,
and a dataset missing the column is skipped for TTL but still compacted. Expiration is the first
maintenance step so the compaction that follows materializes the deletion vectors and reclaims
the storage. Both column names are validated before entering the predicate.

## ADR 0009 — Index-vs-compaction coexistence and the orphan-race guard

Status: Accepted

Ingestion, compaction, and indexing coexist concurrently with zero data loss. Index builds
commute with ingestion, compaction re-plans on conflict rather than re-committing stale
rewrites, and version cleanup runs with an horizon floor (hours) exceeding the longest job so it
never deletes a transaction file a live committer needs to rebase from.

The one genuine race is guarded: an index segment build plans over a fragment set, a concurrent
compaction rewrites those fragments away, and committing would orphan dead fragments.
`commit_existing_index_segments` raises a plain `ValueError`, detected by the shared
`is_stale_fragment_error` predicate across vector, scalar, and FTS paths. A stale plan re-reads
at latest, re-resolves the live fragment set, rebuilds, and re-commits within the replan budget
of the unified plan-execute-commit rounds. It never silently skips indexing.

## ADR 0023 — Iceberg source-table optimization job

Status: Accepted

The upstream Iceberg table has its own maintenance needs, served by Iceberg's stored procedures
rather than reimplementation. `lance_etl/iceberg_optimize.py` issues
`CALL <catalog>.system.<procedure>(...)` statements in a fixed safe order: `rewrite_data_files`
(default on, 512 MiB target), `rewrite_manifests` (default on, after the data rewrite),
`expire_snapshots` (default on, keep last 5 and 7 days), and `remove_orphan_files` (default OFF
— the only step that deletes data files outright, respecting Iceberg's three-day safety
horizon). Each `CALL` plans and executes as a distributed Spark job. The table identifier is
validated as a dotted `catalog.namespace.table` before reaching any statement.

## Superseded decisions

- **ADR 0002 — Two-tier compaction orchestration.** Superseded by ADR 0028: the Tier A / Tier B
  split, FAIR scheduler pools, and driver thread pools were deleted. Its 2026-06 amendment lives
  on — compaction is one step of `MaintenanceJob` (`MaintenanceConfig`), ordered TTL then
  compaction then version cleanup, with the serving-tag and manifest-migration helpers in the
  same package.
- **ADR 0018 (original)** — the standalone `TTLJob` with a global retention window and its own
  CLI subcommand never shipped past the amendment above.
- **ADR 0026 (DAG portion)** — the three separate DAGs were merged into the pipeline DAG by
  ADR 0027. The package and CLI isolation stands.

## Verified at scale (historical verification runs)

- The coexistence stress test runs three concurrent actors (ingester, compactor, indexer) over
  a head-sized dataset plus tail datasets and asserts exact final content, full index coverage,
  and a fragment count in the target band. It passed 40-plus consecutive runs and surfaced the
  orphan-fragment race that ADR 0009's guard closes.
- On the synthetic end-to-end benchmark, the Prewarm RPC roughly halved cold first-query
  latency: about 17.8 ms down to 8.0 ms and 10.6 ms down to 4.7 ms across two orgs.
