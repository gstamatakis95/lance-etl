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
   `update_serving_tags`. The temporary pipeline never advances `HEAD`.

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

## ADR 0035 — Per-dataset failure isolation, partial-failure exit codes, and stamp gating

Status: Accepted

At fleet scale (more than 30k datasets, one dataset per org) a single pathological dataset used to
abort the entire Spark job for every fleet phase, plan, execute or rewrite, commit, index build,
index commit, artifact resolve, tag flip, and prune, discarding a whole round's healthy work along
with the one bad dataset. This is safe to fix because maintenance and indexing are cursor-free:
compaction re-plans from the live manifest and indexing re-plans over uncovered fragments, so a
dataset that is skipped this run simply catches up in full on the next scheduled run, with no
watermark state and no data loss.

Each fleet phase now catches per-dataset exceptions at the Spark-closure boundary and records an
error marker `{"uri", "error", "phase"}` instead of propagating. The shared `fan_out_per_dataset`
helper in `fanout.py` isolates the plan and tools fan-outs and increments `dataset.fanout_error`
tagged with `phase:`. The flat execute, build, and commit jobs isolate per task the same way. A
healthy dataset in the same run always completes and commits regardless of any sibling's failure.
`tests/test_poisoned_dataset.py` exercises plan, commit, and build failures across the maintenance,
indexing, and pipeline entry points and asserts the rest of the fleet still succeeds.

The maintenance, indexing, and pipeline CLIs surface isolated failures as a distinct exit code
rather than swallowing them silently. `main` returns `0` when every dataset in the run succeeded,
`EXIT_PARTIAL_FAILURE = 3` when the run completed but one or more datasets were isolated (so
Airflow or an operator is alerted while the fleet still made progress), and `1` on an unhandled
exception outside the per-dataset boundary. Each CLI's driver process emits a `run.datasets_failed`
gauge and a `dataset.failed` counter tagged `phase:`. `tests/test_poisoned_dataset.py` asserts the
`3` and `0` cases directly against the indexing CLI's `main`.

Exit `3` marks the Airflow task as failed, so the DAG's `default_args` retry policy applies
(`retries = 2` in `lance_etl_common.py`). Airflow therefore re-runs the whole fleet before the next
scheduled interval rather than waiting for it. Idempotency (ADR 0039, verified by
`tests/test_fleet_idempotency.py`) makes that retry a near-no-op for every dataset that already
succeeded, and a dataset that keeps failing past the retry budget is carried to the next scheduled
run. A clean run returns `0` and is not retried.

A partially built dataset must never look complete. `stamp_eligible` in `pipeline/job.py` excludes
a dataset carrying a dataset-level error or any per-index error entry from the pipeline's
post-index interval stamp. The maintenance side is excluded from stamping the same way on a
compaction error. `tests/test_poisoned_dataset.py` covers the build-failure case end to end and
asserts the poisoned dataset is not interval-stamped while its healthy siblings are.

## ADR 0036 — Idle-dataset cleanup rotation

Status: Accepted

`cleanup_old_versions` previously ran for every dataset on every maintenance run, roughly five
object-store LIST calls each, which dominates request cost on a large, mostly-idle fleet where most
datasets receive no writes in a given run. A dataset that did work this run (it committed a
compaction rewrite or a TTL delete) is always cleaned in the same run, since that is exactly when
stale versions accumulate. An idle dataset is cleaned only on its rotation slot.

`MaintenanceConfig` gains two tunables: `cleanup_rotation_slots` (default `8`, so an idle dataset is
cleaned once per eight runs, and `1` reproduces the old always-clean behavior every run) and
`cleanup_rotation_cadence_hours` (default `1`, the run cadence used to derive the current slot). The
active slot is `dataset_cleanup_slot(uri, slots)` in `maintenance/job.py`, keyed by
`hashlib.sha256(uri)` rather than the builtin `hash()`, because the builtin is salted per
interpreter process (`PYTHONHASHSEED` randomization) and would put the same URI in a different slot
on the driver versus an executor, or even across runs of the same process. SHA-256 is stable across
processes and lance-etl versions, which both the determinism and full-coverage guarantees depend
on: every idle dataset is still cleaned at least once within `cleanup_rotation_slots` consecutive
runs as the active slot cycles through `range(cleanup_rotation_slots)`.

New metrics distinguish the two cleanup paths: `dataset.cleanup_rotation_skipped` for an idle
dataset off its slot, `dataset.old_versions_removed` and `dataset.cleaned` for a dataset that was
actually cleaned (whether because it did work or because its slot came up), and a driver-side
`run.cleanup_slot` gauge recording the slot active for the whole run. `tests/test_cleanup_rotation.py`
covers the slot function's determinism, full coverage over `cleanup_rotation_slots` runs, the
`cleanup_rotation_slots=1` degenerate case, pinning to a supplied `now` rather than wall-clock time,
the always-clean-on-work-done rule, and the skip-then-clean behavior of an idle dataset across its
rotation boundary.

## ADR 0037 — Cluster-aware parallelism and discoverable partition counts

Status: Accepted

`MaintenanceConfig.max_tasks` / `batch_partitions` and `IndexJobConfig.max_build_tasks` /
`batch_partitions` were fixed caps (256, 512, 1024 depending on the job), so a fleet job could not
use a larger cluster's full capacity, and the same fixed numbers under-partitioned a large cluster
or over-partitioned a small one. All four fields are now `int | None`, defaulting to `None`. When
left unset, `derive_partitions(configured, spark, multiplier)` in `fanout.py` resolves the partition
count at run time from `spark.sparkContext.defaultParallelism`, scaled by a per-job headroom
multiplier: `REWRITE_PARTITION_FACTOR = 4` for the flat compaction rewrite job,
`FANOUT_PARTITION_FACTOR = 8` for the per-dataset plan and commit fan-outs, and
`BUILD_PARTITION_FACTOR = 16` for the index build and artifact-resolve jobs (finer-grained tasks
even out per-dataset skew and let Spark overlap scheduling with execution). An explicit configured
value always wins over derivation, so an operator who has already tuned a fixed count for their
cluster sees no behavior change.

A new `--discover-partitions` CLI flag (default `64`) separately tunes the executor fan-out width
used when discovering dataset URIs under a base prefix (`cloud_storage.discover_datasets`), which is
a listing operation rather than a per-dataset fleet phase and so is not covered by
`derive_partitions`.

Addendum (2026-07-09): the fixed-cap override fields — `MaintenanceConfig.max_tasks` /
`batch_partitions` and `IndexJobConfig.max_build_tasks` / `batch_partitions` — were subsequently
removed. `derive_partitions` is now called unconditionally with `configured=None` at every call
site, so partition counts always derive from the cluster's `defaultParallelism`. No deployment had
ever set an explicit override, so the "explicit value wins over derivation" escape hatch described
above is no longer reachable.

## ADR 0038 — Ingestion and the maintenance/indexing pipeline must not overlap per dataset

Status: Accepted

Ingestion (the ETL `merge_insert` into a dataset) and the maintenance/indexing pipeline for that
same dataset must not run concurrently. This is an operational scheduling rule, not a code-level
lease: there is no in-code lock, because the fleet jobs are cursor-free and per-dataset failure
isolation (ADR 0035) already guarantees a lagging or failed dataset never extends the pipeline run
window. A failed dataset is retried by the DAG's own retry budget and then by the next scheduled
run, not by holding the current run open. Isolation bounds the blast radius of a failed dataset but
does not, by itself, prevent an ETL write and a pipeline compaction or index build from landing on
the same dataset in the same wall-clock window.

The recommended enforcement lives at the Airflow scheduling layer: couple the ETL DAG
(`lance_etl_etl_dag.py`) and the pipeline DAG (`lance_etl_pipeline_dag.py`) so they never overlap
for the same dataset, either by merging them into a single serialized DAG, or by adding an
`ExternalTaskSensor` (or a dataset- or pool-based mutual exclusion) so the pipeline waits for the
ETL window to finish and vice versa. The pipeline's `max_active_runs=1` only prevents the pipeline
DAG from overlapping itself. It does not guard against the separately scheduled ETL DAG, so the
scheduling coupling above is required in addition, not instead of, `max_active_runs=1`.

The shipped default now staggers the pipeline schedule to reduce, not eliminate, the collision.
`lance_etl_pipeline_schedule` defaults to the cron offset `15 * * * *` instead of `@hourly`, so out
of the box the pipeline trails the hourly ETL DAG by fifteen minutes rather than firing at the same
instant. This is still overridable through the same Variable and is not a substitute for the
structural coupling recommended above. A slow ETL run can still be in flight when the staggered
pipeline run starts, so full mutual exclusion still requires one of the couplings described in the
previous paragraph.

## ADR 0039 — Commit-retry and idempotency hardening for bootstrap and tag operations

Status: Accepted

Two gaps remained between the fleet jobs' commit paths and full idempotency. First, the
vector-index bootstrap `create_index` call (ADR 0030's committed streaming k-means path) did not go
through the shared commit-retry wrapper the way every other commit path does, leaving it without a
fresh-dataset-reopen retry on conflict. It now goes through `commit_with_retries` like the rest.
Second, serving-tag create, update, and delete are plain object-store writes that raise on
conflict rather than optimistic-concurrency commits, so a lost race left the caller without a
consistent outcome. `update_serving_tag` in `maintenance/tools.py` resolves this idempotently: a
create that finds the tag already present falls back to an update, and an update or prune that
finds the tag absent resolves the other way, via `resolve_serving_tag` with a bounded retry
(`MAX_TAG_RACE_ATTEMPTS`, `TAG_RACE_BACKOFF_SECONDS`) for the pathological double-race case.

Separately, the single-fragment compaction skip check in `compaction_skip_reason` now also
inspects `num_deleted_rows` from the dataset stats, not just fragment count, so a lone fragment
that only ever accumulates soft-deletions (no new fragments to trigger a rewrite) is no longer
skipped forever and eventually gets compacted to reclaim its deleted rows.

The whole pipeline is verified idempotent end to end by `tests/test_fleet_idempotency.py`: a second
identical run over an already-maintained fleet is a strict no-op, no new dataset version, a
byte-identical committed index layout, and every plan phase reporting nothing left to do, across
vector, BTREE, BITMAP, and FTS indexes, both through the unified pipeline and through the
maintenance and indexing jobs driven standalone.

## ADR 0041 — Clustered rewrite: centroid-locality dataset reorganization

Status: Qualification-only, production-disabled

The implementation remains for direct tests, but production maintenance exposes no clustered
rewrite command-line flags and `MaintenanceConfig.cluster_rewrite` defaults to `False`. The
Overwrite can clobber an overlapping writer and temporarily removes required indexes. Re-enabling
it as a production path requires explicit memory and writer-overlap qualification.

An IVF partition scan only needs the fragments holding rows assigned to the queried centroids,
but ingestion and ordinary compaction land rows in write order, not centroid order, so partition
membership ends up scattered across every fragment and a query still touches all of them. Lance's
`compact_files` deliberately preserves insertion order, so there is no built-in clustered
compaction. Achieving centroid locality instead requires a full, manual read-reassign-rewrite
pass. `MaintenanceJob` gains an opt-in mode, `cluster_rewrite`, that performs this reorganization
for a dataset and subsumes normal compaction for that dataset in the same run. A clustered dataset
never also receives an ordinary compaction rewrite that would undo its centroid ordering.

The mechanism is a plan-then-shuffle-then-commit pipeline. The plan phase resolves the vector
column and its committed IVF_RQ artifact config (index name, centroids, `rabitq_model`,
`num_partitions`, `rows_at_train`), and runs TTL expiration first so expired rows are never
carried into the new generation. A single flat Spark job then computes a per-centroid row-count
histogram over the pinned read version. The driver derives skew-proof buckets from that
histogram by packing contiguous centroid-id ranges up to a per-task row cap, and any single
centroid larger than the cap is split into salted sub-buckets, the same technique the ETL's
adaptive routing uses for oversized keys, so no write task's memory depends on how skewed one
centroid is. Rows then cross a single shuffle as Arrow IPC keyed by bucket, each bucket is
sorted by its temporary partition-id column, the column is dropped, and `write_fragments` writes
the sorted rows with `mode="create"` and `data_storage_version="2.1"`. The new fragment set
commits as one `LanceOperation.Overwrite`, through `commit_with_retries` like every other fleet
commit path.

Overwrite's semantics were spot-checked against the lance source rather than assumed:

- **Config KV survives.** `Manifest::new_from_previous` clones `previous.config`, so
  `lance-etl.columns` and `lance-etl.vector.{column}` need no post-commit rewrite.
- **Version history and tags survive.** Overwrite commits a new version at the same URI rather
  than creating a new dataset, so existing tags keep resolving to the versions they were pinned
  to before the rewrite.
- **Every index is dropped.** Overwrite sets `final_indices = Vec::new()`, and fragment ids reset
  from zero, so the committed generation starts with zero indexes.

Because Overwrite drops every index, the vector index has to be rebuilt afterward, and it is
rebuilt through the segment path with the SAME centroids the rewrite assigned rows against, never
a retrain. Centroids are read sidecar-first via `load_centroids` (ADR 0040), falling back to
`get_ivf_model` captured before the rewrite when the sidecar misses. Each post-commit shard calls
`create_index_uncommitted` with those `ivf_centroids` and the stored `rabitq_model`, and the
commit fan-out runs `merge_existing_index_segments` followed by `commit_existing_index_segments`,
verified to create the index from nothing when no index of that name exists yet in the freshly
committed, index-free generation. `rows_at_train` in the surviving config KV is untouched by any
of this, so `vector_index_needs_retrain` stays false and the next indexing run does not mistake
the rebuild for a retrain trigger. Rows with a null vector cannot be assigned to any centroid, so
they get a designated tail partition id (`num_partitions`), sorted after every real centroid, and
carry through the rewrite intact rather than being dropped.

Rebuild failure is non-fatal and loudly reported, not silently swallowed. Data is already
committed by the Overwrite, so a failed rebuild leaves the dataset complete but unindexed rather
than incomplete. The run records an error marker and exits `3` (`EXIT_PARTIAL_FAILURE`), and
recovery is automatic: the next indexing run sees no vector index, plans a bootstrap retrain, and
the centroid sidecar still holds the old centroids in case an operator wants to re-drive the
rebuild manually instead of waiting for the retrain.

The temporary `PipelineJob` never publishes `HEAD`. Exact promotion belongs to the durable
reconciler after the complete index set is validated and prewarmed. The clustered rewrite itself
never touches any serving tag. The obsolete post-vector-rebuild promotion field and every
clustered-rewrite CLI flag were removed. Because old tags keep the pre-rewrite version readable,
storage roughly doubles
for an internally qualified rewrite until interval-tag pruning and the cleanup horizon retire the
pre-rewrite generation.

Between the Overwrite commit and the index rebuilds, a serve-latest reader would see the freshly
clustered generation with no indexes at all. The vector gap lasts until the same maintenance
phase's rebuild commits, and the scalar and FTS gap lasts until the next indexing run. The
production pipeline therefore rejects `maintenance.cluster_rewrite=True` at configuration time.
Production CLI callers cannot enable the rewrite. Direct maintenance construction remains an
internal qualification surface only.

**Derived-state skip.** `commit_cluster_overwrite` stamps a generation fingerprint (the committed
generation's sorted data-fragment id list and logical row count) into the dataset config KV under
`lance-etl.cluster_generation`, through the same retried `update_config` path as the vector
artifact config. The next clustered-rewrite plan reads the fingerprint from the already-open
manifest and skips the dataset as a terminal `skipped` outcome when it still matches, so leaving
`cluster_rewrite` enabled on a scheduled pipeline does not re-rewrite the whole eligible fleet
every run. The two fingerprint halves catch disjoint kinds of write, closing the gap a bare
`(fragment_count, row_count)` fingerprint left open. Fragment ids are minted monotonically and
never reused, so any fragment-replacing write mints new ids and diverges the id list even when the
count is preserved — a full re-ingest merge that drops the one old fragment and writes one new
fragment with an identical row count still invalidates the match. A pure delete leaves the ids
untouched but lowers the row count, so it invalidates through the count half. Either kind of write
re-enables eligibility. The stamp lands right after the Overwrite, before the index rebuild,
deliberately: a failed rebuild leaves the data clustered, and re-clustering would not repair the
missing index anyway — the next indexing run does. The index rebuild adds only index segments, not
data fragments, so it leaves the fingerprint matching. A skipped already-clustered dataset does
not pass through into normal compaction, which would re-merge its fragments toward insertion order,
but it is not fully skipped: the plan phase still runs the same rotation-gated idle version cleanup
the normal compaction-skip path uses (`idle_cleanup_bytes`), so the pre-rewrite generation the
Overwrite left behind is reclaimed on a later run once it ages past the cleanup horizon and its
pinning tags are gone, rather than being pinned forever. One accepted trade-off remains: the skip
check runs before TTL, so an idle already-clustered dataset still defers time-based row expiry
until a write re-enables it — only version cleanup runs while it stays clustered, not TTL.

**Driver memory.** Each of the three flat phases (histogram, rewrite shuffle, index rebuild)
broadcasts the per-dataset centroid map, roughly 100 MB per large dataset in a generation. Each
broadcast is destroyed (`Broadcast.destroy`) as soon as its phase's collect returns, so the driver
holds at most ONE live generation of centroid broadcasts instead of accumulating all three. The
residual bound is that one generation: the driver still materializes every eligible dataset's
centroids at once during a phase, so an operator sizing a fleet-wide clustered run should budget
driver heap for the sum of centroid sizes across the datasets clustered in the same run. Moving to
per-plan sidecar reads on the executors (the ADR 0040 pattern) would remove even that bound and
remains the follow-up if fleets outgrow it.

Scalar and FTS index rebuilding is deliberately left to the indexing job the next time
`PipelineJob` runs after maintenance, rather than being folded into the clustered rewrite itself.
Role-discovered scalar and FTS targets rebuild there with no special handling. The one documented
limitation is explicit-config-only BITMAP and ZONEMAP indexes, the ones an operator names by
column rather than ones role discovery finds automatically. Those are restored only if the
following indexing run is invoked with the same explicit columns, since nothing in the rewrite
records what an operator had previously requested by name.

Clustered rewrite is a non-transactional, quiescence-requiring operation, like the manifest
migration tools. A concurrent ETL write between the pinned read version and the Overwrite commit
is clobbered, so it depends on the same ingestion/pipeline non-overlap contract ADR 0038 already
states. Production entry points cannot enable it. Direct qualification callers must provide a
quiescent test dataset and keep the rewrite disabled outside that bounded environment.

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
