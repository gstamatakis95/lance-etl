# Maintenance & Indexing Production-Readiness Report

**Scope:** `src/lance_etl/maintenance/`, `src/lance_etl/indexing/`, `src/lance_etl/pipeline/`, and the
shared `src/lance_etl/fanout.py` and `src/lance_etl/cliutil.py`. The ETL package
(`src/lance_etl/etl/`) is explicitly out of scope and owned by a separate effort (see `report1.md`).
This pass hardened the fleet compaction, maintenance, and indexing jobs for scale (more than 30k
datasets, one Lance dataset per org), for non-OOM behavior on very large single orgs, and for
idempotency under repeated scheduled runs.

---

## Executive summary

The fleet jobs were architecturally sound (driver never opens datasets, all heavy work in executor
closures, Spark job count is O(rounds) not O(datasets)) but had five production-blocking gaps at
fleet scale plus one latent correctness bug. All are now fixed, verified, and documented in ADRs
0035 through 0039 in `docs/adr/fleet-orchestration-and-maintenance.md`.

The fast test gate is green: `etl/venv/bin/pytest -q -m "not integration"` gives `589 passed, 1
skipped, 1 xfailed` (the xfail is the documented pylance 8.0.0 BTREE-delta regression). The
maintenance and indexing integration subset (idempotency end-to-end, concurrency coexistence, V2
manifest migration) is green. `ruff format` and `ruff check` both pass. The changes span 11 source
files (about 1000 insertions) and add 6 new test files. Git was left untouched for the owner to
review and commit.

---

## The three goals and how they are addressed

### 1. Scale to an arbitrary number of orgs (more than 30k datasets)

The dominant blockers were blast radius, per-run request cost, and fixed parallelism.

- **One bad dataset used to abort the whole fleet.** Every fleet phase ran as an all-or-nothing
  flat Spark job. A single unreadable manifest, a non-conflict commit error, or a corrupt fragment
  in any one of 30k datasets aborted the entire round and discarded the healthy work for every
  other org. Now each phase isolates per-dataset failures into markers and the rest of the fleet
  always completes (see Failure isolation below).
- **`cleanup_old_versions` ran for every dataset every run.** That is roughly five object-store
  LIST calls per dataset per run, even for idle datasets that received no writes. On a large
  mostly-idle fleet this is the dominant request cost. A rotation slice now cleans idle datasets
  only on their slot while always cleaning datasets that did work (see Cleanup rotation below).
- **Parallelism was capped at fixed constants** (`max_tasks=256`, `batch_partitions=512`,
  `max_build_tasks=1024`) regardless of cluster size. These are now cluster-aware (see Cluster-aware
  parallelism below). Dataset discovery gained a `--discover-partitions` knob.

### 2. Very large single orgs must not OOM and must break into independent Spark tasks

Maintenance already streams each rewrite (bounded by `batch_size`, not fragment size) and schedules
individual `CompactionTask` rewrites across executors, so one giant dataset's compaction is spread
across the cluster rather than handled by a single executor end to end. Indexing shards a large
dataset's uncovered fragments into independent build tasks. The vector-index bootstrap is the one
sanctioned single-task path (streaming k-means with bounded memory, ADR 0030), and its commit is now
retry-wrapped so a conflict on a big-org build no longer aborts the fleet. Row-order-by-timestamp is
an ETL property (collapse plus last-writer-wins) and is out of scope here. Maintenance and indexing
operate on already-committed state, so operation ordering does not apply to them.

### 3. Idempotency of compaction, maintenance, and indexing

Proven end to end by a new real-Spark test. A second identical pipeline run over an
already-maintained fleet is a strict no-op across vector, BTREE, BITMAP, and FTS indexes. A latent
correctness bug that broke single-fragment idempotency was fixed (see below).

---

## Findings and changes

### Failure isolation (ADR 0035)

Each fleet phase now catches per-dataset exceptions at the Spark-closure boundary and records an
error marker `{"uri", "error", "phase"}` instead of propagating. The shared `fan_out_per_dataset`
helper isolates the plan and operator-tool fan-outs. The flat execute, build, and commit jobs
isolate per task. The isolation lives in the orchestration layer, not the per-dataset business
functions, so their existing raise contracts and unit tests are unchanged. The only behavior that
changed is that `.run()` no longer propagates a single dataset's error. A failed dataset is excluded
from later phases in the same run (a failed rewrite is never committed, a failed build never
publishes a partial index) and every uri still lands exactly one terminal result. This is safe
because the jobs are cursor-free: they re-plan from live dataset state, so a skipped dataset catches
up fully next run with no watermark and no data loss. `tests/test_poisoned_dataset.py` exercises
plan, commit, and build failures across all three entry points.

### Partial-failure exit codes (ADR 0035)

The maintenance, indexing, and pipeline CLIs return `0` when every dataset succeeded,
`EXIT_PARTIAL_FAILURE = 3` when the run completed but one or more datasets were isolated, and `1` on
an unhandled exception. Exit `3` marks the Airflow task failed, so with the DAG's `retries = 2`
policy Airflow re-runs the whole fleet before the next scheduled interval. Idempotency makes that
retry a near-no-op for the datasets that already succeeded, and a dataset that keeps failing past
the retry budget is carried to the next scheduled run. Driver metrics `run.datasets_failed` and
`dataset.failed` (tagged `phase:`) surface the count.

### Stamp gating (ADR 0035) — the correctness bug

The failure-isolation feature initially still HEAD-promoted a dataset whose index build failed. Such
failures are recorded as per-index entries inside the dataset's `indexes` list, but `stamp_eligible`
checked only the top-level `error` key, so a partially-indexed dataset passed the gate and its
serving tag advanced onto an incomplete index, degrading recall for that org while the same dataset
was simultaneously reported failed. `stamp_eligible` now excludes a dataset with a dataset-level
error or any per-index error entry. The fix was proven by empirically running the old logic and
observing the broken dataset get promoted, then confirming the fix blocks it
(`tests/test_poisoned_dataset.py`).

### Cleanup rotation (ADR 0036)

`MaintenanceConfig` gains `cleanup_rotation_slots` (default 8) and `cleanup_rotation_cadence_hours`
(default 1). A dataset that did work this run (committed compaction or TTL delete) is always cleaned.
An idle dataset is cleaned only when its slot matches the run's active slot. The slot is keyed by
`hashlib.sha256(uri)` rather than the builtin `hash()`, because the builtin is salted per interpreter
process and would place the same URI in a different slot on the driver versus an executor. SHA-256 is
stable across processes and versions, which the determinism and full-coverage guarantees depend on:
every idle dataset is still cleaned at least once within `cleanup_rotation_slots` consecutive runs.
New metrics `dataset.cleanup_rotation_skipped`, `dataset.old_versions_removed`, `dataset.cleaned`,
and `run.cleanup_slot`. `tests/test_cleanup_rotation.py` covers determinism, full coverage, the
degenerate single-slot case, and the skip-then-clean boundary.

### Cluster-aware parallelism (ADR 0037)

The four fixed-cap fields are now `int | None` defaulting to `None`. When unset,
`derive_partitions(configured, spark, multiplier)` resolves the partition count at run time from
`spark.sparkContext.defaultParallelism`, scaled by a per-job headroom factor
(`REWRITE_PARTITION_FACTOR=4`, `FANOUT_PARTITION_FACTOR=8`, `BUILD_PARTITION_FACTOR=16`). An explicit
configured value always wins, so an operator who tuned a fixed count sees no change. A new
`--discover-partitions` flag (default 64) tunes the executor fan-out width of base-URI dataset
discovery, which is a listing operation rather than a per-dataset fleet phase.

### Commit-retry and idempotency hardening (ADR 0039)

- The vector-index bootstrap `create_index` was the only commit path not going through the shared
  retry wrapper. It now goes through `commit_with_retries` with a fresh dataset re-open per attempt.
- Serving-tag create, update, and delete are plain object-store writes that raise on conflict rather
  than optimistic-concurrency commits, so `commit_with_retries` would never retry them.
  `update_serving_tag` now resolves lost races idempotently via `resolve_serving_tag`: a create that
  finds the tag present falls back to update, and an update or prune that finds it absent resolves
  the other way, with a bounded retry for the double-race case.
- The single-fragment compaction skip check (`compaction_skip_reason`) short-circuited whenever a
  dataset had at most one fragment, before `Compaction.plan`. Lance marks a single fragment as a real
  `CompactItself` candidate once its deletion fraction exceeds `materialize_deletions_threshold`, so
  a lone fragment accumulating TTL or merge-insert deletions never reclaimed that space. The check
  now also inspects `num_deleted_rows` from the already-loaded dataset stats (zero added I/O) and
  defers to the planner when a single fragment carries deletions.

### Hygiene

- `flatten_shard_tasks` no longer copies an FTS rebuild's entire fragment-id list into every per-shard
  build task. It now emits a minimal whitelist of only the keys the build phase reads.
- `resolve_fleet_artifacts` was converted from `.map` to `.mapPartitions` with one `Telemetry.create`
  per partition, matching every sibling fan-out.
- A stale test docstring in `test_maintenance_fri.py` that falsely claimed the distributed commit
  path ignores `defer_index_remap` options was corrected.

---

## Idempotency verification

`tests/test_fleet_idempotency.py` (marked `integration`) builds a real local Lance fleet with a
multi-fragment dataset (forcing real compaction) and a single-fragment one, configures a mix of
vector IVF_RQ, BTREE, BITMAP, and FTS indexes, and runs `PipelineJob.run` twice. Run 1 compacts four
fragments to one and builds all indexes. Run 2 asserts a strict no-op: every dataset's version is
unchanged, the index layout is byte-identical, every maintenance plan reports zero tasks, and every
index reports already-current. No idempotency bug was found, including on the FTS path. The prune
then maintenance then index ordering means run 1 compacts before indexing the post-compaction
fragments, so run 2 finds full coverage and skips both phases.

---

## Test coverage added

`test_compaction_deletion_skip.py`, `test_index_bootstrap_retry.py`, `test_serving_tag_idempotency.py`,
`test_cleanup_rotation.py`, `test_poisoned_dataset.py`, `test_fleet_idempotency.py`. One existing
test (`test_maintenance_replan.py::test_run_propagates_non_conflict_errors`) was flipped to assert
isolation instead of propagation, which was the only fail-fast pin on `.run()`.

---

## Operational recommendation (ADR 0038)

Ingestion (the ETL merge into a dataset) and the maintenance/indexing pipeline for that same dataset
must not run concurrently. Enforcement belongs at the Airflow scheduling layer, not an in-code lease.
The recommended enforcement is to couple the ETL DAG and the pipeline DAG so they never overlap for
the same dataset, either by merging them into a single serialized DAG or by adding an
`ExternalTaskSensor` or a dataset- or pool-based mutual exclusion. The pipeline's `max_active_runs=1`
only prevents the pipeline DAG from overlapping itself. It does not guard against the separately
scheduled ETL DAG. Because the fleet jobs are cursor-free and now isolate per-dataset failures, a
lagging or failed dataset never extends the pipeline run window, so per-dataset isolation cannot
cause the pipeline to bleed into an ingestion window.

---

## Verification and how to run

```bash
# Fast gate (canonical)
etl/venv/bin/pytest -q -m "not integration"          # 589 passed, 1 skipped, 1 xfailed

# Maintenance/indexing integration subset
etl/venv/bin/pytest -q -m integration \
  tests/test_fleet_idempotency.py \
  tests/test_concurrent_coexistence.py \
  tests/test_v2_manifest_paths.py                     # green, 1 expected xfail

# Lint and format
uvx ruff format src/ tests/ airflow/ bench/
uvx ruff check src/ tests/ airflow/ bench/
```

The full bench end-to-end tier (`test_bench_e2e*`) needs the grpc and bench extras installed and is
out of the changed scope. It was not run in this pass.

---

## Out of scope and known items (not fixed by design)

- The ETL package and the row-order-by-timestamp guarantee are owned separately (`report1.md`).
- The vector-index bootstrap remains a single non-sharded task per dataset by design (ADR 0030). A
  1B-row rebuild runs streaming k-means on one executor with bounded memory. This is a latency, not
  an OOM, tradeoff.
- Orphaned uncommitted segments or rewrite files from a mid-run crash are reclaimed by Lance's 7-day
  unverified-file sweep inside `cleanup_old_versions`, which is acceptable while the pipeline pairs
  the index and maintenance phases. The rotation now emits `dataset.old_versions_removed` for
  visibility.
- The pylance 8.0.0 BTREE-delta concurrent-merge_insert regression remains an upstream xfail.
