# Clustered Rewrite Report

**Scope:** a new occasional, operator-triggered capability that completely reads and rewrites a
Lance dataset so that rows assigned to the same IVF centroid (vector-index partition) land
contiguously, in the same fragment where possible. Shipped as an opt-in mode on `MaintenanceJob`
with the core in `src/lance_etl/maintenance/cluster.py` (ADR 0041).

**Goal:** make vector querying cheaper and more accurate. When same-centroid rows share fragments,
an IVF partition scan touches a handful of fragments instead of every fragment in the dataset.

**Outcome:** implemented, reviewed, and green. `etl/venv/bin/pytest -q -m "not integration"` went
from the 613-test baseline to **636 passed, 0 failed**, with ruff format and check both clean.
Nothing is committed. The tree is yours.

---

## Why a manual pipeline

Lance has no built-in clustered compaction. `compact_files` explicitly preserves insertion order
(`rust/lance/src/dataset/optimize.rs`: "Compacts the files in the dataset without reordering
them"), and no z-order or sort-during-compaction option exists in the tree. The mechanism
therefore has to be a manual distributed rewrite. Three facts, verified against the lance checkout
at `~/IdeaProjects/lance`, make it tractable:

1. **IVF assignment needs no rotation.** Lance assigns a row to a partition by nearest centroid on
   the RAW vector under the index distance type. The RaBitQ rotation applies only to residuals
   after assignment (`rust/lance-index/src/vector/ivf/transform.rs`). A blockwise numpy argmin
   over the stored centroids reproduces lance's own assignment exactly for l2, cosine, and dot.
2. **`LanceOperation.Overwrite` is safe for in-place reorganization.** It commits a new version at
   the same URI. Version history, tags, and the dataset config KV all survive
   (`Manifest::new_from_previous` clones `previous.config`, so `lance-etl.columns` and
   `lance-etl.vector.*` need no re-write). All indexes are dropped (`final_indices = Vec::new()`)
   and fragment ids reset from 0.
3. **The dropped index can be rebuilt without retraining.** `commit_existing_index_segments`
   cleanly creates an index when none of that name exists (verified in
   `rust/lance/src/index.rs:1185`). So the rebuild reuses the exact segment-API artifact tuple the
   incremental path uses, with the SAME centroids and the STORED `rabitq_model`, and
   `rows_at_train` stays untouched so the next indexing run does not retrain.

## Decisions taken (user-confirmed)

- **Placement:** an opt-in flag on `MaintenanceConfig` (`cluster_rewrite`, off by default), not a
  standalone tool. A clustered dataset skips normal compaction that run, since the full rewrite
  subsumes it.
- **Packing:** best-effort sorted. Rows are globally ordered by partition id and `write_fragments`
  splits at the row cap, so at most one centroid straddles each fragment boundary.
- **Centroid source:** the object-store centroid sidecar (ADR 0040,
  `{uri}.artifacts/{index_name}.{rows_at_train}.ivf`, native `IvfModel.save/load` with
  `storage_options`), read sidecar-first with a `get_ivf_model` fallback that backfills the
  sidecar. The sidecar landed in the concurrent review-fix batch and this feature is its first
  consumer that needs centroids after the index no longer exists.

## The pipeline

`run_cluster_rewrites(spark, uris, config, cutoff, driver_telemetry)` runs seven phases. The
driver plans, broadcasts, and validates. Every row-level read and write runs in executor closures.

1. **Plan** (per-dataset fan-out): resolve the vector column from `cluster_column` or the single
   vector-role column, guard eligibility (vector index present, stored config with positive
   `rows_at_train` and a `rabitq_model`, non-empty dataset), run TTL now so expired rows are never
   rewritten, resolve centroids sidecar-first, pin the post-TTL read version, schema, row count,
   and fragment shards. Ineligible datasets pass through into normal maintenance with a recorded
   skip reason.
2. **Histogram** (one flat job): scan only the vector column per fragment shard and count rows per
   partition id, with a tail slot for null vectors.
3. **Bucket derivation** (driver, pure): pack contiguous partition-id ranges into buckets capped
   at `cluster_max_rows_per_file` (default `target_rows_per_fragment`). A single centroid larger
   than the cap splits into salted sub-buckets, so write-task memory is bounded regardless of
   centroid skew. Buckets are enumerated globally across all datasets.
4. **Rewrite shuffle** (one flat job): read full rows per shard at the pinned version, tag each
   row with its partition id, slice by bucket, and emit Arrow IPC chunks keyed by global bucket.
   `partitionBy` co-locates each bucket, and the write side sorts by the temp partition column,
   drops it, and writes fragments via `write_fragments(mode="overwrite", data_storage_version="2.1")`
   (uncommitted fragment metadata only, nothing is visible yet).
5. **Overwrite commit** (per-dataset fan-out): the driver first refuses to commit any dataset
   whose written row total does not equal the planned total, then each dataset commits
   `LanceOperation.Overwrite` under `commit_with_retries`.
6. **Index rebuild** (flat build job + per-dataset commit fan-out): shard the fresh fragment ids,
   `create_index_uncommitted` per shard with the preserved centroids and stored `rabitq_model`,
   then `merge_existing_index_segments` and `commit_existing_index_segments` on an executor.
   Scalar and FTS indexes are deliberately left to the indexing job that `PipelineJob` runs next,
   which rebuilds them through role discovery.
7. **Finalize:** optional blue-green `HEAD` advance (`cluster_serve_tag`, only after a successful
   rebuild commit), then version cleanup.

Null-vector rows go to a designated tail bucket after every real partition, so they are preserved
and grouped rather than scattered.

### CLI

```bash
lance-etl-maintenance run --cluster-rewrite [--cluster-column vector] [--cluster-serve-tag] ...
```

The flags are deliberately not exposed on the pipeline CLI. This is an occasional maintenance-window
operation, not a scheduled phase.

## Review findings and fixes

The implementation was delegated to subagents and each diff was reviewed centrally. Two real bugs
were found by end-to-end testing during integration and one gap was found in review:

1. **Circular import** between `maintenance/job.py` and `maintenance/cluster.py`. Fixed with bare
   module imports plus deferred attribute access on both sides, verified import-order-independent.
2. **`write_fragments(mode="create")` fails against an existing dataset** with
   `Dataset already exists`. The migrate-namespace template writes to a fresh URI, but an in-place
   rewrite targets an existing one. `mode="overwrite"` assigns the same fresh field ids while
   accepting an existing destination, and still commits nothing by itself.
3. **Missing per-dataset failure isolation in the two flat Spark jobs** (review finding). One
   failing shard read, bucket write, or segment build aborted the entire fleet run, including
   datasets whose overwrites had already committed. Fixed with error sentinels through the shuffle
   and tagged results in the rebuild build job. A poisoned dataset now carries an error marker with
   the real exception message, is excluded from every later phase, and never reaches
   `commit_segments` with a partial segment set. Two new two-dataset poisoning tests prove a
   poisoned dataset cannot touch a healthy one, at both the pre-commit (data untouched) and
   post-commit (data intact, unindexed) stages.

## Verification

- `uvx ruff format` and `uvx ruff check` over `src/ tests/ airflow/ bench/`: both clean.
- `etl/venv/bin/pytest -q -m "not integration"`: **636 passed, 32 deselected** (baseline was 613).
- 23 new tests across three files:
  - `tests/test_cluster_assignment.py` (11): assignment math for all three metrics including the
    zero-vector cosine guard, null-to-tail routing, IPC round trips, agreement with real lance
    centroids from a bootstrapped index, and `derive_buckets` packing, salting, and guards.
  - `tests/test_cluster_rewrite.py` (8): end-to-end on FakeSpark with real tmp datasets — row
    multiset preserved, rows non-decreasing in partition id across fragment order, config KVs
    survive, the rebuilt index carries IDENTICAL centroids, `rows_at_train` unchanged with no
    vector work planned afterwards, KNN correct against brute force, null rows in the tail,
    idempotent re-runs, three failure-isolation scenarios, and the Overwrite-preserves-config
    defensive check.
  - `tests/test_cli.py` (4 added): the three new maintenance flags parse and wire into config.

## Operational caveats

- **Quiescence is a hard requirement.** The rewrite is non-transactional against concurrent
  writers. A write landing between the pinned read version and the Overwrite is clobbered. The
  mode is off by default and the CLI help states the requirement (ADR 0038 already forbids
  ingestion overlapping maintenance per dataset).
- **A failed index rebuild is non-fatal by design.** The data is complete but unindexed, the run
  exits with the partial-failure code 3, and the next indexing run bootstraps a retrain. A retrain
  loses the exact fragment-to-partition alignment, so re-drive the rewrite afterwards if that
  alignment matters. The sidecar keeps the old centroids available for that re-drive.
- **Storage roughly doubles** until interval-tag pruning and the cleanup horizon retire the
  pre-rewrite generation, because tagged versions are cleanup-exempt.
- **Shuffle volume:** full rows including vectors cross the Spark shuffle once as Arrow IPC. The
  ETL already ships full row data through shuffles, and the bucket cap bounds each write task, but
  wide-vector fleets should lower `cluster_max_rows_per_file`.
- **A real-cluster smoke run is recommended before first production use.** FakeSpark cannot
  exercise real shuffle co-location, and the histogram phase adds one extra vector-column scan.

## Follow-ups worth considering

- Expose `--cluster-max-rows-per-file` on the maintenance CLI (currently config-only).
- A recall audit (`lance_etl.recall`) before and after a production rewrite to quantify the
  accuracy claim on real traffic.
- If clustered rewrites become routine rather than occasional, revisit whether the histogram scan
  can be folded into the rewrite read to save the extra pass.
