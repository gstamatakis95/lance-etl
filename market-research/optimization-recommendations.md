# Optimization recommendations: compaction, indexing, index maintenance

Every candidate optimization with evidence from the pinned lance checkout, risk assessment, and a verdict.
Citations were spot-verified against `/Users/gstamatakis/IdeaProjects/lance` during this research pass and all
checked paths and line ranges were found accurate.

Workload lens: 30,000 orgs per namespace, up to 1 B rows, power-law distribution, with ingestion, compaction,
and indexing coexisting (see concurrency-and-coexistence.md for the conflict-layer analysis).

## Applied now

### C1. Reorder the DAG to etl >> compact >> index

The compaction planner cannot bin fragments with different index-coverage sets together. A bin is flushed
whenever the covering-index set changes. Tier-B `Compaction.commit` always remaps covering indices inline.
Compacting the fresh, still-uncovered fragments first merges N tiny merge_insert fragments into one before any
index covers them, so indexing then covers one large fragment and the inline remap cost for fresh data
disappears entirely.

- Evidence: bin split on index-set change at
  `/Users/gstamatakis/IdeaProjects/lance/rust/lance/src/dataset/optimize.rs:676-695` (verified, the
  `bin.indices == indices` check). Inline remap on commit at
  `/Users/gstamatakis/IdeaProjects/lance/python/src/dataset/optimize.rs:567-568` (verified,
  `CompactionOptions::default()` hard-coded with a TODO). Current order at
  `/Users/gstamatakis/IdeaProjects/lance-etl/airflow/lance_etl_dag.py:412`.
- Risk: vector/FTS coverage of the newest rows lands slightly later in the run window. The identical
  unindexed-data exposure already exists in the current order during compaction. Cross-check before shipping:
  lancedb issue #2751 reports merge_insert failures after a compact-then-index-optimize ordering in the OSS
  client. Verify against the pinned build in the e2e suite (see production-patterns.md, ordering caveat).
- Verdict: apply now.

### C2. Default CompactionConfig.compaction_mode to "try_binary_copy"

Binary copy skips decode and re-encode entirely when fragments are compatible and falls back to reencode per
task otherwise, a pure CPU win for the append-mostly long tail of orgs. The mode travels to executors because
`CompactionTask` serializes the full options, so it works on both tiers.

- Evidence: TryBinaryCopy fallback at
  `/Users/gstamatakis/IdeaProjects/lance/rust/lance/src/dataset/optimize.rs:135-136, 1504-1513`. Safety gate
  (no blob columns, no deletion files, same field layout and file version, non-legacy format, any error
  disables) at `optimize.rs:417-520`. Options serialized into task JSON at `optimize.rs:1269-1273`. Mode
  accepted from Python at `/Users/gstamatakis/IdeaProjects/lance/python/python/lance/dataset.py:6705-6712`.
- Risk: fragments with deletion files (upsert-heavy orgs) silently fall back to reencode, so the win is
  workload-dependent. Never use `force_binary_copy`, which errors instead of falling back
  (`optimize.rs:1505, 1617`).
- Verdict: apply now.

### I1. Small tier: replace unconditional rebuilds with optimize_indices()

`index_dataset_locally` currently calls `create_index` / `create_scalar_index(replace=True)` unconditionally,
so the entire power-law tail (most of the 30k orgs) pays a full rebuild daily even when nothing changed. Call
`dataset.optimize_indices()` instead when the index already exists. It appends only unindexed fragments,
no-ops cheaply for fully covered scalar indices, and has a rebalance-aware no-op for vector indices. Fall back
to `create_*` only when the index is missing or params changed.

- Evidence: unconditional rebuilds at
  `/Users/gstamatakis/IdeaProjects/lance-etl/src/lance_etl/indexing.py:1100-1108, 1126` (verified). No-op
  gates at `/Users/gstamatakis/IdeaProjects/lance/rust/lance/src/index.rs:1349-1361` and
  `/Users/gstamatakis/IdeaProjects/lance/rust/lance/src/index/append.rs:393-404` (verified, vector gate at
  396-402). Delta append assigns new rows to existing IVF partitions without retraining at
  `/Users/gstamatakis/IdeaProjects/lance/python/python/lance/dataset.py:6745-6776`.
- Risk: delta appends reuse old centroids, so recall can drift as tail orgs grow. Pair with the retrain
  trigger (I3). First-run and param-change paths must still create.
- Verdict: apply now.

### I2. FTS: incremental maintenance via optimize_indices(index_names=[fts_name])

Replace the drop-and-full-rebuild-every-run policy. The Rust merge path handles INVERTED deltas natively,
merging only unindexed fragments into the latest N deltas, and internally falls back to a rebuild from old
plus new data only when the index's update criteria require it. The worst case equals today's cost while the
common case (few new fragments) is dramatically cheaper across 30k datasets.

- Evidence: current full rebuild at
  `/Users/gstamatakis/IdeaProjects/lance-etl/src/lance_etl/indexing.py:920-922, 997-999`. INVERTED incremental
  branch at `/Users/gstamatakis/IdeaProjects/lance/rust/lance/src/index/append.rs:570-640` (verified,
  including the no-op when unindexed is empty and `num_to_merge <= 1` at 576-578 and the
  `requires_old_data` rebuild fallback at 594-625).
- Risk: `optimize_indices` is a single-process call. For head datasets with a large unindexed backlog the
  merge runs on one executor instead of the distributed metadata-merge fan-out. Keep the distributed rebuild
  path for those (trigger on `num_unindexed_fragments` above a threshold) and for tokenizer-param changes.
- Verdict: apply now.

### I3. IVF retrain trigger via rows_at_train in the artifact sidecar

Persist `rows_at_train` in the manifest and force retraining (through the rebuild path) when current rows
exceed for example 4x `rows_at_train`. Today `reuse_artifacts` pins centroids forever, so an org growing from
50k rows (~224 partitions) to 50 M rows keeps 224 stale centroids, degrading recall and partition balance.
Note that `retrain=True` via `optimize_indices` is NOT usable from Python: the binding parses only
`num_indices_to_merge` and `index_names`, so retrain must go through the existing rebuild/segment path.

- Evidence: manifest has no row count at
  `/Users/gstamatakis/IdeaProjects/lance-etl/src/lance_etl/indexing.py:786-794`. Unconditional reuse at
  `indexing.py:756-768`. Binding gap at `/Users/gstamatakis/IdeaProjects/lance/python/src/dataset.rs:2112-2126`
  (verified, only the two kwargs are parsed). `OptimizeOptions.retrain` exists in Rust at
  `/Users/gstamatakis/IdeaProjects/lance/rust/lance-index/src/optimize.rs:29-40`.
- Risk: occasional full retrain of grown datasets adds load spikes. Choose the growth factor so head datasets
  retrain rarely. The manifest schema change invalidates nothing, since a missing field just means
  retrain-once.
- Verdict: apply now.

### I4. Bound delta/segment accumulation with scheduled merges

BTREE segments are committed unmerged and the vector handler merges only each run's NEW segments, so
incremental runs accumulate one extra delta per run per index, and every query consults all of them. Drive a
periodic merge with `optimize_indices(num_indices_to_merge=N)` scheduled from `index_statistics`
(`num_indices` / `num_segments` fields), for example merge when `num_indices > 4`. This also permanently
retires deferred frag-reuse remap debt because merged indices are rewritten against current row addresses.

- Evidence: unmerged BTREE commits at
  `/Users/gstamatakis/IdeaProjects/lance-etl/src/lance_etl/indexing.py:385-392, 834-840`. Per-run-only vector
  merge at `indexing.py:388-390`. Merge semantics at
  `/Users/gstamatakis/IdeaProjects/lance/rust/lance-index/src/optimize.rs:13-24`. Stats fields at
  `/Users/gstamatakis/IdeaProjects/lance/rust/lance/src/index.rs:1569-1575`.
- Risk: BTREE delta merges may rebuild from old plus new data internally (sort-merge), costing close to a
  rebuild, which is exactly why this is threshold-scheduled rather than per-run. The generic scalar path logs
  and skips if a segment cannot be opened (`append.rs:545-556`), so the step degrades safely.
- Verdict: apply now.

## Deferred, with reasons

### D1. max_source_fragments tuning for tier-B head datasets

Tasks are admitted oldest-first with `take_while` until the fragment budget is hit, so a per-run cap (for
example 200-400 source fragments) turns one giant remap-heavy commit into several bounded incremental runs
that coexist better with concurrent ingestion (smaller conflict window per commit).

- Evidence: `take_while` admission at
  `/Users/gstamatakis/IdeaProjects/lance/rust/lance/src/dataset/optimize.rs:718-729` (verified). Option doc at
  `optimize.rs:213-218`. Commit remap unavoidable on tier B at
  `/Users/gstamatakis/IdeaProjects/lance/python/src/dataset/optimize.rs:567-568` (verified).
- Why deferred: head datasets need more runs to fully converge, and choosing the cap requires knowing remap
  cost per fragment for the actual index mix. Operator tuning, not a code default.

### D2. Pin num_threads on the small tier to executor_cores / task_slots

The default is the machine's full compute-CPU count per `Compaction.execute` call, and the small tier runs
many datasets concurrently as Spark tasks on the same executor, so defaults oversubscribe CPU exactly where the
power-law tail concentrates.

- Evidence: default and execute-only usage at
  `/Users/gstamatakis/IdeaProjects/lance/rust/lance/src/dataset/optimize.rs:186-189, 776-780`. Small tier
  batches many datasets per executor at
  `/Users/gstamatakis/IdeaProjects/lance-etl/src/lance_etl/compaction.py:439-456`.
- Why deferred: depends on Spark executor sizing, and setting it too low slows the rare medium dataset on the
  small tier.

### D3. Enable move-stable row ids at dataset bootstrap

With stable row ids the compaction commit needs NO index remapping at all
(`needs_remapping = !uses_stable_row_ids && !defer_index_remap`), which structurally removes the tier-B
inline-remap problem, makes the `defer_index_remap` binding gap moot, and reduces conflicts between all three
jobs.

- Evidence: `/Users/gstamatakis/IdeaProjects/lance/rust/lance/src/dataset/optimize.rs:1893` (verified) and
  1494, 1604-1605, 1669-1671 (rechunk path). Bootstrap currently does not set it at
  `/Users/gstamatakis/IdeaProjects/lance-etl/src/lance_etl/etl.py:392-394`.
- Why deferred: requires recreating or migrating 30k existing datasets, and stable-row-id read paths plus
  interaction with merge_insert on the pinned build need a verification pass before fleet rollout. This is the
  highest-leverage long-term item.

### D4. Keep defer_index_remap opt-in, tier A only, no frag-reuse pruning from Python

`cleanup_frag_reuse_index` exists in Rust but its only callers are tests, with no Python binding, so
deferred-remap debt can only be retired by index optimize or rebuild. The repo also records an observed
vector-query failure under deferral on the pinned build, which outweighs the commit-time savings.

- Evidence: `cleanup_frag_reuse_index` at
  `/Users/gstamatakis/IdeaProjects/lance/rust/lance/src/dataset/index/frag_reuse.rs:28` (callers only in tests
  at `rust/lance/src/dataset/optimize.rs:3697, 3776`). Frag-reuse applied at index load at
  `/Users/gstamatakis/IdeaProjects/lance/rust/lance/src/index/vector/ivf/v2.rs:808, 878`. Observed failure
  note at `/Users/gstamatakis/IdeaProjects/lance-etl/src/lance_etl/compaction.py:66-69`.
- Why deferred: status quo confirmation. Enabling deferral without a verified remap-catch-up step risks broken
  vector queries.

### D5. Leave materialize_deletions_threshold and target_rows_per_fragment at defaults

Any fragment under `target_rows_per_fragment` (1 M) is a CompactWithNeighbors candidate, and noop bins (a
single small fragment with no neighbor) are filtered, so tiny tail orgs with one small fragment correctly do
nothing. Tuning `batch_size` down only matters for wide or vector-heavy rows that OOM during rewrite.

- Evidence: candidacy at `/Users/gstamatakis/IdeaProjects/lance/rust/lance/src/dataset/optimize.rs:647-658`
  (verified). Defaults at `optimize.rs:233-236`. Noop filter and `split_for_size` at `optimize.rs:709-715`
  (verified). Batch-size memory note at
  `/Users/gstamatakis/IdeaProjects/lance/python/python/lance/dataset.py:6700-6702`.
- Why deferred: guidance only, no change needed.

### D6. memory_limit and num_workers for INVERTED builds

Defaults are 2 GiB per worker with `num_compute_cpus` workers per build. Our FTS executor tasks run
per-fragment builds concurrently with other Spark tasks on the same host, so defaults can oversubscribe
executor memory, and a larger explicit `memory_limit` produces fewer FTS shards which are cheaper to search.

- Evidence: `/Users/gstamatakis/IdeaProjects/lance/python/python/lance/dataset.py:3211-3225`. Our params omit
  both at `/Users/gstamatakis/IdeaProjects/lance-etl/src/lance_etl/indexing.py:156-174`.
- Why deferred: correct values depend on executor sizing. Wrong values either OOM or produce many shards.

### D7. Revisit MAX_IVF_PARTITIONS=4096 for head datasets

At 1 B rows the sqrt policy wants ~31.6k partitions but the clamp leaves ~244k rows per partition, inflating
per-query partition scan cost. Raising the cap (or making it size-tiered) for the few head orgs trades training
time (`num_partitions * sample_rate=256` sampled rows) for query latency. Conversely `sample_rate=256` with the
degrade rule is already correct for tiny orgs.

- Evidence: clamp at `/Users/gstamatakis/IdeaProjects/lance-etl/src/lance_etl/indexing.py:56-57, 213-228`.
  Degrade rule at `indexing.py:231-246`. `train_ivf` sample-rate usage at `indexing.py:777-783`.
- Why deferred: more partitions increase training cost and centroid memory, and recall/latency effects need
  benchmarking on head datasets before changing the cap.

## Index maintenance reference (the patterns the apply-now items implement)

- `optimize_indices` is the canonical incremental-maintenance entry point: appends unindexed fragments to
  existing indices without retraining (new rows assigned to existing IVF partitions) and no-ops cheaply when
  nothing changed (`python/python/lance/dataset.py:6745-6776`, `rust/lance/src/index.rs:1328-1403`).
- Delta-index protocol: `num_indices_to_merge=0` creates a new delta (fast append), `Some(N)` merges the delta
  plus latest N indices into one. The intended pattern is a large base snapshot plus a few accumulating deltas
  merged periodically (`rust/lance-index/src/optimize.rs:13-24`, constructors at 70-93).
- Python binding gap: only `num_indices_to_merge` and `index_names` are parsed, while `retrain`,
  `transaction_properties`, and `progress` are silently dropped
  (`python/src/dataset.rs:2112-2126`, verified).
- No-op gates make per-dataset maintenance sweeps cheap across 30k orgs: scalar groups skip when fully covered
  (`index.rs:1349-1361`), vector groups bail unless there is new data, a rebalance candidate, retrain, or an
  explicit merge (`append.rs:393-404`, verified).
- INVERTED indices maintain incrementally through the same path, with automatic internal fallback to an
  old-plus-new rebuild when `update_criteria.requires_old_data` (`append.rs:570-640`, verified).
- Scheduling signals: `index_statistics(name)` returns `num_indices`/`num_segments`, `num_indexed_fragments`,
  and `num_unindexed_fragments`. Use these to decide when to merge deltas or skip a dataset entirely
  (`index.rs:1557-1575`, fragment APIs at `index.rs:1753-1756, 2361`).
- Frag-reuse lifecycle: the deferred remap is applied lazily whenever an index is loaded
  (`index/vector/ivf/v2.rs:808, 878`), indices catch up permanently when merged or optimized, and pruning stale
  frag-reuse versions has no Python binding.
- Compaction-indexing coupling: tier-B `Compaction.commit` always remaps inline
  (`python/src/dataset/optimize.rs:567-568`, verified), and stable row ids remove remap entirely
  (`optimize.rs:1893`, verified). Remap cost is controlled by compacting before indexing (C1), capping
  `max_source_fragments` per run (D1), or migrating to stable row ids (D3).
- Current pipeline gaps the apply-now items close: the small tier rebuilds all indices every run
  (`indexing.py:1100-1126`, verified), FTS drops and rebuilds the whole index every run
  (`indexing.py:920-922, 997-999`), and incremental segment commits accumulate unmerged deltas with no merge
  step anywhere (`indexing.py:385-392`).
