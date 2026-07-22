# `lance_etl.indexing`

`indexing/` owns every Lance index build, commit, and delta-merge across the fleet: vector
(IVF_RQ), scalar (BTREE, BITMAP, ZONEMAP), and full text (INVERTED). It is the concrete
implementation of hard rule 6 in the root [`AGENTS.md`](../../../AGENTS.md) — every index for every
dataset size builds exclusively through Lance's uncommitted-segment API, with one sanctioned
exception for the vector bootstrap. `LanceIndexer` in `runner.py` is the single unified entry point.
There is no separate "small dataset" code path. For where indexing sits in the reconciliation cycle
(`INGEST -> COMPACT -> INDEX -> VALIDATE -> PREWARM -> PUBLISH`), see the package
[README](../README.md). For the pylance API ground truth this package depends on
(`build_rq_model`, `get_ivf_model`, `IvfModel.save`/`load`, `lance_field_id`), see
[`AGENTS.md`](../AGENTS.md).

## Module-by-module

| File | Responsibility |
|---|---|
| `runner.py` | `LanceIndexer`, the unified fleet plan -> build -> commit -> delta-bound orchestrator, and the stale-fragment replan loop |
| `handlers.py` | One policy object per index family (`VectorIndexHandler`, `BTreeIndexHandler`, `BitmapIndexHandler`, `ZonemapIndexHandler`, `FtsIndexHandler`) encoding how each type plans, builds, and merges |
| `segments.py` | Segment (de)serialization, the Lance field-id helper, stale-fragment detection, and the generic commit-with-retry primitives every handler calls |
| `optimize.py` | The vector-artifact config KV, the object-store centroid sidecar cache, and incremental delta-merge (`optimize_indices`) for every index type |
| `config.py` | `IndexJobConfig`, index-name derivation, and the IVF partition-count and retrain policy |
| `cli.py` | Uninstalled operator CLI (`python -m lance_etl.indexing.cli`) |
| `__init__.py` | Re-exports the consumer surface (`LanceIndexer`, `IndexJobConfig`, and the handler/segment symbols) |

## The five index families and their build recipes

Every recipe below is the segment-API flow mandated by root `AGENTS.md` hard rule 6 and
[ADR 0001](../../../docs/adr/indexing.md) / [ADR 0029](../../../docs/adr/indexing.md). `runner.py`
dispatches on kind (`VECTOR_KIND`, `BTREE_KIND`, `BITMAP_KIND`, `ZONEMAP_KIND`, `FTS_KIND` mapped
through `KIND_TO_HANDLER`) to the matching `IndexHandler` in `handlers.py`. Each handler's `merges()`
method is the single source of truth for whether its segments merge before commit.

### Vector (IVF_RQ)

Two distinct modes decided at plan time by `plan_dataset_indexes`, per
[ADR 0030](../../../docs/adr/indexing.md):

- **Bootstrap** — the index is absent, `config.rebuild` is set, or
  `VectorIndexHandler.needs_bootstrap` finds the stored vector config unreusable or growth past
  `retrain_growth_factor`. `bootstrap_vector_index` (`runner.py`) runs as ONE executor task: it
  mints a fresh RaBitQ rotation with `build_rq_model`, then commits directly with
  `dataset.create_index(column, "IVF_RQ", name=, metric=, replace=True, num_partitions=,
  num_bits=, rabitq_model=, streaming_sample_rate=, streaming_refine_passes=)` — the ONE sanctioned
  non-segment build in the whole codebase, wrapped in `commit_index_with_retries`. This is where
  lance's streaming k-means trains centroids with bounded memory. The segment path below refuses
  internal training entirely. After commit, `write_vector_config` persists
  `{rows_at_train, dimension, metric, num_bits, num_partitions, rabitq_model}` to the dataset's
  transactional config KV under `lance-etl.vector.{column}`, and `persist_bootstrap_centroids`
  best-effort seeds the object-store centroid sidecar ([ADR 0040](../../../docs/adr/indexing.md)).
- **Increment** — a reusable config already exists. Each shard task's `VectorIndexHandler.prepare`
  resolves centroids sidecar-first (`optimize.load_centroids`), falling back to
  `dataset.get_ivf_model(index_name).centroids` with a best-effort sidecar backfill, then calls
  `build_vector_segment` (`segments.py`): `dataset.create_index_uncommitted(column=,
  index_type="IVF_RQ", name=, metric=, replace=True, num_partitions=, num_bits=,
  ivf_centroids=, rabitq_model=, fragment_ids=)`. The same stored `rabitq_model` string reaches
  every shard, which is required for correctness — a shard that derived its own rotation would
  merge into an inconsistent index. No fleet-wide artifact broadcast: each shard resolves only its
  own dataset's centroids (ADR 0040).

Both modes converge on the same commit fan-out: `commit_segments` (`segments.py`) drops segments
whose fragments are no longer live, merges the survivors with
`dataset.merge_existing_index_segments(segments)`, then commits with
`latest.commit_existing_index_segments(index_name, column, [merged])`.

### BTREE / BITMAP — committed unmerged

`build_scalar_segment` (`segments.py`) issues `dataset.create_index_uncommitted(column=,
index_type=, name=, replace=True, fragment_ids=)` per shard — no `index_uuid`. Both
`BTreeIndexHandler.merges()` and `BitmapIndexHandler.merges()` return `False`: segments commit as
unmerged deltas via `commit_existing_index_segments`, and Lance unions them at query time.
`BitmapIndexHandler` documents why explicitly — a driver-side merge would materialize every
distinct value's bitmap on one heap, 8-16 GB at fleet scale — so consolidation is deferred to the
delta-merge maintenance pass (`optimize.py`, below) instead of happening at build time.

### ZONEMAP — merged before commit

Same per-shard `build_scalar_segment` call as BTREE/BITMAP, but `ZonemapIndexHandler.merges()`
returns `True`: segments merge with `merge_existing_index_segments` before
`commit_existing_index_segments` publishes the result, the same flow as vector. This is the
[ADR 0033](../../../docs/adr/indexing.md) rule — a lance 8.0.0 capability, not a design
preference. Zonemap deltas are not unioned at query time the way BTREE/BITMAP deltas are, so an
unmerged ZONEMAP segment would leave range-pruning degraded across shards rather than merely
deferred.

### FTS (INVERTED) — shared UUID, driver-side atomic swap

1. **Driver** mints one shared `index_uuid = str(uuid.uuid4())` in `plan_dataset_indexes` for a
   rebuild spec.
2. **Executors** each build one fragment at a time: `dataset.create_scalar_index(column=,
   index_type="INVERTED", name=, replace=True, index_uuid=shared, fragment_ids=[fragment_id],
   **fts_params())`.
3. **Commit metadata**: `commit_fts_index` (`handlers.py`) calls
   `dataset.merge_index_metadata(index_uuid, index_type="INVERTED")` — the only call to this
   function anywhere in the package, and the only index family that ever uses it.
4. **Atomic swap**: `publish_fts_index` (`handlers.py`) re-lists the current committed same-name
   segments (`same_name_index_segments`), builds a new `Index(uuid=index_uuid, name=,
   fields=[field_id], dataset_version=, fragment_ids=, index_version=0)`, and commits
   `lance.LanceOperation.CreateIndex(new_indices=[index], removed_indices=old_segments)` via
   `LanceDataset.commit`. The old index stays queryable until this one transaction lands. Every
   retry re-lists `removed_indices` at the latest version before retrying.

A live index with an unindexed-fragment backlog within `fts_max_unindexed_fragments` skips the
rebuild path entirely: `FtsIndexHandler.maintainable` emits a `"maintain"` spec instead, executed by
`optimize.maintain_index_locally` -> `dataset.optimize.optimize_indices(index_names=[index_name])`.

`lance_field_id(dataset, column)` (`segments.py`) is the single documented helper wrapping the
internal `dataset._ds.lance_schema` lookup that FTS needs a real Lance field id (not an Arrow
positional index) for — the one sanctioned leading-underscore access site, per root `AGENTS.md`
hard rule 1.

## Forbidden operations

- **Never call `create_scalar_index(fragment_ids=)` for BTREE, BITMAP, or ZONEMAP.** It raises on
  current lance main. `build_scalar_segment`'s docstring states this explicitly, and every scalar
  build in the package goes through `create_index_uncommitted` instead.
- **Never call `merge_index_metadata` for anything but INVERTED.** `commit_fts_index` is the only
  call site.
- **Never enable move-stable row IDs.** No handler, segment build, or commit path in this package
  passes `enable_stable_row_ids`. See [ADR 0010](../../../docs/adr/rejected-and-operator-tools.md).
- **Never train centroids inside the segment path.** `create_index_uncommitted` hard-requires
  precomputed `ivf_centroids` — training only ever happens in the one committed bootstrap call in
  `bootstrap_vector_index`.

## The stale-fragment replan loop

A concurrent compaction can rewrite fragments that a segment build or commit targeted mid-flight.
`STALE_FRAGMENT_MARKERS = ("would orphan fragments", "no longer exist")` (`segments.py`) are the
substrings `is_stale_fragment_error` matches against a `ValueError` message to recognize this
condition. `commit_one_index` (`runner.py`) catches such an error and returns `{"stale": True, ...}`
instead of propagating it. `LanceIndexer.run` loops `run_round` up to `config.max_stale_replans`
times (default `MAX_STALE_REPLANS = 3`, `config.py`), re-planning and re-building only the datasets a
prior round reported stale. A dataset still stale after every round is marked failed with
`error_phase = "index-stale-exhausted"` and the `index.stale_replans_exhausted` metric increments —
it is never silently dropped. This is [ADR 0009](../../../docs/adr/fleet-orchestration-and-maintenance.md)
(stale index-plan safety) implemented.

## Incremental delta-merge (`optimize.py`)

`optimize_existing_index` wraps `dataset.optimize.optimize_indices(index_names=[...],
num_indices_to_merge=...)` in `commit_index_with_retries`, appending unindexed fragments to an
existing index without retraining. `merge_index_deltas` reads the current delta count from
`stats.index_stats(name)["num_indices"]` and, once it exceeds `config.max_index_deltas` (default
4), collapses every delta into one — this is what eventually consolidates the BTREE/BITMAP deltas
that build time deliberately leaves unmerged, and it also retires any frag-reuse remap debt from a
prior `defer_index_remap` compaction commit. `LanceIndexer.run` calls this fleet-wide at the end of
every round through `bound_fleet_deltas`, over every `(uri, index_name)` that committed segments
this run or that a plan flagged `needs_delta_merge`. FTS is excluded — it merges its own deltas
through the `"maintain"` spec path described above, not `bound_fleet_deltas`.

## Driver vs executor split

`LanceIndexer.run` / `run_round` (`runner.py`) run entirely on the driver: they own the round loop,
fold per-round results into `stats_by_uri`, and produce the final failure/metrics report
(`report_fleet_failures`). The driver never opens a Lance dataset for row-level work. Every dataset
open happens inside a Spark closure:

| Phase | Function | Fan-out |
|---|---|---|
| Plan | `plan_dataset_indexes` | `mapPartitions` per dataset |
| Build | `build_one_shard` / `bootstrap_vector_index` | one flat Spark job across the whole fleet (`build_fleet_segments`) |
| Commit | `commit_one_index` (-> `commit_segments`, `commit_fts_index`) | `mapPartitions` (`commit_fleet`) |
| Delta-bound | `merge_index_deltas` | `mapPartitions` (`bound_fleet_deltas`) |

Each executor closure calls `Telemetry.create(config.telemetry)` itself, per the per-process
telemetry rule in [`AGENTS.md`](../AGENTS.md).

## `IndexJobConfig` (`config.py`)

| Field | Default | Purpose |
|---|---|---|
| `vector_columns`, `scalar_columns`, `bitmap_columns`, `zonemap_columns`, `text_columns` | — | Explicit column lists. An empty list means role auto-discovery drives which columns get which index type ([ADR 0029](../../../docs/adr/indexing.md)) |
| `index_name_overrides` | — | Per-column name override, otherwise derived (`scalar_index_name`, `bitmap_index_name`, `zonemap_index_name`, `fts_index_name`, `vector_index_name`) |
| `num_partitions` | derived | Explicit IVF partition count. When unset it derives from row count via `derive_num_partitions` |
| `minimum_partitions` / `maximum_partitions` | 16 / 32768 | Clamp bounds for derived partition counts |
| `target_rows_per_partition` | 8192 | Row-count target driving derivation |
| `vector_min_rows` | 10000 | Floor below which vector indexing is skipped |
| `metric` | `"L2"` | IVF_RQ distance metric |
| `num_bits` | 1 | RaBitQ quantization bits |
| `streaming_sample_rate` / `streaming_refine_passes` | 32 / 1 | Streaming k-means bootstrap parameters, bootstrap-only |
| `retrain_growth_factor` | 4.0 | Row growth over `rows_at_train` that forces a bootstrap retrain |
| `fts_with_position`, `fts_base_tokenizer`, `fts_language` | `False`, — , — | INVERTED tokenizer configuration |
| `fragments_per_index_task` | 8 | Shard width for build fan-out |
| `rebuild` | `False` | Force bootstrap/rebuild regardless of stored config |
| `max_index_deltas` | 4 | Delta count that triggers `merge_index_deltas` |
| `max_stale_replans` | `MAX_STALE_REPLANS` (3) | Stale-fragment replan budget |
| `fts_max_unindexed_fragments` | 32 | Backlog threshold below which FTS uses the maintain path instead of a rebuild |
| `commit_retries` | `DEFAULT_COMMIT_RETRIES` (20, from `telemetry.py`) | Retry budget for every commit in this package |
| `commit_backoff_seconds` | 0.5 | Base backoff between commit retries |

## How to invoke

`cli.py` is an uninstalled operator CLI. It is not registered as a console script in
`pyproject.toml` and runs as a module:

```bash
uv run python -m lance_etl.indexing.cli --help
```

Substitute real arguments for `--help` for an actual run.

In production this package is never invoked through its CLI. The reconciler calls it directly:
`ConfiguredPublicationRunner.run_indexing` in `reconciler/workers.py` instantiates
`LanceIndexer(self.index_config(spec, definition)).run(self.spark, [candidate_uri])` once per
`spec.index_definitions` entry, driven by the durable PostgreSQL work item, every time a `PUBLISH`
or `REBUILD` work row runs indexing. The CLI exists for standalone operator use against a dataset
outside the reconciler loop, not as a production code path.

## Testing pointers

| Test file | Covers |
|---|---|
| `tests/test_index_plan_phase.py` | `plan_dataset_indexes` decision logic across index families |
| `tests/test_index_segment_paths.py` | The segment build/merge/commit calls per handler |
| `tests/test_index_bootstrap_retry.py` | Vector bootstrap path and commit retry behavior |
| `tests/test_centroid_sidecar.py` | Object-store centroid cache hit/miss/backfill (ADR 0040) |
| `tests/test_index_maintenance.py` | Delta-merge and `optimize_existing_index` |
| `tests/test_index_replan_guard.py` | The stale-fragment replan loop and `max_stale_replans` exhaustion |
| `tests/test_zonemap_handler.py` | ZONEMAP's merge-before-commit path (ADR 0033) |
| `tests/test_size_policy.py` | IVF partition-count derivation and clamping |
| `tests/test_btree_delta_coexistence.py` | BTREE unmerged-delta coexistence with concurrent merges (the known pylance 8.0.0 regression noted in [`AGENTS.md`](../AGENTS.md)) |

## Invariants a maintainer must not break

- Segment-API-only index builds for every type and every dataset size, with the single sanctioned
  vector bootstrap exception — root `AGENTS.md` hard rule 6,
  [ADR 0001](../../../docs/adr/indexing.md), [ADR 0029](../../../docs/adr/indexing.md).
- One shared RaBitQ model across every shard of one build — a shard that trained its own rotation
  produces a segment that cannot be merged correctly.
- BTREE/BITMAP commit unmerged, ZONEMAP merges before commit — [ADR 0033](../../../docs/adr/indexing.md).
- FTS uses one shared `index_uuid` and an atomic `CreateIndex` swap so an in-flight rebuild never
  makes the index briefly unqueryable — [ADR 0001](../../../docs/adr/indexing.md).
- The stale-fragment replan loop must fail loudly after `max_stale_replans`, never silently drop a
  dataset — [ADR 0009](../../../docs/adr/fleet-orchestration-and-maintenance.md).
- No stable row IDs — root `AGENTS.md` hard rule 8, [ADR 0010](../../../docs/adr/rejected-and-operator-tools.md).
- Driver plans, executors build and commit — root `AGENTS.md` hard rule 5.
