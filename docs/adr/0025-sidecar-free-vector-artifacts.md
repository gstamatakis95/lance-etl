# ADR 0025 — Sidecar-free vector artifacts

**Status**: Accepted

**Date**: 2026-06-10

---

## Context

The indexing job has kept a per-dataset sidecar at `{uri}.artifacts/{column}/` holding two files: a JSON
`manifest.json` (~1–5 KB) and an Arrow `ivf_centroids.arrow` (3–95 MB). These files are read by the driver on
every incremental build and written immediately after each IVF training run. The sidecar creates operational
friction: it must exist on the same object-store prefix as the dataset, it requires pyarrow filesystem access
distinct from the lance Rust store, it is not transactional, and it adds external state that must be managed
alongside the dataset through version cleanup and namespace migrations.

Two verified facts make full elimination possible without any lance patch:

1. **Centroid readback**: `dataset.get_ivf_model(index_name).centroids` returns a `pa.FixedSizeListArray` for
   IVF_RQ that `create_index_uncommitted(ivf_centroids=...)` accepts directly. `num_partitions` equals
   `len(centroids)`. The bulk artifact needs no persistence after the index is committed.
2. **Dataset config KV**: `dataset.update_config()` and `dataset.config()` expose a transactional string KV
   inside the manifest itself. Writes survive compaction and version cleanup, same-key writes conflict
   correctly so concurrent index runs are naturally serialized, and distinct keys commute. The RaBitQ model
   JSON is ~1.5 KB, well within practical manifest size limits.

Alongside this change the corruption-containment machinery
(`covered_fragment_ids`, `remap_requires_rebuild`, `record_coverage`, `index_holds_dead_fragments`,
`drop_stale_index`) is deleted under the standing assumption that the PERM0_INVERSE remap corruption is
resolved on lance branch `fix/ivf-rq-remap-corruption`.

---

## Decision

Vector artifacts are stored exclusively in the dataset's own config KV under the key
`lance-etl.vector.{column}`. The JSON value carries:

```json
{
  "rows_at_train": <int>,
  "dimension":     <int>,
  "metric":        <str>,
  "num_bits":      <int>,
  "num_partitions": <int>,
  "rabitq_model":  <str>
}
```

The `rabitq_model` field is the only field that must persist across runs. All other fields are for diagnostics
or used by the reuse and retrain-trigger checks.

### Reuse decision tree (inside `VectorIndexHandler.prepare`)

1. If `config.rebuild` is set, skip to the train branch.
2. Call `load_vector_config(dataset, column)`. If the key is absent or malformed, fall through to train.
3. Call `config_reusable(cfg, dimension, metric, num_bits)`. If it returns `False` (missing `rabitq_model`,
   or any of dimension/metric/num_bits differ), fall through to train with a warning.
4. Call `growth_requires_retrain(cfg, rows)`. If `rows_at_train` is absent or `rows` exceeds
   `retrain_growth_factor * rows_at_train`, fall through to train.
5. Call `dataset.get_ivf_model(index_name)`. If the model is `None` or its `.centroids` is `None`, fall
   through to train.
6. Reuse: `centroids = ivf_model.centroids`, IPC-serialize with `centroids_to_ipc`, read `rabitq_model`
   from config, derive `num_partitions = len(centroids)`. No network writes.

Train branch: train centroids, mint a fresh RaBitQ model, write config once via `write_vector_config`. No
sidecar files.

### Replan memoization

`VectorIndexHandler.cached_artifacts` memoizes the `prepare` result within one build call. The `build_and_commit_segments` replan loop calls `prepare` on each attempt after a stale-fragment replan.
`LanceIndexer.handlers()` constructs a fresh handler instance per dataset build call, so memoization does
not leak across datasets or runs. `target_fragments` is not memoized because it must be re-evaluated at the
latest version on each replan attempt.

### Corruption-guard deletion

The `remap_requires_rebuild` guard and its coverage-recording mechanism are deleted. This is safe only after
deploying pylance built from `fix/ivf-rq-remap-corruption`. The deploy order is:

1. Build and install pylance from the fix branch (`maturin develop --release`).
2. Deploy this code change.

Deploying step 2 before step 1 restores the pre-guard exposure window: a compaction that rewrites covered
fragments on the unfixed build may silently corrupt the IVF_RQ index. The code change itself is safe to land
in-tree before the pylance rebuild.

---

## Consequences

**No external files**: no sidecar directory is ever created. `discover_datasets` no longer needs to exclude
`.artifacts` paths (though its `.lance`-suffix filter naturally excludes them anyway).

**Legacy sidecars abandoned**: existing `{uri}.artifacts/` directories are left in place and ignored. The
first incremental build after deployment falls through the reuse check (no config key) and retrains once,
writing the new config key. This is a one-time cost per dataset.

**Concurrent same-key `update_config` writes**: two index runs on the same dataset that both reach the train
branch will conflict on the config write. The `write_vector_config` retry loop surfaces this as
`OSError`/`RuntimeError`. This is the same operational constraint that applied with the old sidecar write.
Production orchestration runs at most one index job per dataset at a time.

**`materialize_deletions_threshold` default lowered**: the default changes from `0.5` to `0.1` (lance's own
default). The `0.5` value existed only to dampen the remap-corruption guard trigger. With the guard deleted
the conservative default is no longer needed. An inline index remap on tier B is triggered when covered
fragments are rewritten, so budget commit time accordingly for heavily indexed head datasets.

**`cloud_storage.py` trimmed**: `write_object`, `read_object`, and `object_exists` are deleted. They had no
callers outside the indexing sidecar path.
