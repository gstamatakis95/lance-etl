# Indexing — architecture decisions

This document consolidates the decisions behind index construction: the distributed segment
flows, sidecar-free vector artifacts, role-driven auto-indexing, and the streaming k-means
bootstrap. Each section keeps its original ADR number so references like "ADR 0030" resolve
here.

## ADR 0001 — Distributed indexing via the Lance segment API

Status: Accepted (the driver-side vector training portion superseded by ADR 0030)

Every index builds through Lance's uncommitted-segment APIs with three distinct,
non-interchangeable flows, so the indexer keeps a handler per type:

- **Vector (IVF_RQ)**: executors build one segment per fragment shard with
  `create_index_uncommitted(column, "IVF_RQ", ivf_centroids=, num_bits=, rabitq_model=,
  fragment_ids=)`, then the commit path runs `merge_existing_index_segments` and
  `commit_existing_index_segments`. Sharing one RaBitQ model across shards is required for
  correctness: without it each shard derives its own random rotation and merged segments are
  inconsistent. Where the centroids and rotation come from is governed by ADR 0025 and
  ADR 0030 below (the original driver-trains-and-broadcasts step no longer exists).
- **BTREE and BITMAP**: per-shard `create_index_uncommitted` then straight to
  `commit_existing_index_segments` with no merge step — `merge_index_metadata` rejects scalar
  types.
- **FTS (INVERTED)**: one shared `index_uuid` is minted per build, executors call
  `create_scalar_index(column, "INVERTED", index_uuid=, fragment_ids=)`, then
  `merge_index_metadata` runs and the index publishes with a `LanceOperation.CreateIndex`
  commit.

Field ids for FTS come from the Lance schema
(`dataset._ds.lance_schema.field_case_insensitive(col).id()`), not Arrow positional indices,
which would diverge after schema evolution.

## ADR 0025 — Sidecar-free vector artifacts

Status: Accepted

Vector artifacts live exclusively in the dataset's own transactional config KV under
`lance-etl.vector.{column}` — no `.artifacts/` sidecar files, no separate filesystem access, no
external state to migrate or clean up. The JSON value carries `rows_at_train`, `dimension`,
`metric`, `num_bits`, `num_partitions`, and `rabitq_model` (the only field that must persist:
everything else serves diagnostics and the reuse/retrain checks). Two verified facts make this
possible: `dataset.get_ivf_model(index_name).centroids` returns the committed centroids in the
exact form `create_index_uncommitted(ivf_centroids=...)` accepts, and `update_config` /
`config()` is a transactional KV inside the manifest whose same-key writes conflict correctly.
The reuse checks (`load_vector_config`, `config_reusable`, `growth_requires_retrain`) route any
absent, mismatched, or growth-stale config to a full retrain, which after ADR 0030 means a
streaming bootstrap.

## ADR 0029 — Every index builds distributed through segments, scalar roles auto-index

Status: Accepted (amends ADR 0028, reinforces ADR 0001, depends on pylance >= 8.0.0)

Two guarantees are recorded as decisions rather than implicit code properties. First, every
index type builds exclusively through the distributed segment paths for every dataset size — a
small dataset is the one-task case of the same path, never a different API. No production code
calls plain `create_index` or an unsharded `create_scalar_index` build (the sole sanctioned
exception is ADR 0030's streaming bootstrap, which carries an explicit rotation and stores its
config). The pre-unification small tier showed how an in-process build path corrupts vector
indexes by pairing deltas with mismatched models. Enforcement lives in AGENTS.md hard rule 6,
the unified runner being the only entry point, and the coexistence suite.

Second, role discovery covers all three roles. When no explicit column lists are configured,
every `vector` role column gets an IVF_RQ index, every `scalar` role column (pivoted from the
`metadata` map) gets a BTREE index, and every `text` role column gets a BM25 INVERTED index.
Explicit column lists remain the override and the opt-out. BTREE segments commit unmerged —
Lance unions them at query time and the delta-bound pass consolidates them on an executor.
Index counts grow with an org's schema width, so operators with pathologically wide metadata
maps can fall back to explicit lists.

## ADR 0030 — Streaming k-means bootstrap for IVF_RQ

Status: Accepted (amends ADR 0029, depends on lance >= 8.0.0)

IVF training previously loaded a `num_partitions x sample_rate` sample into one executor heap,
requiring a memory budget and a partition-count cap that cost recall exactly on the largest
datasets. lance 8.0.0's streaming k-means trains incrementally with bounded memory, exposed
only through the committed `create_index` path (the segment path hard-requires precomputed
centroids and refuses internal training — both constraints verified against the released
library).

Vector builds therefore split into two modes at plan time:

- **Bootstrap** (index absent, `rebuild`, or the ADR 0025 artifact triggers): ONE task runs a
  committed `create_index` with the streaming parameters and a freshly minted RaBitQ rotation
  (`build_rq_model`), then stores the artifact config. `replace=True` makes a growth retrain a
  wholesale replacement.
- **Increment** (committed index with reusable config): parallel shard fan-out through the
  segment API, centroids read back via `get_ivf_model` on the executor and the rotation from
  the stored config, so every delta stays on one model by construction.

The in-heap trainer, its semaphore, the training memory budget, the partition-count cap, and
the sticky full-rebuild threading were all deleted — the plan phase re-derives the bootstrap
decision from stored state each round. Training memory is now bounded by the streaming chunk
size regardless of partition count, so partition counts follow the size policy alone.
