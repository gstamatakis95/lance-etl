# Indexing — architecture decisions

This document consolidates the decisions behind index construction: the distributed segment
flows, vector artifact storage, role-driven auto-indexing, the streaming k-means bootstrap, and
the object-store centroid cache. Each section keeps its original ADR number so references like
"ADR 0030" resolve here.

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

Status: Superseded by ADR 0040 (the "centroids are never persisted, no sidecar" stance is
reversed. The config-KV storage of `rabitq_model` and the `rows_at_train` fingerprint is retained)

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
the unified runner being the only entry point, focused segment-API tests, and the local reconciler
end-to-end test.

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

## ADR 0033 — ZONEMAP scalar indexes via merged segment commits

Status: Accepted (depends on lance >= 8.0.0)

ZONEMAP indexes let scalar-column scans prune whole row groups by min/max range instead of
probing every value, which pays off on ordered or clustered scalar columns that BTREE and BITMAP
serve less efficiently. The build follows the same per-shard flow as every other scalar type:
executors call `create_index_uncommitted(column, "ZONEMAP", fragment_ids=)` per shard. Unlike
BTREE and BITMAP, ZONEMAP segments are then merged with `merge_existing_index_segments` before
`commit_existing_index_segments` publishes the result, making ZONEMAP the only scalar type that
merges before commit.

The difference is a lance 8.0.0 capability, not a design preference. Zonemap segment merging was
added upstream in commits e8748a405 and cc657c5e3, and zonemap deltas are not unioned at query
time the way BTREE and BITMAP deltas are, so leaving ZONEMAP unmerged would leave query-time
pruning degraded across shards instead of merely deferred to the delta-merge maintenance pass.
As with BTREE and BITMAP, `create_scalar_index(fragment_ids=)` and `merge_index_metadata` are
never used for ZONEMAP — both raise on current lance main. No version gate is needed:
the repository pins `pylance==8.0.0`, the first release with the ZONEMAP type and segment
merging.

## ADR 0040 — Object-store centroid cache for distributed vector builds

Status: Accepted (supersedes ADR 0025's sidecar-free stance, amends ADR 0030, depends on
lance >= 8.0.0)

The unified indexer previously collected every active vector org's IVF centroids to the driver
into one fleet-wide `artifacts` dict and broadcast that whole dict to every executor, even though
each build task needs only its own dataset's centroids. Driver memory scaled with fleet
composition, the broadcast was recreated every replan round and never `destroy()`ed, and a
JVM-level executor OOM in that broadcast failed the stage and aborted the whole isolated run. This
ADR removes the fleet artifact phase entirely. Each vector segment shard resolves its OWN
dataset's centroids from the already-open, version-pinned handle it builds against.

Centroids are read sidecar-first. `VectorIndexHandler.prepare` calls `load_centroids`, which reads
a native `lance.indices.IvfModel` single file from an object-store sidecar keyed by the stored
`rows_at_train` fingerprint. On a hit the shard reuses those centroids without re-opening the
committed index. On a miss it falls back to `get_ivf_model` on the open handle and backfills the
sidecar best-effort. The streaming bootstrap (ADR 0030) writes the sidecar once after it commits
and stores the config, so the steady state is a hit.

Four properties make this safe and cheap:

- **Correctness is independent of the cache.** The `get_ivf_model` fallback always produces the
  committed centroids, so a missing, partial, or version-incompatible sidecar only costs one index
  read, never a wrong result. `load_centroids` swallows every read error and returns `None`.
- **Liveness is independent of the cache.** Every sidecar write — the bootstrap write and the
  fallback backfill — is best-effort. A write failure is counted and logged, never re-raised, so a
  read-only object store or a transient error never fails a build. The index is already committed
  and the rotation plus fingerprint already live in the config KV.
- **Invalidation is automatic.** The `rows_at_train` fingerprint is part of the sidecar path
  (`{uri}.artifacts/{index_name}.{rows_at_train}.ivf`), so a retrain writes a new path and a stale
  generation can never be mistaken for the current one. Reuse and invalidation need no separate
  bookkeeping.
- **The sidecar is invisible to discovery.** The `{uri}.artifacts` directory is a sibling of the
  `.lance` dataset directory whose final path component ends in `.artifacts`, not `.lance`, so
  `discover_datasets` skips it.

This reverses ADR 0025's "no external state" stance, which was chosen when centroids were only ever
re-read per run through `get_ivf_model`. The mitigations above address the reasons ADR 0025 avoided
sidecars: the fallback keeps correctness, the best-effort writes keep liveness, the fingerprint in
the path makes reuse and invalidation automatic, and orphaned old-generation sidecars are bounded
by the number of retrains and are tiny centroid files rather than accumulating unbounded state.
Dropping an index also leaves its sidecar orphaned, because the sidecar lives outside the Lance
manifest and is reclaimed by neither Lance version cleanup nor the maintenance job. This is the
same bounded, cosmetic residue as a retrain orphan and does not affect correctness. The
`rabitq_model` rotation and the `rows_at_train` fingerprint stay in the transactional config KV as
ADR 0025 defined. Only the centroids move to the sidecar. Per-index failure isolation is preserved
by the existing per-shard build guard: a vector index whose centroids cannot be resolved now raises
inside its build shard, is caught as a `"phase": "build"` per-index error, and is excluded from the
commit phase, replacing the deleted fleet artifact phase's isolation.
