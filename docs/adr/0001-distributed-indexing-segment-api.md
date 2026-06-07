# 0001. Distributed indexing via the Lance segment API

Status: Accepted

## Context

The pipeline must build vector, scalar, and full-text indexes over per-org Lance datasets at a scale of up to
1 billion vectors across 30,000 orgs. Index construction has to run distributed across Spark executors, not on
the driver. The original design chat assumed a single uniform "segment API" for all index types, and an early
verification pass (against a stale Lance branch) wrongly flagged several real APIs as hallucinated. Re-verifying
against Lance main settled the actual surface.

## Decision

Build every index through Lance's uncommitted-segment APIs, with three distinct flows.

Vector (IVF_RQ): the driver trains IVF centroids with `IndicesBuilder.train_ivf` and builds the shared RaBitQ
model with `lance.lance.indices.build_rq_model`, broadcasts both to executors, each executor builds a segment
per fragment shard with `create_index_uncommitted(column, "IVF_RQ", ivf_centroids=, num_bits=, rabitq_model=,
fragment_ids=)`, and the driver runs `merge_existing_index_segments` then `commit_existing_index_segments`. The
broadcast RaBitQ model is required for correctness: without it each shard derives its own random rotation and
merged segments are inconsistent.

BTREE and BITMAP: per-shard `create_index_uncommitted` then straight to `commit_existing_index_segments` with no
merge step. On Lance main `merge_index_metadata` rejects these scalar types.

FTS (INVERTED): the driver mints one shared `index_uuid`, executors call `create_scalar_index(column,
"INVERTED", index_uuid=, fragment_ids=)`, the driver calls `merge_index_metadata` then publishes with
`LanceOperation.CreateIndex` and `LanceDataset.commit`.

## Consequences

All heavy I/O runs in executors and the driver only plans, broadcasts, and commits. Field ids for FTS come from
the Lance schema (`dataset._ds.lance_schema.field_case_insensitive(col).id()`), not Arrow positional indices,
which would diverge after schema evolution. The three flows are not interchangeable, so the indexer keeps a
handler per type. See `market-research/optimization-recommendations.md` for incremental-maintenance follow-ups
(optimize_indices, delta merging) layered on top of this.
