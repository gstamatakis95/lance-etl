# ADR 0029: Every index builds distributed through the segment API, and scalar roles auto-index

## Status

Accepted. Amends ADR 0028 (whose scope left ``scalar`` role columns unindexed) and reinforces
ADR 0001. Depends on pylance ``>=8.0.0``. Amended by ADR 0030: fresh vector builds bootstrap
through a committed ``create_index`` with streaming k-means and an explicit stored rotation.

## Context

The unified fleet orchestration of ADR 0028 gave every index one build path, but two gaps
remained. First, the guarantee that no index is ever built in process was implicit in code
rather than recorded as a decision, and the pre-unification small tier had shown how easily an
in-process ``create_index`` path creeps in (and how it corrupts vector indexes by pairing deltas
with mismatched models). Second, columns pivoted from the ``metadata`` map were recorded with
the ``scalar`` role in ``lance-etl.columns`` but received no index unless an operator passed
explicit ``--scalar-column`` flags, so filtered queries on metadata fields scanned unindexed
columns on most datasets.

## Decision

Every index type builds exclusively through the distributed segment paths, for every dataset
size. A small dataset is the one-task case of the same path, never a different API:

- Vector (IVF_RQ): artifacts resolve on an executor (centroid reuse from the committed index,
  or in-process training on that executor under the train semaphore), each shard builds with
  ``create_index_uncommitted``, and the commit fan-out merges with
  ``merge_existing_index_segments`` and publishes with ``commit_existing_index_segments``.
- BTREE and BITMAP: each shard builds with ``create_index_uncommitted`` and the segments are
  committed unmerged. Lance unions them at query time and the delta-bound pass consolidates
  them on an executor.
- FTS (INVERTED): each fragment builds under one shared ``index_uuid``, the commit fan-out
  merges metadata with ``merge_index_metadata`` and publishes with a ``CreateIndex`` commit.

No production code calls plain ``create_index`` or an unsharded ``create_scalar_index`` build.
Enforcement lives in three places: hard rule 6 in AGENTS.md, the unified runner being the only
index entry point, and the coexistence suite driving the same phase functions in process.

Role discovery now covers all three roles. When no explicit column lists are configured, every
``vector`` role column gets an IVF_RQ index, every ``scalar`` role column gets a BTREE index,
and every ``text`` role column gets a BM25 INVERTED index. Explicit column lists remain the
override and the opt-out: setting any list disables discovery entirely for that run.

## Consequences

- Filtered queries on metadata-derived columns hit BTREE indexes on every dataset without
  per-dataset flags. Index counts per dataset grow with the number of metadata keys an org
  uses, so build time, delta-merge work, and index storage scale with schema width. Operators
  with pathologically wide metadata maps can fall back to explicit column lists.
- The distributed-only guarantee removes the mismatched-model corruption class permanently:
  every vector index stores its artifact config, and no code path can mint a private model.
- BTREE segments commit unmerged, so datasets briefly serve queries from segment unions until
  the delta-bound pass consolidates them. This is the existing ADR 0028 behavior, now covering
  the discovered scalar indexes as well.
