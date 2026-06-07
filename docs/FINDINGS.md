# lance-etl — Findings Report

This is the narrative companion to the [ADRs](adr/README.md). It records what was verified against the real
Lance APIs, the production patterns adopted, the scale design, the live-verified numbers, and the open items.
The detailed evidence lives in [`../market-research/`](../market-research).

## What this project is

A PySpark pipeline that ingests embeddings and text from Apache Iceberg into per-org Lance datasets, builds
distributed IVF_RQ vector, BTREE, BITMAP, and INVERTED (FTS) indexes via the Lance segment API, and compacts
them. A companion tokio gRPC service serves vector, full-text, and hybrid search over the same datasets. The
scale target is up to 1 billion vectors across 30,000 orgs, with a power-law size distribution.

## The stale-branch correction episode

The first API verification ran against a stale checkout of Lance and wrongly flagged several real APIs as
hallucinated, including `lance.lance.indices.build_rq_model` and the `rabitq_model=` kwarg. Re-verifying against
Lance main reversed those findings: the APIs are real and required. The lesson held for the rest of the project,
every API claim is checked against the pinned checkout (`466405f47`) with a path and line, never trusted from
memory or from a prior summary. The most striking reversal was distributed BTREE and BITMAP, where both the
original artifact and the stale-report replacement were wrong, and the correct flow is per-shard
`create_index_uncommitted` straight to `commit_existing_index_segments`.

## Scale design for 1B vectors over 30k orgs

The power law is the central constraint, see [ADR 0002](adr/0002-two-tier-compaction-orchestration.md). A
sequential per-dataset driver loop would launch on the order of 120k blocking Spark jobs. Compaction and
indexing instead run in two tiers: small datasets batched whole-dataset-per-task into one job, large datasets
through the distributed fan-out driven concurrently with FAIR scheduler pools. Index policy is size-aware,
`num_partitions` near sqrt(rows), vector indexing skipped below a row floor. Incremental index maintenance
(`optimize_indices`, delta merging) replaces unconditional rebuilds so the long tail is a near-free sweep. V2
manifest paths ([ADR 0012](adr/0012-v2-manifest-paths.md)) make each of the 30k dataset opens a single
object-store request.

## Coexistence, proven

The three jobs (ingest, compact, index) coexist with zero data loss and convergence, see
[ADR 0009](adr/0009-compaction-index-coexistence.md). The coexistence stress test runs three concurrent actors
over a head-sized dataset (about 191k rows) plus several tail datasets and asserts exact final content, full
index coverage, and a fragment count in the target band. Measured on a passing run: zero data loss across all
datasets, around 10 commit conflicts retried, 2 tier-B re-plans, head compacted to 2 fragments, every index with
zero unindexed fragments. It passes 40-plus consecutive runs. It also surfaced a genuine race (an index build
orphaning fragments compaction removed) that is now guarded.

## Serving, observability, recall

The gRPC service is layered domain / lance / grpc / cache / telemetry with a typed filter AST and no raw SQL,
see [ADR 0005](adr/0005-rust-grpc-layering-typed-filter.md). It supports vector, FTS, hybrid, Clusters, and
Prewarm, with date-range fan-out and dedup-keep-best ([ADR 0006](adr/0006-date-range-fanout-dedup.md)). A
disk-backed index and metadata cache excludes raw data ([ADR 0007](adr/0007-disk-cache-and-prewarm.md)).
Observability taps real Lance trace surfaces and samples queries for offline exact-recall auditing pinned to the
dataset version that served each query ([ADR 0008](adr/0008-observability-and-recall-audit.md)).

Live-verified number: on the synthetic end-to-end benchmark, the Prewarm RPC roughly halved cold first-query
latency (about 17.8 ms down to 8.0 ms, and 10.6 ms down to 4.7 ms across two orgs).

## Rejected and deferred

- Move-stable row IDs are rejected, see [ADR 0010](adr/0010-stable-row-ids-rejected.md). The structural win
  (no inline index remap, mitigations deletable) was proven sequentially, but the production concurrent workload
  trips an upstream `RowIdIndex` overlapping-chunk defect that panics in debug and risks a silently wrong row-id
  index in release. The remap problem is instead contained by the orphan-race guard and inline remap. This is a
  rejection, not a deferral.
- `defer_index_remap` defaults to False because deferred remap leaves indexed vector queries broken until the
  remap runs on the pinned build.
- Tag-based blue/green serving is designed and Proposed, not yet implemented, see
  [ADR 0013](adr/0013-blue-green-serving.md). The prewarm-versus-tag correctness contract is fully specified.

## Open items

- Implement the prewarm and tag-resolution blue-green safety in the Rust service (design ready).
- Implement the ETL insert-only fast path (fragment writes plus batched commit) for first-write bulk loads.
- The benchmark package runs SIFT1M (and a synthetic adapter) through the real pipeline and the live server, and
  is extensible to other datasets via the dataset-adapter registry.
