# Architecture Decision Records

This directory records the load-bearing decisions behind lance-etl. Each ADR is immutable once accepted. A
later decision that reverses an earlier one gets its own ADR and the old one is marked Superseded or Rejected.

The companion narrative, with the live-verified numbers and the stale-branch correction episode, is in
[`../FINDINGS.md`](../FINDINGS.md). The evidence behind each ADR lives in
[`../../market-research/`](../../market-research).

| ADR | Title | Status |
|---|---|---|
| [0001](0001-distributed-indexing-segment-api.md) | Distributed indexing via the Lance segment API | Accepted |
| [0002](0002-two-tier-compaction-orchestration.md) | Two-tier compaction orchestration (30k-org power law) | Superseded by [0028](0028-unified-task-fleet-orchestration.md) |
| [0003](0003-read-increment-snapshot-bounds.md) | Incremental Iceberg reads via snapshot-id bounds | Accepted |
| [0004](0004-dynamic-partition-targets.md) | Dynamic write-partition targets and duplicate semantics | Accepted |
| [0005](0005-rust-grpc-layering-typed-filter.md) | Rust gRPC service layering and the typed filter AST | Accepted |
| [0006](0006-date-range-fanout-dedup.md) | Date-range fan-out search with dedup-keep-best | Superseded by [0014](0014-drop-by-date-partitioning.md) |
| [0007](0007-disk-cache-and-prewarm.md) | Disk-backed index/metadata cache and the Prewarm RPC | Accepted |
| [0008](0008-observability-and-recall-audit.md) | Datadog observability, trace taps, and recall auditing | Accepted |
| [0009](0009-compaction-index-coexistence.md) | Index-vs-compaction coexistence and the orphan-race guard | Accepted |
| [0010](0010-stable-row-ids-rejected.md) | Move-stable row IDs | Rejected |
| [0011](0011-ingested-at-column.md) | The `_ingested_at` ingestion-timestamp column | Superseded by [0016](0016-event-time-canonical-clock.md) |
| [0012](0012-v2-manifest-paths.md) | V2 manifest paths fleet-wide | Accepted |
| [0013](0013-blue-green-serving.md) | Tag-based blue/green serving | Proposed |
| [0014](0014-drop-by-date-partitioning.md) | Drop by-date partitioning and cross-date fan-out | Accepted |
| [0015](0015-cli-and-config-knob-reduction.md) | CLI and config knob reduction: opinionated defaults | Accepted |
| [0016](0016-event-time-canonical-clock.md) | Event-time canonical clock: remove `_ingested_at`, use source event timestamp | Accepted |
| [0017](0017-rust-intake-service.md) | Rust intake service with a pluggable record sink | Accepted |
| [0018](0018-ttl-expiration.md) | TTL data-expiration by event age | Accepted |
| [0019](0019-namespace-migrate-utility.md) | Namespace copy/migrate utility | Accepted |
| [0020](0020-map-pivot-to-concrete-columns.md) | Pivot named vectors and texts out of maps into concrete indexable columns | Superseded by [0024](0024-dynamic-map-pivot.md) |
| [0021](0021-grpc-event-time-range-search.md) | gRPC event-time range on the vector, text, and hybrid search RPCs | Accepted |
| [0022](0022-object-store-request-tracing.md) | Object-store request counts and IO info on per-RPC search spans | Accepted |
| [0023](0023-iceberg-table-optimization-job.md) | Iceberg source-table optimization job | Accepted |
| [0024](0024-dynamic-map-pivot.md) | Dynamic per-dataset map pivot: every key becomes a column | Accepted |
| [0025](0025-sidecar-free-vector-artifacts.md) | Sidecar-free vector artifacts in the dataset config KV | Accepted |
| [0026](0026-three-job-isolation.md) | Three-job isolation | Accepted |
| [0027](0027-unified-pipeline.md) | Unified pipeline job | Accepted |
| [0028](0028-unified-task-fleet-orchestration.md) | Unified task-based fleet orchestration, format 2.1, column roles | Accepted |
| [0029](0029-all-indexes-distributed-segments.md) | Every index builds distributed through segments, scalar roles auto-index | Accepted |
| [0030](0030-streaming-kmeans-bootstrap.md) | Streaming k-means bootstrap for IVF_RQ vector indexes | Accepted |
| [0031](0031-pluggable-cache-backend.md) | Pluggable cache backend for the search service (disk, redis, memory) | Accepted |
| [0032](0032-hourly-interval-tags-and-query-pinning.md) | Hourly interval tags at ETL write time and per-query tag pinning | Accepted |
