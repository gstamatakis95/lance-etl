# Architecture Decision Records

This directory records the load-bearing decisions behind lance-etl. Each ADR is immutable once accepted. A
later decision that reverses an earlier one gets its own ADR and the old one is marked Superseded or Rejected.

The companion narrative, with the live-verified numbers and the stale-branch correction episode, is in
[`../FINDINGS.md`](../FINDINGS.md). The evidence behind each ADR lives in
[`../../market-research/`](../../market-research).

| ADR | Title | Status |
|---|---|---|
| [0001](0001-distributed-indexing-segment-api.md) | Distributed indexing via the Lance segment API | Accepted |
| [0002](0002-two-tier-compaction-orchestration.md) | Two-tier compaction orchestration (30k-org power law) | Accepted |
| [0003](0003-read-increment-snapshot-bounds.md) | Incremental Iceberg reads via snapshot-id bounds | Accepted |
| [0004](0004-dynamic-partition-targets.md) | Dynamic write-partition targets and duplicate semantics | Accepted |
| [0005](0005-rust-grpc-layering-typed-filter.md) | Rust gRPC service layering and the typed filter AST | Accepted |
| [0006](0006-date-range-fanout-dedup.md) | Date-range fan-out search with dedup-keep-best | Accepted |
| [0007](0007-disk-cache-and-prewarm.md) | Disk-backed index/metadata cache and the Prewarm RPC | Accepted |
| [0008](0008-observability-and-recall-audit.md) | Datadog observability, trace taps, and recall auditing | Accepted |
| [0009](0009-compaction-index-coexistence.md) | Index-vs-compaction coexistence and the orphan-race guard | Accepted |
| [0010](0010-stable-row-ids-rejected.md) | Move-stable row IDs | Rejected |
| [0011](0011-ingested-at-column.md) | The `_ingested_at` ingestion-timestamp column | Accepted |
| [0012](0012-v2-manifest-paths.md) | V2 manifest paths fleet-wide | Accepted |
| [0013](0013-blue-green-serving.md) | Tag-based blue/green serving | Proposed |
