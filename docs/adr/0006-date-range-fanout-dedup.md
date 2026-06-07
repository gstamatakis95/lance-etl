# 0006. Date-range fan-out search with dedup-keep-best

Status: Accepted

## Context

With date-partitioned datasets ([0004](0004-dynamic-partition-targets.md)) one org maps to many
`{org}/{tenant}/{namespace}/{date}.lance` datasets, and the same `vector_id` may exist in several date
partitions by design. A search request must be able to span a date range and return one coherent top-k.

## Decision

Search requests carry a `DatasetTarget` message: `org_id`, `tenant_id`, `namespace`, and an optional
`date_range`. Without a date range the server resolves the single dataset (the existing template path). With a
date range it resolves one dataset URI per day in the range, fans out the query across them with bounded
concurrency, then merges in the domain layer with dedup-by-id keeping the best score (minimum distance for
vector, maximum score for FTS and hybrid) before truncating to k. A missing day is skipped. Only a fully empty
range is NotFound. Hybrid runs both legs per day, merges each leg globally, then applies a single RRF fusion so
fused ranks are global.

## Consequences

Dedup and merge live in `domain/merge.rs` with unit tests for ties, duplicate ids, and empty legs. The
`DatasetProvider` gained range resolution. Fan-out width and per-day leg latency are instrumented (see
[0008](0008-observability-and-recall-audit.md)). When an explicit projection omits the id column the backend
projects it internally and strips it from the response so dedup still works. This is the serving-side answer to
the duplicate semantics that [0004](0004-dynamic-partition-targets.md) accepts on the write side.
