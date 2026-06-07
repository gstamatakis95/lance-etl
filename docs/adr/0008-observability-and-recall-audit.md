# 0008. Datadog observability, trace taps, and recall auditing

Status: Accepted

## Context

The pipeline and the search service need production observability, and we want to measure the search service's
real recall against ground truth in production, not just in the benchmark.

## Decision

Instrument both sides for Datadog. The Python jobs bridge Lance trace events to DogStatsD and ddtrace, including
the raw object-store `requests` field distinct from coalesced `iops`. The Rust service emits per-RPC OTLP traces
and DogStatsD metrics, JSON logs with trace correlation, a disabled mode for tests, and taps two Lance trace
surfaces: per-query execution stats via `scan_stats_callback` (`query.iops`, `query.bytes_read`,
`query.parts_loaded`) and the object-store throttle target (`throttle.errors`, `throttle.new_rate`). Metric tag
cardinality is kept low: rpc and status only, never org or tenant.

For recall auditing, the service samples a deterministic fraction of vector queries (`SEARCH_API_RECALL_SAMPLE_RATE`)
onto the request span, recording the query vector, served result ids and distances, params, the typed filter
AST, and crucially the dataset version that served the query. An offline `recall` PySpark job pulls those spans
from the Datadog Spans API, opens each dataset pinned at the recorded version, brute-forces exact top-k
(replaying the filter AST internally with strict identifier validation), computes recall at k, and prints
per-org and per-params tables to stdout.

## Consequences

Sampling is lock-free and exactly `floor(N * rate)` of eligible requests, with no RNG. Recording the dataset
version makes the offline score exact rather than approximate-under-churn, since the brute force runs against the
identical snapshot the query saw. Wall-clock recall auditing depends on the sampled version still existing, so
it must run inside the cleanup retention horizon ([0009](0009-compaction-index-coexistence.md)). Filtered
queries are scored by replaying the AST, which honors the no-raw-SQL rule because the string is generated
internally from the typed AST the service captured, never supplied by a client. Datadog needs a retention filter
on `recall.sample:true` for the spans to persist.
