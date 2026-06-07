# 0022. Object-store request counts and IO info on per-RPC search spans

Status: Accepted

## Context

[ADR 0008](0008-observability-and-recall-audit.md) established the observability split: low-cardinality
DogStatsD metrics carry fleet aggregates, and high-cardinality per-request detail lives on OTLP trace spans.
The search service already taps Lance's execution-stats callback and emits `query.iops`, `query.bytes_read`,
and `query.parts_loaded` as metric distributions, plus an object-store throttle tap. What was missing was the
ability to drill into one slow query and see how much object-store traffic it caused. A metric distribution
cannot answer "why was this specific request slow", because metrics deliberately drop the org, tenant, and
request identity that would let an operator pivot from a slow trace to its S3 volume.

Lance's performance events expose aggregate IO counts only. The execution-summary callback
(`ExecutionSummaryCounts`) reports `requests` (object-store requests to the storage layer), `iops` (I/O
operations after coalescing), `bytes_read`, `parts_loaded` (index partitions loaded), and `indices_loaded`. A
precise GET / HEAD / LIST breakdown is not available outside the `test-util` build, so it cannot be captured in
production. The throttle event stream (`previous_rate` / `new_rate` / `error`) remains a per-process signal,
already metered.

## Decision

In the same execution-stats callback that already emits the `query.*` metrics, also attach the captured counts
to the search span as `s3.*` attributes: `s3.requests`, `s3.iops`, `s3.bytes_read`, `s3.parts_loaded`, and
`s3.indices_loaded`. The attributes are counts only, with no org, tenant, or version identifier, so they stay
low cardinality on the span. The existing `query.*` metrics keep being emitted unchanged. A GET / HEAD / LIST
breakdown is intentionally not emitted because Lance does not expose it in a production build. The attributes
land on the per-query-leg span (`lance.vector_query` / `lance.text_query`), which is a child of the per-RPC
server span, so a hybrid request shows the object-store volume of each leg separately under the one RPC trace
while a single-leg request has exactly one such child.

Capture stays infallible, matching ADR 0008. The callback only emits through the metrics facade (a no-op when
the Datadog Agent is unreachable) and writes span attributes through the OpenTelemetry layer (a no-op when
telemetry is disabled), so an unreachable Agent never panics and never fails a request.

## Consequences

An operator who finds a slow search trace can now read its object-store request count and bytes directly off
the span and decide whether the latency was IO volume, throttling, or compute. The signal is sourced from
Lance's own performance events, so it stays accurate as the engine evolves and adds no extra IO of its own. The
known limitation is the missing GET / HEAD / LIST split: only the aggregate request count and byte totals that
Lance surfaces in production are available, which is sufficient for volume-based triage. Should a future Lance
release expose the per-method breakdown outside `test-util`, it can be added as further `s3.*` attributes
without changing this design.
