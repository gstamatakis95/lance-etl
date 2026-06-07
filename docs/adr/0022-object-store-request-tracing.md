# 0022. Lance trace-event bridge: object-store stats, IO/dataset/file events on spans and metrics

Status: Accepted (amended)

## Amendment note

This ADR originally covered only the per-query object-store stats attached to search spans as `s3.*`
attributes. It is broadened here to the full Lance trace-event bridge. Two things changed. First, the span
attributes are renamed from `s3.*` to provider-neutral `object_store.*` because the service deploys over AWS S3,
Azure Blob, and GCS through Lance's provider-agnostic object store, so a provider-specific prefix was
misleading. Second, three more Lance tracing targets are admitted and metered alongside the throttle tap and
the scan execution stats: `lance::io_events`, `lance::dataset_events`, and `lance::file_audit`. The sections
below describe the bridge as a whole.

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
to the search span as `object_store.*` attributes: `object_store.requests`, `object_store.iops`,
`object_store.bytes_read`, `object_store.parts_loaded`, and `object_store.indices_loaded`. The names are
provider-neutral, since the same code path serves S3, Azure Blob, and GCS. The attributes are counts only, with
no org, tenant, or version identifier, so they stay low cardinality on the span. The existing `query.*` metrics
keep being emitted unchanged. A GET / HEAD / LIST breakdown is intentionally not emitted because Lance does not
expose it in a production build. The attributes land on the per-query-leg span (`lance.vector_query` /
`lance.text_query`), which is a child of the per-RPC server span, so a hybrid request shows the object-store
volume of each leg separately under the one RPC trace while a single-leg request has exactly one such child.

Beyond the scan stats, three more Lance tracing targets are bridged. Each target is force-admitted at `info`
through the `EnvFilter` (mirroring the existing throttle directive) so it survives a narrowing `RUST_LOG`, which
gives two outputs at once. The OTLP layer records each Lance event as a span event on whatever span is active
when it fires. A dataset open runs inside the `provider.dataset` span, so its `lance::dataset_events`
`event=loading` and the `lance::io_events` index opens show up as span events there. The per-query-leg spans
likewise carry the IO and file events that occur during a scan. In parallel, a single `LanceEventMetricsLayer`
(which also subsumes the throttle tap) turns each event into a low-cardinality DogStatsD counter through the
typed facade. The events and their tags are the fixed Lance enums and nothing else.

- `lance::io_events` becomes `lance.io_events` tagged `io_type` (open_scalar_index, open_vector_index,
  open_frag_reuse_index, open_mem_wal_index, load_vector_part, load_scalar_part).
- `lance::dataset_events` becomes `lance.dataset_events` tagged `event` (loading, writing, committed,
  dropping_column, deleting, compacting, cleaning). `event:loading` counts a dataset open.
- `lance::file_audit` becomes `lance.file_audit` tagged `mode` (create, delete, delete_unverified) and `type`
  (manifest, index, data, deletion).

No uri, org, tenant, or path is ever placed on a metric tag. Capture stays infallible, matching ADR 0008. The
layer and the callback only emit through the metrics facade (a no-op when the Datadog Agent is unreachable) and
write span attributes through the OpenTelemetry layer (a no-op when telemetry is disabled), so an unreachable
Agent never panics and never fails a request. The layer reads the event fields with a tracing field visitor and
ignores any event on a non-Lance target.

## Consequences

An operator who finds a slow search trace can now read its object-store request count and bytes directly off
the span and decide whether the latency was IO volume, throttling, or compute. They can also see, as span
events on the open and per-leg spans, exactly which indexes were opened and loaded and whether the served
version had to be loaded cold, plus fleet-wide counters for index opens, dataset opens, and file
creates/deletes. The signal is sourced from Lance's own performance events, so it stays accurate as the engine
evolves and adds no extra IO of its own.

There are two honest limitations. The GET / HEAD / LIST split is still missing: only the aggregate request
count and byte totals that Lance surfaces in production are available, which is sufficient for volume-based
triage. Should a future Lance release expose the per-method breakdown outside `test-util`, it can be added as
further `object_store.*` attributes without changing this design. Second, `lance::io_events` and the other
bridged targets are discrete point events, not durations, so they count how often an index was opened or a part
loaded but do not give an in-flight concurrency gauge. They answer "how many opens happened" and "what fired
during this span", not "how many were happening at once".
