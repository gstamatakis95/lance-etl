# Datadog dashboard guide for local lance-etl

The Python reconciler and Rust search process emit low-cardinality Datadog telemetry. Telemetry is
diagnostic only. An absent Datadog Agent never fails reconciliation or search.

## Naming and tags

Python metrics use the configured prefix, default `lance.pipeline`. Rust metrics use
`search_api.*`. Constant process tags may include `env` and `service`.

Do not add tenant, organization, namespace, dataset UUID, work UUID, publication UUID, Lance URI,
or Lance version as metric tags. Put high-cardinality evidence in logs, traces, and PostgreSQL.

## Reconciler health board

Create six query-value widgets from the gauges emitted by `TelemetrySloEmitter`:

| Metric suffix | Meaning | Healthy condition |
|---|---|---|
| `reconciler.healthy` | Aggregate local control-loop health | `1` |
| `reconciler.due_work` | Due claimable work rows | Below `ReconcilerSettings.max_due_work` |
| `reconciler.blocked_work` | Work requiring diagnosis | `0` |
| `reconciler.blocked_source_snapshots` | Source transitions rejected or blocked | `0` |
| `reconciler.oldest_open_age_seconds` | Age of oldest unfinished work | Below `max_open_work_age_seconds` |
| `reconciler.retention_age_seconds` | Age of oldest cleanup-eligible evidence | Below `max_retention_age_seconds` |

Use the `ReconcilerSettings` values loaded from environment variables at process startup as the
alert thresholds. They are the durable policy and may differ from an older dashboard snapshot until
the process is restarted.

Recommended alert order:

1. `reconciler.healthy` is zero for two consecutive local cycles.
2. blocked source snapshots are nonzero.
3. blocked work is nonzero.
4. oldest open age exceeds its configured bound.
5. due work exceeds its configured bound.
6. retention age exceeds its configured bound.

## Reconciler investigation

Start with the local status command:

```bash
uv run lance-etl-reconcile status
```

Then inspect bounded PostgreSQL details. Metric tags deliberately cannot identify the dataset:

```sql
SELECT work_id, dataset_id, kind, phase, state, attempt_count,
       next_attempt_at, lease_expires_at, error_code
FROM dataset_work
WHERE state <> 'SUCCEEDED'
ORDER BY next_attempt_at, created_at;
```

For source failures:

```sql
SELECT source_snapshot_seq, snapshot_id, parent_snapshot_id,
       iceberg_sequence_number, kind, state, error_code
FROM source_snapshots
WHERE state = 'BLOCKED'
ORDER BY source_snapshot_seq DESC;
```

## Python data-path telemetry

The Python Lance event bridge converts structured Lance events into metrics and correlated logs.
Execution distributions live under `lance.pipeline.lance.execution.*`. Throttle gauges live under
`lance.pipeline.lance.throttle.*`. The exact suffixes follow numeric fields exposed by the pinned
Lance event contract.

Build a compact panel for:

- commit conflict count and retry latency from the ETL, compaction, and indexing spans
- Lance execution IO, bytes, and parts distributions
- object-store throttle rate
- work phase duration and error count from trace spans
- local Spark cycle duration

Use traces to correlate a slow phase with its dataset and exact source or publication evidence.

## Search request board

The Rust service emits these primary metrics:

| Metric | Type | Useful grouping |
|---|---|---|
| `search_api.rpc.requests` | count | `rpc`, `status` |
| `search_api.rpc.errors` | count | `rpc`, `status` |
| `search_api.rpc.duration_ms` | distribution | `rpc`, `status` |
| `search_api.query.iops` | distribution | `rpc` |
| `search_api.query.bytes_read` | distribution | `rpc` |
| `search_api.query.parts_loaded` | distribution | `rpc` |
| `search_api.dataset.open.duration_ms` | distribution | `cold` |
| `search_api.cache.handles.entries` | gauge | none |
| `search_api.cache.handles.weighted_size` | gauge | none |

Recommended widgets:

- request rate by RPC
- error ratio by RPC and closed status
- p50, p95, and p99 duration by RPC
- p95 IO operations and bytes read by RPC
- cold versus cached dataset-open latency
- handle cache entries and weighted capacity utilization

The service handles vector, text, and hybrid RPCs. Health checks are intentionally excluded from
the request board.

## Search cache board

| Metric | Meaning |
|---|---|
| `search_api.cache.lookup` | Hit or miss by cache and tier |
| `search_api.cache.insert_bytes` | Bytes written by cache and tier |
| `search_api.cache.backend_errors` | Persistent cache failures degraded to misses or dropped writes |
| `search_api.cache.disk.bytes` | Current local disk bytes by cache |
| `search_api.cache.disk.entries` | Current local disk entries by cache |
| `search_api.cache.sweep.duration_ms` | Local cache sweep duration |
| `search_api.cache.sweep.removed` | Entries removed per sweep |
| `search_api.cache.evictions` | Evictions by cache and reason |
| `search_api.cache.serialize_errors` | Values kept memory-only after serialization failure |

Graph hit ratio separately for index and metadata cache. A cache backend error is not a search
error, but a sustained rate predicts higher latency and object-store traffic.

## Search prewarm and Lance event board

| Metric | Meaning |
|---|---|
| `search_api.prewarm.duration_ms` | Exact-version prewarm duration by result status |
| `search_api.prewarm.index.duration_ms` | Per-index prewarm duration by index family |
| `search_api.prewarm.indexes_warmed` | Warmed index count |
| `search_api.prewarm.warmed_bytes` | Approximate resident index bytes |
| `search_api.lance.io_events` | Lance index and partition IO events |
| `search_api.lance.dataset_events` | Dataset lifecycle events |
| `search_api.lance.file_audit` | Lance file create and delete audit events |
| `search_api.throttle.errors` | Object-store throttle errors |
| `search_api.throttle.new_rate` | New local limiter rate after throttling |

The Python reconciler performs its required publication prewarm locally. Rust prewarm metrics are
still useful when directly exercising the search service or benchmark traits.

## Recall board

`search_api.recall.samples` counts sampled vector, text, and hybrid queries with closed
`query_type` and `filtered` tags. The request span contains the stable query, typed filter, results,
and exact dataset version. The Python recall audit replays that evidence with exact scans and emits
recall, nDCG, and MRR results.

Recall sampling defaults to zero. Enable it in code only after reviewing trace volume and privacy.
Never copy raw query values into metric tags.

## Trace panels

Useful trace searches include:

- Python reconciliation cycle and individual work phase spans
- Lance merge, compaction, index segment, and commit spans
- Rust gRPC server spans grouped by RPC and status
- `lance.vector_query` and `lance.text_query` spans with aggregate `object_store.*` attributes
- cold dataset open and cache backend error logs joined by trace ID

High-cardinality route, work, source snapshot, and publication identities belong here rather than in
metric dimensions.

## Local no-agent behavior

For a quiet local search process:

```bash
export SEARCH_API_TELEMETRY_DISABLED=true
```

Python telemetry and Rust metric construction both degrade to no-op behavior if their local agent is
unavailable. Verify application correctness through PostgreSQL state and test assertions rather than
requiring telemetry delivery.

## Dashboard review checklist

- Thresholds match the current `ReconcilerSettings` environment configuration.
- No high-cardinality dataset or version tags exist.
- Reconciler health, blocked source, blocked work, open age, due work, and retention age are visible.
- Search request rate, error ratio, latency, IO, cache, throttle, and open-handle capacity are visible.
- Trace links preserve route-level evidence without expanding metric cardinality.
- Missing telemetry cannot change work or serving state.
