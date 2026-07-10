# Serving, filters, and tags — architecture decisions

This document consolidates the decisions behind the Rust search service's shape: crate layering,
the typed filter AST, single-dataset targeting, event-time windows, blue-green serving, and
per-query version pinning. Each section keeps its original ADR number so references like
"ADR 0032" resolve here. Superseded decisions are summarized at the end.

## ADR 0005 — Rust gRPC service layering and the typed filter AST

Status: Accepted

The crate is layered with hard boundaries: `domain` holds engine- and transport-agnostic types
and traits (the typed `Filter` AST, query types, `SearchBackend`, fusion, errors) and references
neither proto nor tonic nor lance, `lance` holds the engine implementations, `grpc` is a thin
tonic adapter and the only place proto types appear, and `cache` and `telemetry` stand alone.

Filtering uses a typed `Filter` AST (comparison, in-list, is-null, between, and/or/not), never a
SQL string. Column names are validated against the dataset schema and the identifier allowlist
`[A-Za-z_][A-Za-z0-9_]*`, literals become typed DataFusion `lit` expressions, and injection
attempts are rejected at the allowlist (covered by tests). The `SearchService` and
`IntakeService` share one proto file (`proto/lance_etl/v1/lance_etl.proto`) and one
`DatasetTarget` message. `HybridSearchRequest` additionally accepts a request-level typed filter
that is ANDed into both legs before either index search runs.

## ADR 0014 — One dataset per target, time queries as scalar filters

Status: Accepted (supersedes ADR 0006, amends ADR 0004)

By-date partitioning and the serving-side date fan-out were removed entirely. Every deployment
routes on the stable identity trio, so each search target resolves to exactly one dataset, and
time-bounded queries are scalar range filters on the event-timestamp column handled by the
typed filter path with Lance scanner pushdown. Deleted with the fan-out: the strftime-to-Spark
partition derivations, the `DateRange` proto message, the 191-line dedup-keep-best merge module,
per-day 404 semantics, and the `SEARCH_API_FANOUT_CONCURRENCY` / `SEARCH_API_ID_COLUMN` knobs
whose writer/server coupling could silently mis-deduplicate. N-day queries stopped costing N
dataset opens and an O(N x k) merge.

## ADR 0021 — Event-time range on the search RPCs

Status: Accepted

An optional `TimeRange { start_ms, end_ms }` message (epoch milliseconds, start inclusive, end
exclusive, either bound optional) rides on all three search requests and always applies to the
event-timestamp column, fixed to the `DEFAULT_EVENT_TIMESTAMP_COLUMN` constant in `config.rs`
(`event_timestamp`, no longer env-configurable). The
range translates through the typed-filter path — each bound becomes a literal of the column's
own Arrow type (timestamp scaled to the column `TimeUnit` with its timezone, or a plain integer
for epoch-integer columns), so no cross-type coercion occurs. The range ANDs with any
caller-provided filter, a hybrid window applies to both legs, and an absent `time_range` leaves
every path unchanged.

## ADR 0013 — Tag-based blue-green serving

Status: Accepted (originally Proposed, since implemented and extended by ADR 0032)

A serving tag (default `HEAD`, configurable) updated via `tags.update` provides O(1) cutover.
The safety rules that make it correct with version-keyed caches:

- Prewarm accepts an explicit version or tag and returns the resolved version, so green is
  warmed BEFORE the flip — never flip then warm.
- The provider resolves the serve tag to a concrete version, keys the open-handle LRU and the
  caches on the resolved version (not the tag string), and bounds tag resolution with a short
  TTL (fixed by `DEFAULT_SERVE_TAG_TTL_SECS` in `config.rs`, 10 seconds, no longer
  env-configurable) so a flip is observed promptly without per-request manifest reads.
- The byte cache never caches the latest-version pointer in either manifest layout
  (`_latest.manifest`, `latest_version_hint.json`).
- Telemetry makes a flip-without-prewarm observable (`serve.cold_open` tagged `warmed`).
- Version cleanup never deletes a tagged version, and green is tagged before cleanup runs.

Serving through the tag is opt-in via `SEARCH_API_SERVE_BY_TAG`. The Python tag helper only
writes the tag and logs the safe sequence — the serving layer is never assumed to auto-refresh.

## ADR 0032 (query half) — Per-query tag and version pinning

Status: Accepted

Every search request carries an optional `version_ref` oneof (a committed version id or a tag
name, such as an ETL hourly interval tag). Unset means the serve policy — latest, or the
resolved serve tag when serve-by-tag is on — so the common latest path pays nothing. The
unpinned latest handle is itself freshness-bounded by the serve-tag TTL (per-entry expiry in
the handle LRU), so a new commit becomes visible within one TTL window even on a low-traffic
tenant whose handle capacity pressure would never evict. A pinned
request opens exactly that snapshot, and a hybrid pin opens both legs at the same resolved
version so fusion dedup stays consistent. Open INTENT is explicit rather than inferred from the
reference: prewarm opens route through `DatasetProvider::dataset_for_prewarm` while serving
opens (pinned or not) emit the cold-open metric.

Caching needs no new machinery because every layer keys on the resolved version: a pinned
handle coexists with the serve handle in the LRU, tag resolutions are TTL-cached and coalesced
per `(uri, tag)`, and the version-scoped index and metadata caches serve the pinned version's
entries directly. The monitored caveats: each distinct pinned version is its own weighted
handle, and each distinct tag costs one live tag-JSON read per TTL window per process. The
write-time half of ADR 0032 (hourly stamping) lives in `etl-and-data-model.md`.

## Superseded decisions

- **ADR 0006 — Date-range fan-out search with dedup-keep-best.** Superseded by ADR 0014. The
  `DateRange` targeting, the fan-out, and `domain/merge.rs` were deleted.
