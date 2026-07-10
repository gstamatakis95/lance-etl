# Caching and observability — architecture decisions

This document consolidates the decisions behind the search service's persistent caching, the
Prewarm RPC, the Lance trace-event bridge, and recall auditing. Each section keeps its original
ADR number so references like "ADR 0031" resolve here.

## ADR 0007 — Persistent index/metadata cache and the Prewarm RPC

Status: Accepted (the disk-only framing reshaped by ADR 0031)

Lance ships only an in-memory cache backend, so the service injects a hybrid
persistent-plus-memory `CacheBackend` into the shared Lance `Session` index cache and wraps
object stores with a metadata byte cache (`WrappingObjectStore`) covering versioned manifests,
transactions, and small index files only — raw `data/` reads always pass through, provably
excluded by the path classifier. A Prewarm RPC opens a dataset through the shared session and
calls `load_indices` plus per-index `prewarm_index`, so a caller warms an org before traffic
arrives.

The invariants that outlived every backend change:

- Cache keys are URI plus index-UUID plus version-manifest-path, so entries are
  version-correct and one shared session safely spans 30k datasets.
- The byte cache must NEVER cache the latest-version pointer (`_latest.manifest` in V1, the
  `latest_version_hint.json` hint in V2), or a tag flip would be invisible to a replica.
- Prewarm must be able to target an explicit version or tag, because warming the live version
  and then flipping a serve tag leaves the new version cold (see
  `serving-filters-and-tags.md`, ADR 0013).

## ADR 0031 — Pluggable cache backend (disk | redis | memory)

Status: Accepted

The persistence beneath both cache tiers is one local seam, the `EntryStore` trait
(`src/cache/entry_store.rs`), keyed by `(dir, file)` where the dir is the invalidation unit.
The compositions — `HybridIndexCacheBackend` (memory hot tier, promotion, single-flight, LEC2
blake3-checksummed framing) and `MetadataByteCache` (path routing, never-cache invariants) —
are written once and work over any store. Writing second full `CacheBackend`/`ObjectStore`
implementations was rejected: it would duplicate hundreds of lines of subtle, tested logic.

`SEARCH_API_CACHE_BACKEND` selects the store: `disk` (default, `DiskEntryStore`, byte-identical
to the pre-seam layout so existing caches survived the refactor), `redis` (`RedisEntryStore`),
or `memory` (no persistence). The Redis layout stores each dir as ONE HASH under
`{namespace}:{stamp}:{tier}:{dir}` — dir invalidation is a single `DEL`, the store tier's
entry-plus-sidecar read is one `HMGET`, and `allkeys-lru` evicts whole objects. TTLs are native
(per-dir `EXPIRE`, 7 days, refreshed on access) and capacity is the server's `maxmemory`, so
the disk janitor does not run for Redis. The prefix registry HASH carries no TTL (expiring it
would break prefix invalidation, a correctness path) and an hourly hygiene pass reclaims dead
rows. Each replica's local seen-set is time-bounded to the hygiene cadence, so a row a sibling
reclaimed is re-registered within one window once its prefix warms again — a purge can miss a
re-warmed prefix for at most that window, never indefinitely. Being shared, the registry lets
one replica's invalidation cover entries written by siblings.

The `{stamp}` segment (`v{CACHE_SCHEMA_VERSION}-lance-{version}`) binds keys to the cache
schema and lance versions: bump `LANCE_CACHE_STAMP` in `src/cache/layout.rs` together with the
lance crates, old disk directories are wiped and old Redis generations age out via TTL.
Failure semantics: an unreachable Redis at startup falls back to memory-only with a warning,
and every per-call error degrades to a miss or dropped write metered by
`cache.backend_errors` — a down cache never fails a search.

## ADR 0022 — Lance trace-event bridge

Status: Accepted

Per-query object-store stats from Lance's execution-summary callback attach to the query-leg
spans (`lance.vector_query` / `lance.text_query`) as provider-neutral `object_store.*`
attributes: `object_store.requests`, `object_store.iops`, `object_store.bytes_read`,
`object_store.parts_loaded`, `object_store.indices_loaded` (renamed from the earlier `s3.*`
because the same code path serves S3, Azure Blob, and GCS). They are aggregate counts only — a
GET/HEAD/LIST split is not available in production Lance builds — and carry no org, tenant, or
version identifier. Three further Lance tracing targets (`lance::io_events`,
`lance::dataset_events`, `lance::file_audit`) are force-admitted through the `EnvFilter` and
bridged both as span events on the active span and as low-cardinality DogStatsD counters
through `LanceEventMetricsLayer`, which also subsumes the object-store throttle tap.

## ADR 0008 — Datadog observability and recall auditing

Status: Accepted

Both sides instrument for Datadog with a strict cardinality split: low-cardinality DogStatsD
metrics carry fleet aggregates (never org, tenant, or version tags), and high-cardinality
per-request detail lives on OTLP trace spans and JSON logs with trace correlation. The Python
jobs bridge Lance trace events on first `Telemetry.create` per process. The Rust service emits
per-RPC traces and typed, infallible metric emitters with a disabled mode for tests.

For recall auditing, the service deterministically samples a fraction of queries
(`DEFAULT_RECALL_SAMPLE_RATE` in `config.rs`, exactly `floor(N x rate)`, no RNG) onto the request span,
recording the query, served ids and distances, params, the typed filter AST, and the dataset
version that served the query. The offline `recall` job pulls those spans from the Datadog
Spans API, opens each dataset PINNED at the recorded version, brute-forces exact top-k
(replaying the filter AST with strict identifier validation, honoring the no-raw-SQL rule),
and scores recall@k. Recording the version makes the score exact rather than
approximate-under-churn, and the audit must run inside the cleanup retention horizon so the
sampled versions still exist. Datadog needs a retention filter on `recall.sample:true`.
