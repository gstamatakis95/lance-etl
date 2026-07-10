# search-api

A tonic gRPC service that serves vector, full-text, and hybrid search over per-tenant Lance
datasets. It is the read (and light-write) path of the `lance-etl` project: the PySpark ETL and
indexing jobs under `src/lance_etl/` build and maintain the Lance datasets and their IVF_RQ /
BTREE / BITMAP / ZONEMAP / FTS indices offline, and this service is what queries them online. It
never writes indices and never runs Spark. The one write path it exposes, `IntakeService`, is a
thin validate-and-forward seam in front of a pluggable `RecordSink`, not a replacement for the ETL
job.

Package name: `search-api` (binary `search-api`, library crate `search_api`).

---

## What it serves

Two gRPC services defined in one proto file, `proto/lance_etl/v1/lance_etl.proto` (package
`lance_etl.v1`), sharing one `DatasetTarget` message:

| RPC | Purpose |
|---|---|
| `SearchService/VectorSearch` | Nearest-neighbor search, optional rerank, optional event-time window |
| `SearchService/TextSearch` | BM25 full-text search, optional rerank, optional event-time window |
| `SearchService/HybridSearch` | Fused vector + text (RRF or weighted), optional request-level typed filter applied to both legs |
| `SearchService/Prewarm` | Pulls caches for a dataset at an explicit version or tag |
| `SearchService/Clusters` | Reads the IVF centroid vectors of a vector index |
| `IntakeService/Write` | Applies one batch of record writes (UPSERT/DELETE) to a single dataset |
| `IntakeService/WriteStream` | Client-streaming batches of record writes, one aggregated response on half-close |

Every request carries a `DatasetTarget` (`org_id`, `tenant_id`, `namespace`) that resolves to
exactly one dataset at `{base_uri}/{org_id}/{tenant_id}/{namespace}.lance`. There is no
cross-dataset or cross-org query surface anywhere in the API.

---

## Architecture

Five layers, each with a narrow, one-directional dependency on the layer below it (see the
crate-root doc comment in `src/lib.rs` for the authoritative statement):

| Layer | Path | Owns |
|---|---|---|
| `domain` | `src/domain/` | Engine- and transport-agnostic types and traits. Never references protobuf, tonic, or Lance. |
| `cache` | `src/cache/` | Persistent caching (disk or Redis) plugged into Lance's own cache and object-store seams. Never references datasets, queries, or domain types. |
| `lance` | `src/lance/` | Lance-backed implementations of the domain traits. The only place Lance types appear. |
| `grpc` | `src/grpc/` | Thin tonic transport mapping protobuf onto the domain traits. The only place proto/tonic types appear. |
| `telemetry` | `src/telemetry/` | Datadog tracing/metrics/logging facade, usable from every layer above `domain`. |

Extension points follow directly from this layering (see `src/lib.rs` for the full list). A new
search engine implements `domain::SearchBackend` (+ `Prewarmer`, `ClusterReader`) purely in
domain types and the transport needs no change. A new dataset-resolution strategy implements
`lance::DatasetProvider`. A new persistence backend implements `cache::entry_store::EntryStore`. A
new fusion strategy is a variant on `domain::FusionSpec`.

### Module map

| Module | Key types | Purpose |
|---|---|---|
| `domain::filter` | `Filter`, `CompareOp`, `Literal` | Typed predicate AST — no raw SQL accepted anywhere |
| `domain::query` | `VectorQuery`, `TextQuery`, `HybridQuery`, `Hit`, `FusedHit` | Query and result types |
| `domain::target` | `DatasetTarget`, `DatasetRef` | Dataset addressing and version/tag selection |
| `domain::backend` | `SearchBackend` | The engine trait: `vector_search` / `text_search` / `hybrid_search` |
| `domain::prewarm` | `PrewarmSpec`, `PrewarmReport`, `Prewarmer` | Cache-warming trait |
| `domain::clusters` | `ClusterSpec`, `ClusterReport`, `ClusterReader` | IVF centroid introspection trait |
| `domain::fusion` | `FusionSpec` (`Rrf`, `Weighted`) | Within-dataset hybrid fusion, a pure function of the leg lists |
| `domain::intake` | `IntakeBatch`, `Record`, `RecordWrite`, `WriteOp`, `RecordSink`, `StdoutSink` | Write-path domain types and the sink seam |
| `cache::entry_store` | `EntryStore` | The persistent byte-store trait beneath both cache tiers |
| `cache::disk_store` | `DiskEntryStore` | Local-disk backend (the default) |
| `cache::redis_store` | `RedisEntryStore` | Shared-Redis backend: one Redis HASH per dir, native TTL, registry hygiene |
| `cache::index_cache` | `HybridIndexCacheBackend` | Moka hot tier + pluggable persistent `CacheBackend` for the Lance index cache |
| `cache::store_cache` | (metadata byte cache) | Read-through byte cache for immutable object-store metadata |
| `cache::layout` | `LANCE_CACHE_STAMP`, `CACHE_SCHEMA_VERSION`, frame/hash helpers | Versioned stamp naming, key hashing, checksummed framing, atomic writes |
| `cache::janitor` | (sweep loop) | Periodic TTL + byte-budget sweep over the disk tiers |
| `lance::backend` | `LanceSearchBackend<P>` | `SearchBackend` impl: single-dataset dispatch, vector/text/hybrid fusion, `object_store.*` span accounting |
| `lance::provider` | `DatasetProvider`, `CachingDatasetProvider` | Shared `Session` + Moka LRU of open dataset handles, tag-version resolution |
| `lance::filter` | `filter_to_expr` | Domain `Filter` -> DataFusion `Expr` |
| `lance::text` | (FTS param mapping) | Domain text query tree -> Lance FTS parameters |
| `lance::rows` | (row conversion) | Arrow record batch -> JSON row conversion |
| `lance::prewarm` | `Prewarmer` impl | Cache warming over Lance prewarm APIs |
| `lance::index_reader` | `ClusterReader` impl | IVF centroid extraction |
| `grpc::mod` | `SearchGrpc<B>` | Tonic adapter for `SearchService`, generic over `SearchBackend` |
| `grpc::convert` | (proto <-> domain) | Conversion for the search service |
| `grpc::intake` | `IntakeGrpc<S>` | Tonic adapter for `IntakeService`, generic over `RecordSink` |
| `grpc::intake_convert` | (proto <-> domain) | Conversion for the intake service |
| `telemetry::traces` | `init_tracing`, `LanceEventMetricsLayer` | OTLP span export, JSON stdout logs, bridges Lance throttle/io/dataset/file-audit trace events into metrics |
| `telemetry::metrics` | `Metrics`, `Rpc`, `IntakeRpc`, `CacheName`, `Tier` | Typed DogStatsD facade |
| `telemetry::recall` | `RecallCapture`, `RecallRecord` | Deterministic sampled-query capture into `recall.*` span attributes |
| `config` | `Config`, `CacheBackendKind` | Environment-driven runtime configuration |

---

## Invariants a contributor must not break

**No raw SQL.** `domain::filter::Filter` is a typed AST (`Compare`, `InList`, `IsNull`,
`IsNotNull`, `Between`, `And`, `Or`, `Not`). Column names are validated against the dataset schema
at translation time and literals become typed DataFusion `lit` expressions via
`lance::filter::filter_to_expr`. Do not accept, construct, or pass a raw SQL string anywhere in
the `grpc` or `domain` layers — this mirrors hard rule 3 in the repository root `AGENTS.md`. The
AST also has a stable serde JSON shape (documented in `domain/filter.rs`) because the same JSON is
written into the `recall.filter` span attribute and parsed by the Python recall audit job, so
field names and enum tagging are a cross-language contract, not just an internal detail.

**One dataset per request.** Every RPC carries exactly one `DatasetTarget`
(`org_id`/`tenant_id`/`namespace`), each validated against `[A-Za-z0-9_-]+` in
`domain::target::validate_path_segment` to block path traversal. There is no fan-out to multiple
datasets inside a single request and no cross-org query path — this is a firm project-wide rule,
not just a service detail.

**Two-tier cache, index and metadata only.** `HybridIndexCacheBackend` and the metadata
`store_cache` compose a Moka in-memory hot tier over a pluggable persistent `EntryStore` (local
disk by default, or shared Redis). Both tiers cache serialized Lance index structures and
immutable object-store metadata (byte ranges under `_indices/`, `ObjectMeta` sidecars). Raw
dataset row data is never cached at either tier — only index and metadata bytes cross the
`EntryStore` seam.

**`LANCE_CACHE_STAMP` versioning contract.** `cache::layout::LANCE_CACHE_STAMP` (currently
`"8.0.0"`) is baked into the on-disk stamp directory name (`v{CACHE_SCHEMA_VERSION}-lance-{stamp}`)
because the cache codec format is explicitly unstable across lance releases. Bump it together with
the `lance`/`lance-core`/`lance-index`/`lance-io`/`lance-linalg` versions in `Cargo.toml` and the
`pylance` pin in the Python project. A bump makes `prepare_cache_root` delete any sibling stamp
directory on next startup (old-generation disk cache wiped) and the same stamp is folded into
every Redis key, so old-generation Redis keys simply age out through their TTLs rather than being
actively purged (ADR 0031, `docs/adr/caching-and-observability.md`).

**Metric emitters are infallible.** `Metrics` falls back to a no-op `NopMetricSink` with a warning
when the DogStatsD client cannot be constructed, and every per-request emission call is designed
to never block and never fail the request. An unreachable Datadog Agent degrades observability,
never availability. The same degrade-not-fail rule applies to the Redis `EntryStore`: a Redis
round-trip error becomes a cache miss or dropped write, counted by `cache.backend_errors`, never a
failed search.

**Low-cardinality span attributes.** The per-query-leg spans (`lance.vector_query` /
`lance.text_query` internally, surfaced through `LanceSearchBackend`) carry `object_store.*`
attributes sourced from Lance execution-stats events: `object_store.requests`,
`object_store.iops`, `object_store.bytes_read`, `object_store.parts_loaded`,
`object_store.indices_loaded`. These are aggregate counts only — no per-method GET/HEAD/LIST
split, and no org, tenant, or version identifier is attached. Do not add high-cardinality
identifiers to span attributes or metric tags.

**No stable row IDs, no cross-org sharing.** These are repository-wide rules the search service
inherits: `enable_stable_row_ids` is rejected everywhere (ADR 0010,
`docs/adr/rejected-and-operator-tools.md`), and one Lance dataset per org is firm — never add a
shared-dataset or cross-org query mode to the domain or gRPC layers.

**Query-at-a-tag.** Every search RPC carries an optional `version_ref` naming a committed version
id or a tag (e.g. an ETL hourly interval tag). `DatasetRef::Serve` (the default) follows the
provider's configured serve policy so the common latest-version path pays no extra cost. A pinned
request opens exactly that snapshot, coexisting in the handle LRU with the serve handle (ADR 0032,
`docs/adr/serving-filters-and-tags.md`). Blue-green flips go through a named tag: build the green
version, prewarm every replica against it explicitly via the `Prewarm` RPC's `version`/`tag`
field, then flip the tag. Never flip before warming.

---

## Configuration

All configuration is read once at startup by `Config::from_env` in `src/config.rs`.
`LANCE_ETL_BASE_URI` is the only required variable. Every other tuning knob (dataset-handle cache
sizing, index/metadata/disk cache budgets, serve-tag TTL, IO concurrency, ANN probe/refine/
fast-search defaults, gRPC timeout and concurrency limits, the event-timestamp column, recall
sampling, and the search `k` ceiling) is a fixed constant in `src/config.rs`, not an env knob.

| Variable | Default | Purpose |
|---|---|---|
| `LANCE_ETL_BASE_URI` | (required) | Base URI all dataset paths resolve under: `{base}/{org_id}/{tenant_id}/{namespace}.lance` |
| `SEARCH_API_PORT` | `8080` | TCP port |
| `SEARCH_API_CACHE_BACKEND` | `disk` | Persistent cache backend: `disk`, `redis`, or `memory` |
| `SEARCH_API_REDIS_URL` | (none) | Redis connection URL (`redis://` or `rediss://`), required when the backend is `redis` |
| `SEARCH_API_REDIS_NAMESPACE` | `search-api` | Key namespace prepended to every Redis cache key |
| `SEARCH_API_CACHE_DIR` | `/tmp/rust-search/cache` | Root directory for the `disk` backend's caches |
| `SEARCH_API_DISK_CACHE_DISABLED` | `false` | Deprecated alias for `SEARCH_API_CACHE_BACKEND=memory`, honored only when the latter is unset |
| `SEARCH_API_SERVE_BY_TAG` | `false` | Resolve the configured serve tag instead of opening the latest committed version |
| `SEARCH_API_SERVE_TAG` | `HEAD` | Tag name resolved when `SEARCH_API_SERVE_BY_TAG=true` |
| `SEARCH_API_PREWARM_TARGETS_PATH` | (empty, disabled) | Path to a startup prewarm-targets file, one `{org_id}/{tenant_id}/{namespace}` per line, warmed in the background before those datasets would otherwise be opened cold |
| `SEARCH_API_STATSD_ADDR` | `127.0.0.1:8125` (or `{DD_AGENT_HOST}:8125` when `DD_AGENT_HOST` is set) | DogStatsD UDP address |
| `SEARCH_API_TELEMETRY_DISABLED` | `false` | Disables trace export and DogStatsD entirely (JSON logs only) |

Two process-global Lance IO knobs (`LANCE_IO_THREADS`, `OBJECT_STORE_CLIENT_RETRY_TIMEOUT`) are
stamped into the process environment by `main` before the tokio runtime starts, from fixed
constants in `config.rs` (`DEFAULT_IO_CONCURRENCY` = 256, `DEFAULT_OBJECT_STORE_TIMEOUT_SECS` =
120). This must happen while the process is still single-threaded — `std::env::set_var` is unsound
once other threads exist — so `main` is a synchronous entry point that runs it before building the
runtime.

---

## Build, test, lint

```bash
cd rust/search-api
cargo fmt                    # format
cargo clippy -- -D warnings  # lint (must be clean)
cargo build                  # compile
cargo test                   # unit tests
```

The Redis cache-backend integration tests (`tests/redis_cache.rs`) spawn a throwaway local
`redis-server` per test and self-skip with a message when the binary is not installed, so
`cargo test` stays green without Redis. Install `redis-server` to run them unskipped.

### Running the service

```bash
cargo build --release
LANCE_ETL_BASE_URI=s3://my-bucket/lance \
  SEARCH_API_PORT=8080 \
  ./target/release/search-api
```

The intake service is wired to `StdoutSink`, a structured-print placeholder. A future `KafkaSink`
implements the same `RecordSink` trait and replaces it at the construction site in `main` without
any change to the proto, transport, or domain types.

---

## Observability

Traces export over OTLP gRPC to the Datadog Agent (`init_tracing` in `src/telemetry/traces.rs`).
Endpoint resolution honors `OTEL_EXPORTER_OTLP_ENDPOINT` first, falling back to
`http://{DD_AGENT_HOST}:4317`. JSON logs on stdout carry trace/span correlation fields so a log
line and its span join in Datadog. Every RPC flows through an OpenTelemetry tower layer (health
checks excluded) that extracts inbound trace context and opens the per-request server span.
`LanceEventMetricsLayer` bridges Lance's own tracing events (object-store throttle, IO, dataset
lifecycle, file audit) into DogStatsD metrics without the telemetry layer depending on Lance
types.

Metrics go through the typed `Metrics` facade in `src/telemetry/metrics.rs`, all `search_api.*`
prefixed and tagged with small closed enums (`Rpc`, `IntakeRpc`, `CacheName`, `Tier`) rather than
free-form strings, keeping cardinality bounded by construction.

`telemetry::recall::RecallCapture` deterministically samples a slice of `VectorSearch`,
`TextSearch`, and `HybridSearch` requests (sample rate fixed at `DEFAULT_RECALL_SAMPLE_RATE`,
currently `0.0`, i.e. disabled) using an allocation-free per-process counter, and attaches a flat
group of `recall.*` attributes (query, filter, result ids/scores/distances, dataset version, and
so on) to the request's tracing span. The schema is a cross-language contract: Rust writes it here
and the Python `RecallAuditJob` (`src/lance_etl/recall/`) reads it back from Datadog Spans to
replay each query as an exact brute-force or exact BM25 scan and score recall@k, nDCG@k, and MRR.
The full attribute schema is documented in `src/telemetry/recall.rs`.
