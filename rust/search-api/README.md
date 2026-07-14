# search-api

A tonic gRPC service that serves vector, full-text, and hybrid search over per-tenant Lance
datasets. It is the read path of the `lance-etl` project: the PySpark ETL and indexing jobs under
`src/lance_etl/` build and maintain the Lance datasets and their IVF_RQ / BTREE / BITMAP / ZONEMAP /
FTS indices offline, and this service is what queries them online. It never writes indices, accepts
record writes, or runs Spark.

Package name: `search-api` (binary `search-api`, library crate `search_api`).

---

## What it serves

One gRPC service defined in `proto/lance_etl/v1/lance_etl.proto` (package `lance_etl.v1`):

| RPC | Purpose |
|---|---|
| `SearchService/VectorSearch` | Nearest-neighbor search with a typed filter and optional event-time window |
| `SearchService/TextSearch` | BM25 full-text search with a typed filter and optional event-time window |
| `SearchService/HybridSearch` | Vector and text search fused by a code-owned product mode |

Every request carries only a logical `DatasetTarget` (`org_id`, `tenant_id`, `namespace`). The
server resolves it through PostgreSQL to an allowlisted Lance URI, exact committed version, and
code-owned serving profile. Clients cannot select a URI, version, tag, index, ANN execution knob,
physical row identifier, result offset, or raw fusion weight. There is no cross-dataset or
cross-org query surface anywhere in the API.

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
search engine implements `domain::SearchBackend` purely in domain types and the transport needs no
change. A new serving catalog implements `domain::ServingCatalog`. A new dataset-resolution
strategy implements `lance::DatasetProvider`. A new persistence backend implements
`cache::entry_store::EntryStore`. A new fusion strategy is a variant on `domain::FusionSpec`.

### Module map

| Module | Key types | Purpose |
|---|---|---|
| `domain::filter` | `Filter`, `CompareOp`, `Literal` | Typed predicate AST — no raw SQL accepted anywhere |
| `domain::query` | `VectorQuery`, `TextQuery`, `HybridQuery`, `Hit`, `FusedHit` | Query and result types |
| `domain::target` | `DatasetTarget`, `DatasetRef` | Validated logical addressing and internal snapshot selection |
| `domain::catalog` | `ServingCatalog`, `ServingRoute` | Logical-target to exact-serving-route contract |
| `domain::backend` | `SearchBackend` | The engine trait: `vector_search` / `text_search` / `hybrid_search` |
| `domain::prewarm` | `PrewarmSpec`, `PrewarmReport`, `Prewarmer` | Cache-warming trait |
| `domain::clusters` | `ClusterSpec`, `ClusterReport`, `ClusterReader` | IVF centroid introspection trait |
| `domain::fusion` | `FusionSpec` (`Rrf`, `Weighted`) | Within-dataset hybrid fusion, a pure function of the leg lists |
| `cache::entry_store` | `EntryStore` | The persistent byte-store trait beneath both cache tiers |
| `cache::disk_store` | `DiskEntryStore` | Local-disk backend (the default) |
| `cache::redis_store` | `RedisEntryStore` | Shared-Redis backend: one Redis HASH per dir, native TTL, registry hygiene |
| `cache::index_cache` | `HybridIndexCacheBackend` | Moka hot tier + pluggable persistent `CacheBackend` for the Lance index cache |
| `cache::store_cache` | (metadata byte cache) | Read-through byte cache for immutable object-store metadata |
| `cache::layout` | `LANCE_CACHE_STAMP`, `CACHE_SCHEMA_VERSION`, frame/hash helpers | Versioned stamp naming, key hashing, checksummed framing, atomic writes |
| `cache::janitor` | (sweep loop) | Periodic TTL + byte-budget sweep over the disk tiers |
| `lance::backend` | `LanceSearchBackend<P>` | `SearchBackend` impl: single-dataset dispatch, vector/text/hybrid fusion, `object_store.*` span accounting |
| `catalog` | `PostgresServingCatalog` | TLS-only PostgreSQL implementation of exact target resolution |
| `lance::provider` | `DatasetProvider`, `CachingDatasetProvider` | Catalog validation, exact-version opens, shared `Session`, and handle LRU |
| `lance::filter` | `filter_to_expr` | Domain `Filter` -> DataFusion `Expr` |
| `lance::text` | (FTS param mapping) | Domain text query tree -> Lance FTS parameters |
| `lance::rows` | (row conversion) | Arrow record batch -> JSON row conversion |
| `lance::prewarm` | `Prewarmer` impl | Cache warming over Lance prewarm APIs |
| `lance::index_reader` | `ClusterReader` impl | IVF centroid extraction |
| `grpc::mod` | `SearchGrpc<B>` | Tonic adapter for `SearchService`, generic over `SearchBackend` |
| `grpc::convert` | (proto <-> domain) | Conversion for the search service |
| `telemetry::traces` | `init_tracing`, `LanceEventMetricsLayer` | OTLP span export, JSON stdout logs, bridges Lance throttle/io/dataset/file-audit trace events into metrics |
| `telemetry::metrics` | `Metrics`, `Rpc`, `CacheName`, `Tier` | Typed DogStatsD facade |
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

**Never drop-and-recreate a dataset at the same URI.** The persistent caches key on
`(store prefix, object path)` under the assumption that everything they cache is immutable:
version manifests, transaction files, index pages. That assumption is exactly what Lance's naming
guarantees — until a dataset is deleted and a new one is created at the same
`{org}/{tenant}/{namespace}` path. The new dataset restarts version numbering, so its
`_versions/1.manifest` collides with the cached manifest of the dead dataset and replicas can
serve phantom fragments for up to the cache TTL (7 days). The mutable latest-version pointers are
never cached, but they point into the poisoned immutable namespace. There is no clean read-path
fix: validating the cached bytes against the live object's etag would cost a conditional request
per read, which is the cost the cache exists to avoid. The operational rule is therefore: retire
a dataset by retiring its namespace (create the replacement under a new `namespace` segment), or,
if the URI truly must be reused, wipe the cache generation first (clear `SEARCH_API_CACHE_DIR` or
flush the Redis namespace on every replica) before the new dataset serves. The same rule protects
Lance's own in-session caches and the open-handle LRU, which share the URI-keyed design.

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

**Catalog-pinned serving.** Public requests cannot choose a version or tag. `DatasetRef::Serve`
resolves the validated logical target through `ServingCatalog`, rejects routes outside
`LANCE_ETL_BASE_URI`, and opens exactly `served_lance_version`. Responses return that immutable
`served_version`. Publication changes are therefore atomic catalog changes and an in-flight
request remains pinned to the version it resolved.

---

## Configuration

All configuration is read once at startup by `Config::from_env` in `src/config.rs`. The serving
prefix, verified PostgreSQL connection, TLS identity, and JWT trust settings are required. Every
execution tuning knob (dataset-handle cache
sizing, index/metadata/disk cache budgets, serve-tag TTL, IO concurrency, ANN probe/refine/
fast-search defaults, gRPC timeout and concurrency limits, the event-timestamp column, recall
sampling, and the search `k` ceiling) is a fixed constant in `src/config.rs`, not an env knob.

| Variable | Default | Purpose |
|---|---|---|
| `LANCE_ETL_BASE_URI` | (required) | Base URI all dataset paths resolve under: `{base}/{org_id}/{tenant_id}/{namespace}.lance` |
| `LANCE_ETL_DATABASE_URL` | (required) | PostgreSQL control-plane URL. Must set exactly one `sslmode=verify-full`. The `postgresql+psycopg://` scheme is accepted. |
| `SEARCH_API_DATABASE_CA_PATH` | (required) | Mounted PEM root CA for PostgreSQL certificate and hostname verification |
| `SEARCH_API_TLS_CERT_PATH` | (required) | Mounted PEM certificate chain for the TLS search listener |
| `SEARCH_API_TLS_KEY_PATH` | (required) | Mounted PEM private key for the TLS search listener |
| `SEARCH_API_JWT_ISSUER` | (required) | Exact trusted bearer-token issuer |
| `SEARCH_API_JWT_AUDIENCE` | (required) | Exact trusted bearer-token audience |
| `SEARCH_API_JWKS_URI` | (required) | HTTPS signing-key set fetched at startup and refreshed boundedly |
| `SEARCH_API_PORT` | `8080` | TCP port |
| `SEARCH_API_CACHE_BACKEND` | `disk` | Persistent cache backend: `disk`, `redis`, or `memory` |
| `SEARCH_API_REDIS_URL` | (none) | Redis connection URL (`redis://` or `rediss://`), required when the backend is `redis` |
| `SEARCH_API_REDIS_NAMESPACE` | `search-api` | Key namespace prepended to every Redis cache key |
| `SEARCH_API_CACHE_DIR` | `/tmp/rust-search/cache` | Root directory for the `disk` backend's caches |
| `SEARCH_API_STATSD_ADDR` | `127.0.0.1:8125` (or `{DD_AGENT_HOST}:8125` when `DD_AGENT_HOST` is set) | DogStatsD UDP address |
| `SEARCH_API_TELEMETRY_DISABLED` | `false` | Disables trace export and DogStatsD entirely (JSON logs only) |

Two process-global Lance IO knobs (`LANCE_IO_THREADS`, `OBJECT_STORE_CLIENT_RETRY_TIMEOUT`) are
stamped into the process environment by `main` before the tokio runtime starts, from fixed
constants in `config.rs` (`DEFAULT_IO_CONCURRENCY` = 256, `DEFAULT_OBJECT_STORE_TIMEOUT_SECS` =
120). A matching pre-set value is accepted and a conflicting value fails startup. This must happen
while the process is still single-threaded — `std::env::set_var` is unsound once other threads
exist — so `main` is a synchronous entry point that runs it before building the runtime.

---

## Build, test, lint

Building requires `protoc` (the protobuf compiler) on PATH. `build.rs` compiles the proto through
`tonic_prost_build`, which shells out to `protoc`. GitHub runners and fresh machines do not
preinstall it, so add it first (`apt-get install protobuf-compiler`, `brew install protobuf`, or
equivalent). The CI rust job installs it explicitly for the same reason.

```bash
cd rust/search-api
cargo fmt                    # format
cargo clippy -- -D warnings  # lint (must be clean)
cargo build                  # compile
cargo test                   # unit tests
```

The Redis cache-backend integration tests use `SEARCH_API_TEST_REDIS_URL` when CI supplies it.
Otherwise they spawn a throwaway local `redis-server` per test and self-skip with a message when
the binary is not installed. The forced-outage test always requires a local disposable server.

### Running the service

```bash
cargo build --release
LANCE_ETL_BASE_URI=s3://my-bucket/lance \
  LANCE_ETL_DATABASE_URL='postgresql://search@catalog/control?sslmode=verify-full' \
  SEARCH_API_DATABASE_CA_PATH=/run/secrets/postgres-ca.pem \
  SEARCH_API_TLS_CERT_PATH=/run/secrets/tls.crt \
  SEARCH_API_TLS_KEY_PATH=/run/secrets/tls.key \
  SEARCH_API_JWT_ISSUER=https://identity.example.com \
  SEARCH_API_JWT_AUDIENCE=lance-search \
  SEARCH_API_JWKS_URI=https://identity.example.com/.well-known/jwks.json \
  SEARCH_API_PORT=8080 \
  ./target/release/search-api
```

Port 8080 requires TLS and a bearer JWT whose `org_id`, `tenant_id`, and `namespace` claims exactly
match the request plus a `search` role. Fixed port 8081 exposes only the standard plaintext gRPC
health service for Kubernetes native probes. It reports serving only after catalog and JWKS
initialization, tracks those dependencies, and becomes not-serving when bounded drain begins.

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
prefixed and tagged with small closed enums (`Rpc`, `CacheName`, `Tier`) rather than
free-form strings, keeping cardinality bounded by construction.

`telemetry::recall::RecallCapture` deterministically samples a slice of `VectorSearch`,
`TextSearch`, and `HybridSearch` requests (sample rate fixed at `DEFAULT_RECALL_SAMPLE_RATE`,
currently `0.0`, i.e. disabled) using an allocation-free per-process counter, and attaches a flat
group of `recall.*` attributes (query, filter, result ids/scores/distances, dataset version, and
so on) to the request's tracing span. The schema is a cross-language contract: Rust writes it here
and the Python `RecallAuditJob` (`src/lance_etl/recall/`) reads it back from Datadog Spans to
replay each query as an exact brute-force or exact BM25 scan and score recall@k, nDCG@k, and MRR.
The full attribute schema is documented in `src/telemetry/recall.rs`.
