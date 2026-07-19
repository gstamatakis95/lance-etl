# AGENTS.md — Rust search service (`rust/search-api/`)

The repository-root `AGENTS.md` is the canonical rulebook. Its ten **Hard coding rules** apply
here in full. For Rust the relevant ones are rule 2 (`///` doc comments only on public items, no
`//` inline comments in production code paths), rule 7 (no raw SQL strings in the filter API), and
rule 8 (no stable row IDs). This file adds the service specifics: the crate layout, cargo commands,
the lance-crate version-bump coupling, Rust telemetry conventions, the filter-AST rule detail, and
proto surface facts not already in the sibling `README.md`.

`README.md` in this directory is the developer-facing service overview (what it serves, the
five-layer architecture, invariants, configuration, observability). Read it first and do not
duplicate it here. This file is agent-facing and complementary.

---

## Layout

```
rust/search-api/        Rust gRPC search service (tonic, lance crate)
  proto/                lance_etl/v1/lance_etl.proto (SearchService and DatasetTarget)
  src/domain/           Transport-agnostic types and traits
    target.rs           DatasetTarget, DatasetRef — dataset addressing (one dataset per request)
    query.rs            VectorQuery, TextQuery, HybridQuery, Hit, FusedHit
    filter.rs           Typed predicate AST (no raw SQL)
    backend.rs          SearchBackend trait
    prewarm.rs          PrewarmSpec, PrewarmReport, Prewarmer trait
    clusters.rs         ClusterSpec, ClusterReport, ClusterReader trait
    fusion.rs           FusionSpec (Rrf and Weighted variants) and within-dataset fusion logic
    error.rs            SearchError
  src/cache/            Persistent two-tier caching layer (index + metadata, no raw data), pluggable disk/redis backends
    layout.rs           Versioned stamp naming, key hashing, framing, atomic writes, TTL/budget sweep
    entry_store.rs      EntryStore trait: the persistent byte-store seam beneath both tiers
    disk_store.rs       Local-disk EntryStore (the default backend, prefixes.json registry)
    redis_store.rs      Shared-Redis EntryStore (hash-per-dir keys, native TTL, registry hygiene)
    index_cache.rs      HybridIndexCacheBackend: Moka hot tier + pluggable persistent CacheBackend
    store_cache.rs      Read-through byte cache for immutable metadata of wrapped stores
    janitor.rs          Periodic TTL + budget sweep over the disk tiers (redis needs none)
  src/lance/            Lance backend implementations
    backend.rs          LanceSearchBackend — single-dataset dispatch, vector/text/hybrid fusion
    provider.rs         DatasetProvider trait, CachingDatasetProvider (shared session + LRU)
    filter.rs           filter_to_expr: domain Filter -> DataFusion Expr
    text.rs             Domain text query tree -> Lance FTS parameters
    rows.rs             Arrow record batch -> JSON row conversion
    prewarm.rs          Prewarmer impl over Lance prewarm APIs
    index_reader.rs     IVF centroid extraction, ClusterReader impl
    error.rs            Lance error classification into SearchError
  src/grpc/             Tonic transport
    mod.rs              SearchGrpc<B>: tonic service adapter
    convert.rs          Proto <-> domain conversion for the search service
  src/telemetry/        Datadog observability
    traces.rs           OTLP span export, JSON stdout logs with trace correlation
    metrics.rs          Typed DogStatsD facade (Metrics struct + Rpc tag enum)
    recall.rs           Deterministic sampled-query capture into recall.* span attributes
  src/config.rs         Config from env vars
  src/lib.rs            Crate root
  src/main.rs           Binary entry point
  Cargo.toml            Workspace root for the crate
```

---

## Build, test, lint

`build.rs` compiles the proto via `tonic_prost_build`, which requires `protoc` on PATH. Install the
protobuf compiler before building (`apt-get install protobuf-compiler` on Debian or `brew install
protobuf` on macOS). Without it `cargo build`/`clippy`/`test` fail in the build script.

```bash
cd rust/search-api
cargo fmt                         # format
cargo clippy --locked -- -D warnings  # lint (must be clean)
cargo build --locked               # compile
cargo test --locked                # unit tests
```

The Redis cache-backend integration tests (`tests/redis_cache.rs`) spawn a throwaway local
`redis-server` per test and self-skip with a message when the binary is not installed, so
`cargo test` stays green without Redis. Install `redis-server` to run them unskipped.

---

## Lance-crate version-bump coupling

The lance crates are sourced from crates.io (`lance = "8.0.0"` and friends in `Cargo.toml`). Bump
them together with three coupled things:

1. The `pylance` dependency pin in the Python project (`../../pyproject.toml`).
2. `LANCE_CACHE_STAMP` in `src/cache/layout.rs`. It is baked into the on-disk stamp directory name,
   because the cache codec format is unstable between lance releases. Bumping it makes
   `prepare_cache_root` wipe any sibling stamp directory on next startup.
3. The same stamp namespaces every Redis cache key, so after a bump the old generation of keys
   simply ages out through its TTLs rather than being actively purged (ADR 0031,
   `../../docs/adr/caching-and-observability.md`).

---

## Dataset URIs are never reused (operational requirement)

The persistent caches (and Lance's own session caches plus the open-handle LRU) key immutable
metadata by `(store prefix, object path)` with no etag or generation binding. A dataset that is
dropped and recreated at the same `{org}/{tenant}/{namespace}` URI restarts version numbering, so
its new `_versions/1.manifest` collides with the cached manifest of the dead dataset and a local
search process can serve phantom fragments for up to the 7-day cache TTL. Do not build or propose
flows that delete a dataset and recreate it at the same URI. Replace a dataset by writing the
successor under a new `namespace` segment and flipping traffic to it. If a URI absolutely must be
reused, every local cache generation has to be wiped first. Clear `SEARCH_API_CACHE_DIR` or flush
the Redis namespace. See the matching invariant in `README.md`.

---

## Filter-AST rule (hard rule 7 detail)

`domain::filter::Filter` is a typed AST (`Compare`, `InList`, `IsNull`, `IsNotNull`, `Between`,
`And`, `Or`, `Not`). Column names are validated against the dataset schema at translation time and
the identifier allowlist `[A-Za-z_][A-Za-z0-9_]*`. Literals become typed DataFusion `lit`
expressions via `lance::filter::filter_to_expr`. Do not accept, construct, or pass a raw SQL string
anywhere in the `grpc` or `domain` layers. The AST also has a stable serde JSON shape (documented
in `domain/filter.rs`) because the same JSON is written into the `recall.filter` span attribute and
parsed by the Python recall audit job. Field names and enum tagging are a cross-language contract,
not just an internal detail.

---

## Telemetry conventions (Rust)

- The service emits `search_api.*` metrics via the typed `Metrics` facade in
  `src/telemetry/metrics.rs`, tagged with small closed enums (`Rpc`, `CacheName`, `Tier`) rather
  than free-form strings.
- All metric emitters are infallible. An unreachable Datadog Agent never panics and never fails a
  request. The same degrade-not-fail rule applies to the Redis `EntryStore`: a Redis round-trip
  error becomes a cache miss or dropped write, counted by `cache.backend_errors`, never a failed
  search.
- Per-query-leg spans (`lance.vector_query` / `lance.text_query`) carry `object_store.*` attributes
  sourced from Lance execution-stats events: `object_store.requests`, `object_store.iops`,
  `object_store.bytes_read`, `object_store.parts_loaded`, `object_store.indices_loaded`. These are
  aggregate counts only (no per-method GET/HEAD/LIST split, as Lance does not expose that in
  production builds). They stay low cardinality: no org, tenant, or version identifier is attached
  to span attributes or metric tags.

---

## Proto surface notes

The `README.md` in this directory lists the RPCs and their purposes and the full environment-
variable table. These finer-grained normative facts are recorded here so they are not lost.

- **TimeRange windowing.** `VectorSearch`, `TextSearch`, and `HybridSearch` accept an optional
  `TimeRange { optional int64 start_ms; optional int64 end_ms }` (epoch milliseconds, start
  inclusive, end exclusive, either bound optional). The window always applies to the fixed
  event-timestamp column (`event_timestamp`) and is translated to a typed range predicate ANDed
  with any `Filter`, pruned by a BTREE or zone-map on that column. A `TimeRange` absent from the
  request leaves every search path behaving exactly as before.
- **HybridSearch request-level filter.** `HybridSearch` accepts a request-level typed `filter`
  (field 8) that is ANDed into both legs through server-owned prefilter policy. Absent means only
  the mandatory live-row predicate applies.
- **Fusion.** Public `HybridSearch` accepts only the closed product modes `BALANCED`,
  `SEMANTIC_PRIORITY`, and `LEXICAL_PRIORITY`. The service maps them to fixed code-owned fusion
  policy. It never accepts raw weights or reciprocal-rank constants.
- **Serving resolution.** Public search requests carry only `DatasetTarget`. The server resolves it
  through `ServingCatalog` by joining the PostgreSQL dataset registry, active publication pointer,
  and immutable publication row. The result contains only an allowlisted URI and exact committed
  version. URI, version, tag, prewarm, and IVF-cluster inspection are not public search surfaces.
  The publication retains its immutable dataset specification revision for audit.
- **Local transport.** Set `SEARCH_API_LOCAL_MODE=true` for this repository's supported runtime. It
  binds search and health to loopback, permits only a loopback PostgreSQL URL, and disables TLS and
  JWT checks. Fixed port 8081 exposes only standard gRPC health. It contains no search or
  administration methods.
