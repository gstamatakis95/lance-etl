# `search-api`

`search-api` is the optional local read process for Lance datasets published by `lance-etl`. It is
a tonic gRPC service. It never writes source rows, runs Spark, builds indexes, or chooses a mutable
Lance version.

Every request carries one logical `DatasetTarget` with `tenant_id`, `namespace`, and `org_id`. The
service joins `datasets`, `dataset_state`, and `dataset_publications` in PostgreSQL and opens the
active publication's allowlisted URI at its exact committed version. Clients cannot supply a URI,
version, tag, index name, raw SQL predicate, ANN execution knob, or fusion weight.

## RPCs

| RPC | Purpose |
|---|---|
| `SearchService/VectorSearch` | Nearest-neighbor search with a typed filter and optional event-time window |
| `SearchService/TextSearch` | BM25 full-text search with a typed filter and optional event-time window |
| `SearchService/HybridSearch` | Vector and text search fused by a closed code-owned mode |

There is no cross-dataset or cross-organization query path.

## Serving catalog

The PostgreSQL read is:

```text
datasets
  -> dataset_state.active_publication_id
  -> dataset_publications.lance_uri + lance_version
```

Only active datasets with an active publication resolve. The publication retains the immutable
dataset specification revision and source snapshot used to create it. The serving route exposes
only URI and version to the Lance provider.

The provider rejects catalog URIs outside `LANCE_ETL_BASE_URI`. An in-flight request stays pinned to
the exact version it resolved even if another publication becomes active concurrently.

## Architecture

| Layer | Path | Responsibility |
|---|---|---|
| Domain | `src/domain/` | Engine-neutral routes, typed filters, queries, results, and traits |
| Cache | `src/cache/` | Memory plus disk, Redis, or memory-only index and metadata caching |
| Lance | `src/lance/` | Exact-version dataset opens and vector, text, hybrid, prewarm, and cluster operations |
| Catalog | `src/catalog/` | PostgreSQL logical-route resolution |
| gRPC | `src/grpc/` | Protobuf conversion and tonic transport |
| Telemetry | `src/telemetry/` | Datadog metrics, spans, logs, and recall sampling |

The `domain` layer never imports Lance or tonic. The gRPC layer never constructs raw engine
predicates. Cache storage never depends on dataset or query domain types.

## Query invariants

### Typed filters only

`Filter` is a typed AST containing compare, list, null, range, Boolean, and negation nodes. Column
names are validated against the opened dataset schema and the identifier allowlist. Literals become
typed DataFusion expressions. Raw SQL is never accepted or constructed.

### One dataset per request

Each route component is validated against `[A-Za-z0-9_-]+`. A request cannot fan out to another
dataset or organization. Hybrid fusion combines two result legs from the same exact version.

### Exact publication version

Requests cannot select a version. The catalog active pointer is authoritative. Mutable Lance tags
are not used for serving resolution.

### URI reuse is prohibited

Lance session caches and the persistent cache treat version manifests and index pages as immutable
under their URI. Deleting a dataset and creating unrelated content at the same URI can expose stale
cached bytes. Create the successor under a fresh URI. If reuse is unavoidable, stop the local
process and clear its entire cache generation first.

### No stable row IDs

The service inherits the repository-wide rejection of move-stable row IDs. It must not expose a
physical row identifier as a public compatibility contract.

## Local configuration

Use explicit local mode. It binds search and health listeners to loopback, allows a loopback
PostgreSQL connection without TLS, and disables bearer-token checks.

| Variable | Default | Purpose |
|---|---|---|
| `SEARCH_API_LOCAL_MODE` | `false` | Set to `true` for the supported local runtime |
| `LANCE_ETL_BASE_URI` | required | Allowlisted Lance storage namespace |
| `LANCE_ETL_DATABASE_URL` | required | Loopback PostgreSQL catalog URL |
| `SEARCH_API_PORT` | `8080` | Local search port |
| `SEARCH_API_CACHE_BACKEND` | `disk` | `disk`, `redis`, or `memory` |
| `SEARCH_API_CACHE_DIR` | `/tmp/rust-search/cache` | Local disk cache root |
| `SEARCH_API_REDIS_URL` | none | Redis URL when the Redis cache is selected |
| `SEARCH_API_REDIS_NAMESPACE` | `search-api` | Prefix for Redis keys |
| `SEARCH_API_STATSD_ADDR` | `127.0.0.1:8125` | DogStatsD address |
| `SEARCH_API_TELEMETRY_DISABLED` | `false` | Disable trace and metric export |

Query limits, IO concurrency, request deadlines, cache budgets, ANN probe range, refine factor,
event-time column, and recall sample rate are code-owned constants in `src/config.rs`.

## Run locally

Building requires `protoc` on `PATH`.

```bash
brew install protobuf
cargo build --locked
```

Start the service against the same local database and Lance namespace as the reconciler:

```bash
SEARCH_API_LOCAL_MODE=true \
LANCE_ETL_BASE_URI="$PWD/../../.lance-etl/lance" \
LANCE_ETL_DATABASE_URL='postgresql://lance_etl:lance_etl@localhost/lance_etl' \
SEARCH_API_TELEMETRY_DISABLED=true \
cargo run --locked
```

Search listens on `127.0.0.1:8080`. Standard gRPC health listens on `127.0.0.1:8081`. The health
port exposes no search or administration methods.

## Cache model

The service caches index structures and immutable metadata only. Raw row data is not persisted in
the cache layer. A Moka hot tier sits over a selectable persistent entry store:

- disk is the local default
- memory disables persistence
- Redis is optional for cache behavior experiments

`LANCE_CACHE_STAMP` namespaces the cache generation by Lance version. Bump it with every Lance crate
and Python `pylance` version change.

## Observability

Metrics use the `search_api.*` prefix and closed tag enums. Emitters are infallible, so an absent
local Datadog Agent cannot fail a request. Query spans record aggregate object-store request, IO,
byte, part, and index counts without route or version tags.

Recall sampling writes the stable typed query and result contract to span attributes for the Python
offline recall job. The default sample rate is zero.

## Build and test

```bash
cargo fmt
cargo clippy --locked -- -D warnings
cargo build --locked
cargo test --locked
```

Redis integration tests spawn a disposable local `redis-server` when it is installed and otherwise
self-skip.
