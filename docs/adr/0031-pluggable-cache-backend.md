# ADR 0031: Pluggable cache backend for the search service (disk | redis | memory)

Status: Accepted

Date: 2026-07-03

## Context

ADR 0007 gave the search service a persistent two-tier cache: a hybrid memory-plus-disk
`CacheBackend` injected into the shared Lance session for serialized index pages, and a
path-filtered `WrappingObjectStore` byte cache for immutable metadata (versioned manifests,
transactions, small index files). Both tiers persisted exclusively to local disk. That is the
right default for a stable node with an attached volume, but it makes every replica warm its
own cache from the object store. On ephemeral nodes (spot instances, autoscaled pods) the disk
cache dies with the node, and a fleet of N replicas pays N cold-open storms after every deploy.

The operator asked for the choice of a cache backend, with Redis as the alternative: one shared
warm cache that survives any single replica and is shared by all of them.

## Decision

### One seam below the compositions, not two parallel implementations

The cache semantics — the Moka memory hot tier with promotion, single-flight `get_or_insert`,
the LEC2 blake3-checksummed value framing, the never-cache invariants for
`_latest.manifest` and `latest_version_hint.json`, conditional-read pass-through, and the
corrupt-entry purge — are backend-independent and stay written once. We extract only the
persistence beneath them into a local async trait, `EntryStore` (`src/cache/entry_store.rs`),
keyed by `(dir, file)` where the dir is the invalidation unit:

- the index tier uses a hashed cache-key prefix as the dir and one file per
  `(key, type_name)` pair,
- the store tier uses a hashed `(store_prefix, location)` pair as the dir holding a
  `meta.json` sidecar plus one file per request shape.

`DiskIndexCacheBackend` became `HybridIndexCacheBackend` (`src/cache/index_cache.rs`)
composing over any `EntryStore`. `MetadataByteCache` composes the same way. Two stores
implement the seam: `DiskEntryStore` (`src/cache/disk_store.rs`) and `RedisEntryStore`
(`src/cache/redis_store.rs`). The disk store keeps the exact pre-seam file layout
(`{root}/{dir}/{file}`, the `prefixes.json` registry sidecar, mtime recency), so existing
on-disk caches survive the refactor and `CACHE_SCHEMA_VERSION` stays at 2.

The alternative — writing a second full `CacheBackend` and a second full `ObjectStore`
implementation for Redis — was rejected because it would duplicate several hundred lines of
subtle, tested composition logic (the single-flight strong-count invariant alone earns its
docstring) and every future fix would need applying twice.

### Redis key layout: one HASH per dir

Each dir is ONE Redis HASH, `{namespace}:{stamp}:{tier}:{dir}`, whose fields are the entry
file names. This choice keeps all three cache-management operations at the granularity the
tiers already use:

- dir invalidation (dataset purge, corrupt object) is a single `DEL`,
- the store tier's entry-plus-sidecar read is one `HMGET` round trip,
- server-side eviction under `maxmemory-policy allkeys-lru` removes whole objects, matching
  the disk sweep's behavior.

Flat one-key-per-entry layouts were rejected: dir invalidation would need `SCAN MATCH` over
the keyspace or per-dir member sets maintained on every write.

The `{stamp}` segment reuses `stamp_dir_name()` (`v{CACHE_SCHEMA_VERSION}-lance-{version}`),
so a lance upgrade or a frame-format change segregates keys the same way it swaps disk
directories. Old-generation keys need no wipe: they expire through their TTLs. Values keep the
LEC2 blake3 frame, which now also guards corruption in transit or inside Redis itself.

### TTL, capacity, and the janitor

Redis expiry is native: every put and every hit refreshes a per-dir `EXPIRE` with the same
7-day constant the disk sweep uses (`DEFAULT_DISK_CACHE_TTL_SECS`). Per-field hash TTLs were
rejected because `HEXPIRE` requires Redis 7.4 and a dir's entries have correlated lifetimes
anyway. Capacity is the Redis server's `maxmemory` with `allkeys-lru` recommended. The
`CacheJanitor` therefore stays disk-only, and the provider builds it only for the disk
backend.

One Redis-specific hygiene concern remains. The index tier's prefix registry (raw prefix to
dir hash, needed so string-prefix invalidation can find its dirs) is a HASH with NO TTL,
because expiring it would silently break `invalidate_prefix` — a correctness path. Since index
UUIDs churn on every rebuild, `RedisEntryStore` runs an hourly hygiene pass (`HSCAN` plus
pipelined `EXISTS`) that deletes registry rows whose dir key has expired or been evicted. As a
side benefit over disk, the registry is shared: an invalidation on one replica also covers
dirs written by sibling replicas, which the disk backend's process-local sidecar cannot do.

### Selection, failure, and observability

`SEARCH_API_CACHE_BACKEND` selects `disk` (default), `redis`, or `memory`. The old
`SEARCH_API_DISK_CACHE_DISABLED=true` is honored as a deprecated alias for `memory`. The
`redis` backend requires `SEARCH_API_REDIS_URL` (`redis://` or `rediss://`, TLS via rustls)
and namespaces keys under `SEARCH_API_REDIS_NAMESPACE` (default `search-api`).

Failure semantics mirror the disk backend's: an unreachable Redis at startup (2 s connect
timeout) logs a warning and falls back to memory-only caching, and every per-call Redis error
degrades to a miss or a dropped write — never a failed search — counted by
`search_api.cache.backend_errors` tagged `cache` and `op`. Cache lookups against Redis are
tagged `tier:remote` beside the existing `memory` and `disk` tiers.

`approx_stats` for Redis reports the bytes and entries written by the local process since
start, a deliberately cheap approximation: the authoritative residency bound is the server's
`maxmemory`, and exact figures would cost a keyspace scan per stats call.

Provider construction became `async` to allow the startup reachability check, which is the
price of having a real fallback instead of a lazily discovered dead backend.

## Consequences

- A replica fleet can share one warm cache: a prewarm on any replica serves cold opens on all
  of them, and rolling deploys stop paying per-replica cold-open storms.
- The disk backend's on-disk format is unchanged, so existing caches survive the refactor with
  no schema bump.
- Redis capacity must be sized by the operator (`maxmemory` plus `allkeys-lru`). The service
  cannot enforce a byte budget remotely and does not try.
- A future backend (memcached, S3 express, a distributed KV) implements one trait with eleven
  small methods instead of two Lance-facing traits.
- The integration tests spawn a throwaway local `redis-server` per test and self-skip when the
  binary is absent, keeping the default `cargo test` green on machines without Redis.
