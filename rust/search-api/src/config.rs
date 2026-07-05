//! Runtime configuration sourced from environment variables.

use std::path::PathBuf;
use std::str::FromStr;

/// Default index cache budget in bytes (1 GiB).
pub const DEFAULT_INDEX_CACHE_BYTES: usize = 1024 * 1024 * 1024;

/// Default metadata cache budget in bytes (256 MiB).
pub const DEFAULT_METADATA_CACHE_BYTES: usize = 256 * 1024 * 1024;

/// Default weighted capacity of the open-dataset-handle LRU.
///
/// The handle cache is weighted by a cheap per-handle proxy (open fragment count, clamped to
/// [`crate::lance::provider::MAX_HANDLE_WEIGHT`]) rather than a flat entry count, so a tiny tenant
/// handle costs one unit while a whale handle costs at most `MAX_HANDLE_WEIGHT` units. This budget
/// is therefore "total resident handle weight", not a handle count.
///
/// Sized for a 30 000-tenant fleet whose load is a power-law tail of cheap tiny handles. A tiny
/// handle holds only the manifest fragment list, the schema, and the shared session `Arc` — on the
/// order of a few KiB resident. At one weight unit each, 16 384 units keeps over half the fleet's
/// tiny handles resident (covering a realistic active working set many times over) for well under
/// ~200 MiB of handle memory, which sits comfortably beside the 1 GiB index and 256 MiB metadata
/// budgets. Because whale handles are weight-clamped, a burst of whale opens can consume at most
/// `16384 / MAX_HANDLE_WEIGHT` units of the budget, so they can never evict the entire tiny tail.
/// Raising the old flat 1024-handle cap to 16 384 weighted units directly removes the cold-open
/// churn the tail paid against the previous count cap. Env: `SEARCH_API_DATASET_CACHE_CAPACITY`.
pub const DEFAULT_DATASET_CACHE_CAPACITY: u64 = 16384;

/// Default TCP port for the gRPC server.
pub const DEFAULT_PORT: u16 = 8080;

/// Default root directory for the persistent disk caches.
pub const DEFAULT_CACHE_DIR: &str = "/tmp/rust-search/cache";

/// Default Redis key namespace for the `redis` cache backend.
pub const DEFAULT_REDIS_NAMESPACE: &str = "search-api";

/// Fixed interval in seconds between Redis prefix-registry hygiene passes.
///
/// The registry hash maps raw cache-key prefixes to their dir keys and carries no TTL (expiring
/// it would silently break prefix invalidation), so a background pass drops rows whose dir key
/// has since expired or been evicted. Hardcoded: an hourly cadence is universal, not an env knob.
pub const REDIS_REGISTRY_HYGIENE_SECS: u64 = 3600;

/// Which persistent backend the two cache tiers use.
///
/// `Disk` is the default two-tier layout (memory hot tier plus local files under `cache_dir`).
/// `Redis` keeps the same memory hot tier but persists entries in a shared Redis server, so
/// replicas on ephemeral nodes share one warm cache. `Memory` disables persistence entirely and
/// serves both tiers from the in-process caches alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheBackendKind {
    /// Local-disk persistence under `cache_dir` (the default).
    Disk,
    /// Shared Redis persistence at `redis_url`.
    Redis,
    /// No persistence: in-memory caches only.
    Memory,
}

impl FromStr for CacheBackendKind {
    type Err = String;

    /// Parses a backend name case-insensitively: `disk`, `redis`, or `memory`.
    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.to_ascii_lowercase().as_str() {
            "disk" => Ok(Self::Disk),
            "redis" => Ok(Self::Redis),
            "memory" => Ok(Self::Memory),
            _ => Err(format!("expected one of disk, redis, memory, got {raw:?}")),
        }
    }
}

/// Default disk budget for the serialized index cache tier (8 GiB).
pub const DEFAULT_DISK_INDEX_CACHE_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Default disk budget for the metadata byte cache (2 GiB).
pub const DEFAULT_DISK_STORE_CACHE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Fixed TTL for disk cache entries (7 days).
///
/// Hardcoded: a 7-day TTL is universal across deployments, so this is no longer an env knob.
pub const DEFAULT_DISK_CACHE_TTL_SECS: u64 = 7 * 24 * 60 * 60;

/// Fixed largest single byte-range under `_indices/` stored by the byte cache (4 MiB).
///
/// Hardcoded: this is index page-size tuning that no deployment varies, so it is no longer an env
/// knob.
pub const DEFAULT_STORE_CACHE_MAX_RANGE_BYTES: u64 = 4 * 1024 * 1024;

/// Fixed janitor sweep interval in seconds.
///
/// Hardcoded: the 300 s sweep cadence is an internal maintenance constant, no longer an env knob.
pub const DEFAULT_DISK_CACHE_SWEEP_SECS: u64 = 300;

/// Default number of indexes prewarmed concurrently per Prewarm RPC.
pub const DEFAULT_PREWARM_CONCURRENCY: usize = 4;

/// Fixed logical id column captured for recall scoring.
///
/// Hardcoded: matches the standardized ETL and recall schema, so it is no longer an env knob.
pub const DEFAULT_ID_COLUMN: &str = "vector_id";

/// Default event-timestamp column a search time range is applied to.
///
/// The canonical ETL event clock. A request time range always filters this column. Override with
/// `SEARCH_API_EVENT_TIMESTAMP_COLUMN` when a deployment names it differently.
pub const DEFAULT_EVENT_TIMESTAMP_COLUMN: &str = "event_timestamp";

/// Default DogStatsD address when neither `SEARCH_API_STATSD_ADDR` nor `DD_AGENT_HOST` is set.
pub const DEFAULT_STATSD_ADDR: &str = "127.0.0.1:8125";

/// Default recall sample rate (0.0 disables sampled-query recall capture).
pub const DEFAULT_RECALL_SAMPLE_RATE: f64 = 0.0;

/// Default IO concurrency (number of parallel in-flight object-store requests per dataset).
///
/// This feeds `LANCE_IO_THREADS`, which is read at every `ObjectStore::io_parallelism()` call.
/// Lance's cloud default is 64; 256 saturates typical 10 Gbit S3 links without hitting the
/// AIMD ~5 000 req/s/process ceiling because the parallelism drives concurrent range-reads,
/// not independent request opens.
pub const DEFAULT_IO_CONCURRENCY: usize = 256;

/// Default minimum number of IVF partitions probed per vector query.
///
/// Lance's own default is 1, which means a whale dataset with 4096+ IVF partitions probes a
/// single partition — correct for filter-heavy prefilter queries but disastrous for recall on
/// unfiltered or lightly filtered queries. Raising this to 8 gives a practical recall floor for
/// IVF_RQ datasets with up to ~512 partitions without meaningfully affecting flat-KNN latency on
/// the small-org tail (those datasets have no index and ignore this knob entirely).
pub const DEFAULT_MINIMUM_NPROBES: usize = 8;

/// Default maximum number of IVF partitions probed per vector query.
///
/// When a prefilter is highly selective, Lance will probe as many partitions as needed to satisfy
/// the filter — up to ALL partitions if the filter passes very few rows (scanner.rs ~1664-1674).
/// Setting a finite ceiling prevents a single whale query under a rare org-specific filter from
/// traversing the entire index, which would destroy p99 for every concurrent tenant. 32 gives
/// roughly 4x coverage over [`DEFAULT_MINIMUM_NPROBES`] and keeps worst-case probe latency well
/// under the 500 ms p99 target on 1B-row IVF_RQ datasets.
pub const DEFAULT_MAXIMUM_NPROBES: usize = 32;

/// Hard ceiling applied to any client-supplied nprobes / minimum_nprobes / maximum_nprobes.
///
/// Clients that self-tune their probe count can drift high during load testing and survive into
/// production configs. The ceiling guarantees that even an operator mistake or a misconfigured
/// client cannot submit a whale query that linearly scans every IVF partition. 64 is 2× the
/// [`DEFAULT_MAXIMUM_NPROBES`] and still fast enough (<200 ms) on a 1B-row IVF_RQ dataset over
/// S3 with warm caches.
pub const DEFAULT_NPROBES_CEILING: usize = 64;

/// Default refine factor: re-rank this many candidates per requested k with exact distances.
///
/// With 1-bit RaBitQ encoding (IVF_RQ 1-bit), the compressed distances used during partition
/// scan are coarse approximations. Fetching 2 × k candidates and re-ranking them with full
/// float32 vectors via `refine_factor = 2` recovers the recall lost to quantisation at modest
/// extra IO. Setting this to 0 in the env disables the default (the query then uses whatever
/// Lance chooses — currently `None`, which means no refinement).
pub const DEFAULT_REFINE_FACTOR: u32 = 2;

/// Default for whether the service applies `fast_search` when an index exists for the queried
/// column.
///
/// `fast_search = true` tells Lance to query only indexed fragments and skip fragments added after
/// the last index build, trading freshness for bounded latency. The unindexed tail is served stale
/// until the next nightly index run. On datasets with NO index at all (the small-org unindexed
/// tier), `fast_search` causes Lance to return an empty result immediately (scanner.rs:3804-3807
/// for vector, scanner.rs:~3527 for FTS), so this flag is guarded: it is only applied when the
/// dataset has at least one matching index for the queried column or columns. An explicit
/// per-request `fast_search` value — including `false` for read-after-write freshness — always
/// wins over this server default for both the vector and text legs.
pub const DEFAULT_FAST_SEARCH: bool = true;

/// Default request timeout in milliseconds applied to every incoming gRPC call.
///
/// 800 ms is chosen to be comfortably below the 1 s client-side deadline most callers use while
/// leaving 200 ms headroom for serialization and network overhead. Slow cold-opens or high-nprobes
/// whale queries that exceed this budget are cancelled rather than allowed to pile up and exhaust
/// the runtime's available concurrency. Env: `SEARCH_API_REQUEST_TIMEOUT_MS`.
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 800;

/// Default maximum concurrent streams (and connections) the gRPC server admits.
///
/// Caps the number of in-flight requests per connection at the tonic transport layer, protecting
/// the tokio runtime against queue saturation when a slow whale query stalls the executor pool.
/// 256 matches the per-process IO concurrency budget, which ensures the runtime is never asked to
/// do more in-flight work than it has IO threads to service. Env: `SEARCH_API_MAX_CONCURRENT_STREAMS`.
pub const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 256;

/// Default concurrency limit per gRPC connection.
///
/// Applied via tonic's `concurrency_limit_per_connection` builder. Works with
/// `http2_max_concurrent_streams` at the H2 frame level to provide back-pressure at the service
/// layer: once this many requests are in flight on a single connection, new ones are queued at
/// the tower layer rather than spawned immediately.
pub const DEFAULT_CONCURRENCY_LIMIT_PER_CONNECTION: usize = 256;

/// Path to the startup prewarm targets file (empty = disabled).
///
/// When set, the service reads this file on startup and prewarms each listed dataset in a
/// background task before it would otherwise be opened cold by a live request. This eliminates
/// the cold-open penalty for designated whale datasets on every rolling deploy. The file format
/// is one target per line: `{org_id}/{tenant_id}/{namespace}` using the same path segments the
/// service resolves to `{base_uri}/{org_id}/{tenant_id}/{namespace}.lance`. Blank lines and lines
/// with invalid segments are skipped with a warning. Errors per target are logged but never fatal.
/// Env: `SEARCH_API_PREWARM_TARGETS_PATH`.
pub const DEFAULT_PREWARM_TARGETS_PATH: &str = "";

/// Fixed minimum object-store request size in bytes (IO buffer / block size) — 256 KiB.
///
/// Passed as `ObjectStoreParams::block_size` when opening every dataset.  Larger values
/// reduce round-trip count for sequential scans at the cost of over-fetching for small
/// random reads.  256 KiB is a practical balance for S3 given typical index page sizes.
/// Hardcoded: no deployment varies it, so it is no longer an env knob.
pub const DEFAULT_IO_BLOCK_SIZE_BYTES: usize = 256 * 1024;

/// Default for whether serving resolves the configured serve tag instead of opening latest.
///
/// Off by default so the legacy latest-resolution behavior is preserved until an operator has
/// verified prewarm-by-version and is ready to cut serving over to tag-based blue-green.
pub const DEFAULT_SERVE_BY_TAG: bool = false;

/// Default serve tag resolved to a concrete version when serve-by-tag is enabled.
pub const DEFAULT_SERVE_TAG: &str = "HEAD";

/// Default TTL in seconds for trusting a resolved serve-tag version before re-reading the tag.
///
/// Bounds how long a tag flip can go unobserved by a replica. 10 s keeps the steady-state cost at
/// zero extra manifest reads per request while flips propagate within roughly the TTL.
pub const DEFAULT_SERVE_TAG_TTL_SECS: u64 = 10;

/// Fixed object-store retry-window timeout in seconds — 120 s.
///
/// This feeds `OBJECT_STORE_CLIENT_RETRY_TIMEOUT`, which is picked up by the S3/GCS/Azure
/// client builders.  It is the total wall-clock budget across all retries for one cloud
/// request, not a per-attempt connect timeout.  120 s is generous enough to survive S3
/// throttle back-offs without exceeding a reasonable P99 SLO.
/// Hardcoded: no deployment varies it, so it is no longer an env knob.
pub const DEFAULT_OBJECT_STORE_TIMEOUT_SECS: u64 = 120;

/// Runtime configuration for the search API.
#[derive(Debug, Clone)]
pub struct Config {
    /// Base URI under which all datasets live, e.g. `s3://bucket/lance`. Each dataset resolves to
    /// `{base}/{org_id}/{tenant_id}/{namespace}.lance`.
    pub base_uri: String,
    /// Weighted capacity of the open-`Dataset` handle LRU (total resident handle weight, not a
    /// flat count). Handles are weighed by clamped open fragment count, so many cheap tiny handles
    /// coexist while a few heavy whale handles are bounded. Env: `SEARCH_API_DATASET_CACHE_CAPACITY`.
    pub dataset_cache_capacity: u64,
    /// Byte budget for the Lance index cache.
    ///
    /// Dual role depending on the cache backend:
    /// - `disk` or `redis`: sizes the in-memory hot tier of the two-tier hybrid backend
    ///   (`HybridIndexCacheBackend`). The persistent tier is bounded separately: by
    ///   `disk_index_cache_bytes` on disk, or by the Redis server's `maxmemory` policy.
    ///   Hot entries evict from Moka under this budget while their serialised copies persist.
    /// - `memory`: sizes the Lance session's in-process Moka index cache directly
    ///   (the only index cache tier).
    ///
    /// In both cases this budget covers IVF centroid pages, RaBitQ codebook pages, and HNSW
    /// graph pages — the data structures that dominate search latency on cold opens.
    /// Env: `SEARCH_API_INDEX_CACHE_BYTES`.
    pub index_cache_bytes: usize,
    /// Byte budget for the shared session metadata cache.
    pub metadata_cache_bytes: usize,
    /// TCP port the gRPC server binds to.
    pub port: u16,
    /// Root directory for the `disk` backend's caches. Default `/tmp/rust-search/cache`.
    /// Env: `SEARCH_API_CACHE_DIR`.
    pub cache_dir: PathBuf,
    /// Disk budget in bytes for the serialized index cache tier (default 8 GiB).
    /// Env: `SEARCH_API_DISK_INDEX_CACHE_BYTES`.
    pub disk_index_cache_bytes: u64,
    /// Disk budget in bytes for the metadata byte cache (default 2 GiB). Env: `SEARCH_API_DISK_STORE_CACHE_BYTES`.
    pub disk_store_cache_bytes: u64,
    /// Which persistent backend the cache tiers use (default `Disk`). Env: `SEARCH_API_CACHE_BACKEND`
    /// (`disk`, `redis`, or `memory`). The deprecated `SEARCH_API_DISK_CACHE_DISABLED=true` is
    /// honored as an alias for `memory` when `SEARCH_API_CACHE_BACKEND` is unset.
    pub cache_backend: CacheBackendKind,
    /// Redis connection URL (`redis://` or `rediss://`), required when the backend is `redis`.
    /// Env: `SEARCH_API_REDIS_URL`.
    pub redis_url: Option<String>,
    /// Key namespace prepended to every Redis cache key (default `search-api`), letting multiple
    /// services or environments share one Redis server. Env: `SEARCH_API_REDIS_NAMESPACE`.
    pub redis_namespace: String,
    /// Max indexes prewarmed concurrently per Prewarm RPC (default 4). Env: `SEARCH_API_PREWARM_CONCURRENCY`.
    pub prewarm_concurrency: usize,
    /// DogStatsD (UDP) address metrics are sent to. Defaults to `{DD_AGENT_HOST}:8125` when
    /// `DD_AGENT_HOST` is set, else `127.0.0.1:8125`. Env: `SEARCH_API_STATSD_ADDR`.
    pub statsd_addr: String,
    /// Disables trace export and DogStatsD entirely (tests / local runs keep JSON logs only).
    /// Env: `SEARCH_API_TELEMETRY_DISABLED`.
    pub telemetry_disabled: bool,
    /// Fraction of eligible VectorSearch requests whose query and served results are captured as
    /// `recall.*` span attributes for offline recall scoring. Must lie in `[0, 1]`. Default 0.0
    /// (disabled). Env: `SEARCH_API_RECALL_SAMPLE_RATE`.
    pub recall_sample_rate: f64,
    /// Number of parallel in-flight object-store requests per dataset (default 256).
    ///
    /// Written to the process-global `LANCE_IO_THREADS` env var at startup, which Lance reads at
    /// every `ObjectStore::io_parallelism()` call.  Higher values increase S3 throughput up to the
    /// AIMD ~5 000 req/s/process ceiling; beyond that the AIMD throttle layer absorbs excess.
    /// Env: `SEARCH_API_IO_CONCURRENCY`.
    pub io_concurrency: usize,
    /// Whether serving resolves the configured serve tag to a concrete version instead of opening
    /// the latest committed version (default false). When on, the provider keys its caches on the
    /// resolved version so blue and green coexist and a tag flip is observed within the serve-tag
    /// TTL. Env: `SEARCH_API_SERVE_BY_TAG`.
    pub serve_by_tag: bool,
    /// Tag serving resolves to a committed version when `serve_by_tag` is on (default `HEAD`).
    /// Env: `SEARCH_API_SERVE_TAG`.
    pub serve_tag: String,
    /// Column a request time range is applied to (default `event_timestamp`). A vector, text, or
    /// hybrid request time range is translated into a typed range predicate on this column and
    /// ANDed with any caller-provided filter. Env: `SEARCH_API_EVENT_TIMESTAMP_COLUMN`.
    pub event_timestamp_column: String,
    /// Seconds a resolved serve-tag version is trusted before the tag JSON is re-read (default
    /// 10). Bounds how long a tag flip can go unobserved by a replica while keeping the
    /// steady-state per-request cost at zero extra manifest reads. Env:
    /// `SEARCH_API_SERVE_TAG_TTL_SECS`.
    pub serve_tag_ttl_secs: u64,
    /// Minimum number of IVF partitions probed when the request does not specify `nprobes` (default 8).
    ///
    /// Lance's own default is 1, which delivers broken recall on whale datasets with many
    /// partitions. This floor ensures every untuned request probes at least enough partitions for
    /// useful recall. Env: `SEARCH_API_DEFAULT_MINIMUM_NPROBES`.
    pub default_minimum_nprobes: usize,
    /// Maximum number of IVF partitions probed when the request does not specify `maximum_nprobes`
    /// (default 32). Also applied as a ceiling on any client-supplied `maximum_nprobes` when it
    /// exceeds `nprobes_ceiling`. When a prefilter is selective, Lance would otherwise probe every
    /// partition — see `scanner.rs` ~1664-1674 — which is unbounded latency on whale datasets.
    /// Env: `SEARCH_API_DEFAULT_MAXIMUM_NPROBES`.
    pub default_maximum_nprobes: usize,
    /// Hard ceiling applied to any client-supplied nprobes / minimum_nprobes / maximum_nprobes
    /// (default 64). Prevents a misconfigured client or operator from submitting a whale query
    /// that linearly scans every IVF partition, destroying p99 for all concurrent tenants.
    /// Env: `SEARCH_API_NPROBES_CEILING`.
    pub nprobes_ceiling: usize,
    /// Refine factor applied when the request leaves `refine_factor` unset (default 2).
    ///
    /// With 1-bit RaBitQ quantisation, compressed distances are coarse. Fetching `k * refine_factor`
    /// candidates and re-ranking with exact float32 vectors recovers quantisation loss at modest
    /// extra IO. Set to 0 to disable the default and let Lance choose (no refinement). Requests that
    /// explicitly set `refine_factor` always win. Env: `SEARCH_API_DEFAULT_REFINE_FACTOR`.
    pub default_refine_factor: u32,
    /// Whether the service applies `fast_search` by default when the dataset has a matching index
    /// for the queried column (default true).
    ///
    /// When on, indexed fragments are returned immediately and fragments added after the last index
    /// build are skipped until the next index run (index-only freshness). The default is guarded:
    /// it fires only when the dataset has a vector index (for the vector leg) or an FTS index (for
    /// the text leg) covering the queried column — so unindexed datasets on the small-org tier are
    /// unaffected and flat scans work normally. An explicit per-request `fast_search` value —
    /// including `false` for read-after-write freshness — always wins over this default.
    /// Env: `SEARCH_API_FAST_SEARCH_DEFAULT`.
    pub fast_search_default: bool,
    /// Wall-clock timeout in milliseconds applied to every incoming gRPC call (default 800).
    ///
    /// Requests that exceed this budget are cancelled with DEADLINE_EXCEEDED, freeing executor
    /// capacity for the next request. Sized below a typical 1 s client-side deadline to ensure
    /// the server cancels before the client times out, avoiding orphan work. Set to 0 to disable
    /// the server-side timeout entirely. Env: `SEARCH_API_REQUEST_TIMEOUT_MS`.
    pub request_timeout_ms: u64,
    /// Maximum number of concurrent HTTP/2 streams admitted per connection (default 256).
    ///
    /// Sent to clients in the HTTP/2 SETTINGS frame. Combined with
    /// `concurrency_limit_per_connection`, this provides two layers of back-pressure at the tonic
    /// transport and tower service layers. Env: `SEARCH_API_MAX_CONCURRENT_STREAMS`.
    pub max_concurrent_streams: u32,
    /// Maximum number of in-flight requests the tower layer admits per connection (default 256).
    ///
    /// Requests that exceed this limit are queued at the tower layer rather than immediately
    /// spawned onto the runtime, protecting the executor thread pool from oversubscription during
    /// burst traffic. Env: `SEARCH_API_CONCURRENCY_LIMIT_PER_CONNECTION`.
    pub concurrency_limit_per_connection: usize,
    /// Path to the startup prewarm targets file (empty = disabled).
    ///
    /// File format: one target per line as `{org_id}/{tenant_id}/{namespace}`. The service reads
    /// this file on startup and prewarms each dataset in a background task, eliminating cold-open
    /// latency for designated whale datasets on rolling deploys. Blank lines and malformed segments
    /// are skipped with a warning. Per-target errors are logged but never fatal to startup.
    /// Env: `SEARCH_API_PREWARM_TARGETS_PATH`.
    pub prewarm_targets_path: Option<std::path::PathBuf>,
}

impl Config {
    /// Builds a configuration from environment variables.
    ///
    /// `LANCE_ETL_BASE_URI` is required: the base URI all dataset paths are resolved under
    /// (a trailing slash is stripped). Optional overrides: `SEARCH_API_DATASET_CACHE_CAPACITY`,
    /// `SEARCH_API_INDEX_CACHE_BYTES`, `SEARCH_API_METADATA_CACHE_BYTES`, `SEARCH_API_PORT`,
    /// `SEARCH_API_CACHE_DIR`, `SEARCH_API_DISK_INDEX_CACHE_BYTES`,
    /// `SEARCH_API_DISK_STORE_CACHE_BYTES`, `SEARCH_API_CACHE_BACKEND` (`disk`, `redis`, or
    /// `memory`, with `SEARCH_API_DISK_CACHE_DISABLED=true` honored as a deprecated alias for
    /// `memory`), `SEARCH_API_REDIS_URL` (required for the `redis` backend),
    /// `SEARCH_API_REDIS_NAMESPACE` (default `search-api`),
    /// `SEARCH_API_PREWARM_CONCURRENCY`, `SEARCH_API_STATSD_ADDR`
    /// (default honors `DD_AGENT_HOST`), `SEARCH_API_TELEMETRY_DISABLED`,
    /// `SEARCH_API_RECALL_SAMPLE_RATE` (must lie in `[0, 1]`),
    /// `SEARCH_API_IO_CONCURRENCY` (default 256),
    /// `SEARCH_API_SERVE_BY_TAG` (default false), `SEARCH_API_SERVE_TAG` (default `HEAD`),
    /// `SEARCH_API_SERVE_TAG_TTL_SECS` (default 10),
    /// `SEARCH_API_EVENT_TIMESTAMP_COLUMN` (default `event_timestamp`),
    /// `SEARCH_API_DEFAULT_MINIMUM_NPROBES` (default 8),
    /// `SEARCH_API_DEFAULT_MAXIMUM_NPROBES` (default 32),
    /// `SEARCH_API_NPROBES_CEILING` (default 64),
    /// `SEARCH_API_DEFAULT_REFINE_FACTOR` (default 2, set 0 to disable),
    /// `SEARCH_API_FAST_SEARCH_DEFAULT` (default true),
    /// `SEARCH_API_REQUEST_TIMEOUT_MS` (default 800, set 0 to disable),
    /// `SEARCH_API_MAX_CONCURRENT_STREAMS` (default 256),
    /// `SEARCH_API_CONCURRENCY_LIMIT_PER_CONNECTION` (default 256), and
    /// `SEARCH_API_PREWARM_TARGETS_PATH` (default empty, disabled).
    ///
    /// The disk cache TTL, byte-cache max range, janitor sweep interval, IO block size,
    /// object-store retry timeout, and the recall id column are fixed constants (see
    /// [`DEFAULT_DISK_CACHE_TTL_SECS`] and siblings) and are no longer env-configurable.
    pub fn from_env() -> Result<Self, String> {
        let base_uri = std::env::var("LANCE_ETL_BASE_URI")
            .map_err(|_| "LANCE_ETL_BASE_URI must be set".to_string())?
            .trim_end_matches('/')
            .to_string();
        if base_uri.is_empty() {
            return Err("LANCE_ETL_BASE_URI must be a non-empty base URI".to_string());
        }
        let cache_backend = env_cache_backend()?;
        let redis_url = std::env::var("SEARCH_API_REDIS_URL").ok().filter(|url| !url.is_empty());
        if cache_backend == CacheBackendKind::Redis && redis_url.is_none() {
            return Err("SEARCH_API_REDIS_URL must be set when SEARCH_API_CACHE_BACKEND=redis".to_string());
        }
        Ok(Self {
            base_uri,
            dataset_cache_capacity: env_number("SEARCH_API_DATASET_CACHE_CAPACITY", DEFAULT_DATASET_CACHE_CAPACITY)?,
            index_cache_bytes: env_number("SEARCH_API_INDEX_CACHE_BYTES", DEFAULT_INDEX_CACHE_BYTES)?,
            metadata_cache_bytes: env_number("SEARCH_API_METADATA_CACHE_BYTES", DEFAULT_METADATA_CACHE_BYTES)?,
            port: env_number("SEARCH_API_PORT", DEFAULT_PORT)?,
            cache_dir: PathBuf::from(env_string("SEARCH_API_CACHE_DIR", DEFAULT_CACHE_DIR)),
            disk_index_cache_bytes: env_number("SEARCH_API_DISK_INDEX_CACHE_BYTES", DEFAULT_DISK_INDEX_CACHE_BYTES)?,
            disk_store_cache_bytes: env_number("SEARCH_API_DISK_STORE_CACHE_BYTES", DEFAULT_DISK_STORE_CACHE_BYTES)?,
            cache_backend,
            redis_url,
            redis_namespace: env_string("SEARCH_API_REDIS_NAMESPACE", DEFAULT_REDIS_NAMESPACE),
            prewarm_concurrency: env_number("SEARCH_API_PREWARM_CONCURRENCY", DEFAULT_PREWARM_CONCURRENCY)?,
            statsd_addr: env_string("SEARCH_API_STATSD_ADDR", &default_statsd_addr()),
            telemetry_disabled: env_bool("SEARCH_API_TELEMETRY_DISABLED", false)?,
            recall_sample_rate: env_unit_fraction("SEARCH_API_RECALL_SAMPLE_RATE", DEFAULT_RECALL_SAMPLE_RATE)?,
            io_concurrency: env_number("SEARCH_API_IO_CONCURRENCY", DEFAULT_IO_CONCURRENCY)?,
            serve_by_tag: env_bool("SEARCH_API_SERVE_BY_TAG", DEFAULT_SERVE_BY_TAG)?,
            serve_tag: env_string("SEARCH_API_SERVE_TAG", DEFAULT_SERVE_TAG),
            serve_tag_ttl_secs: env_number("SEARCH_API_SERVE_TAG_TTL_SECS", DEFAULT_SERVE_TAG_TTL_SECS)?,
            event_timestamp_column: env_string("SEARCH_API_EVENT_TIMESTAMP_COLUMN", DEFAULT_EVENT_TIMESTAMP_COLUMN),
            default_minimum_nprobes: env_number("SEARCH_API_DEFAULT_MINIMUM_NPROBES", DEFAULT_MINIMUM_NPROBES)?,
            default_maximum_nprobes: env_number("SEARCH_API_DEFAULT_MAXIMUM_NPROBES", DEFAULT_MAXIMUM_NPROBES)?,
            nprobes_ceiling: env_number("SEARCH_API_NPROBES_CEILING", DEFAULT_NPROBES_CEILING)?,
            default_refine_factor: env_number("SEARCH_API_DEFAULT_REFINE_FACTOR", DEFAULT_REFINE_FACTOR)?,
            fast_search_default: env_bool("SEARCH_API_FAST_SEARCH_DEFAULT", DEFAULT_FAST_SEARCH)?,
            request_timeout_ms: env_number("SEARCH_API_REQUEST_TIMEOUT_MS", DEFAULT_REQUEST_TIMEOUT_MS)?,
            max_concurrent_streams: env_number("SEARCH_API_MAX_CONCURRENT_STREAMS", DEFAULT_MAX_CONCURRENT_STREAMS)?,
            concurrency_limit_per_connection: env_number(
                "SEARCH_API_CONCURRENCY_LIMIT_PER_CONNECTION",
                DEFAULT_CONCURRENCY_LIMIT_PER_CONNECTION,
            )?,
            prewarm_targets_path: {
                let raw = env_string("SEARCH_API_PREWARM_TARGETS_PATH", DEFAULT_PREWARM_TARGETS_PATH);
                if raw.is_empty() {
                    None
                } else {
                    Some(std::path::PathBuf::from(raw))
                }
            },
        })
    }
}

/// Resolves the cache backend selection.
///
/// `SEARCH_API_CACHE_BACKEND` wins when set. Otherwise the deprecated
/// `SEARCH_API_DISK_CACHE_DISABLED=true` alias maps to [`CacheBackendKind::Memory`] (with a
/// deprecation warning), and the default is [`CacheBackendKind::Disk`].
fn env_cache_backend() -> Result<CacheBackendKind, String> {
    if let Ok(raw) = std::env::var("SEARCH_API_CACHE_BACKEND") {
        return raw
            .parse::<CacheBackendKind>()
            .map_err(|err| format!("SEARCH_API_CACHE_BACKEND {err}"));
    }
    if env_bool("SEARCH_API_DISK_CACHE_DISABLED", false)? {
        tracing::warn!("SEARCH_API_DISK_CACHE_DISABLED is deprecated, use SEARCH_API_CACHE_BACKEND=memory");
        return Ok(CacheBackendKind::Memory);
    }
    Ok(CacheBackendKind::Disk)
}

/// Default DogStatsD address: the Datadog Agent host when advertised, else localhost.
fn default_statsd_addr() -> String {
    match std::env::var("DD_AGENT_HOST") {
        Ok(host) => format!("{host}:8125"),
        Err(_) => DEFAULT_STATSD_ADDR.to_string(),
    }
}

/// Reads an environment variable as a number, falling back to `default` when unset.
fn env_number<T: FromStr>(name: &str, default: T) -> Result<T, String> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<T>()
            .map_err(|_| format!("{name} must be a valid number, got {raw:?}")),
        Err(_) => Ok(default),
    }
}

/// Reads an environment variable as an `f64` in `[0, 1]`, falling back to `default` when unset.
fn env_unit_fraction(name: &str, default: f64) -> Result<f64, String> {
    let value: f64 = env_number(name, default)?;
    if !(0.0..=1.0).contains(&value) {
        return Err(format!("{name} must lie in [0, 1], got {value}"));
    }
    Ok(value)
}

/// Reads an environment variable as a string, falling back to `default` when unset.
fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// Reads an environment variable as a boolean, falling back to `default` when unset.
///
/// Accepts `true`/`false`, `1`/`0`, `yes`/`no`, and `on`/`off`, case-insensitively.
fn env_bool(name: &str, default: bool) -> Result<bool, String> {
    match std::env::var(name) {
        Ok(raw) => match raw.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(true),
            "false" | "0" | "no" | "off" => Ok(false),
            _ => Err(format!("{name} must be a boolean, got {raw:?}")),
        },
        Err(_) => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes env-mutating tests. The process environment is shared across threads.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Runs `body` with the given env vars set, restoring the previous state afterwards.
    fn with_env(vars: &[(&str, Option<&str>)], body: impl FnOnce()) {
        let guard = ENV_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous: Vec<(String, Option<String>)> = vars
            .iter()
            .map(|(name, _)| ((*name).to_string(), std::env::var(name).ok()))
            .collect();
        for (name, value) in vars {
            match value {
                Some(value) => unsafe { std::env::set_var(name, value) },
                None => unsafe { std::env::remove_var(name) },
            }
        }
        body();
        for (name, value) in previous {
            match value {
                Some(value) => unsafe { std::env::set_var(&name, value) },
                None => unsafe { std::env::remove_var(&name) },
            }
        }
        drop(guard);
    }

    /// Env var names cleared so defaults apply in tests.
    const OPTIONAL_VARS: [&str; 30] = [
        "SEARCH_API_CACHE_BACKEND",
        "SEARCH_API_REDIS_URL",
        "SEARCH_API_REDIS_NAMESPACE",
        "SEARCH_API_SERVE_BY_TAG",
        "SEARCH_API_SERVE_TAG",
        "SEARCH_API_SERVE_TAG_TTL_SECS",
        "SEARCH_API_EVENT_TIMESTAMP_COLUMN",
        "SEARCH_API_DATASET_CACHE_CAPACITY",
        "SEARCH_API_INDEX_CACHE_BYTES",
        "SEARCH_API_METADATA_CACHE_BYTES",
        "SEARCH_API_PORT",
        "SEARCH_API_CACHE_DIR",
        "SEARCH_API_DISK_INDEX_CACHE_BYTES",
        "SEARCH_API_DISK_STORE_CACHE_BYTES",
        "SEARCH_API_DISK_CACHE_DISABLED",
        "SEARCH_API_PREWARM_CONCURRENCY",
        "SEARCH_API_STATSD_ADDR",
        "SEARCH_API_TELEMETRY_DISABLED",
        "SEARCH_API_RECALL_SAMPLE_RATE",
        "SEARCH_API_IO_CONCURRENCY",
        "DD_AGENT_HOST",
        "SEARCH_API_DEFAULT_MINIMUM_NPROBES",
        "SEARCH_API_DEFAULT_MAXIMUM_NPROBES",
        "SEARCH_API_NPROBES_CEILING",
        "SEARCH_API_DEFAULT_REFINE_FACTOR",
        "SEARCH_API_FAST_SEARCH_DEFAULT",
        "SEARCH_API_REQUEST_TIMEOUT_MS",
        "SEARCH_API_MAX_CONCURRENT_STREAMS",
        "SEARCH_API_CONCURRENCY_LIMIT_PER_CONNECTION",
        "SEARCH_API_PREWARM_TARGETS_PATH",
    ];

    #[test]
    fn defaults_apply_when_env_unset() {
        let mut vars: Vec<(&str, Option<&str>)> = vec![("LANCE_ETL_BASE_URI", Some("/data/lance/"))];
        vars.extend(OPTIONAL_VARS.iter().map(|name| (*name, None)));
        with_env(&vars, || {
            let config = Config::from_env().unwrap();
            assert_eq!(config.base_uri, "/data/lance", "trailing slash must be stripped");
            assert_eq!(config.dataset_cache_capacity, DEFAULT_DATASET_CACHE_CAPACITY);
            assert_eq!(config.cache_dir, PathBuf::from(DEFAULT_CACHE_DIR));
            assert_eq!(config.disk_index_cache_bytes, DEFAULT_DISK_INDEX_CACHE_BYTES);
            assert_eq!(config.disk_store_cache_bytes, DEFAULT_DISK_STORE_CACHE_BYTES);
            assert_eq!(config.cache_backend, CacheBackendKind::Disk);
            assert!(config.redis_url.is_none());
            assert_eq!(config.redis_namespace, DEFAULT_REDIS_NAMESPACE);
            assert_eq!(config.prewarm_concurrency, DEFAULT_PREWARM_CONCURRENCY);
            assert_eq!(config.statsd_addr, DEFAULT_STATSD_ADDR);
            assert!(!config.telemetry_disabled);
            assert_eq!(config.recall_sample_rate, DEFAULT_RECALL_SAMPLE_RATE);
            assert_eq!(config.io_concurrency, DEFAULT_IO_CONCURRENCY);
            assert_eq!(config.serve_by_tag, DEFAULT_SERVE_BY_TAG);
            assert_eq!(config.serve_tag, DEFAULT_SERVE_TAG);
            assert_eq!(config.serve_tag, "HEAD", "the default serve tag is HEAD");
            assert_eq!(config.serve_tag_ttl_secs, DEFAULT_SERVE_TAG_TTL_SECS);
            assert_eq!(config.event_timestamp_column, DEFAULT_EVENT_TIMESTAMP_COLUMN);
            assert_eq!(config.default_minimum_nprobes, DEFAULT_MINIMUM_NPROBES);
            assert_eq!(config.default_maximum_nprobes, DEFAULT_MAXIMUM_NPROBES);
            assert_eq!(config.nprobes_ceiling, DEFAULT_NPROBES_CEILING);
            assert_eq!(config.default_refine_factor, DEFAULT_REFINE_FACTOR);
            assert_eq!(config.fast_search_default, DEFAULT_FAST_SEARCH);
            assert_eq!(config.request_timeout_ms, DEFAULT_REQUEST_TIMEOUT_MS);
            assert_eq!(config.max_concurrent_streams, DEFAULT_MAX_CONCURRENT_STREAMS);
            assert_eq!(
                config.concurrency_limit_per_connection,
                DEFAULT_CONCURRENCY_LIMIT_PER_CONNECTION
            );
            assert!(config.prewarm_targets_path.is_none());
        });
    }

    #[test]
    fn event_timestamp_column_env_override_applies() {
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                ("SEARCH_API_EVENT_TIMESTAMP_COLUMN", Some("ingested_at")),
            ],
            || {
                assert_eq!(Config::from_env().unwrap().event_timestamp_column, "ingested_at");
            },
        );
    }

    #[test]
    fn serve_tag_env_overrides_apply() {
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                ("SEARCH_API_SERVE_BY_TAG", Some("true")),
                ("SEARCH_API_SERVE_TAG", Some("green")),
                ("SEARCH_API_SERVE_TAG_TTL_SECS", Some("3")),
            ],
            || {
                let config = Config::from_env().unwrap();
                assert!(config.serve_by_tag);
                assert_eq!(config.serve_tag, "green");
                assert_eq!(config.serve_tag_ttl_secs, 3);
            },
        );
    }

    #[test]
    fn statsd_default_honors_dd_agent_host_and_env_overrides_win() {
        let mut vars: Vec<(&str, Option<&str>)> = vec![
            ("LANCE_ETL_BASE_URI", Some("/data/lance")),
            ("DD_AGENT_HOST", Some("agent.internal")),
        ];
        vars.extend(
            OPTIONAL_VARS
                .iter()
                .filter(|name| **name != "DD_AGENT_HOST")
                .map(|name| (*name, None)),
        );
        with_env(&vars, || {
            let config = Config::from_env().unwrap();
            assert_eq!(config.statsd_addr, "agent.internal:8125");
        });
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                ("DD_AGENT_HOST", Some("agent.internal")),
                ("SEARCH_API_STATSD_ADDR", Some("10.0.0.5:9125")),
                ("SEARCH_API_TELEMETRY_DISABLED", Some("true")),
            ],
            || {
                let config = Config::from_env().unwrap();
                assert_eq!(config.statsd_addr, "10.0.0.5:9125");
                assert!(config.telemetry_disabled);
            },
        );
    }

    #[test]
    fn env_overrides_apply() {
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                ("SEARCH_API_CACHE_DIR", Some("/var/cache/search")),
                ("SEARCH_API_DISK_INDEX_CACHE_BYTES", Some("4096")),
                ("SEARCH_API_CACHE_BACKEND", None),
                ("SEARCH_API_DISK_CACHE_DISABLED", Some("true")),
                ("SEARCH_API_PREWARM_CONCURRENCY", Some("9")),
                ("SEARCH_API_RECALL_SAMPLE_RATE", Some("0.25")),
            ],
            || {
                let config = Config::from_env().unwrap();
                assert_eq!(config.cache_dir, PathBuf::from("/var/cache/search"));
                assert_eq!(config.disk_index_cache_bytes, 4096);
                assert_eq!(config.cache_backend, CacheBackendKind::Memory);
                assert_eq!(config.prewarm_concurrency, 9);
                assert_eq!(config.recall_sample_rate, 0.25);
            },
        );
    }

    #[test]
    fn dataset_cache_capacity_env_override_applies() {
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                ("SEARCH_API_DATASET_CACHE_CAPACITY", Some("4096")),
            ],
            || {
                assert_eq!(Config::from_env().unwrap().dataset_cache_capacity, 4096);
            },
        );
    }

    #[test]
    fn recall_sample_rate_outside_unit_interval_is_rejected() {
        for bad in ["1.5", "-0.1", "rate"] {
            with_env(
                &[
                    ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                    ("SEARCH_API_RECALL_SAMPLE_RATE", Some(bad)),
                ],
                || {
                    let err = Config::from_env().unwrap_err();
                    assert!(err.contains("SEARCH_API_RECALL_SAMPLE_RATE"), "unexpected error: {err}");
                },
            );
        }
    }

    #[test]
    fn bool_parsing_accepts_common_spellings_and_rejects_garbage() {
        for (raw, expected) in [
            ("1", CacheBackendKind::Memory),
            ("Yes", CacheBackendKind::Memory),
            ("off", CacheBackendKind::Disk),
            ("FALSE", CacheBackendKind::Disk),
        ] {
            with_env(
                &[
                    ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                    ("SEARCH_API_CACHE_BACKEND", None),
                    ("SEARCH_API_DISK_CACHE_DISABLED", Some(raw)),
                ],
                || {
                    assert_eq!(Config::from_env().unwrap().cache_backend, expected);
                },
            );
        }
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                ("SEARCH_API_CACHE_BACKEND", None),
                ("SEARCH_API_DISK_CACHE_DISABLED", Some("maybe")),
            ],
            || {
                assert!(Config::from_env().is_err());
            },
        );
    }

    #[test]
    fn cache_backend_parses_case_insensitively_and_rejects_garbage() {
        for (raw, expected) in [
            ("disk", CacheBackendKind::Disk),
            ("Redis", CacheBackendKind::Redis),
            ("MEMORY", CacheBackendKind::Memory),
        ] {
            with_env(
                &[
                    ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                    ("SEARCH_API_CACHE_BACKEND", Some(raw)),
                    ("SEARCH_API_REDIS_URL", Some("redis://127.0.0.1:6379")),
                ],
                || {
                    assert_eq!(Config::from_env().unwrap().cache_backend, expected);
                },
            );
        }
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                ("SEARCH_API_CACHE_BACKEND", Some("tape")),
            ],
            || {
                let err = Config::from_env().unwrap_err();
                assert!(err.contains("SEARCH_API_CACHE_BACKEND"), "unexpected error: {err}");
            },
        );
    }

    #[test]
    fn redis_backend_requires_a_url() {
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                ("SEARCH_API_CACHE_BACKEND", Some("redis")),
                ("SEARCH_API_REDIS_URL", None),
            ],
            || {
                let err = Config::from_env().unwrap_err();
                assert!(err.contains("SEARCH_API_REDIS_URL"), "unexpected error: {err}");
            },
        );
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                ("SEARCH_API_CACHE_BACKEND", Some("redis")),
                ("SEARCH_API_REDIS_URL", Some("rediss://cache.internal:6380")),
                ("SEARCH_API_REDIS_NAMESPACE", Some("staging")),
            ],
            || {
                let config = Config::from_env().unwrap();
                assert_eq!(config.cache_backend, CacheBackendKind::Redis);
                assert_eq!(config.redis_url.as_deref(), Some("rediss://cache.internal:6380"));
                assert_eq!(config.redis_namespace, "staging");
            },
        );
    }

    #[test]
    fn explicit_backend_wins_over_the_deprecated_disabled_alias() {
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                ("SEARCH_API_CACHE_BACKEND", Some("disk")),
                ("SEARCH_API_DISK_CACHE_DISABLED", Some("true")),
            ],
            || {
                assert_eq!(Config::from_env().unwrap().cache_backend, CacheBackendKind::Disk);
            },
        );
    }

    #[test]
    fn invalid_numbers_are_rejected() {
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                ("SEARCH_API_DISK_INDEX_CACHE_BYTES", Some("lots")),
            ],
            || {
                let err = Config::from_env().unwrap_err();
                assert!(err.contains("SEARCH_API_DISK_INDEX_CACHE_BYTES"));
            },
        );
    }

    #[test]
    fn io_tuning_defaults_are_production_leaning() {
        let mut vars: Vec<(&str, Option<&str>)> = vec![("LANCE_ETL_BASE_URI", Some("/data/lance"))];
        vars.extend(OPTIONAL_VARS.iter().map(|name| (*name, None)));
        with_env(&vars, || {
            let config = Config::from_env().unwrap();
            assert_eq!(
                config.io_concurrency, DEFAULT_IO_CONCURRENCY,
                "io_concurrency must default to {DEFAULT_IO_CONCURRENCY}"
            );
        });
    }

    #[test]
    fn io_tuning_env_overrides_apply() {
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                ("SEARCH_API_IO_CONCURRENCY", Some("128")),
            ],
            || {
                let config = Config::from_env().unwrap();
                assert_eq!(config.io_concurrency, 128);
            },
        );
    }

    #[test]
    fn io_tuning_invalid_values_are_rejected() {
        let (var, bad) = ("SEARCH_API_IO_CONCURRENCY", "many");
        with_env(&[("LANCE_ETL_BASE_URI", Some("/data/lance")), (var, Some(bad))], || {
            let err = Config::from_env().unwrap_err();
            assert!(err.contains(var), "expected error to name {var}, got: {err}");
        });
    }
}
