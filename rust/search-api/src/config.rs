//! Runtime configuration sourced from environment variables.

use std::path::PathBuf;
use std::str::FromStr;

/// Fixed byte budget for the Lance index cache (1 GiB).
///
/// Hardcoded: no deployment has ever retuned this, so it is no longer an env knob.
pub const DEFAULT_INDEX_CACHE_BYTES: usize = 1024 * 1024 * 1024;

/// Fixed byte budget for the shared session metadata cache (256 MiB).
///
/// Hardcoded: no deployment has ever retuned this, so it is no longer an env knob.
pub const DEFAULT_METADATA_CACHE_BYTES: usize = 256 * 1024 * 1024;

/// Fixed weighted capacity of the open-dataset-handle LRU.
///
/// Weighted by a cheap per-handle proxy (open fragment count, clamped to
/// [`crate::lance::provider::MAX_HANDLE_WEIGHT`]), so a tiny tenant handle costs one unit while a
/// whale handle costs at most `MAX_HANDLE_WEIGHT` units. Sized for a 30 000-tenant fleet whose
/// load is a power-law tail of cheap tiny handles. Hardcoded: no deployment has ever retuned
/// this, so it is no longer an env knob.
pub const DEFAULT_DATASET_CACHE_CAPACITY: u64 = 16384;

/// Default TCP port for the gRPC server.
pub const DEFAULT_PORT: u16 = 8080;

/// Default root directory for the persistent disk caches.
pub const DEFAULT_CACHE_DIR: &str = "/tmp/rust-search/cache";

/// Default Redis key namespace for the `redis` cache backend.
pub const DEFAULT_REDIS_NAMESPACE: &str = "search-api";

/// Fixed interval in seconds between Redis prefix-registry hygiene passes.
///
/// The registry hash maps raw cache-key prefixes to their dir keys and carries no TTL, so a
/// background pass drops rows whose dir key has since expired or been evicted. Hardcoded: an
/// hourly cadence is universal, not an env knob.
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

/// Fixed disk budget for the serialized index cache tier (8 GiB).
///
/// Hardcoded: no deployment has ever retuned this, so it is no longer an env knob.
pub const DEFAULT_DISK_INDEX_CACHE_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Fixed disk budget for the metadata byte cache (2 GiB).
///
/// Hardcoded: no deployment has ever retuned this, so it is no longer an env knob.
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

/// Fixed number of indexes prewarmed concurrently per Prewarm RPC.
///
/// Hardcoded: no deployment has ever retuned this, so it is no longer an env knob.
pub const DEFAULT_PREWARM_CONCURRENCY: usize = 4;

/// Fixed logical id column captured for recall scoring.
///
/// Hardcoded: matches the standardized ETL and recall schema, so it is no longer an env knob.
pub const DEFAULT_ID_COLUMN: &str = "vector_id";

/// Fixed event-timestamp column a search time range is applied to.
///
/// Hardcoded: matches the standardized ETL event clock, so it is no longer an env knob.
pub const DEFAULT_EVENT_TIMESTAMP_COLUMN: &str = "event_timestamp";

/// Default DogStatsD address when neither `SEARCH_API_STATSD_ADDR` nor `DD_AGENT_HOST` is set.
pub const DEFAULT_STATSD_ADDR: &str = "127.0.0.1:8125";

/// Fixed recall sample rate: sampled-query recall capture is disabled.
///
/// Hardcoded: no deployment has enabled sampled recall capture, so this is no longer an env knob.
pub const DEFAULT_RECALL_SAMPLE_RATE: f64 = 0.0;

/// Fixed IO concurrency (number of parallel in-flight object-store requests per dataset).
///
/// Feeds `LANCE_IO_THREADS`. 256 saturates a typical 10 Gbit S3 link without hitting the AIMD
/// ~5 000 req/s/process ceiling. Hardcoded: no deployment has ever retuned this, so it is no
/// longer an env knob.
pub const DEFAULT_IO_CONCURRENCY: usize = 256;

/// Fixed minimum number of IVF partitions probed per vector query.
///
/// Lance's own default is 1, which is disastrous for recall on whale datasets with many IVF
/// partitions. Hardcoded: no deployment has ever retuned this, so it is no longer an env knob.
pub const DEFAULT_MINIMUM_NPROBES: usize = 8;

/// Fixed maximum number of IVF partitions probed per vector query.
///
/// Bounds a highly selective prefilter from driving Lance to probe every partition. Hardcoded: no
/// deployment has ever retuned this, so it is no longer an env knob.
pub const DEFAULT_MAXIMUM_NPROBES: usize = 32;

/// Fixed hard ceiling applied to any client-supplied nprobes / minimum_nprobes / maximum_nprobes.
///
/// Guards against a misconfigured client submitting a query that linearly scans every IVF
/// partition. Hardcoded: no deployment has ever retuned this, so it is no longer an env knob.
pub const DEFAULT_NPROBES_CEILING: usize = 64;

/// Fixed refine factor: re-ranks `k * refine_factor` candidates per query with exact distances.
///
/// Recovers recall lost to 1-bit RaBitQ quantisation at modest extra IO. Hardcoded: no deployment
/// has ever retuned this, so it is no longer an env knob.
pub const DEFAULT_REFINE_FACTOR: u32 = 2;

/// Fixed default for whether the service applies `fast_search` when an index exists for the
/// queried column.
///
/// Guarded so it only fires when the dataset actually has a matching index; an explicit
/// per-request value always wins. Hardcoded: no deployment has ever retuned this, so it is no
/// longer an env knob.
pub const DEFAULT_FAST_SEARCH: bool = true;

/// Fixed request timeout in milliseconds applied to search-tier gRPC calls.
///
/// 800 ms sits comfortably below the 1 s client-side deadline most callers use. Applied per
/// route by [`crate::grpc::RouteTimeoutLayer`]: the drain-heavy RPCs listed in
/// [`crate::grpc::timeout::LONG_TIMEOUT_ROUTES`] get [`DEFAULT_LONG_REQUEST_TIMEOUT_MS`]
/// instead. Hardcoded: no deployment has ever retuned this, so it is no longer an env knob.
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 800;

/// Fixed long request timeout in milliseconds for the drain-heavy RPCs (10 minutes).
///
/// `SearchService/Prewarm` loads every requested IVF partition and BTree page over the object
/// store, so it cannot live under the 800 ms search budget. Hardcoded: no deployment has ever
/// retuned this, so it is not an env knob.
pub const DEFAULT_LONG_REQUEST_TIMEOUT_MS: u64 = 600_000;

/// Fixed TTL in seconds for the negative cache of failed dataset opens (NotFound only).
///
/// Bounds how long a hot loop of requests for a nonexistent dataset is answered without touching
/// the object store, and equally how long a freshly created dataset can be reported missing by a
/// replica that probed it just before creation. Hardcoded: no deployment has ever retuned this,
/// so it is not an env knob.
pub const DEFAULT_NEGATIVE_OPEN_TTL_SECS: u64 = 5;

/// Fixed maximum concurrent streams (and connections) the gRPC server admits.
///
/// Matches the per-process IO concurrency budget. Hardcoded: no deployment has ever retuned this,
/// so it is no longer an env knob.
pub const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 256;

/// Fixed concurrency limit per gRPC connection, applied via tonic's tower layer.
///
/// Hardcoded: no deployment has ever retuned this, so it is no longer an env knob.
pub const DEFAULT_CONCURRENCY_LIMIT_PER_CONNECTION: usize = 256;

/// Path to the startup prewarm targets file (empty = disabled).
///
/// When set, the service reads this file on startup and prewarms each listed dataset in a
/// background task before it would otherwise be opened cold by a live request. The file format
/// is one target per line: `{org_id}/{tenant_id}/{namespace}` using the same path segments the
/// service resolves to `{base_uri}/{org_id}/{tenant_id}/{namespace}.lance`. Blank lines and lines
/// with invalid segments are skipped with a warning. Errors per target are logged but never fatal.
/// Env: `SEARCH_API_PREWARM_TARGETS_PATH`.
pub const DEFAULT_PREWARM_TARGETS_PATH: &str = "";

/// Fixed minimum object-store request size in bytes (IO buffer / block size) — 256 KiB.
///
/// Passed as `ObjectStoreParams::block_size` when opening every dataset. Hardcoded: no deployment
/// varies it, so it is no longer an env knob.
pub const DEFAULT_IO_BLOCK_SIZE_BYTES: usize = 256 * 1024;

/// Default for whether serving resolves the configured serve tag instead of opening latest.
///
/// Off by default so the legacy latest-resolution behavior is preserved until an operator has
/// verified prewarm-by-version and is ready to cut serving over to tag-based blue-green.
pub const DEFAULT_SERVE_BY_TAG: bool = false;

/// Default serve tag resolved to a concrete version when serve-by-tag is enabled.
pub const DEFAULT_SERVE_TAG: &str = "HEAD";

/// Fixed TTL in seconds for trusting a resolved serve-tag version before re-reading the tag.
///
/// Bounds how long a tag flip can go unobserved by a replica. Hardcoded: no deployment has ever
/// retuned this, so it is no longer an env knob.
pub const DEFAULT_SERVE_TAG_TTL_SECS: u64 = 10;

/// Fixed object-store retry-window timeout in seconds — 120 s.
///
/// Feeds `OBJECT_STORE_CLIENT_RETRY_TIMEOUT`, picked up by the S3/GCS/Azure client builders.
/// Hardcoded: no deployment varies it, so it is no longer an env knob.
pub const DEFAULT_OBJECT_STORE_TIMEOUT_SECS: u64 = 120;

/// Fixed hard ceiling on a client-supplied `k` (and the derived `k + offset` fetch count) for any
/// vector, text, or hybrid-leg search.
///
/// Bounds the worst-case response size on the flat/unindexed scan path. Hardcoded: no deployment
/// has ever retuned this, so it is no longer an env knob.
pub const DEFAULT_SEARCH_MAX_K: usize = 10_000;

/// Runtime configuration for the search API.
#[derive(Debug, Clone)]
pub struct Config {
    /// Base URI under which all datasets live, e.g. `s3://bucket/lance`. Each dataset resolves to
    /// `{base}/{org_id}/{tenant_id}/{namespace}.lance`.
    pub base_uri: String,
    /// Weighted capacity of the open-`Dataset` handle LRU (default [`DEFAULT_DATASET_CACHE_CAPACITY`]).
    /// Fixed: no longer env-configurable.
    pub dataset_cache_capacity: u64,
    /// Byte budget for the Lance index cache (default [`DEFAULT_INDEX_CACHE_BYTES`]). Fixed: no
    /// longer env-configurable.
    pub index_cache_bytes: usize,
    /// Byte budget for the shared session metadata cache (default [`DEFAULT_METADATA_CACHE_BYTES`]).
    /// Fixed: no longer env-configurable.
    pub metadata_cache_bytes: usize,
    /// TCP port the gRPC server binds to.
    pub port: u16,
    /// Root directory for the `disk` backend's caches. Default `/tmp/rust-search/cache`.
    /// Env: `SEARCH_API_CACHE_DIR`.
    pub cache_dir: PathBuf,
    /// Disk budget in bytes for the serialized index cache tier (default
    /// [`DEFAULT_DISK_INDEX_CACHE_BYTES`]). Fixed: no longer env-configurable.
    pub disk_index_cache_bytes: u64,
    /// Disk budget in bytes for the metadata byte cache (default [`DEFAULT_DISK_STORE_CACHE_BYTES`]).
    /// Fixed: no longer env-configurable.
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
    /// DogStatsD (UDP) address metrics are sent to. Defaults to `{DD_AGENT_HOST}:8125` when
    /// `DD_AGENT_HOST` is set, else `127.0.0.1:8125`. Env: `SEARCH_API_STATSD_ADDR`.
    pub statsd_addr: String,
    /// Disables trace export and DogStatsD entirely (tests / local runs keep JSON logs only).
    /// Env: `SEARCH_API_TELEMETRY_DISABLED`.
    pub telemetry_disabled: bool,
    /// Whether serving resolves the configured serve tag to a concrete version instead of opening
    /// the latest committed version (default false). When on, the provider keys its caches on the
    /// resolved version so blue and green coexist and a tag flip is observed within the serve-tag
    /// TTL. Env: `SEARCH_API_SERVE_BY_TAG`.
    pub serve_by_tag: bool,
    /// Tag serving resolves to a committed version when `serve_by_tag` is on (default `HEAD`).
    /// Env: `SEARCH_API_SERVE_TAG`.
    pub serve_tag: String,
    /// Seconds a resolved serve-tag version is trusted before the tag JSON is re-read (default
    /// [`DEFAULT_SERVE_TAG_TTL_SECS`]). Fixed: no longer env-configurable.
    pub serve_tag_ttl_secs: u64,
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
    /// (a trailing slash is stripped). Optional overrides: `SEARCH_API_PORT`,
    /// `SEARCH_API_CACHE_DIR`, `SEARCH_API_CACHE_BACKEND` (`disk`, `redis`, or `memory`, with
    /// `SEARCH_API_DISK_CACHE_DISABLED=true` honored as a deprecated alias for `memory`),
    /// `SEARCH_API_REDIS_URL` (required for the `redis` backend), `SEARCH_API_REDIS_NAMESPACE`
    /// (default `search-api`), `SEARCH_API_STATSD_ADDR` (default honors `DD_AGENT_HOST`),
    /// `SEARCH_API_TELEMETRY_DISABLED`, `SEARCH_API_SERVE_BY_TAG` (default false),
    /// `SEARCH_API_SERVE_TAG` (default `HEAD`), and `SEARCH_API_PREWARM_TARGETS_PATH` (default
    /// empty, disabled).
    ///
    /// Every other knob — dataset-handle cache sizing, index/metadata/disk cache budgets,
    /// serve-tag TTL, IO concurrency, ANN probe/refine/fast-search defaults, gRPC timeout and
    /// concurrency limits, the event-timestamp column, recall sampling, and the search `k`
    /// ceiling — is a fixed constant (see [`DEFAULT_DISK_CACHE_TTL_SECS`] and siblings) and is no
    /// longer env-configurable.
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
            dataset_cache_capacity: DEFAULT_DATASET_CACHE_CAPACITY,
            index_cache_bytes: DEFAULT_INDEX_CACHE_BYTES,
            metadata_cache_bytes: DEFAULT_METADATA_CACHE_BYTES,
            port: env_number("SEARCH_API_PORT", DEFAULT_PORT)?,
            cache_dir: PathBuf::from(env_string("SEARCH_API_CACHE_DIR", DEFAULT_CACHE_DIR)),
            disk_index_cache_bytes: DEFAULT_DISK_INDEX_CACHE_BYTES,
            disk_store_cache_bytes: DEFAULT_DISK_STORE_CACHE_BYTES,
            cache_backend,
            redis_url,
            redis_namespace: env_string("SEARCH_API_REDIS_NAMESPACE", DEFAULT_REDIS_NAMESPACE),
            statsd_addr: env_string("SEARCH_API_STATSD_ADDR", &default_statsd_addr()),
            telemetry_disabled: env_bool("SEARCH_API_TELEMETRY_DISABLED", false)?,
            serve_by_tag: env_bool("SEARCH_API_SERVE_BY_TAG", DEFAULT_SERVE_BY_TAG)?,
            serve_tag: env_string("SEARCH_API_SERVE_TAG", DEFAULT_SERVE_TAG),
            serve_tag_ttl_secs: DEFAULT_SERVE_TAG_TTL_SECS,
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
    const OPTIONAL_VARS: [&str; 12] = [
        "SEARCH_API_CACHE_BACKEND",
        "SEARCH_API_REDIS_URL",
        "SEARCH_API_REDIS_NAMESPACE",
        "SEARCH_API_SERVE_BY_TAG",
        "SEARCH_API_SERVE_TAG",
        "SEARCH_API_PORT",
        "SEARCH_API_CACHE_DIR",
        "SEARCH_API_DISK_CACHE_DISABLED",
        "SEARCH_API_STATSD_ADDR",
        "SEARCH_API_TELEMETRY_DISABLED",
        "DD_AGENT_HOST",
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
            assert_eq!(config.index_cache_bytes, DEFAULT_INDEX_CACHE_BYTES);
            assert_eq!(config.metadata_cache_bytes, DEFAULT_METADATA_CACHE_BYTES);
            assert_eq!(config.cache_dir, PathBuf::from(DEFAULT_CACHE_DIR));
            assert_eq!(config.disk_index_cache_bytes, DEFAULT_DISK_INDEX_CACHE_BYTES);
            assert_eq!(config.disk_store_cache_bytes, DEFAULT_DISK_STORE_CACHE_BYTES);
            assert_eq!(config.cache_backend, CacheBackendKind::Disk);
            assert!(config.redis_url.is_none());
            assert_eq!(config.redis_namespace, DEFAULT_REDIS_NAMESPACE);
            assert_eq!(config.statsd_addr, DEFAULT_STATSD_ADDR);
            assert!(!config.telemetry_disabled);
            assert_eq!(config.serve_by_tag, DEFAULT_SERVE_BY_TAG);
            assert_eq!(config.serve_tag, DEFAULT_SERVE_TAG);
            assert_eq!(config.serve_tag, "HEAD", "the default serve tag is HEAD");
            assert_eq!(config.serve_tag_ttl_secs, DEFAULT_SERVE_TAG_TTL_SECS);
            assert!(config.prewarm_targets_path.is_none());
        });
    }

    #[test]
    fn serve_tag_env_overrides_apply() {
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                ("SEARCH_API_SERVE_BY_TAG", Some("true")),
                ("SEARCH_API_SERVE_TAG", Some("green")),
            ],
            || {
                let config = Config::from_env().unwrap();
                assert!(config.serve_by_tag);
                assert_eq!(config.serve_tag, "green");
                assert_eq!(
                    config.serve_tag_ttl_secs, DEFAULT_SERVE_TAG_TTL_SECS,
                    "serve_tag_ttl_secs is fixed and no longer env-configurable"
                );
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
                ("SEARCH_API_CACHE_BACKEND", None),
                ("SEARCH_API_DISK_CACHE_DISABLED", Some("true")),
            ],
            || {
                let config = Config::from_env().unwrap();
                assert_eq!(config.cache_dir, PathBuf::from("/var/cache/search"));
                assert_eq!(config.cache_backend, CacheBackendKind::Memory);
            },
        );
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
                ("SEARCH_API_PORT", Some("lots")),
            ],
            || {
                let err = Config::from_env().unwrap_err();
                assert!(err.contains("SEARCH_API_PORT"));
            },
        );
    }
}
