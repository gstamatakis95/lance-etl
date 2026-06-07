//! Runtime configuration sourced from environment variables.

use std::path::PathBuf;
use std::str::FromStr;

/// Default index cache budget in bytes (1 GiB).
pub const DEFAULT_INDEX_CACHE_BYTES: usize = 1024 * 1024 * 1024;

/// Default metadata cache budget in bytes (256 MiB).
pub const DEFAULT_METADATA_CACHE_BYTES: usize = 256 * 1024 * 1024;

/// Default capacity of the open-dataset-handle LRU.
pub const DEFAULT_DATASET_CACHE_CAPACITY: u64 = 1024;

/// Default TCP port for the gRPC server.
pub const DEFAULT_PORT: u16 = 8080;

/// Default root directory for the persistent disk caches.
pub const DEFAULT_CACHE_DIR: &str = "/tmp/rust-search/cache";

/// Default disk budget for the serialized index cache tier (8 GiB).
pub const DEFAULT_DISK_INDEX_CACHE_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Default disk budget for the metadata byte cache (2 GiB).
pub const DEFAULT_DISK_STORE_CACHE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Default TTL for disk cache entries (7 days).
pub const DEFAULT_DISK_CACHE_TTL_SECS: u64 = 7 * 24 * 60 * 60;

/// Default largest single byte-range under `_indices/` stored by the byte cache (4 MiB).
pub const DEFAULT_STORE_CACHE_MAX_RANGE_BYTES: u64 = 4 * 1024 * 1024;

/// Default janitor sweep interval in seconds.
pub const DEFAULT_DISK_CACHE_SWEEP_SECS: u64 = 300;

/// Default number of indexes prewarmed concurrently per Prewarm RPC.
pub const DEFAULT_PREWARM_CONCURRENCY: usize = 4;

/// Default number of per-day datasets queried concurrently by one date-range fan-out.
pub const DEFAULT_FANOUT_CONCURRENCY: usize = 8;

/// Default logical id column used to deduplicate fan-out results across date partitions.
pub const DEFAULT_ID_COLUMN: &str = "vector_id";

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

/// Default minimum object-store request size in bytes (IO buffer / block size) — 256 KiB.
///
/// Passed as `ObjectStoreParams::block_size` when opening every dataset.  Larger values
/// reduce round-trip count for sequential scans at the cost of over-fetching for small
/// random reads.  256 KiB is a practical balance for S3 given typical index page sizes.
pub const DEFAULT_IO_BLOCK_SIZE_BYTES: usize = 256 * 1024;

/// Default for whether serving resolves the configured serve tag instead of opening latest.
///
/// Off by default so the legacy latest-resolution behavior is preserved until an operator has
/// verified prewarm-by-version and is ready to cut serving over to tag-based blue-green.
pub const DEFAULT_SERVE_BY_TAG: bool = false;

/// Default serve tag resolved to a concrete version when serve-by-tag is enabled.
pub const DEFAULT_SERVE_TAG: &str = "prod";

/// Default TTL in seconds for trusting a resolved serve-tag version before re-reading the tag.
///
/// Bounds how long a tag flip can go unobserved by a replica. 10 s keeps the steady-state cost at
/// zero extra manifest reads per request while flips propagate within roughly the TTL.
pub const DEFAULT_SERVE_TAG_TTL_SECS: u64 = 10;

/// Default object-store retry-window timeout in seconds — 120 s.
///
/// This feeds `OBJECT_STORE_CLIENT_RETRY_TIMEOUT`, which is picked up by the S3/GCS/Azure
/// client builders.  It is the total wall-clock budget across all retries for one cloud
/// request, not a per-attempt connect timeout.  120 s is generous enough to survive S3
/// throttle back-offs without exceeding a reasonable P99 SLO.
pub const DEFAULT_OBJECT_STORE_TIMEOUT_SECS: u64 = 120;

/// Runtime configuration for the search API.
#[derive(Debug, Clone)]
pub struct Config {
    /// Base URI under which all datasets live, e.g. `s3://bucket/lance`. Datasets resolve to
    /// `{base}/{org_id}/{tenant_id}/{namespace}.lance` and date-partitioned ones to
    /// `{base}/{org_id}/{tenant_id}/{namespace}/{event_date}.lance`.
    pub base_uri: String,
    /// Maximum number of open `Dataset` handles kept in the LRU map.
    pub dataset_cache_capacity: u64,
    /// Byte budget for the in-memory tier of the shared session index cache.
    pub index_cache_bytes: usize,
    /// Byte budget for the shared session metadata cache.
    pub metadata_cache_bytes: usize,
    /// TCP port the gRPC server binds to.
    pub port: u16,
    /// Root directory for all persistent caches. Default `/tmp/rust-search/cache`. Env: `SEARCH_API_CACHE_DIR`.
    pub cache_dir: PathBuf,
    /// Disk budget in bytes for the serialized index cache tier (default 8 GiB).
    /// Env: `SEARCH_API_DISK_INDEX_CACHE_BYTES`.
    pub disk_index_cache_bytes: u64,
    /// Disk budget in bytes for the metadata byte cache (default 2 GiB). Env: `SEARCH_API_DISK_STORE_CACHE_BYTES`.
    pub disk_store_cache_bytes: u64,
    /// TTL in seconds for disk cache entries (default 7 days). Env: `SEARCH_API_DISK_CACHE_TTL_SECS`.
    pub disk_cache_ttl_secs: u64,
    /// Largest single byte-range under `_indices/` that the byte cache stores (default 4 MiB). Larger ranges
    /// (bulk partition payloads) pass through. Env: `SEARCH_API_STORE_CACHE_MAX_RANGE_BYTES`.
    pub store_cache_max_range_bytes: u64,
    /// Janitor sweep interval in seconds (default 300). Env: `SEARCH_API_DISK_CACHE_SWEEP_SECS`.
    pub disk_cache_sweep_secs: u64,
    /// Set to disable disk caching entirely (pure in-memory fallback). Env: `SEARCH_API_DISK_CACHE_DISABLED`.
    pub disk_cache_disabled: bool,
    /// Max indexes prewarmed concurrently per Prewarm RPC (default 4). Env: `SEARCH_API_PREWARM_CONCURRENCY`.
    pub prewarm_concurrency: usize,
    /// Max per-day datasets queried concurrently by one date-range fan-out (default 8).
    /// Env: `SEARCH_API_FANOUT_CONCURRENCY`.
    pub fanout_concurrency: usize,
    /// Logical id column deduplicating fan-out results across date partitions (default
    /// `vector_id`). Env: `SEARCH_API_ID_COLUMN`.
    pub id_column: String,
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
    /// Minimum object-store request size in bytes (block size) passed to every dataset open
    /// (default 256 KiB).
    ///
    /// Wired as `ObjectStoreParams::block_size` in the provider on each cache miss.  Larger
    /// values cut round-trip count for sequential scans; 256 KiB fits typical Lance index page
    /// sizes without wasteful over-fetching.  Env: `SEARCH_API_IO_BLOCK_SIZE_BYTES`.
    pub io_block_size_bytes: usize,
    /// Object-store retry-window timeout in seconds (default 120).
    ///
    /// Written to the process-global `OBJECT_STORE_CLIENT_RETRY_TIMEOUT` env var at startup,
    /// which is picked up by the S3/GCS/Azure client builders inside Lance.  This is the total
    /// wall-clock budget across all retries for one cloud request.  120 s comfortably outlasts
    /// S3 throttle back-offs without breaching a typical search-service SLO.
    /// Env: `SEARCH_API_OBJECT_STORE_TIMEOUT_SECS`.
    pub object_store_timeout_secs: u64,
    /// Whether serving resolves the configured serve tag to a concrete version instead of opening
    /// the latest committed version (default false). When on, the provider keys its caches on the
    /// resolved version so blue and green coexist and a tag flip is observed within the serve-tag
    /// TTL. Env: `SEARCH_API_SERVE_BY_TAG`.
    pub serve_by_tag: bool,
    /// Tag serving resolves to a committed version when `serve_by_tag` is on (default `prod`).
    /// Env: `SEARCH_API_SERVE_TAG`.
    pub serve_tag: String,
    /// Seconds a resolved serve-tag version is trusted before the tag JSON is re-read (default
    /// 10). Bounds how long a tag flip can go unobserved by a replica while keeping the
    /// steady-state per-request cost at zero extra manifest reads. Env:
    /// `SEARCH_API_SERVE_TAG_TTL_SECS`.
    pub serve_tag_ttl_secs: u64,
}

impl Config {
    /// Builds a configuration from environment variables.
    ///
    /// `LANCE_ETL_BASE_URI` is required: the base URI all dataset paths are resolved under
    /// (a trailing slash is stripped). Optional overrides: `SEARCH_API_DATASET_CACHE_CAPACITY`,
    /// `SEARCH_API_INDEX_CACHE_BYTES`, `SEARCH_API_METADATA_CACHE_BYTES`, `SEARCH_API_PORT`,
    /// `SEARCH_API_CACHE_DIR`, `SEARCH_API_DISK_INDEX_CACHE_BYTES`,
    /// `SEARCH_API_DISK_STORE_CACHE_BYTES`, `SEARCH_API_DISK_CACHE_TTL_SECS`,
    /// `SEARCH_API_STORE_CACHE_MAX_RANGE_BYTES`, `SEARCH_API_DISK_CACHE_SWEEP_SECS`,
    /// `SEARCH_API_DISK_CACHE_DISABLED`, `SEARCH_API_PREWARM_CONCURRENCY`,
    /// `SEARCH_API_FANOUT_CONCURRENCY`, `SEARCH_API_ID_COLUMN`, `SEARCH_API_STATSD_ADDR`
    /// (default honors `DD_AGENT_HOST`), `SEARCH_API_TELEMETRY_DISABLED`,
    /// `SEARCH_API_RECALL_SAMPLE_RATE` (must lie in `[0, 1]`),
    /// `SEARCH_API_IO_CONCURRENCY` (default 256), `SEARCH_API_IO_BLOCK_SIZE_BYTES`
    /// (default 256 KiB), `SEARCH_API_OBJECT_STORE_TIMEOUT_SECS` (default 120),
    /// `SEARCH_API_SERVE_BY_TAG` (default false), `SEARCH_API_SERVE_TAG` (default `prod`), and
    /// `SEARCH_API_SERVE_TAG_TTL_SECS` (default 10).
    pub fn from_env() -> Result<Self, String> {
        let base_uri = std::env::var("LANCE_ETL_BASE_URI")
            .map_err(|_| "LANCE_ETL_BASE_URI must be set".to_string())?
            .trim_end_matches('/')
            .to_string();
        if base_uri.is_empty() {
            return Err("LANCE_ETL_BASE_URI must be a non-empty base URI".to_string());
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
            disk_cache_ttl_secs: env_number("SEARCH_API_DISK_CACHE_TTL_SECS", DEFAULT_DISK_CACHE_TTL_SECS)?,
            store_cache_max_range_bytes: env_number(
                "SEARCH_API_STORE_CACHE_MAX_RANGE_BYTES",
                DEFAULT_STORE_CACHE_MAX_RANGE_BYTES,
            )?,
            disk_cache_sweep_secs: env_number("SEARCH_API_DISK_CACHE_SWEEP_SECS", DEFAULT_DISK_CACHE_SWEEP_SECS)?,
            disk_cache_disabled: env_bool("SEARCH_API_DISK_CACHE_DISABLED", false)?,
            prewarm_concurrency: env_number("SEARCH_API_PREWARM_CONCURRENCY", DEFAULT_PREWARM_CONCURRENCY)?,
            fanout_concurrency: env_number("SEARCH_API_FANOUT_CONCURRENCY", DEFAULT_FANOUT_CONCURRENCY)?,
            id_column: env_string("SEARCH_API_ID_COLUMN", DEFAULT_ID_COLUMN),
            statsd_addr: env_string("SEARCH_API_STATSD_ADDR", &default_statsd_addr()),
            telemetry_disabled: env_bool("SEARCH_API_TELEMETRY_DISABLED", false)?,
            recall_sample_rate: env_unit_fraction("SEARCH_API_RECALL_SAMPLE_RATE", DEFAULT_RECALL_SAMPLE_RATE)?,
            io_concurrency: env_number("SEARCH_API_IO_CONCURRENCY", DEFAULT_IO_CONCURRENCY)?,
            io_block_size_bytes: env_number("SEARCH_API_IO_BLOCK_SIZE_BYTES", DEFAULT_IO_BLOCK_SIZE_BYTES)?,
            object_store_timeout_secs: env_number(
                "SEARCH_API_OBJECT_STORE_TIMEOUT_SECS",
                DEFAULT_OBJECT_STORE_TIMEOUT_SECS,
            )?,
            serve_by_tag: env_bool("SEARCH_API_SERVE_BY_TAG", DEFAULT_SERVE_BY_TAG)?,
            serve_tag: env_string("SEARCH_API_SERVE_TAG", DEFAULT_SERVE_TAG),
            serve_tag_ttl_secs: env_number("SEARCH_API_SERVE_TAG_TTL_SECS", DEFAULT_SERVE_TAG_TTL_SECS)?,
        })
    }
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
    const OPTIONAL_VARS: [&str; 24] = [
        "SEARCH_API_SERVE_BY_TAG",
        "SEARCH_API_SERVE_TAG",
        "SEARCH_API_SERVE_TAG_TTL_SECS",
        "SEARCH_API_DATASET_CACHE_CAPACITY",
        "SEARCH_API_INDEX_CACHE_BYTES",
        "SEARCH_API_METADATA_CACHE_BYTES",
        "SEARCH_API_PORT",
        "SEARCH_API_CACHE_DIR",
        "SEARCH_API_DISK_INDEX_CACHE_BYTES",
        "SEARCH_API_DISK_STORE_CACHE_BYTES",
        "SEARCH_API_DISK_CACHE_TTL_SECS",
        "SEARCH_API_STORE_CACHE_MAX_RANGE_BYTES",
        "SEARCH_API_DISK_CACHE_SWEEP_SECS",
        "SEARCH_API_DISK_CACHE_DISABLED",
        "SEARCH_API_PREWARM_CONCURRENCY",
        "SEARCH_API_FANOUT_CONCURRENCY",
        "SEARCH_API_ID_COLUMN",
        "SEARCH_API_STATSD_ADDR",
        "SEARCH_API_TELEMETRY_DISABLED",
        "SEARCH_API_RECALL_SAMPLE_RATE",
        "SEARCH_API_IO_CONCURRENCY",
        "SEARCH_API_IO_BLOCK_SIZE_BYTES",
        "SEARCH_API_OBJECT_STORE_TIMEOUT_SECS",
        "DD_AGENT_HOST",
    ];

    #[test]
    fn defaults_apply_when_env_unset() {
        let mut vars: Vec<(&str, Option<&str>)> = vec![("LANCE_ETL_BASE_URI", Some("/data/lance/"))];
        vars.extend(OPTIONAL_VARS.iter().map(|name| (*name, None)));
        with_env(&vars, || {
            let config = Config::from_env().unwrap();
            assert_eq!(config.base_uri, "/data/lance", "trailing slash must be stripped");
            assert_eq!(config.cache_dir, PathBuf::from(DEFAULT_CACHE_DIR));
            assert_eq!(config.disk_index_cache_bytes, DEFAULT_DISK_INDEX_CACHE_BYTES);
            assert_eq!(config.disk_store_cache_bytes, DEFAULT_DISK_STORE_CACHE_BYTES);
            assert_eq!(config.disk_cache_ttl_secs, DEFAULT_DISK_CACHE_TTL_SECS);
            assert_eq!(config.store_cache_max_range_bytes, DEFAULT_STORE_CACHE_MAX_RANGE_BYTES);
            assert_eq!(config.disk_cache_sweep_secs, DEFAULT_DISK_CACHE_SWEEP_SECS);
            assert!(!config.disk_cache_disabled);
            assert_eq!(config.prewarm_concurrency, DEFAULT_PREWARM_CONCURRENCY);
            assert_eq!(config.fanout_concurrency, DEFAULT_FANOUT_CONCURRENCY);
            assert_eq!(config.id_column, DEFAULT_ID_COLUMN);
            assert_eq!(config.statsd_addr, DEFAULT_STATSD_ADDR);
            assert!(!config.telemetry_disabled);
            assert_eq!(config.recall_sample_rate, DEFAULT_RECALL_SAMPLE_RATE);
            assert_eq!(config.io_concurrency, DEFAULT_IO_CONCURRENCY);
            assert_eq!(config.io_block_size_bytes, DEFAULT_IO_BLOCK_SIZE_BYTES);
            assert_eq!(config.object_store_timeout_secs, DEFAULT_OBJECT_STORE_TIMEOUT_SECS);
            assert_eq!(config.serve_by_tag, DEFAULT_SERVE_BY_TAG);
            assert_eq!(config.serve_tag, DEFAULT_SERVE_TAG);
            assert_eq!(config.serve_tag_ttl_secs, DEFAULT_SERVE_TAG_TTL_SECS);
        });
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
                ("SEARCH_API_DISK_CACHE_DISABLED", Some("true")),
                ("SEARCH_API_PREWARM_CONCURRENCY", Some("9")),
                ("SEARCH_API_RECALL_SAMPLE_RATE", Some("0.25")),
            ],
            || {
                let config = Config::from_env().unwrap();
                assert_eq!(config.cache_dir, PathBuf::from("/var/cache/search"));
                assert_eq!(config.disk_index_cache_bytes, 4096);
                assert!(config.disk_cache_disabled);
                assert_eq!(config.prewarm_concurrency, 9);
                assert_eq!(config.recall_sample_rate, 0.25);
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
        for (raw, expected) in [("1", true), ("Yes", true), ("off", false), ("FALSE", false)] {
            with_env(
                &[
                    ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                    ("SEARCH_API_DISK_CACHE_DISABLED", Some(raw)),
                ],
                || {
                    assert_eq!(Config::from_env().unwrap().disk_cache_disabled, expected);
                },
            );
        }
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                ("SEARCH_API_DISK_CACHE_DISABLED", Some("maybe")),
            ],
            || {
                assert!(Config::from_env().is_err());
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
            assert_eq!(
                config.io_block_size_bytes, DEFAULT_IO_BLOCK_SIZE_BYTES,
                "io_block_size_bytes must default to {DEFAULT_IO_BLOCK_SIZE_BYTES}"
            );
            assert_eq!(
                config.object_store_timeout_secs, DEFAULT_OBJECT_STORE_TIMEOUT_SECS,
                "object_store_timeout_secs must default to {DEFAULT_OBJECT_STORE_TIMEOUT_SECS}",
            );
        });
    }

    #[test]
    fn io_tuning_env_overrides_apply() {
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/lance")),
                ("SEARCH_API_IO_CONCURRENCY", Some("128")),
                ("SEARCH_API_IO_BLOCK_SIZE_BYTES", Some("131072")),
                ("SEARCH_API_OBJECT_STORE_TIMEOUT_SECS", Some("60")),
            ],
            || {
                let config = Config::from_env().unwrap();
                assert_eq!(config.io_concurrency, 128);
                assert_eq!(config.io_block_size_bytes, 131_072);
                assert_eq!(config.object_store_timeout_secs, 60);
            },
        );
    }

    #[test]
    fn io_tuning_invalid_values_are_rejected() {
        for (var, bad) in [
            ("SEARCH_API_IO_CONCURRENCY", "many"),
            ("SEARCH_API_IO_BLOCK_SIZE_BYTES", "big"),
            ("SEARCH_API_OBJECT_STORE_TIMEOUT_SECS", "forever"),
        ] {
            with_env(&[("LANCE_ETL_BASE_URI", Some("/data/lance")), (var, Some(bad))], || {
                let err = Config::from_env().unwrap_err();
                assert!(err.contains(var), "expected error to name {var}, got: {err}");
            });
        }
    }
}
