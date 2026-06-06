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

/// Default DogStatsD address when neither `SEARCH_API_STATSD_ADDR` nor `DD_AGENT_HOST` is set.
pub const DEFAULT_STATSD_ADDR: &str = "127.0.0.1:8125";

/// Runtime configuration for the search API.
#[derive(Debug, Clone)]
pub struct Config {
    /// Dataset URI template containing an `{org_id}` placeholder, e.g. `s3://bucket/lance/{org_id}.lance`.
    pub base_uri_template: String,
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
    /// Largest single byte-range under `_indices/` that the byte cache stores (default 4 MiB); larger ranges
    /// (bulk partition payloads) pass through. Env: `SEARCH_API_STORE_CACHE_MAX_RANGE_BYTES`.
    pub store_cache_max_range_bytes: u64,
    /// Janitor sweep interval in seconds (default 300). Env: `SEARCH_API_DISK_CACHE_SWEEP_SECS`.
    pub disk_cache_sweep_secs: u64,
    /// Set to disable disk caching entirely (pure in-memory fallback). Env: `SEARCH_API_DISK_CACHE_DISABLED`.
    pub disk_cache_disabled: bool,
    /// Max indexes prewarmed concurrently per Prewarm RPC (default 4). Env: `SEARCH_API_PREWARM_CONCURRENCY`.
    pub prewarm_concurrency: usize,
    /// DogStatsD (UDP) address metrics are sent to. Defaults to `{DD_AGENT_HOST}:8125` when
    /// `DD_AGENT_HOST` is set, else `127.0.0.1:8125`. Env: `SEARCH_API_STATSD_ADDR`.
    pub statsd_addr: String,
    /// Disables trace export and DogStatsD entirely (tests / local runs keep JSON logs only).
    /// Env: `SEARCH_API_TELEMETRY_DISABLED`.
    pub telemetry_disabled: bool,
}

impl Config {
    /// Builds a configuration from environment variables.
    ///
    /// `LANCE_ETL_BASE_URI` is required and must contain an `{org_id}` placeholder. Optional
    /// overrides: `SEARCH_API_DATASET_CACHE_CAPACITY`, `SEARCH_API_INDEX_CACHE_BYTES`,
    /// `SEARCH_API_METADATA_CACHE_BYTES`, `SEARCH_API_PORT`, `SEARCH_API_CACHE_DIR`,
    /// `SEARCH_API_DISK_INDEX_CACHE_BYTES`, `SEARCH_API_DISK_STORE_CACHE_BYTES`,
    /// `SEARCH_API_DISK_CACHE_TTL_SECS`, `SEARCH_API_STORE_CACHE_MAX_RANGE_BYTES`,
    /// `SEARCH_API_DISK_CACHE_SWEEP_SECS`, `SEARCH_API_DISK_CACHE_DISABLED`,
    /// `SEARCH_API_PREWARM_CONCURRENCY`, `SEARCH_API_STATSD_ADDR` (default honors
    /// `DD_AGENT_HOST`), and `SEARCH_API_TELEMETRY_DISABLED`.
    pub fn from_env() -> Result<Self, String> {
        let base_uri_template =
            std::env::var("LANCE_ETL_BASE_URI").map_err(|_| "LANCE_ETL_BASE_URI must be set".to_string())?;
        if !base_uri_template.contains("{org_id}") {
            return Err("LANCE_ETL_BASE_URI must contain an {org_id} placeholder".to_string());
        }
        Ok(Self {
            base_uri_template,
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
            statsd_addr: env_string("SEARCH_API_STATSD_ADDR", &default_statsd_addr()),
            telemetry_disabled: env_bool("SEARCH_API_TELEMETRY_DISABLED", false)?,
        })
    }

    /// Resolves the dataset URI for one organization by substituting the `{org_id}` placeholder.
    pub fn dataset_uri(&self, org_id: &str) -> String {
        self.base_uri_template.replace("{org_id}", org_id)
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

    /// Serializes env-mutating tests; the process environment is shared across threads.
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
    const OPTIONAL_VARS: [&str; 14] = [
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
        "SEARCH_API_STATSD_ADDR",
        "SEARCH_API_TELEMETRY_DISABLED",
        "DD_AGENT_HOST",
    ];

    #[test]
    fn defaults_apply_when_env_unset() {
        let mut vars: Vec<(&str, Option<&str>)> = vec![("LANCE_ETL_BASE_URI", Some("/data/{org_id}.lance"))];
        vars.extend(OPTIONAL_VARS.iter().map(|name| (*name, None)));
        with_env(&vars, || {
            let config = Config::from_env().unwrap();
            assert_eq!(config.cache_dir, PathBuf::from(DEFAULT_CACHE_DIR));
            assert_eq!(config.disk_index_cache_bytes, DEFAULT_DISK_INDEX_CACHE_BYTES);
            assert_eq!(config.disk_store_cache_bytes, DEFAULT_DISK_STORE_CACHE_BYTES);
            assert_eq!(config.disk_cache_ttl_secs, DEFAULT_DISK_CACHE_TTL_SECS);
            assert_eq!(config.store_cache_max_range_bytes, DEFAULT_STORE_CACHE_MAX_RANGE_BYTES);
            assert_eq!(config.disk_cache_sweep_secs, DEFAULT_DISK_CACHE_SWEEP_SECS);
            assert!(!config.disk_cache_disabled);
            assert_eq!(config.prewarm_concurrency, DEFAULT_PREWARM_CONCURRENCY);
            assert_eq!(config.statsd_addr, DEFAULT_STATSD_ADDR);
            assert!(!config.telemetry_disabled);
        });
    }

    #[test]
    fn statsd_default_honors_dd_agent_host_and_env_overrides_win() {
        let mut vars: Vec<(&str, Option<&str>)> = vec![
            ("LANCE_ETL_BASE_URI", Some("/data/{org_id}.lance")),
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
                ("LANCE_ETL_BASE_URI", Some("/data/{org_id}.lance")),
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
                ("LANCE_ETL_BASE_URI", Some("/data/{org_id}.lance")),
                ("SEARCH_API_CACHE_DIR", Some("/var/cache/search")),
                ("SEARCH_API_DISK_INDEX_CACHE_BYTES", Some("4096")),
                ("SEARCH_API_DISK_CACHE_DISABLED", Some("true")),
                ("SEARCH_API_PREWARM_CONCURRENCY", Some("9")),
            ],
            || {
                let config = Config::from_env().unwrap();
                assert_eq!(config.cache_dir, PathBuf::from("/var/cache/search"));
                assert_eq!(config.disk_index_cache_bytes, 4096);
                assert!(config.disk_cache_disabled);
                assert_eq!(config.prewarm_concurrency, 9);
            },
        );
    }

    #[test]
    fn bool_parsing_accepts_common_spellings_and_rejects_garbage() {
        for (raw, expected) in [("1", true), ("Yes", true), ("off", false), ("FALSE", false)] {
            with_env(
                &[
                    ("LANCE_ETL_BASE_URI", Some("/data/{org_id}.lance")),
                    ("SEARCH_API_DISK_CACHE_DISABLED", Some(raw)),
                ],
                || {
                    assert_eq!(Config::from_env().unwrap().disk_cache_disabled, expected);
                },
            );
        }
        with_env(
            &[
                ("LANCE_ETL_BASE_URI", Some("/data/{org_id}.lance")),
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
                ("LANCE_ETL_BASE_URI", Some("/data/{org_id}.lance")),
                ("SEARCH_API_DISK_INDEX_CACHE_BYTES", Some("lots")),
            ],
            || {
                let err = Config::from_env().unwrap_err();
                assert!(err.contains("SEARCH_API_DISK_INDEX_CACHE_BYTES"));
            },
        );
    }
}
