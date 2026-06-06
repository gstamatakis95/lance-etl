//! Runtime configuration sourced from environment variables.

use std::str::FromStr;

/// Default index cache budget in bytes (1 GiB).
pub const DEFAULT_INDEX_CACHE_BYTES: usize = 1024 * 1024 * 1024;

/// Default metadata cache budget in bytes (256 MiB).
pub const DEFAULT_METADATA_CACHE_BYTES: usize = 256 * 1024 * 1024;

/// Default capacity of the open-dataset-handle LRU.
pub const DEFAULT_DATASET_CACHE_CAPACITY: u64 = 1024;

/// Default TCP port for the gRPC server.
pub const DEFAULT_PORT: u16 = 8080;

/// Runtime configuration for the search API.
#[derive(Debug, Clone)]
pub struct Config {
    /// Dataset URI template containing an `{org_id}` placeholder, e.g. `s3://bucket/lance/{org_id}.lance`.
    pub base_uri_template: String,
    /// Maximum number of open `Dataset` handles kept in the LRU map.
    pub dataset_cache_capacity: u64,
    /// Byte budget for the shared session index cache.
    pub index_cache_bytes: usize,
    /// Byte budget for the shared session metadata cache.
    pub metadata_cache_bytes: usize,
    /// TCP port the gRPC server binds to.
    pub port: u16,
}

impl Config {
    /// Builds a configuration from environment variables.
    ///
    /// `LANCE_ETL_BASE_URI` is required and must contain an `{org_id}` placeholder. Optional
    /// overrides: `SEARCH_API_DATASET_CACHE_CAPACITY`, `SEARCH_API_INDEX_CACHE_BYTES`,
    /// `SEARCH_API_METADATA_CACHE_BYTES`, and `SEARCH_API_PORT`.
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
        })
    }

    /// Resolves the dataset URI for one organization by substituting the `{org_id}` placeholder.
    pub fn dataset_uri(&self, org_id: &str) -> String {
        self.base_uri_template.replace("{org_id}", org_id)
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
