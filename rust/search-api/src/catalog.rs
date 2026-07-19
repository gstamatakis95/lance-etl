//! PostgreSQL implementation of the exact serving catalog.

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Config, NoTls, Row, config::SslMode};

use crate::domain::{DatasetTarget, SearchError, ServingCatalog, ServingRoute};

/// Fixed maximum number of PostgreSQL connections used by one search process.
const CATALOG_POOL_SIZE: u32 = 16;

/// Client-safe message for a transient catalog dependency failure.
const CATALOG_UNAVAILABLE_MESSAGE: &str = "serving catalog unavailable";

/// Client-safe message for malformed state returned by the catalog.
const CATALOG_INVALID_MESSAGE: &str = "serving catalog returned invalid state";

/// PostgreSQL-backed serving catalog over datasets and their active publications.
pub struct PostgresServingCatalog {
    pool: CatalogPool,
}

enum CatalogPool {
    Verified(Pool<PostgresConnectionManager<MakeTlsConnector>>),
    Local(Pool<PostgresConnectionManager<NoTls>>),
}

impl PostgresServingCatalog {
    /// Connects a bounded pool to verified PostgreSQL or to loopback-only plaintext PostgreSQL.
    pub async fn connect(database_url: &str, ca_path: Option<&std::path::Path>) -> Result<Self, String> {
        let pool = match ca_path {
            Some(ca_path) => CatalogPool::Verified(connect_verified(database_url, ca_path).await?),
            None => CatalogPool::Local(connect_local(database_url).await?),
        };
        Ok(Self { pool })
    }

    /// Verifies that the catalog pool can execute a trivial query.
    pub async fn health(&self) -> Result<(), String> {
        match &self.pool {
            CatalogPool::Verified(pool) => {
                let connection = pool
                    .get()
                    .await
                    .map_err(|error| format!("serving catalog unavailable: {error}"))?;
                connection
                    .simple_query("SELECT 1")
                    .await
                    .map_err(|error| format!("serving catalog health query failed: {error}"))?;
            }
            CatalogPool::Local(pool) => {
                let connection = pool
                    .get()
                    .await
                    .map_err(|error| format!("serving catalog unavailable: {error}"))?;
                connection
                    .simple_query("SELECT 1")
                    .await
                    .map_err(|error| format!("serving catalog health query failed: {error}"))?;
            }
        }
        Ok(())
    }
}

/// Builds a verified-TLS PostgreSQL pool using the configured private root certificate.
async fn connect_verified(
    database_url: &str,
    ca_path: &std::path::Path,
) -> Result<Pool<PostgresConnectionManager<MakeTlsConnector>>, String> {
    let config = verified_catalog_config(database_url)?;
    let ca_pem = tokio::fs::read(ca_path)
        .await
        .map_err(|error| format!("failed to read PostgreSQL CA certificate: {error}"))?;
    let ca = Certificate::from_pem(&ca_pem).map_err(|error| format!("invalid PostgreSQL CA certificate: {error}"))?;
    let connector = TlsConnector::builder()
        .add_root_certificate(ca)
        .build()
        .map_err(|error| format!("failed to configure serving catalog TLS: {error}"))?;
    let manager = PostgresConnectionManager::new(config, MakeTlsConnector::new(connector));
    Pool::builder()
        .max_size(CATALOG_POOL_SIZE)
        .build(manager)
        .await
        .map_err(|error| format!("failed to connect to the serving catalog: {error}"))
}

/// Builds a plaintext PostgreSQL pool after proving the URL targets the loopback interface.
async fn connect_local(database_url: &str) -> Result<Pool<PostgresConnectionManager<NoTls>>, String> {
    let config = local_catalog_config(database_url)?;
    let manager = PostgresConnectionManager::new(config, NoTls);
    Pool::builder()
        .max_size(CATALOG_POOL_SIZE)
        .build(manager)
        .await
        .map_err(|error| format!("failed to connect to the serving catalog: {error}"))
}

/// Parses a PostgreSQL URL and requires certificate and hostname verification.
fn verified_catalog_config(database_url: &str) -> Result<Config, String> {
    let normalized_url = database_url.replacen("postgresql+psycopg://", "postgresql://", 1);
    let parsed =
        reqwest::Url::parse(&normalized_url).map_err(|error| format!("invalid LANCE_ETL_DATABASE_URL: {error}"))?;
    let ssl_modes: Vec<_> = parsed
        .query_pairs()
        .filter(|(name, _)| name == "sslmode")
        .map(|(_, value)| value.into_owned())
        .collect();
    if ssl_modes != ["verify-full"] {
        return Err("LANCE_ETL_DATABASE_URL must set exactly one sslmode=verify-full".to_owned());
    }
    let driver_url = normalized_url.replace("sslmode=verify-full", "sslmode=require");
    let config = driver_url
        .parse::<Config>()
        .map_err(|error| format!("invalid LANCE_ETL_DATABASE_URL: {error}"))?;
    if config.get_ssl_mode() != SslMode::Require {
        return Err("LANCE_ETL_DATABASE_URL did not enable verified TLS".to_owned());
    }
    Ok(config)
}

/// Parses a loopback PostgreSQL URL and rejects any non-plaintext transport declaration.
fn local_catalog_config(database_url: &str) -> Result<Config, String> {
    let normalized_url = database_url.replacen("postgresql+psycopg://", "postgresql://", 1);
    let parsed =
        reqwest::Url::parse(&normalized_url).map_err(|error| format!("invalid LANCE_ETL_DATABASE_URL: {error}"))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| "local LANCE_ETL_DATABASE_URL must include a loopback host".to_owned())?;
    let normalized_host = host.trim_start_matches('[').trim_end_matches(']');
    let is_loopback = host == "localhost"
        || normalized_host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if !is_loopback {
        return Err("local LANCE_ETL_DATABASE_URL must target localhost or a loopback IP address".to_owned());
    }
    let ssl_modes: Vec<_> = parsed
        .query_pairs()
        .filter(|(name, _)| name == "sslmode")
        .map(|(_, value)| value.into_owned())
        .collect();
    if !ssl_modes.is_empty() && ssl_modes != ["disable"] {
        return Err("local LANCE_ETL_DATABASE_URL may only omit sslmode or set sslmode=disable".to_owned());
    }
    let mut config = normalized_url
        .parse::<Config>()
        .map_err(|error| format!("invalid LANCE_ETL_DATABASE_URL: {error}"))?;
    config.ssl_mode(SslMode::Disable);
    Ok(config)
}

/// Executes the serving-route query through either transport-specific connection pool.
async fn resolve_row(pool: &CatalogPool, target: &DatasetTarget) -> Result<Option<Row>, SearchError> {
    const QUERY: &str = "SELECT p.lance_uri, p.lance_version \
         FROM datasets AS d \
         JOIN dataset_publications AS p \
           ON p.publication_id = d.active_publication_id \
          AND p.dataset_id = d.dataset_id \
         WHERE d.tenant_id = $1 \
           AND d.namespace = $2 \
           AND d.org_id = $3";
    match pool {
        CatalogPool::Verified(pool) => {
            let connection = pool.get().await.map_err(|_| catalog_unavailable("pool"))?;
            connection
                .query_opt(QUERY, &[&target.tenant_id, &target.namespace, &target.org_id])
                .await
                .map_err(|_| catalog_unavailable("query"))
        }
        CatalogPool::Local(pool) => {
            let connection = pool.get().await.map_err(|_| catalog_unavailable("pool"))?;
            connection
                .query_opt(QUERY, &[&target.tenant_id, &target.namespace, &target.org_id])
                .await
                .map_err(|_| catalog_unavailable("query"))
        }
    }
}

#[async_trait::async_trait]
impl ServingCatalog for PostgresServingCatalog {
    async fn resolve(&self, target: &DatasetTarget) -> Result<ServingRoute, SearchError> {
        target.validate()?;
        let row = resolve_row(&self.pool, target)
            .await?
            .ok_or_else(|| SearchError::not_found("target is not published"))?;
        let lance_uri = row
            .try_get::<_, Option<String>>(0)
            .map_err(|_| catalog_invalid("uri"))?
            .ok_or_else(|| SearchError::not_found("target is not published"))?;
        let raw_version = row
            .try_get::<_, Option<i64>>(1)
            .map_err(|_| catalog_invalid("version"))?
            .ok_or_else(|| SearchError::not_found("target is not published"))?;
        let lance_version = u64::try_from(raw_version)
            .ok()
            .filter(|version| *version > 0)
            .ok_or_else(|| SearchError::internal("serving catalog contains an invalid Lance version"))?;
        Ok(ServingRoute {
            lance_uri,
            lance_version,
        })
    }
}

/// Emits only a closed failure category and returns a bounded retriable error.
fn catalog_unavailable(category: &'static str) -> SearchError {
    tracing::warn!(failure_category = category, "serving catalog request failed");
    SearchError::unavailable(CATALOG_UNAVAILABLE_MESSAGE)
}

/// Emits only a closed decode category and returns a bounded internal error.
fn catalog_invalid(category: &'static str) -> SearchError {
    tracing::error!(failure_category = category, "serving catalog state is invalid");
    SearchError::internal(CATALOG_INVALID_MESSAGE)
}

#[cfg(test)]
mod tests {
    use super::{local_catalog_config, verified_catalog_config};

    #[test]
    fn catalog_transport_requires_certificate_and_hostname_verification() {
        let plaintext = verified_catalog_config("postgresql://localhost/control?sslmode=disable").unwrap_err();
        assert!(plaintext.contains("sslmode=verify-full"));
        assert!(verified_catalog_config("postgresql://localhost/control?sslmode=require").is_err());
        verified_catalog_config("postgresql://localhost/control?sslmode=verify-full").unwrap();
        verified_catalog_config("postgresql+psycopg://localhost/control?sslmode=verify-full").unwrap();
    }

    #[test]
    fn local_catalog_transport_is_plaintext_and_loopback_only() {
        let implicit = local_catalog_config("postgresql://localhost/control").unwrap();
        assert_eq!(implicit.get_ssl_mode(), tokio_postgres::config::SslMode::Disable);
        local_catalog_config("postgresql://127.0.0.1/control?sslmode=disable").unwrap();
        local_catalog_config("postgresql://[::1]/control").unwrap();
        assert!(local_catalog_config("postgresql://catalog/control?sslmode=disable").is_err());
        assert!(local_catalog_config("postgresql://localhost/control?sslmode=require").is_err());
        assert!(local_catalog_config("postgresql://localhost/control?sslmode=disable&sslmode=disable").is_err());
    }
}
