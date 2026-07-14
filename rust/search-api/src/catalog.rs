//! PostgreSQL implementation of the exact serving catalog.

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Config, config::SslMode};

use crate::domain::{DatasetTarget, SearchError, ServingCatalog, ServingRoute};

/// Fixed maximum number of PostgreSQL connections used by one search process.
const CATALOG_POOL_SIZE: u32 = 16;

/// Client-safe message for a transient catalog dependency failure.
const CATALOG_UNAVAILABLE_MESSAGE: &str = "serving catalog unavailable";

/// Client-safe message for malformed state returned by the catalog.
const CATALOG_INVALID_MESSAGE: &str = "serving catalog returned invalid state";

/// PostgreSQL-backed serving catalog over the three-table control plane.
pub struct PostgresServingCatalog {
    pool: Pool<PostgresConnectionManager<MakeTlsConnector>>,
}

impl PostgresServingCatalog {
    /// Connects a bounded pool to the control-plane PostgreSQL database.
    pub async fn connect(database_url: &str, ca_path: &std::path::Path) -> Result<Self, String> {
        let config = catalog_config(database_url)?;
        let ca_pem = tokio::fs::read(ca_path)
            .await
            .map_err(|error| format!("failed to read PostgreSQL CA certificate: {error}"))?;
        let ca =
            Certificate::from_pem(&ca_pem).map_err(|error| format!("invalid PostgreSQL CA certificate: {error}"))?;
        let connector = TlsConnector::builder()
            .add_root_certificate(ca)
            .build()
            .map_err(|error| format!("failed to configure serving catalog TLS: {error}"))?;
        let manager = PostgresConnectionManager::new(config, MakeTlsConnector::new(connector));
        let pool = Pool::builder()
            .max_size(CATALOG_POOL_SIZE)
            .build(manager)
            .await
            .map_err(|error| format!("failed to connect to the serving catalog: {error}"))?;
        Ok(Self { pool })
    }

    /// Verifies that the catalog pool can execute a trivial query.
    pub async fn health(&self) -> Result<(), String> {
        let connection = self
            .pool
            .get()
            .await
            .map_err(|error| format!("serving catalog unavailable: {error}"))?;
        connection
            .simple_query("SELECT 1")
            .await
            .map_err(|error| format!("serving catalog health query failed: {error}"))?;
        Ok(())
    }
}

/// Parses a PostgreSQL URL and rejects any configuration that permits plaintext transport.
fn catalog_config(database_url: &str) -> Result<Config, String> {
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

#[async_trait::async_trait]
impl ServingCatalog for PostgresServingCatalog {
    async fn resolve(&self, target: &DatasetTarget) -> Result<ServingRoute, SearchError> {
        target.validate()?;
        let connection = self.pool.get().await.map_err(|_| catalog_unavailable("pool"))?;
        let row = connection
            .query_opt(
                "SELECT served_lance_uri, served_lance_version, profile_id FROM targets WHERE tenant_id = $1 AND namespace = $2 AND org_id = $3",
                &[&target.tenant_id, &target.namespace, &target.org_id],
            )
            .await
            .map_err(|_| catalog_unavailable("query"))?
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
        let profile_id = row.try_get::<_, String>(2).map_err(|_| catalog_invalid("profile"))?;
        Ok(ServingRoute {
            lance_uri,
            lance_version,
            profile_id,
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
    use super::catalog_config;

    #[test]
    fn catalog_transport_requires_certificate_and_hostname_verification() {
        let plaintext = catalog_config("postgresql://localhost/control?sslmode=disable").unwrap_err();
        assert!(plaintext.contains("sslmode=verify-full"));
        assert!(catalog_config("postgresql://localhost/control?sslmode=require").is_err());
        catalog_config("postgresql://localhost/control?sslmode=verify-full").unwrap();
        catalog_config("postgresql+psycopg://localhost/control?sslmode=verify-full").unwrap();
    }
}
