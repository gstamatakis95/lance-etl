//! PostgreSQL implementation of the exact serving catalog.

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::{Config, NoTls, Row};

use crate::domain::{DatasetTarget, SearchError, ServingCatalog, ServingRoute};

/// Fixed maximum number of PostgreSQL connections used by one search process.
const CATALOG_POOL_SIZE: u32 = 16;

/// Client-safe message for a transient catalog dependency failure.
const CATALOG_UNAVAILABLE_MESSAGE: &str = "serving catalog unavailable";

/// Client-safe message for malformed state returned by the catalog.
const CATALOG_INVALID_MESSAGE: &str = "serving catalog returned invalid state";

/// PostgreSQL-backed serving catalog over datasets and their active publications.
pub struct PostgresServingCatalog {
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

impl PostgresServingCatalog {
    /// Connects a bounded pool using the connection string exactly as given.
    ///
    /// The process has no TLS implementation wired in, so a connection string that does not
    /// request TLS just works, and one that does (`sslmode=require` and above) fails with
    /// `tokio_postgres`'s own "no TLS implementation configured" error rather than being silently
    /// downgraded.
    pub async fn connect(database_url: &str) -> Result<Self, String> {
        let config = catalog_config(database_url)?;
        let manager = PostgresConnectionManager::new(config, NoTls);
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

/// Parses a PostgreSQL URL exactly as given, beyond normalizing the Python-driver
/// `postgresql+psycopg://` scheme prefix to the standard `postgresql://` one.
///
/// No `sslmode` (or any other) parameter is rewritten or stripped. Whatever the caller wrote is
/// handed to `tokio_postgres::Config` unchanged, so it decides what is and is not a valid
/// connection string on its own terms.
fn catalog_config(database_url: &str) -> Result<Config, String> {
    let normalized_url = database_url.replacen("postgresql+psycopg://", "postgresql://", 1);
    normalized_url
        .parse::<Config>()
        .map_err(|error| format!("invalid LANCE_ETL_DATABASE_URL: {error}"))
}

/// Executes the serving-route query against the catalog connection pool.
async fn resolve_row(
    pool: &Pool<PostgresConnectionManager<NoTls>>,
    target: &DatasetTarget,
) -> Result<Option<Row>, SearchError> {
    const QUERY: &str = "SELECT p.lance_uri, p.lance_version \
         FROM datasets AS d \
         JOIN dataset_publications AS p \
           ON p.publication_id = d.active_publication_id \
          AND p.dataset_id = d.dataset_id \
         WHERE d.tenant_id = $1 \
           AND d.namespace = $2 \
           AND d.org_id = $3";
    let connection = pool.get().await.map_err(|_| catalog_unavailable("pool"))?;
    connection
        .query_opt(QUERY, &[&target.tenant_id, &target.namespace, &target.org_id])
        .await
        .map_err(|_| catalog_unavailable("query"))
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
    use super::catalog_config;
    use tokio_postgres::config::SslMode;

    #[test]
    fn plain_urls_default_to_the_driver_default_sslmode() {
        let plain = catalog_config("postgresql://localhost/control").unwrap();
        assert_eq!(plain.get_ssl_mode(), SslMode::Prefer);
        let remote_host = catalog_config("postgresql://catalog.internal/control").unwrap();
        assert_eq!(remote_host.get_ssl_mode(), SslMode::Prefer);
    }

    #[test]
    fn an_explicit_sslmode_is_passed_through_unchanged() {
        let disabled = catalog_config("postgresql://localhost/control?sslmode=disable").unwrap();
        assert_eq!(disabled.get_ssl_mode(), SslMode::Disable);
        let required = catalog_config("postgresql://localhost/control?sslmode=require").unwrap();
        assert_eq!(required.get_ssl_mode(), SslMode::Require);
    }

    #[test]
    fn a_verification_sslmode_is_not_rewritten_and_is_rejected_by_the_driver_as_given() {
        let error = catalog_config("postgresql://localhost/control?sslmode=verify-full").unwrap_err();
        assert!(
            error.contains("invalid LANCE_ETL_DATABASE_URL"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn the_psycopg_driver_scheme_is_normalized() {
        let psycopg_scheme = catalog_config("postgresql+psycopg://localhost/control").unwrap();
        assert_eq!(psycopg_scheme.get_ssl_mode(), SslMode::Prefer);
    }
}
