//! Dataset resolution: the provider trait and the caching base-URI-template implementation.

use std::sync::Arc;

use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::session::Session;
use lance_io::object_store::ObjectStoreRegistry;
use moka::future::Cache;

use crate::config::Config;
use crate::domain::SearchError;
use crate::lance::error::classify_lance_error;

/// Resolves an organization id to an open Lance dataset handle.
///
/// This is the seam for swapping dataset resolution strategies (URI templates, catalogs,
/// per-tenant registries) without touching the search backend.
pub trait DatasetProvider: Send + Sync + 'static {
    /// Returns an open dataset handle for one organization.
    fn dataset(&self, org_id: &str) -> impl Future<Output = Result<Arc<Dataset>, SearchError>> + Send;
}

/// Builds the single shared Lance session used by every dataset handle in the process.
///
/// Index and metadata cache entries are URI- and index-UUID-prefixed inside the session, so one
/// global cache safely spans tens of thousands of datasets. The caches use the default in-memory
/// Moka backend sized by the given byte budgets via `Session::new`. A disk cache tier can later be
/// injected here by switching this constructor to `Session::with_index_cache_backend` with a
/// custom `CacheBackend` implementation, without touching any other part of the service.
pub fn build_session(index_cache_bytes: usize, metadata_cache_bytes: usize) -> Arc<Session> {
    Arc::new(Session::new(
        index_cache_bytes,
        metadata_cache_bytes,
        Arc::new(ObjectStoreRegistry::default()),
    ))
}

/// Rejects org ids that are empty or contain characters outside `[A-Za-z0-9_-]`.
pub fn validate_org_id(org_id: &str) -> Result<(), SearchError> {
    let valid = !org_id.is_empty()
        && org_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if valid {
        Ok(())
    } else {
        Err(SearchError::invalid_argument(
            "org_id must be non-empty and match [A-Za-z0-9_-]+",
        ))
    }
}

/// Default provider: base-URI template, one shared Lance session, and an LRU of open handles.
pub struct CachingDatasetProvider {
    base_uri_template: String,
    session: Arc<Session>,
    datasets: Cache<String, Arc<Dataset>>,
}

impl CachingDatasetProvider {
    /// Creates the provider, building the shared session and sizing the dataset-handle LRU.
    pub fn new(config: &Config) -> Self {
        Self {
            base_uri_template: config.base_uri_template.clone(),
            session: build_session(config.index_cache_bytes, config.metadata_cache_bytes),
            datasets: Cache::new(config.dataset_cache_capacity),
        }
    }

    /// Resolves the dataset URI for one organization by substituting the `{org_id}` placeholder.
    fn dataset_uri(&self, org_id: &str) -> String {
        self.base_uri_template.replace("{org_id}", org_id)
    }
}

impl DatasetProvider for CachingDatasetProvider {
    /// Returns an open dataset handle for one organization, opening and caching it on a miss.
    ///
    /// Concurrent requests for the same org coalesce onto a single open via the Moka future cache.
    async fn dataset(&self, org_id: &str) -> Result<Arc<Dataset>, SearchError> {
        validate_org_id(org_id)?;
        let uri = self.dataset_uri(org_id);
        let session = self.session.clone();
        let open_uri = uri.clone();
        self.datasets
            .try_get_with(uri, async move {
                DatasetBuilder::from_uri(&open_uri)
                    .with_session(session)
                    .load()
                    .await
                    .map(Arc::new)
            })
            .await
            .map_err(|err: Arc<lance::Error>| classify_lance_error(err.as_ref()))
    }
}
