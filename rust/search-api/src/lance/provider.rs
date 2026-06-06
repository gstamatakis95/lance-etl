//! Dataset resolution: the provider trait and the caching base-URI implementation.

use std::sync::Arc;
use std::time::Duration;

use chrono::NaiveDate;
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::session::Session;
use lance_core::cache::CacheBackend;
use lance_io::object_store::{ChainedWrappingObjectStore, ObjectStoreParams, ObjectStoreRegistry, WrappingObjectStore};
use moka::future::Cache;

use crate::cache::disk_cache::DiskIndexCacheBackend;
use crate::cache::janitor::CacheJanitor;
use crate::cache::layout::prepare_cache_root;
use crate::cache::store_cache::MetadataByteCache;
use crate::config::Config;
use crate::domain::{DatasetTarget, SearchError};
use crate::lance::error::classify_lance_error;
use crate::telemetry::{CacheName, Metrics, Tier};

/// Resolves a dataset target (plus an optional day partition) to an open Lance dataset handle.
///
/// This is the seam for swapping dataset resolution strategies (URI layouts, catalogs,
/// per-tenant registries) without touching the search backend.
pub trait DatasetProvider: Send + Sync + 'static {
    /// Returns an open dataset handle for one target.
    ///
    /// `date` of `None` resolves the rangeless dataset
    /// (`{base}/{org}/{tenant}/{namespace}.lance`). `Some(day)` resolves that day's partition
    /// (`{base}/{org}/{tenant}/{namespace}/{day}.lance`).
    fn dataset(
        &self,
        target: &DatasetTarget,
        date: Option<NaiveDate>,
    ) -> impl Future<Output = Result<Arc<Dataset>, SearchError>> + Send;

    /// Approximate bytes resident in the shared index cache. Providers without one report 0.
    fn index_cache_size_bytes(&self) -> u64 {
        0
    }
}

/// Builds the single shared Lance session used by every dataset handle in the process.
///
/// Index and metadata cache entries are URI- and index-UUID-prefixed inside the session, so one
/// global cache safely spans tens of thousands of datasets. When a disk backend is given, the
/// index cache persists codec-bearing entries to local disk. The metadata cache always uses the
/// in-memory Moka backend sized by `metadata_cache_bytes` (lance exposes no metadata-cache
/// backend injection, persistent metadata comes from the [`MetadataByteCache`] store wrapper).
pub fn build_session(config: &Config, disk_backend: Option<Arc<DiskIndexCacheBackend>>) -> Arc<Session> {
    match disk_backend {
        Some(backend) => Arc::new(Session::with_index_cache_backend(
            backend as Arc<dyn CacheBackend>,
            config.metadata_cache_bytes,
            Arc::new(ObjectStoreRegistry::default()),
        )),
        None => Arc::new(Session::new(
            config.index_cache_bytes,
            config.metadata_cache_bytes,
            Arc::new(ObjectStoreRegistry::default()),
        )),
    }
}

/// Default provider: base-URI layout, one shared Lance session with optional disk-backed caches,
/// and an LRU of open handles.
pub struct CachingDatasetProvider {
    base_uri: String,
    session: Arc<Session>,
    datasets: Cache<String, Arc<Dataset>>,
    store_params: Option<ObjectStoreParams>,
    disk_index_cache: Option<Arc<DiskIndexCacheBackend>>,
    store_cache: Option<Arc<MetadataByteCache>>,
    metrics: Arc<Metrics>,
}

impl CachingDatasetProvider {
    /// Creates the provider with telemetry disabled, building the shared session, the disk cache
    /// tiers (unless disabled), and sizing the dataset-handle LRU. Disk cache setup failures fall
    /// back to in-memory caching so the service still serves traffic.
    pub fn new(config: &Config) -> Self {
        Self::build(config, Arc::new(Metrics::disabled()), None)
    }

    /// Like [`Self::new`] but emitting cache and dataset-resolution metrics through `metrics`.
    pub fn with_telemetry(config: &Config, metrics: Arc<Metrics>) -> Self {
        Self::build(config, metrics, None)
    }

    /// Like [`Self::new`] but chains an extra wrapper *inside* the metadata byte cache (between
    /// the cache and the real store). Used by tests to count the reads that pass through.
    pub fn with_inner_store_wrapper(config: &Config, inner_wrapper: Option<Arc<dyn WrappingObjectStore>>) -> Self {
        Self::build(config, Arc::new(Metrics::disabled()), inner_wrapper)
    }

    /// Shared constructor wiring the disk tiers, the store wrapper chain, and telemetry.
    fn build(config: &Config, metrics: Arc<Metrics>, inner_wrapper: Option<Arc<dyn WrappingObjectStore>>) -> Self {
        let (disk_index_cache, store_cache) = if config.disk_cache_disabled {
            (None, None)
        } else {
            match build_disk_caches(config, metrics.clone()) {
                Ok(caches) => caches,
                Err(error) => {
                    tracing::warn!(error = %error, "disk cache setup failed, falling back to memory-only caching");
                    (None, None)
                }
            }
        };
        let mut wrappers: Vec<Arc<dyn WrappingObjectStore>> = Vec::new();
        if let Some(inner) = inner_wrapper {
            wrappers.push(inner);
        }
        if let Some(store_cache) = &store_cache {
            wrappers.push(store_cache.clone());
        }
        let store_params = if wrappers.is_empty() {
            None
        } else {
            let wrapper: Arc<dyn WrappingObjectStore> = Arc::new(ChainedWrappingObjectStore::new(wrappers));
            Some(ObjectStoreParams {
                object_store_wrapper: Some(wrapper),
                ..Default::default()
            })
        };
        Self {
            base_uri: config.base_uri.clone(),
            session: build_session(config, disk_index_cache.clone()),
            datasets: Cache::new(config.dataset_cache_capacity),
            store_params,
            disk_index_cache,
            store_cache,
            metrics,
        }
    }

    /// Builds the janitor over both disk tiers. `None` when disk caching is disabled.
    pub fn janitor(&self, config: &Config) -> Option<CacheJanitor> {
        Some(CacheJanitor::new(
            self.disk_index_cache.clone()?,
            self.store_cache.clone()?,
            Duration::from_secs(config.disk_cache_ttl_secs),
            config.disk_index_cache_bytes,
            config.disk_store_cache_bytes,
            self.metrics.clone(),
        ))
    }

    /// The disk index cache backend, when disk caching is active.
    pub fn disk_index_cache(&self) -> Option<&Arc<DiskIndexCacheBackend>> {
        self.disk_index_cache.as_ref()
    }

    /// The metadata byte cache, when disk caching is active.
    pub fn store_cache(&self) -> Option<&Arc<MetadataByteCache>> {
        self.store_cache.as_ref()
    }

    /// Resolves the dataset URI of one target, optionally selecting one day partition.
    fn dataset_uri(&self, target: &DatasetTarget, date: Option<NaiveDate>) -> String {
        let base = &self.base_uri;
        let (org, tenant, namespace) = (&target.org_id, &target.tenant_id, &target.namespace);
        match date {
            None => format!("{base}/{org}/{tenant}/{namespace}.lance"),
            Some(day) => format!("{base}/{org}/{tenant}/{namespace}/{day}.lance"),
        }
    }
}

/// The two optional disk tiers: the index cache backend and the metadata byte cache.
type DiskCaches = (Option<Arc<DiskIndexCacheBackend>>, Option<Arc<MetadataByteCache>>);

/// Opens the two disk cache tiers under the versioned stamp directory.
fn build_disk_caches(config: &Config, metrics: Arc<Metrics>) -> std::io::Result<DiskCaches> {
    let root = prepare_cache_root(&config.cache_dir)?;
    let index_backend = DiskIndexCacheBackend::open(root.join("index"), config.index_cache_bytes, metrics.clone())?;
    let store_cache = MetadataByteCache::open(root.join("store"), config.store_cache_max_range_bytes, metrics)?;
    Ok((Some(Arc::new(index_backend)), Some(Arc::new(store_cache))))
}

impl DatasetProvider for CachingDatasetProvider {
    /// Returns an open dataset handle for one target, opening and caching it on a miss.
    ///
    /// Concurrent requests for the same URI coalesce onto a single open via the Moka future
    /// cache.
    #[tracing::instrument(
        name = "provider.dataset",
        skip_all,
        fields(
            org_id = %target.org_id,
            tenant_id = %target.tenant_id,
            namespace = %target.namespace,
            cache.dataset_handle_hit = tracing::field::Empty,
        )
    )]
    async fn dataset(&self, target: &DatasetTarget, date: Option<NaiveDate>) -> Result<Arc<Dataset>, SearchError> {
        target.validate()?;
        let started = std::time::Instant::now();
        let uri = self.dataset_uri(target, date);
        let session = self.session.clone();
        let open_uri = uri.clone();
        let store_params = self.store_params.clone();
        let opened = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let opened_flag = opened.clone();
        let result = self
            .datasets
            .try_get_with(uri, async move {
                opened_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                let mut builder = DatasetBuilder::from_uri(&open_uri).with_session(session);
                if let Some(params) = store_params {
                    builder = builder.with_store_params(params);
                }
                builder.load().await.map(Arc::new)
            })
            .await
            .map_err(|err: Arc<lance::Error>| classify_lance_error(err.as_ref()));
        let cold = opened.load(std::sync::atomic::Ordering::Relaxed);
        tracing::Span::current().record("cache.dataset_handle_hit", !cold);
        self.metrics.cache_lookup(CacheName::Handles, Tier::Memory, !cold);
        self.metrics.dataset_open(cold, started.elapsed());
        self.metrics.dataset_handles(self.datasets.entry_count());
        result
    }

    /// Approximate bytes resident in the shared index cache (memory tier plus disk tier).
    fn index_cache_size_bytes(&self) -> u64 {
        self.disk_index_cache
            .as_ref()
            .map(|backend| backend.approx_size_bytes() as u64)
            .unwrap_or(0)
    }
}
