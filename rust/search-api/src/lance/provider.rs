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
use crate::domain::{DatasetRef, DatasetTarget, SearchError};
use crate::lance::error::classify_lance_error;
use crate::telemetry::{CacheName, Metrics, Tier};

/// Upper bound on the weight a single open-dataset handle contributes to the handle-cache budget.
///
/// The handle cache is weighted by open fragment count (a cheap O(1) manifest read). Clamping that
/// proxy to this ceiling keeps a few whale handles from each consuming hundreds of budget units
/// while still guaranteeing every whale is admittable: as long as the configured weighted capacity
/// stays well above this value (the default is 16 384, this is 64), a whale handle is never
/// rejected for exceeding the cap. A whale therefore costs at most this many tiny-handle slots.
pub const MAX_HANDLE_WEIGHT: u32 = 64;

/// Weighs one open-dataset handle for the handle cache by its clamped open fragment count.
///
/// `count_fragments` reads only the in-memory manifest length, so this is O(1). The result is
/// clamped to `[1, MAX_HANDLE_WEIGHT]`: a tiny single-fragment handle weighs one unit, and a heavy
/// whale handle weighs at most [`MAX_HANDLE_WEIGHT`], bounding its share of the budget.
pub fn handle_weight(dataset: &Dataset) -> u32 {
    (dataset.count_fragments() as u64).clamp(1, MAX_HANDLE_WEIGHT as u64) as u32
}

/// Resolves a dataset target (plus an optional day partition and a version selector) to an open
/// Lance dataset handle.
///
/// This is the seam for swapping dataset resolution strategies (URI layouts, catalogs,
/// per-tenant registries, alternative blue-green schemes) without touching the search backend.
/// Implementations own version/tag resolution and the open-handle cache. The backend only states
/// *which* version it wants via [`DatasetRef`].
pub trait DatasetProvider: Send + Sync + 'static {
    /// Returns an open dataset handle for one target at the selected version.
    ///
    /// `date` of `None` resolves the rangeless dataset
    /// (`{base}/{org}/{tenant}/{namespace}.lance`). `Some(day)` resolves that day's partition
    /// (`{base}/{org}/{tenant}/{namespace}/{day}.lance`).
    ///
    /// `reference` selects the committed version: [`DatasetRef::Serve`] follows the provider's
    /// configured serve policy (a resolved serve tag, or latest), [`DatasetRef::Latest`] always
    /// opens latest, and [`DatasetRef::Version`]/[`DatasetRef::Tag`] pin an explicit version. A
    /// version-pinned open keys the handle cache on the resolved version, so blue and green
    /// versions of one dataset coexist and a tag flip selects a different handle rather than
    /// mutating one.
    fn dataset(
        &self,
        target: &DatasetTarget,
        date: Option<NaiveDate>,
        reference: DatasetRef,
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
/// and an LRU of open handles keyed by `(uri, resolved version)` and bounded by total handle
/// weight ([`handle_weight`]) rather than a flat entry count, so the cheap tiny-tenant tail stays
/// resident while a few heavy whale handles are capped.
///
/// Blue-green serving: when `serve_by_tag` is on, [`DatasetRef::Serve`] resolves `serve_tag` to a
/// concrete version through `tag_versions` (a short-TTL cache, so a tag flip propagates within the
/// TTL without a manifest read per request) and opens that exact version. Because the handle cache
/// and Lance's own version- and index-UUID-scoped disk/metadata caches all key on the resolved
/// version, a freshly built green version that was prewarmed by version is served warm the moment
/// the tag flips onto it, while the draining blue handle ages out by capacity.
pub struct CachingDatasetProvider {
    base_uri: String,
    session: Arc<Session>,
    /// Open-handle LRU keyed by `(uri, resolved version)` and bounded by total [`handle_weight`].
    datasets: Cache<(String, Option<u64>), Arc<Dataset>>,
    tag_versions: Cache<(String, String), u64>,
    last_tag_version: Cache<(String, String), u64>,
    last_prewarmed: Cache<String, u64>,
    serve_by_tag: bool,
    serve_tag: String,
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
        let block_size = Some(config.io_block_size_bytes);
        let store_params = if wrappers.is_empty() {
            Some(ObjectStoreParams {
                block_size,
                ..Default::default()
            })
        } else {
            let wrapper: Arc<dyn WrappingObjectStore> = Arc::new(ChainedWrappingObjectStore::new(wrappers));
            Some(ObjectStoreParams {
                block_size,
                object_store_wrapper: Some(wrapper),
                ..Default::default()
            })
        };
        Self {
            base_uri: config.base_uri.clone(),
            session: build_session(config, disk_index_cache.clone()),
            datasets: Cache::builder()
                .max_capacity(config.dataset_cache_capacity)
                .weigher(|_key: &(String, Option<u64>), dataset: &Arc<Dataset>| handle_weight(dataset))
                .build(),
            tag_versions: Cache::builder()
                .max_capacity(config.dataset_cache_capacity)
                .time_to_live(Duration::from_secs(config.serve_tag_ttl_secs))
                .build(),
            last_tag_version: Cache::new(config.dataset_cache_capacity),
            last_prewarmed: Cache::new(config.dataset_cache_capacity),
            serve_by_tag: config.serve_by_tag,
            serve_tag: config.serve_tag.clone(),
            store_params,
            disk_index_cache,
            store_cache,
            metrics,
        }
    }

    /// Forces any pending handle-cache maintenance to run, then reports the cache's
    /// `(entry_count, weighted_size)`.
    ///
    /// Moka applies admissions and evictions lazily on background maintenance, so the live
    /// `entry_count`/`weighted_size` are only eventually consistent. Tests and introspection that
    /// need the settled figures call this to drain pending tasks first. The weighted size is the
    /// sum of every resident handle's [`handle_weight`] and never exceeds the configured capacity.
    pub async fn handle_cache_stats(&self) -> (u64, u64) {
        self.datasets.run_pending_tasks().await;
        (self.datasets.entry_count(), self.datasets.weighted_size())
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

    /// Resolves a [`DatasetRef`] to the concrete version to open and whether the open is a
    /// warm-intent (prewarm) pin.
    ///
    /// `Serve` follows the serve policy: the serve tag when `serve_by_tag` is on, else latest, and
    /// is the only serving (non-warm) reference. `Latest`, `Version`, and `Tag` all come from the
    /// prewarm path and mark the open as warm-intent, which is how prewarm is distinguished from
    /// serving for the cold-open telemetry. `Latest` resolves to no version (the `(uri, None)`
    /// handle key), so a later serving open of the same latest handle reads back the prewarmed
    /// version and reports `warmed:true`. A returned version of `None` means open the latest
    /// manifest.
    async fn resolve_reference(&self, uri: &str, reference: DatasetRef) -> Result<ResolvedRef, SearchError> {
        let warm_intent = matches!(
            reference,
            DatasetRef::Version(_) | DatasetRef::Tag(_) | DatasetRef::Latest
        );
        let version = match reference {
            DatasetRef::Latest => None,
            DatasetRef::Serve if !self.serve_by_tag => None,
            DatasetRef::Serve => Some(self.resolve_tag_version(uri, &self.serve_tag).await?),
            DatasetRef::Version(version) => Some(version),
            DatasetRef::Tag(tag) => Some(self.resolve_tag_version(uri, &tag).await?),
        };
        Ok(ResolvedRef { version, warm_intent })
    }

    /// Resolves a tag to its committed version, trusting a cached resolution for the serve-tag TTL.
    ///
    /// Within the TTL the cached version is reused (zero manifest reads). On a miss the tag JSON is
    /// re-read once (it is never cached by the byte cache, so the read is always live) and the
    /// `serve.tag_resolved` counter is emitted, tagged with whether the resolved version changed
    /// from the last time this tag was resolved. This bounds tag-flip propagation to the TTL.
    ///
    /// Concurrent callers for the same key coalesce onto a single read through the Moka future
    /// cache's `try_get_with`, which runs the loader at most once per key per TTL window. This caps
    /// a fleet-wide simultaneous-expiry burst at one live manifest read per process per tag.
    async fn resolve_tag_version(&self, uri: &str, tag: &str) -> Result<u64, SearchError> {
        let key = (uri.to_string(), tag.to_string());
        self.tag_versions
            .try_get_with(key.clone(), async {
                let version = self.read_tag_version(uri, tag).await?;
                let changed = self.last_tag_version.get(&key).await != Some(version);
                self.metrics.serve_tag_resolved(changed);
                self.last_tag_version.insert(key.clone(), version).await;
                Ok::<u64, SearchError>(version)
            })
            .await
            .map_err(|err| (*err).clone())
    }

    /// Reads which committed version a tag currently points at by opening at the tag and reporting
    /// the loaded manifest version. The manifest read is cache-served, the tag JSON read is live.
    async fn read_tag_version(&self, uri: &str, tag: &str) -> Result<u64, SearchError> {
        let mut builder = DatasetBuilder::from_uri(uri)
            .with_session(self.session.clone())
            .with_tag(tag);
        if let Some(params) = self.store_params.clone() {
            builder = builder.with_store_params(params);
        }
        let dataset = builder.load().await.map_err(|err| classify_lance_error(&err))?;
        Ok(dataset.version_id())
    }

    /// Records cold-open telemetry for one freshly opened handle.
    ///
    /// A warm-intent (prewarm) open records the warmed version per URI. A serving open compares
    /// the opened version against the last prewarmed version for that URI and emits
    /// `serve.cold_open` tagged `warmed`, so a flip that serving reached before prewarm did shows
    /// up as `warmed:false`.
    async fn record_cold_open(&self, uri: &str, opened_version: u64, warm_intent: bool) {
        if warm_intent {
            self.last_prewarmed.insert(uri.to_string(), opened_version).await;
        } else {
            let warmed = self.last_prewarmed.get(uri).await == Some(opened_version);
            self.metrics.serve_cold_open(warmed);
        }
    }
}

/// A [`DatasetRef`] resolved against the serve policy: the concrete version to open (`None` means
/// latest) and whether the open is a warm-intent (prewarm) pin.
struct ResolvedRef {
    /// The committed version to open, or `None` to open the latest manifest.
    version: Option<u64>,
    /// True when the open came from an explicit version/tag pin (prewarm), false for serving.
    warm_intent: bool,
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
            dataset.version = tracing::field::Empty,
            cache.dataset_handle_hit = tracing::field::Empty,
        )
    )]
    async fn dataset(
        &self,
        target: &DatasetTarget,
        date: Option<NaiveDate>,
        reference: DatasetRef,
    ) -> Result<Arc<Dataset>, SearchError> {
        target.validate()?;
        let started = std::time::Instant::now();
        let uri = self.dataset_uri(target, date);
        let resolved = self.resolve_reference(&uri, reference).await?;
        let key = (uri.clone(), resolved.version);
        let session = self.session.clone();
        let open_uri = uri.clone();
        let store_params = self.store_params.clone();
        let version = resolved.version;
        let opened = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let opened_flag = opened.clone();
        let result = self
            .datasets
            .try_get_with(key, async move {
                opened_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                let mut builder = DatasetBuilder::from_uri(&open_uri).with_session(session);
                if let Some(version) = version {
                    builder = builder.with_version(version);
                }
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
        self.metrics.dataset_handles_weighted(self.datasets.weighted_size());
        if cold && let Ok(dataset) = &result {
            let opened_version = dataset.version_id();
            tracing::Span::current().record("dataset.version", opened_version);
            self.record_cold_open(&uri, opened_version, resolved.warm_intent).await;
        }
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
