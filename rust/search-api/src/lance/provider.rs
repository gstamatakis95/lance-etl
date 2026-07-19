//! Dataset resolution: the provider trait and the caching base-URI implementation.

use std::sync::Arc;
use std::time::{Duration, Instant};

use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::session::Session;
use lance_core::cache::CacheBackend;
use lance_io::object_store::{ChainedWrappingObjectStore, ObjectStoreParams, ObjectStoreRegistry, WrappingObjectStore};
use moka::Expiry;
use moka::future::Cache;

use crate::cache::disk_store::DiskEntryStore;
use crate::cache::index_cache::HybridIndexCacheBackend;
use crate::cache::janitor::CacheJanitor;
use crate::cache::layout::prepare_cache_root;
use crate::cache::redis_store::RedisEntryStore;
use crate::cache::store_cache::MetadataByteCache;
use crate::config::{CacheBackendKind, Config, PRODUCTION_SERVE_TAG};
use crate::domain::{DatasetRef, DatasetTarget, SearchError, ServingCatalog, ServingRoute};
use crate::lance::error::{classify_lance_error, is_definitive_open_absence};
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

/// Per-entry expiry policy for the open-handle LRU.
///
/// An unpinned `(uri, None)` handle tracks the dataset's *latest* version, so it must not
/// outlive the freshness window — otherwise a low-traffic tenant would keep serving the version
/// captured at first open until capacity pressure happened to evict the handle. Version-pinned
/// `(uri, Some(v))` handles are immutable snapshots and never expire by time; capacity weighting
/// alone bounds them. The window reuses `serve_tag_ttl_secs` for the explicit `Latest` path.
struct UnpinnedHandleExpiry {
    ttl: Duration,
}

impl Expiry<(String, Option<u64>), Arc<Dataset>> for UnpinnedHandleExpiry {
    fn expire_after_create(
        &self,
        key: &(String, Option<u64>),
        _value: &Arc<Dataset>,
        _created_at: Instant,
    ) -> Option<Duration> {
        match key.1 {
            None => Some(self.ttl),
            Some(_) => None,
        }
    }
}

/// Resolves a dataset target (plus a version selector) to an open Lance dataset handle.
///
/// This is the seam for swapping dataset resolution strategies (URI layouts, catalogs,
/// per-tenant registries, alternative blue-green schemes) without touching the search backend.
/// Implementations own version/tag resolution and the open-handle cache. The backend only states
/// *which* version it wants via [`DatasetRef`].
pub trait DatasetProvider: Send + Sync + 'static {
    /// Returns an open dataset handle for one target at the selected version.
    ///
    /// The target resolves to `{base}/{org}/{tenant}/{namespace}.lance`.
    ///
    /// `reference` selects the committed version: [`DatasetRef::Serve`] resolves the fixed
    /// production `HEAD` tag, [`DatasetRef::Latest`] opens latest (freshness-bounded: a cached
    /// latest handle is refreshed within the tag TTL, so a new commit becomes visible within that
    /// window), and
    /// [`DatasetRef::Version`]/[`DatasetRef::Tag`] pin an explicit version. A version-pinned
    /// open keys the handle cache on the resolved version, so blue and green versions of one
    /// dataset coexist and a tag flip selects a different handle rather than mutating one.
    fn dataset(
        &self,
        target: &DatasetTarget,
        reference: DatasetRef,
    ) -> impl Future<Output = Result<Arc<Dataset>, SearchError>> + Send;

    /// Returns an open dataset handle for a PREWARM open of one target.
    ///
    /// Identical to [`DatasetProvider::dataset`] except the open carries warm intent: providers
    /// tracking cold-open telemetry record it as a prewarm instead of a serving open. The
    /// intent must be explicit because serving requests can pin the same
    /// [`DatasetRef::Tag`]/[`DatasetRef::Version`] references prewarm uses. The default
    /// implementation just delegates, for providers without cold-open telemetry.
    fn dataset_for_prewarm(
        &self,
        target: &DatasetTarget,
        reference: DatasetRef,
    ) -> impl Future<Output = Result<Arc<Dataset>, SearchError>> + Send {
        self.dataset(target, reference)
    }

    /// Opens one caller-supplied exact route for authenticated replica-local prewarm.
    fn dataset_for_exact_prewarm(
        &self,
        target: &DatasetTarget,
        route: ServingRoute,
    ) -> impl Future<Output = Result<Arc<Dataset>, SearchError>> + Send {
        async move {
            let _ = (target, route);
            Err(SearchError::internal("exact prewarm is not supported by this provider"))
        }
    }

    /// Approximate bytes resident in the shared index cache. Providers without one report 0.
    fn index_cache_size_bytes(&self) -> u64 {
        0
    }
}

/// Builds the single shared Lance session used by every dataset handle in the process.
///
/// Index and metadata cache entries are URI- and index-UUID-prefixed inside the session, so one
/// global cache safely spans tens of thousands of datasets. When a hybrid backend is given, the
/// index cache persists codec-bearing entries through its configured store (disk or Redis). The
/// metadata cache always uses the in-memory Moka backend sized by `metadata_cache_bytes` (lance
/// exposes no metadata-cache backend injection, persistent metadata comes from the
/// [`MetadataByteCache`] store wrapper).
pub fn build_session(config: &Config, index_backend: Option<Arc<HybridIndexCacheBackend>>) -> Arc<Session> {
    match index_backend {
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

/// Default provider: base-URI layout, one shared Lance session with the configured persistent
/// caches (disk, Redis, or memory-only),
/// and an LRU of open handles keyed by `(uri, resolved version)` and bounded by total handle
/// weight ([`handle_weight`]) rather than a flat entry count, so the cheap tiny-tenant tail stays
/// resident while a few heavy whale handles are capped.
///
/// Blue-green serving: [`DatasetRef::Serve`] resolves the fixed production `HEAD` tag to a concrete
/// version through `tag_versions` (a short-TTL cache, so a tag flip propagates within the TTL
/// without a manifest read per request) and opens that exact version. Because the handle cache
/// and Lance's own version- and index-UUID-scoped disk/metadata caches all key on the resolved
/// version, a freshly built green version that was prewarmed by version is served warm the moment
/// the tag flips onto it, while the draining blue handle ages out by capacity.
pub struct CachingDatasetProvider {
    base_uri: String,
    catalog: Option<Arc<dyn ServingCatalog>>,
    serving_routes: Cache<DatasetTarget, ServingRoute>,
    session: Arc<Session>,
    /// Open-handle LRU keyed by `(uri, resolved version)` and bounded by total [`handle_weight`].
    datasets: Cache<(String, Option<u64>), Arc<Dataset>>,
    /// Short-TTL negative cache of dataset opens that failed with NotFound, keyed by
    /// `(uri, selector)` where the selector encodes the resolved reference intent (`latest`,
    /// `version:{n}`, or `tag:{name}`). Keying on the reference — rather than a resolved version
    /// that a failed tag resolution never produces — lets the cache cover tag-addressed misses
    /// (`DatasetRef::Tag` and every production `HEAD` request) as well as `Latest`/`Version`, so a hot
    /// loop of requests for a nonexistent dataset or tag is answered from here instead of hammering
    /// the object store. Only definitive dataset, reference, or version absence is cached. A
    /// generic missing object inside a live dataset is treated as transient and never cached. The TTL
    /// ([`crate::config::DEFAULT_NEGATIVE_OPEN_TTL_SECS`]) bounds how long a freshly created
    /// dataset can still be reported missing.
    negative_opens: Cache<(String, String), SearchError>,
    tag_versions: Cache<(String, String), u64>,
    last_tag_version: Cache<(String, String), u64>,
    last_prewarmed: Cache<String, u64>,
    store_params: Option<ObjectStoreParams>,
    index_cache: Option<Arc<HybridIndexCacheBackend>>,
    store_cache: Option<Arc<MetadataByteCache>>,
    disk_stores: Option<(Arc<DiskEntryStore>, Arc<DiskEntryStore>)>,
    metrics: Arc<Metrics>,
}

impl CachingDatasetProvider {
    /// Creates the provider with telemetry disabled, building the shared session, the configured
    /// persistent cache tiers, and sizing the dataset-handle LRU. Cache backend setup failures
    /// fall back to in-memory caching so the service still serves traffic.
    pub async fn new(config: &Config) -> Self {
        Self::build(config, Arc::new(Metrics::disabled()), None, None).await
    }

    /// Like [`Self::new`] but emitting cache and dataset-resolution metrics through `metrics`.
    pub async fn with_telemetry(config: &Config, metrics: Arc<Metrics>) -> Self {
        Self::build(config, metrics, None, None).await
    }

    /// Creates the production provider backed by an exact serving catalog.
    pub async fn with_catalog_and_telemetry(
        config: &Config,
        catalog: Arc<dyn ServingCatalog>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self::build(config, metrics, None, Some(catalog)).await
    }

    /// Creates a provider with an exact serving catalog and an optional inner store wrapper.
    pub async fn with_catalog_and_inner_store_wrapper(
        config: &Config,
        catalog: Arc<dyn ServingCatalog>,
        inner_wrapper: Option<Arc<dyn WrappingObjectStore>>,
    ) -> Self {
        Self::build(config, Arc::new(Metrics::disabled()), inner_wrapper, Some(catalog)).await
    }

    /// Like [`Self::new`] but chains an extra wrapper *inside* the metadata byte cache (between
    /// the cache and the real store). Used by tests to count the reads that pass through.
    pub async fn with_inner_store_wrapper(
        config: &Config,
        inner_wrapper: Option<Arc<dyn WrappingObjectStore>>,
    ) -> Self {
        Self::build(config, Arc::new(Metrics::disabled()), inner_wrapper, None).await
    }

    /// Shared constructor wiring the persistent tiers, the store wrapper chain, and telemetry.
    async fn build(
        config: &Config,
        metrics: Arc<Metrics>,
        inner_wrapper: Option<Arc<dyn WrappingObjectStore>>,
        catalog: Option<Arc<dyn ServingCatalog>>,
    ) -> Self {
        let caches = match config.cache_backend {
            CacheBackendKind::Memory => BuiltCaches::none(),
            CacheBackendKind::Disk => build_disk_caches(config, metrics.clone()).unwrap_or_else(|error| {
                tracing::warn!(error = %error, "disk cache setup failed, falling back to memory-only caching");
                BuiltCaches::none()
            }),
            CacheBackendKind::Redis => build_redis_caches(config, metrics.clone())
                .await
                .unwrap_or_else(|error| {
                    tracing::warn!(error = %error, "redis cache setup failed, falling back to memory-only caching");
                    BuiltCaches::none()
                }),
        };
        let BuiltCaches {
            index_cache,
            store_cache,
            disk_stores,
        } = caches;
        let mut wrappers: Vec<Arc<dyn WrappingObjectStore>> = Vec::new();
        if let Some(inner) = inner_wrapper {
            wrappers.push(inner);
        }
        if let Some(store_cache) = &store_cache {
            wrappers.push(store_cache.clone());
        }
        let block_size = Some(crate::config::DEFAULT_IO_BLOCK_SIZE_BYTES);
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
            catalog,
            serving_routes: Cache::builder()
                .max_capacity(config.dataset_cache_capacity)
                .time_to_live(Duration::from_secs(crate::config::DEFAULT_SERVING_CATALOG_TTL_SECS))
                .build(),
            session: build_session(config, index_cache.clone()),
            datasets: Cache::builder()
                .max_capacity(config.dataset_cache_capacity)
                .weigher(|_key: &(String, Option<u64>), dataset: &Arc<Dataset>| handle_weight(dataset))
                .expire_after(UnpinnedHandleExpiry {
                    ttl: Duration::from_secs(config.serve_tag_ttl_secs),
                })
                .build(),
            negative_opens: Cache::builder()
                .max_capacity(config.dataset_cache_capacity)
                .time_to_live(Duration::from_secs(crate::config::DEFAULT_NEGATIVE_OPEN_TTL_SECS))
                .build(),
            tag_versions: Cache::builder()
                .max_capacity(config.dataset_cache_capacity)
                .time_to_live(Duration::from_secs(config.serve_tag_ttl_secs))
                .build(),
            last_tag_version: Cache::new(config.dataset_cache_capacity),
            last_prewarmed: Cache::new(config.dataset_cache_capacity),
            store_params,
            index_cache,
            store_cache,
            disk_stores,
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

    /// Builds the janitor over both disk tiers. `None` for the redis and memory backends, whose
    /// expiry and capacity are handled by the Redis server (native TTL plus `maxmemory`) or by
    /// Moka respectively.
    pub fn janitor(&self, config: &Config) -> Option<CacheJanitor> {
        let (index_store, metadata_store) = self.disk_stores.clone()?;
        Some(CacheJanitor::new(
            index_store,
            metadata_store,
            Duration::from_secs(crate::config::DEFAULT_DISK_CACHE_TTL_SECS),
            config.disk_index_cache_bytes,
            config.disk_store_cache_bytes,
            self.metrics.clone(),
        ))
    }

    /// The hybrid index cache backend, when a persistent backend is active.
    pub fn index_cache(&self) -> Option<&Arc<HybridIndexCacheBackend>> {
        self.index_cache.as_ref()
    }

    /// The metadata byte cache, when a persistent backend is active.
    pub fn store_cache(&self) -> Option<&Arc<MetadataByteCache>> {
        self.store_cache.as_ref()
    }

    /// Resolves the dataset URI of one target.
    fn dataset_uri(&self, target: &DatasetTarget) -> String {
        let base = &self.base_uri;
        let (org, tenant, namespace) = (&target.org_id, &target.tenant_id, &target.namespace);
        format!("{base}/{org}/{tenant}/{namespace}.lance")
    }

    /// Resolves and validates the exact catalog tuple for one production serving request.
    async fn serving_route(&self, target: &DatasetTarget) -> Result<ServingRoute, SearchError> {
        target.validate()?;
        let catalog = self
            .catalog
            .as_ref()
            .ok_or_else(|| SearchError::internal("serving catalog is not configured"))?
            .clone();
        let target_key = target.clone();
        let route = self
            .serving_routes
            .try_get_with(target_key.clone(), async move { catalog.resolve(&target_key).await })
            .await
            .map_err(|error| error.as_ref().clone())?;
        validate_serving_route(&self.base_uri, &route)?;
        Ok(route)
    }

    /// Builds the `negative_opens` key for a `(uri, reference)` pair.
    ///
    /// The selector mirrors what [`Self::resolve_reference`] will consult, so a NotFound recorded
    /// under this key short-circuits every subsequent identical request within the TTL — including
    /// the tag paths, whose resolution failure happens before any version is known. `Serve`
    /// collapses onto `tag:HEAD`, and an explicit `Tag("HEAD")` request shares that key because
    /// both open the same thing.
    fn negative_open_key(&self, uri: &str, reference: &DatasetRef) -> (String, String) {
        let selector = match reference {
            DatasetRef::Latest => "latest".to_string(),
            DatasetRef::Serve => format!("tag:{PRODUCTION_SERVE_TAG}"),
            DatasetRef::Version(version) => format!("version:{version}"),
            DatasetRef::Tag(tag) => format!("tag:{tag}"),
        };
        (uri.to_string(), selector)
    }

    /// Resolves a [`DatasetRef`] to the concrete version to open.
    ///
    /// `Serve` resolves the fixed production `HEAD` tag. `Latest` resolves to no version (the
    /// `(uri, None)` handle key), so a later open
    /// of the same latest handle reads back a prewarmed version and reports `warmed:true`. The
    /// `(uri, None)` handle itself expires after the serve-tag TTL ([`UnpinnedHandleExpiry`]),
    /// so a commit that lands after the open becomes visible within one TTL window.
    /// `Version` and `Tag` pin an explicit committed version, whether the open is a prewarm or
    /// a serving request pinned by `version_ref` — the open's INTENT is carried separately (see
    /// [`DatasetProvider::dataset_for_prewarm`]), never inferred from the ref. A returned
    /// version of `None` means open the latest manifest.
    async fn resolve_reference(&self, uri: &str, reference: DatasetRef) -> Result<Option<u64>, Arc<lance::Error>> {
        Ok(match reference {
            DatasetRef::Latest => None,
            DatasetRef::Serve => Some(self.resolve_tag_version(uri, PRODUCTION_SERVE_TAG).await?),
            DatasetRef::Version(version) => Some(version),
            DatasetRef::Tag(tag) => Some(self.resolve_tag_version(uri, &tag).await?),
        })
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
    async fn resolve_tag_version(&self, uri: &str, tag: &str) -> Result<u64, Arc<lance::Error>> {
        let key = (uri.to_string(), tag.to_string());
        self.tag_versions
            .try_get_with(key.clone(), async {
                let version = self.read_tag_version(uri, tag).await?;
                let changed = self.last_tag_version.get(&key).await != Some(version);
                self.metrics.serve_tag_resolved(changed);
                self.last_tag_version.insert(key.clone(), version).await;
                Ok::<u64, lance::Error>(version)
            })
            .await
    }

    /// Reads which committed version a tag currently points at by opening at the tag and reporting
    /// the loaded manifest version. The manifest read is cache-served, the tag JSON read is live.
    async fn read_tag_version(&self, uri: &str, tag: &str) -> Result<u64, lance::Error> {
        let mut builder = DatasetBuilder::from_uri(uri)
            .with_session(self.session.clone())
            .with_tag(tag);
        if let Some(params) = self.store_params.clone() {
            builder = builder.with_store_params(params);
        }
        let dataset = builder.load().await?;
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

/// The constructed persistent cache tiers plus the raw disk stores the janitor sweeps.
struct BuiltCaches {
    /// Hybrid index cache backend injected into the Lance session, when persistence is active.
    index_cache: Option<Arc<HybridIndexCacheBackend>>,
    /// Metadata byte cache pushed into the object-store wrapper chain, when persistence is active.
    store_cache: Option<Arc<MetadataByteCache>>,
    /// The two disk stores for janitor construction. `None` for the redis and memory backends.
    disk_stores: Option<(Arc<DiskEntryStore>, Arc<DiskEntryStore>)>,
}

impl BuiltCaches {
    /// The memory-only outcome: no persistent tiers and nothing for the janitor to sweep.
    fn none() -> Self {
        Self {
            index_cache: None,
            store_cache: None,
            disk_stores: None,
        }
    }
}

/// Classifies one raw open failure and records it only when absence is definitive.
async fn classify_open_failure(
    negative_opens: &Cache<(String, String), SearchError>,
    negative_key: (String, String),
    raw: &lance::Error,
) -> SearchError {
    let error = classify_lance_error(raw);
    if is_definitive_open_absence(raw) {
        negative_opens.insert(negative_key, error.clone()).await;
    }
    error
}

/// Validates that a catalog route stays inside the deployment-owned storage prefix.
fn validate_serving_route(base_uri: &str, route: &ServingRoute) -> Result<(), SearchError> {
    if route.lance_version == 0 {
        return Err(SearchError::internal("serving catalog contains Lance version zero"));
    }
    let allowed_prefix = format!("{}/", base_uri.trim_end_matches('/'));
    if !route.lance_uri.starts_with(&allowed_prefix)
        || route.lance_uri[allowed_prefix.len()..]
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(SearchError::internal(
            "serving catalog URI is outside the allowed base URI",
        ));
    }
    Ok(())
}

/// Opens the two disk cache tiers under the versioned stamp directory.
fn build_disk_caches(config: &Config, metrics: Arc<Metrics>) -> std::io::Result<BuiltCaches> {
    let root = prepare_cache_root(&config.cache_dir)?;
    let index_store = Arc::new(DiskEntryStore::open(root.join("index"))?);
    let metadata_store = Arc::new(DiskEntryStore::open(root.join("store"))?);
    let index_backend = HybridIndexCacheBackend::new(index_store.clone(), config.index_cache_bytes, metrics.clone());
    let store_cache = MetadataByteCache::new(
        metadata_store.clone(),
        crate::config::DEFAULT_STORE_CACHE_MAX_RANGE_BYTES,
        metrics,
    );
    Ok(BuiltCaches {
        index_cache: Some(Arc::new(index_backend)),
        store_cache: Some(Arc::new(store_cache)),
        disk_stores: Some((index_store, metadata_store)),
    })
}

/// Connects the two Redis-backed cache tiers and spawns the index tier's registry hygiene loop.
///
/// Each tier gets its own connection under its own key namespace segment (`index` / `store`).
/// A connection failure (bounded by a short timeout) surfaces here so the caller can fall back
/// to memory-only caching, exactly as a disk setup failure does.
async fn build_redis_caches(config: &Config, metrics: Arc<Metrics>) -> Result<BuiltCaches, redis::RedisError> {
    let url = config.redis_url.as_deref().ok_or_else(|| {
        redis::RedisError::from((
            redis::ErrorKind::InvalidClientConfig,
            "redis_url must be set for the redis cache backend",
        ))
    })?;
    let ttl = Duration::from_secs(crate::config::DEFAULT_DISK_CACHE_TTL_SECS);
    let index_store = Arc::new(
        RedisEntryStore::connect(
            url,
            &config.redis_namespace,
            "index",
            ttl,
            CacheName::Index,
            metrics.clone(),
        )
        .await?,
    );
    let metadata_store = Arc::new(
        RedisEntryStore::connect(
            url,
            &config.redis_namespace,
            "store",
            ttl,
            CacheName::Store,
            metrics.clone(),
        )
        .await?,
    );
    drop(index_store.spawn_registry_hygiene(Duration::from_secs(crate::config::REDIS_REGISTRY_HYGIENE_SECS)));
    let index_backend = HybridIndexCacheBackend::new(index_store, config.index_cache_bytes, metrics.clone());
    let store_cache = MetadataByteCache::new(
        metadata_store,
        crate::config::DEFAULT_STORE_CACHE_MAX_RANGE_BYTES,
        metrics,
    );
    Ok(BuiltCaches {
        index_cache: Some(Arc::new(index_backend)),
        store_cache: Some(Arc::new(store_cache)),
        disk_stores: None,
    })
}

impl CachingDatasetProvider {
    /// Returns an open dataset handle for one target, opening and caching it on a miss.
    ///
    /// Concurrent requests for the same URI coalesce onto a single open via the Moka future
    /// cache. `warm_intent` states whether the open came from the prewarm path (recorded as
    /// the warmed version) or from serving (compared against the warmed version for the
    /// `serve.cold_open` metric) — it is passed explicitly by the two trait entry points
    /// because serving requests can pin the same tag/version references prewarm uses.
    ///
    /// A definitive absent dataset, reference, or version is negatively cached for a short TTL
    /// (`negative_opens`), so a hot loop does not hammer the object store. Generic missing-object
    /// and other transient failures are never negatively cached.
    #[tracing::instrument(
        name = "provider.dataset",
        skip_all,
        fields(
            dataset.version = tracing::field::Empty,
            cache.dataset_handle_hit = tracing::field::Empty,
        )
    )]
    async fn open(
        &self,
        target: &DatasetTarget,
        reference: DatasetRef,
        warm_intent: bool,
        route_override: Option<ServingRoute>,
    ) -> Result<Arc<Dataset>, SearchError> {
        target.validate()?;
        let started = std::time::Instant::now();
        if let Some(route) = &route_override {
            validate_serving_route(&self.base_uri, route)?;
        }
        let catalog_route = if route_override.is_some() {
            route_override
        } else if reference == DatasetRef::Serve && self.catalog.is_some() {
            Some(self.serving_route(target).await?)
        } else {
            None
        };
        let uri = catalog_route
            .as_ref()
            .map(|route| route.lance_uri.clone())
            .unwrap_or_else(|| self.dataset_uri(target));
        let negative_key = match &catalog_route {
            Some(route) => (uri.clone(), format!("version:{}", route.lance_version)),
            None => self.negative_open_key(&uri, &reference),
        };
        if let Some(cached) = self.negative_opens.get(&negative_key).await {
            return Err(cached);
        }
        let version = match catalog_route {
            Some(route) => Some(route.lance_version),
            None => match self.resolve_reference(&uri, reference).await {
                Ok(version) => version,
                Err(raw) => {
                    let error = classify_open_failure(&self.negative_opens, negative_key, raw.as_ref()).await;
                    return Err(error);
                }
            },
        };
        let key = (uri.clone(), version);
        let session = self.session.clone();
        let open_uri = uri.clone();
        let store_params = self.store_params.clone();
        let opened = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let opened_flag = opened.clone();
        let loaded = self
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
            .await;
        let result = match loaded {
            Ok(dataset) => Ok(dataset),
            Err(raw) => Err(classify_open_failure(&self.negative_opens, negative_key, raw.as_ref()).await),
        };
        let cold = opened.load(std::sync::atomic::Ordering::Relaxed);
        tracing::Span::current().record("cache.dataset_handle_hit", !cold);
        self.metrics.cache_lookup(CacheName::Handles, Tier::Memory, !cold);
        self.metrics.dataset_open(cold, started.elapsed());
        self.metrics.dataset_handles(self.datasets.entry_count());
        self.metrics.dataset_handles_weighted(self.datasets.weighted_size());
        if cold && let Ok(dataset) = &result {
            let opened_version = dataset.version_id();
            tracing::Span::current().record("dataset.version", opened_version);
            self.record_cold_open(&uri, opened_version, warm_intent).await;
        }
        result
    }
}

impl DatasetProvider for CachingDatasetProvider {
    /// Returns an open dataset handle for a serving open, counting cold-open telemetry.
    async fn dataset(&self, target: &DatasetTarget, reference: DatasetRef) -> Result<Arc<Dataset>, SearchError> {
        self.open(target, reference, false, None).await
    }

    /// Returns an open dataset handle for a prewarm open, recording the warmed version.
    async fn dataset_for_prewarm(
        &self,
        target: &DatasetTarget,
        reference: DatasetRef,
    ) -> Result<Arc<Dataset>, SearchError> {
        self.open(target, reference, true, None).await
    }

    /// Opens and records one authenticated exact candidate route without consulting mutable state.
    async fn dataset_for_exact_prewarm(
        &self,
        target: &DatasetTarget,
        route: ServingRoute,
    ) -> Result<Arc<Dataset>, SearchError> {
        self.open(target, DatasetRef::Version(route.lance_version), true, Some(route))
            .await
    }

    /// Approximate bytes resident in the shared index cache (memory tier plus persistent tier).
    fn index_cache_size_bytes(&self) -> u64 {
        self.index_cache
            .as_ref()
            .map(|backend| backend.approx_size_bytes() as u64)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds the small negative cache used by provenance tests.
    fn negative_cache() -> Cache<(String, String), SearchError> {
        Cache::builder().max_capacity(8).build()
    }

    #[tokio::test]
    async fn generic_missing_object_does_not_poison_the_negative_cache() {
        let cache = negative_cache();
        let key = ("memory://live".to_string(), "version:3".to_string());
        let raw = lance::Error::not_found("memory://live/_versions/3.manifest");
        let error = classify_open_failure(&cache, key.clone(), &raw).await;
        assert!(matches!(error, SearchError::NotFound(_)));
        assert!(cache.get(&key).await.is_none());
    }

    #[tokio::test]
    async fn definitive_dataset_absence_is_recorded_in_the_negative_cache() {
        let cache = negative_cache();
        let key = ("memory://absent".to_string(), "latest".to_string());
        let raw = lance::Error::dataset_not_found("memory://absent", "no manifest".into());
        let error = classify_open_failure(&cache, key.clone(), &raw).await;
        assert!(matches!(error, SearchError::NotFound(_)));
        assert_eq!(cache.get(&key).await, Some(error));
    }
}
