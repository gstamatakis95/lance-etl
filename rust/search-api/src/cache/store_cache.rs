//! Path-filtered read-through byte cache wrapping a Lance object store.
//!
//! Caches immutable metadata reads (version manifests, transactions, index file ranges) in the
//! configured persistent [`EntryStore`] and passes every other operation, including all raw data
//! reads under `data/`, straight through to the wrapped store.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::BoxStream;
use lance_io::object_store::WrappingObjectStore;
use object_store::path::Path as ObjectPath;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult,
};
use serde_json::Value;

use crate::cache::entry_store::EntryStore;
use crate::cache::layout::{META_FILE, frame_bytes, hash_hex, unframe_bytes};
use crate::telemetry::{CacheName, EvictionReason, Metrics};

/// File name for cached full-object bytes.
const FULL_OBJECT_FILE: &str = "full.bin";

/// Directory holding version manifests.
const VERSIONS_DIR: &str = "_versions";

/// Directory holding transaction files.
const TRANSACTIONS_DIR: &str = "_transactions";

/// Directory holding index files.
const INDICES_DIR: &str = "_indices";

/// File name of the V1 mutable latest-manifest pointer, which must never be cached.
const LATEST_MANIFEST_FILE: &str = "_latest.manifest";

/// File name of the V2 mutable latest-version hint, which must never be cached.
///
/// V2 datasets advertise their newest version through `_versions/latest_version_hint.json` instead
/// of the V1 `_latest.manifest`. Caching either would let a replica serve a stale latest pointer
/// across a commit or a blue-green flip, so both are pinned to always-pass-through here. The hint
/// already fails the `.manifest` suffix check, but naming it explicitly guards the never-cache
/// invariant against a future refactor of the suffix logic.
const LATEST_VERSION_HINT_FILE: &str = "latest_version_hint.json";

/// Which class of immutable metadata object a path belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathKind {
    /// An immutable `_versions/{n}.manifest` file.
    Manifest,
    /// An immutable `_transactions/...` file.
    Transaction,
    /// A file under `_indices/`. Only reads up to the configured byte limit are cached, so
    /// re-open inputs (headers, footers, small token/doc files) persist while bulk partition
    /// payloads pass through (their decoded form lives in the disk index cache instead).
    Index,
}

/// Classifies a path. `None` means the path must always pass through (notably `data/` and the
/// mutable latest-version pointers of both the V1 and V2 manifest naming schemes).
fn classify(location: &ObjectPath) -> Option<PathKind> {
    let mut kind = None;
    for part in location.parts() {
        kind = kind.or(match part.as_ref() {
            VERSIONS_DIR => Some(PathKind::Manifest),
            TRANSACTIONS_DIR => Some(PathKind::Transaction),
            INDICES_DIR => Some(PathKind::Index),
            _ => None,
        });
    }
    let filename = location.filename().unwrap_or("");
    if filename == LATEST_MANIFEST_FILE || filename == LATEST_VERSION_HINT_FILE {
        return None;
    }
    match kind {
        Some(PathKind::Manifest) if filename.ends_with(".manifest") => Some(PathKind::Manifest),
        Some(PathKind::Manifest) => None,
        other => other,
    }
}

/// Shared state of the byte cache across all wrapped stores.
struct StoreCacheState {
    store: Arc<dyn EntryStore>,
    max_index_range_bytes: u64,
    metrics: Arc<Metrics>,
}

impl StoreCacheState {
    /// Store dir holding all cached entries of one `(store_prefix, path)` object.
    fn object_dir(&self, store_prefix: &str, location: &ObjectPath) -> String {
        hash_hex(&format!("{store_prefix}\n{location}"), 32)
    }

    /// Removes every cached entry of one object.
    async fn invalidate_object(&self, store_prefix: &str, location: &ObjectPath) {
        self.store.remove_dir(&self.object_dir(store_prefix, location)).await;
    }
}

/// Path-filtered read-through byte cache. Inject via `ObjectStoreParams::object_store_wrapper`.
pub struct MetadataByteCache {
    state: Arc<StoreCacheState>,
}

impl std::fmt::Debug for MetadataByteCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataByteCache")
            .field("store", &self.state.store)
            .finish()
    }
}

impl MetadataByteCache {
    /// Composes the byte cache over a persistent store. `max_index_range_bytes` bounds the
    /// largest single `_indices/` byte range persisted.
    pub fn new(store: Arc<dyn EntryStore>, max_index_range_bytes: u64, metrics: Arc<Metrics>) -> Self {
        Self {
            state: Arc::new(StoreCacheState {
                store,
                max_index_range_bytes,
                metrics,
            }),
        }
    }

    /// Approximate bytes currently persisted by the byte cache.
    pub fn approx_size_bytes(&self) -> u64 {
        self.state.store.approx_stats().0
    }
}

impl WrappingObjectStore for MetadataByteCache {
    fn wrap(&self, store_prefix: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(CachedStore {
            inner: original,
            store_prefix: store_prefix.to_string(),
            state: self.state.clone(),
        })
    }
}

/// One wrapped object store: serves cacheable metadata reads from disk, delegates the rest.
pub struct CachedStore {
    inner: Arc<dyn ObjectStore>,
    store_prefix: String,
    state: Arc<StoreCacheState>,
}

impl std::fmt::Debug for CachedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedStore")
            .field("store_prefix", &self.store_prefix)
            .field("inner", &self.inner)
            .finish()
    }
}

impl std::fmt::Display for CachedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetadataByteCache({})", self.inner)
    }
}

/// Returns true when the request carries conditions that must always reach the real store.
fn is_conditional(options: &GetOptions) -> bool {
    options.if_match.is_some()
        || options.if_none_match.is_some()
        || options.if_modified_since.is_some()
        || options.if_unmodified_since.is_some()
        || options.version.is_some()
}

/// Cache entry file name for one request shape.
fn entry_file_name(range: &Option<GetRange>) -> String {
    match range {
        None => FULL_OBJECT_FILE.to_string(),
        Some(GetRange::Bounded(bounds)) => format!("{}-{}.bin", bounds.start, bounds.end),
        Some(GetRange::Offset(offset)) => format!("offset-{offset}.bin"),
        Some(GetRange::Suffix(suffix)) => format!("suffix-{suffix}.bin"),
    }
}

/// Builds a `GetResult` over already-materialized bytes.
fn synthesize_result(bytes: Bytes, meta: ObjectMeta, start: u64) -> GetResult {
    let len = bytes.len() as u64;
    GetResult {
        payload: GetResultPayload::Stream(futures::stream::once(async move { Ok(bytes) }).boxed()),
        meta,
        range: start..start + len,
        attributes: Attributes::default(),
    }
}

/// Fallback metadata when the sidecar is missing. Only `.bytes()`-style consumption relies on it.
fn fallback_meta(location: &ObjectPath, size: u64) -> ObjectMeta {
    ObjectMeta {
        location: location.clone(),
        last_modified: DateTime::<Utc>::from_timestamp(0, 0).unwrap_or_default(),
        size,
        e_tag: None,
        version: None,
    }
}

/// Serializes an `ObjectMeta` sidecar.
fn meta_to_json(meta: &ObjectMeta) -> String {
    let mut object = serde_json::Map::new();
    object.insert("size".to_string(), Value::from(meta.size));
    object.insert(
        "last_modified".to_string(),
        Value::String(meta.last_modified.to_rfc3339()),
    );
    if let Some(e_tag) = &meta.e_tag {
        object.insert("e_tag".to_string(), Value::String(e_tag.clone()));
    }
    if let Some(version) = &meta.version {
        object.insert("version".to_string(), Value::String(version.clone()));
    }
    Value::Object(object).to_string()
}

/// Deserializes an `ObjectMeta` sidecar. Malformed content yields `None`.
fn meta_from_json(raw: &str, location: &ObjectPath) -> Option<ObjectMeta> {
    let Value::Object(object) = serde_json::from_str::<Value>(raw).ok()? else {
        return None;
    };
    let size = object.get("size")?.as_u64()?;
    let last_modified = object
        .get("last_modified")
        .and_then(Value::as_str)
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|parsed| parsed.with_timezone(&Utc))
        .unwrap_or_default();
    Some(ObjectMeta {
        location: location.clone(),
        last_modified,
        size,
        e_tag: object.get("e_tag").and_then(Value::as_str).map(str::to_string),
        version: object.get("version").and_then(Value::as_str).map(str::to_string),
    })
}

/// Tag value of one metadata path class for span attributes.
fn path_kind_tag(kind: PathKind) -> &'static str {
    match kind {
        PathKind::Manifest => "manifest",
        PathKind::Transaction => "transaction",
        PathKind::Index => "index",
    }
}

impl CachedStore {
    /// Serves one cacheable read: disk hit, or inner fetch followed by a persist.
    #[tracing::instrument(
        name = "store_cache.get",
        level = "trace",
        skip_all,
        fields(store.path_kind = path_kind_tag(kind), cache.hit = tracing::field::Empty)
    )]
    async fn cached_get(
        &self,
        location: &ObjectPath,
        options: GetOptions,
        kind: PathKind,
    ) -> ObjectStoreResult<GetResult> {
        let store = &self.state.store;
        let object_dir = self.state.object_dir(&self.store_prefix, location);
        let entry_file = entry_file_name(&options.range);
        let (entry_read, sidecar_read) = store.get_pair(&object_dir, &entry_file, META_FILE).await;
        if let Some(buf) = entry_read {
            match unframe_bytes(buf) {
                None => {
                    self.state.invalidate_object(&self.store_prefix, location).await;
                    self.state
                        .metrics
                        .cache_evictions(CacheName::Store, EvictionReason::Corrupt, 1);
                }
                Some(payload) => {
                    store.touch(&object_dir, &entry_file);
                    self.state.metrics.cache_lookup(CacheName::Store, store.tier(), true);
                    tracing::Span::current().record("cache.hit", true);
                    let meta = sidecar_read
                        .and_then(|raw| String::from_utf8(raw).ok())
                        .and_then(|raw| meta_from_json(&raw, location))
                        .unwrap_or_else(|| fallback_meta(location, payload.len() as u64));
                    let start = match &options.range {
                        None => 0,
                        Some(GetRange::Bounded(bounds)) => bounds.start,
                        Some(GetRange::Offset(offset)) => *offset,
                        Some(GetRange::Suffix(suffix)) => meta.size.saturating_sub(*suffix),
                    };
                    return Ok(synthesize_result(payload, meta, start));
                }
            }
        }
        self.state.metrics.cache_lookup(CacheName::Store, store.tier(), false);
        tracing::Span::current().record("cache.hit", false);
        let result = self.inner.get_opts(location, options.clone()).await?;
        let meta = result.meta.clone();
        let start = result.range.start;
        let bytes = result.bytes().await?;
        let within_limit = kind != PathKind::Index || bytes.len() as u64 <= self.state.max_index_range_bytes;
        if within_limit {
            store
                .put_if_absent(&object_dir, META_FILE, meta_to_json(&meta).as_bytes())
                .await;
            let framed = frame_bytes(&bytes);
            store.put_if_absent(&object_dir, &entry_file, &framed).await;
            self.state
                .metrics
                .cache_insert_bytes(CacheName::Store, store.tier(), framed.len() as u64);
        }
        Ok(synthesize_result(bytes, meta, start))
    }
}

#[async_trait]
impl ObjectStore for CachedStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if classify(location).is_some() {
            self.state.invalidate_object(&self.store_prefix, location).await;
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        if classify(location).is_some() {
            self.state.invalidate_object(&self.store_prefix, location).await;
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let Some(kind) = classify(location) else {
            return self.inner.get_opts(location, options).await;
        };
        if options.head || is_conditional(&options) {
            return self.inner.get_opts(location, options).await;
        }
        match (&options.range, kind) {
            (Some(GetRange::Bounded(bounds)), PathKind::Index)
                if bounds.end.saturating_sub(bounds.start) > self.state.max_index_range_bytes =>
            {
                self.inner.get_opts(location, options).await
            }
            _ => self.cached_get(location, options, kind).await,
        }
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[std::ops::Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        if classify(location).is_none() {
            return self.inner.get_ranges(location, ranges).await;
        }
        let mut results = Vec::with_capacity(ranges.len());
        for range in ranges {
            let options = GetOptions {
                range: Some(GetRange::Bounded(range.clone())),
                ..Default::default()
            };
            results.push(self.get_opts(location, options).await?.bytes().await?);
        }
        Ok(results)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
        let state = self.state.clone();
        let store_prefix = self.store_prefix.clone();
        self.inner
            .delete_stream(locations)
            .then(move |result| {
                let state = state.clone();
                let store_prefix = store_prefix.clone();
                async move {
                    if let Ok(path) = &result
                        && classify(path).is_some()
                    {
                        state.invalidate_object(&store_prefix, path).await;
                    }
                    result
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &ObjectPath, to: &ObjectPath, options: CopyOptions) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &ObjectPath, to: &ObjectPath, options: RenameOptions) -> ObjectStoreResult<()> {
        if classify(to).is_some() {
            self.state.invalidate_object(&self.store_prefix, to).await;
        }
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::disk_store::DiskEntryStore;
    use crate::cache::entry_store::fake::MemoryEntryStore;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Inner store counting reads so tests can assert what passes through the cache.
    #[derive(Debug)]
    struct CountingStore {
        inner: InMemory,
        gets: AtomicU64,
        lists: AtomicU64,
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore({})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> ObjectStoreResult<GetResult> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.lists.fetch_add(1, Ordering::SeqCst);
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &ObjectPath, to: &ObjectPath, options: CopyOptions) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Builds a cache + counting store pair with the given object pre-populated.
    async fn setup(
        max_range: u64,
        objects: &[(&str, usize)],
    ) -> (tempfile::TempDir, Arc<CountingStore>, Arc<dyn ObjectStore>) {
        let tmp = tempfile::TempDir::new().unwrap();
        let counting = Arc::new(CountingStore {
            inner: InMemory::new(),
            gets: AtomicU64::new(0),
            lists: AtomicU64::new(0),
        });
        for (path, len) in objects {
            counting
                .inner
                .put(&ObjectPath::from(*path), vec![1u8; *len].into())
                .await
                .unwrap();
        }
        let store = Arc::new(DiskEntryStore::open(tmp.path().to_path_buf()).unwrap());
        let cache = MetadataByteCache::new(store, max_range, Arc::new(Metrics::disabled()));
        let wrapped = cache.wrap("test$store", counting.clone());
        (tmp, counting, wrapped)
    }

    #[tokio::test]
    async fn cached_get_pairs_the_entry_and_sidecar_in_one_lookup() {
        let counting = Arc::new(CountingStore {
            inner: InMemory::new(),
            gets: AtomicU64::new(0),
            lists: AtomicU64::new(0),
        });
        counting
            .inner
            .put(&ObjectPath::from("ds/_versions/7.manifest"), vec![1u8; 64].into())
            .await
            .unwrap();
        let store = Arc::new(MemoryEntryStore::default());
        let cache = MetadataByteCache::new(store.clone(), 4096, Arc::new(Metrics::disabled()));
        let wrapped = cache.wrap("test$store", counting.clone());
        let path = ObjectPath::from("ds/_versions/7.manifest");
        wrapped.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(store.puts.load(Ordering::SeqCst), 2, "one entry plus one sidecar");
        let lookups_before = store.gets.load(Ordering::SeqCst);
        let served = wrapped.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(served.len(), 64);
        assert_eq!(
            store.gets.load(Ordering::SeqCst),
            lookups_before + 1,
            "the hit must pair the entry and sidecar reads into one store lookup"
        );
        assert_eq!(counting.gets.load(Ordering::SeqCst), 1);
        assert!(store.touches.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn versioned_manifest_full_get_is_cached() {
        let (tmp_dir, counting, store) = setup(4096, &[("ds/_versions/12.manifest", 64)]).await;
        let path = ObjectPath::from("ds/_versions/12.manifest");
        let first = store.get(&path).await.unwrap().bytes().await.unwrap();
        let second = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(first, second);
        assert_eq!(counting.gets.load(Ordering::SeqCst), 1);
        drop(tmp_dir);
    }

    #[tokio::test]
    async fn latest_manifest_is_never_cached() {
        let (tmp_dir, counting, store) = setup(4096, &[("ds/_versions/_latest.manifest", 64)]).await;
        let path = ObjectPath::from("ds/_versions/_latest.manifest");
        store.get(&path).await.unwrap().bytes().await.unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(counting.gets.load(Ordering::SeqCst), 2);
        drop(tmp_dir);
    }

    #[tokio::test]
    async fn latest_version_hint_is_never_cached() {
        let (tmp_dir, counting, store) = setup(4096, &[("ds/_versions/latest_version_hint.json", 64)]).await;
        let path = ObjectPath::from("ds/_versions/latest_version_hint.json");
        store.get(&path).await.unwrap().bytes().await.unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(
            counting.gets.load(Ordering::SeqCst),
            2,
            "the V2 latest-version hint must always pass through to the real store"
        );
        drop(tmp_dir);
    }

    #[tokio::test]
    async fn transaction_files_are_cached() {
        let (tmp_dir, counting, store) = setup(4096, &[("ds/_transactions/12-abc.txn", 64)]).await;
        let path = ObjectPath::from("ds/_transactions/12-abc.txn");
        store.get(&path).await.unwrap().bytes().await.unwrap();
        store.get_range(&path, 0..16).await.unwrap();
        store.get_range(&path, 0..16).await.unwrap();
        assert_eq!(counting.gets.load(Ordering::SeqCst), 2);
        drop(tmp_dir);
    }

    #[tokio::test]
    async fn index_ranges_respect_the_size_limit() {
        let (tmp_dir, counting, store) = setup(32, &[("ds/_indices/uuid-1/index.idx", 256)]).await;
        let path = ObjectPath::from("ds/_indices/uuid-1/index.idx");
        store.get_range(&path, 0..16).await.unwrap();
        store.get_range(&path, 0..16).await.unwrap();
        assert_eq!(counting.gets.load(Ordering::SeqCst), 1, "small range should be cached");
        store.get_range(&path, 0..128).await.unwrap();
        store.get_range(&path, 0..128).await.unwrap();
        assert_eq!(counting.gets.load(Ordering::SeqCst), 3, "large range must pass through");
        store.get(&path).await.unwrap().bytes().await.unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(
            counting.gets.load(Ordering::SeqCst),
            5,
            "full index get larger than the limit must not be cached"
        );
        drop(tmp_dir);
    }

    #[tokio::test]
    async fn small_index_objects_are_cached_whole() {
        let (tmp_dir, counting, store) = setup(4096, &[("ds/_indices/uuid-1/metadata.lance", 64)]).await;
        let path = ObjectPath::from("ds/_indices/uuid-1/metadata.lance");
        store.get(&path).await.unwrap().bytes().await.unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(
            counting.gets.load(Ordering::SeqCst),
            1,
            "small full index objects are cold-reopen inputs and must be cached"
        );
        drop(tmp_dir);
    }

    #[tokio::test]
    async fn data_files_always_pass_through() {
        let (tmp_dir, counting, store) = setup(4096, &[("ds/data/abc.lance", 64)]).await;
        let path = ObjectPath::from("ds/data/abc.lance");
        store.get_range(&path, 0..16).await.unwrap();
        store.get_range(&path, 0..16).await.unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(counting.gets.load(Ordering::SeqCst), 3);
        drop(tmp_dir);
    }

    #[tokio::test]
    async fn head_and_list_always_pass_through() {
        let (tmp_dir, counting, store) = setup(4096, &[("ds/_versions/12.manifest", 64)]).await;
        let path = ObjectPath::from("ds/_versions/12.manifest");
        store.head(&path).await.unwrap();
        store.head(&path).await.unwrap();
        assert_eq!(counting.gets.load(Ordering::SeqCst), 2, "head is a conditional read");
        let listed: Vec<_> = store.list(None).collect().await;
        assert_eq!(listed.len(), 1);
        assert_eq!(counting.lists.load(Ordering::SeqCst), 1);
        drop(tmp_dir);
    }
}
