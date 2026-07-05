//! Hybrid memory + persistent [`CacheBackend`] for the Lance index cache.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Future;
use lance_core::Result as LanceResult;
use lance_core::cache::{CacheBackend, CacheCodec, CacheDecode, CacheEntry, InternalCacheKey, MokaCacheBackend};

use crate::cache::entry_store::EntryStore;
use crate::cache::layout::{frame_bytes, hash_hex, unframe_bytes};
use crate::telemetry::{CacheName, EvictionReason, Metrics, Tier};

/// Hybrid persistent + memory cache backend for the Lance index cache.
///
/// Entries whose key carries a [`CacheCodec`] are serialized into the persistent
/// [`EntryStore`] (local disk or shared Redis) and also kept in an in-memory hot tier.
/// Codec-less entries are delegated entirely to the inner Moka backend, as the
/// [`CacheBackend`] contract requires. Persisted names bind the full
/// `(prefix, key, type_name)` triple via blake3 hashes, so 30k org datasets share one cache
/// without collision risk and per-dataset purges stay O(#prefixes-for-dataset).
pub struct HybridIndexCacheBackend {
    store: Arc<dyn EntryStore>,
    memory_tier: MokaCacheBackend,
    inflight: tokio::sync::Mutex<HashMap<InternalCacheKey, Arc<tokio::sync::Mutex<()>>>>,
    metrics: Arc<Metrics>,
}

impl std::fmt::Debug for HybridIndexCacheBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridIndexCacheBackend")
            .field("store", &self.store)
            .finish()
    }
}

/// The `(dir, file)` store names of one cache key: a hashed-prefix dir and a
/// `{key_hash}-{type_hash}.bin` file, byte-identical to the pre-seam on-disk layout.
fn entry_names(key: &InternalCacheKey) -> (String, String) {
    let dir = hash_hex(key.prefix(), 32);
    let file = format!("{}-{}.bin", hash_hex(key.key(), 32), hash_hex(key.type_name(), 16));
    (dir, file)
}

impl HybridIndexCacheBackend {
    /// Composes the hybrid backend over a persistent store, sizing the in-memory hot tier to
    /// `memory_bytes`.
    pub fn new(store: Arc<dyn EntryStore>, memory_bytes: usize, metrics: Arc<Metrics>) -> Self {
        Self {
            store,
            memory_tier: MokaCacheBackend::with_capacity(memory_bytes),
            inflight: tokio::sync::Mutex::new(HashMap::new()),
            metrics,
        }
    }

    /// Approximate bytes currently held by the persistent store (excludes the memory tier).
    pub fn persisted_size_bytes(&self) -> u64 {
        self.store.approx_stats().0
    }

    /// Reads, verifies, and deserializes a persisted entry. Any failure removes the entry and
    /// reports a miss. Verification happens before the codec runs: a torn write or bit rot
    /// fails the frame checksum instead of reaching the deserializer (which cannot detect flips
    /// that still decode).
    async fn read_persisted_entry(&self, key: &InternalCacheKey, codec: &CacheCodec) -> Option<(CacheEntry, usize)> {
        let (dir, file) = entry_names(key);
        let buf = self.store.get(&dir, &file).await?;
        let Some(payload) = unframe_bytes(buf) else {
            self.store.remove_entry(&dir, &file).await;
            self.metrics
                .cache_evictions(CacheName::Index, EvictionReason::Corrupt, 1);
            return None;
        };
        let size = payload.len();
        match codec.deserialize(&payload) {
            CacheDecode::Hit(entry) => {
                self.store.touch(&dir, &file);
                Some((entry, size))
            }
            CacheDecode::Miss(_) => {
                self.store.remove_entry(&dir, &file).await;
                self.metrics
                    .cache_evictions(CacheName::Index, EvictionReason::Corrupt, 1);
                None
            }
        }
    }

    /// Serializes and persists one entry. Failures are swallowed so cache writes never fail loads.
    async fn write_persisted_entry(&self, key: &InternalCacheKey, entry: &CacheEntry, codec: &CacheCodec) {
        let mut buf = Vec::new();
        if codec.serialize(entry, &mut buf).is_err() {
            tracing::warn!(
                cache.key_type = key.type_name(),
                "index cache entry failed to serialize, kept memory-only"
            );
            self.metrics.cache_serialize_error(CacheName::Index);
            return;
        }
        let buf = frame_bytes(&buf);
        let (dir, file) = entry_names(key);
        self.store.register_prefix(key.prefix(), &dir).await;
        self.store.put(&dir, &file, &buf).await;
        self.metrics
            .cache_insert_bytes(CacheName::Index, self.store.tier(), buf.len() as u64);
    }
}

#[async_trait]
impl CacheBackend for HybridIndexCacheBackend {
    #[tracing::instrument(
        name = "index_cache.get",
        level = "trace",
        skip_all,
        fields(cache.key_type = key.type_name(), cache.tier = tracing::field::Empty, cache.hit = tracing::field::Empty)
    )]
    async fn get(&self, key: &InternalCacheKey, codec: Option<CacheCodec>) -> Option<CacheEntry> {
        let span = tracing::Span::current();
        let Some(codec) = codec else {
            let entry = self.memory_tier.get(key, None).await;
            self.metrics
                .cache_lookup(CacheName::Index, Tier::Memory, entry.is_some());
            span.record("cache.tier", "memory");
            span.record("cache.hit", entry.is_some());
            return entry;
        };
        if let Some(entry) = self.memory_tier.get(key, Some(codec)).await {
            self.metrics.cache_lookup(CacheName::Index, Tier::Memory, true);
            span.record("cache.tier", "memory");
            span.record("cache.hit", true);
            return Some(entry);
        }
        self.metrics.cache_lookup(CacheName::Index, Tier::Memory, false);
        span.record("cache.tier", self.store.tier().as_tag());
        let persisted = self.read_persisted_entry(key, &codec).await;
        self.metrics
            .cache_lookup(CacheName::Index, self.store.tier(), persisted.is_some());
        span.record("cache.hit", persisted.is_some());
        let (entry, size) = persisted?;
        self.memory_tier.insert(key, entry.clone(), size, Some(codec)).await;
        Some(entry)
    }

    #[tracing::instrument(
        name = "index_cache.insert",
        level = "trace",
        skip_all,
        fields(cache.key_type = key.type_name(), cache.size_bytes = size_bytes)
    )]
    async fn insert(&self, key: &InternalCacheKey, entry: CacheEntry, size_bytes: usize, codec: Option<CacheCodec>) {
        self.memory_tier.insert(key, entry.clone(), size_bytes, codec).await;
        if let Some(codec) = codec {
            self.write_persisted_entry(key, &entry, &codec).await;
        }
    }

    /// Single-flights the loader per key through an `inflight` map of per-key mutexes.
    ///
    /// The cleanup after `drop(guard)` relies on a strong-count invariant: the count is exactly 2
    /// when only the `inflight` map entry and this task's local `lock` hold the Arc, and every
    /// other task clones the Arc only while holding the `inflight` mutex — the same mutex held
    /// here — so the count cannot change between the check and the `remove`. The local `lock` is
    /// dropped only after `inflight.remove(key)`, intentionally, so the map entry is gone before
    /// the Arc refcount can reach zero and any concurrent caller inserts a fresh entry instead of
    /// resurrecting a dying one.
    #[tracing::instrument(
        name = "index_cache.get_or_insert",
        level = "trace",
        skip_all,
        fields(cache.key_type = key.type_name())
    )]
    async fn get_or_insert<'a>(
        &self,
        key: &InternalCacheKey,
        loader: Pin<Box<dyn Future<Output = LanceResult<(CacheEntry, usize)>> + Send + 'a>>,
        codec: Option<CacheCodec>,
    ) -> LanceResult<(CacheEntry, bool)> {
        let Some(codec) = codec else {
            return self.memory_tier.get_or_insert(key, loader, None).await;
        };
        let lock = {
            let mut inflight = self.inflight.lock().await;
            inflight
                .entry(key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let guard = lock.lock().await;
        let result = async {
            if let Some(entry) = self.get(key, Some(codec)).await {
                return Ok((entry, true));
            }
            let (entry, size) = loader.await?;
            self.insert(key, entry.clone(), size, Some(codec)).await;
            Ok((entry, false))
        }
        .await;
        drop(guard);
        let mut inflight = self.inflight.lock().await;
        let no_other_waiters = inflight
            .get(key)
            .is_some_and(|existing| Arc::strong_count(existing) == 2);
        if no_other_waiters {
            inflight.remove(key);
        }
        drop(lock);
        result
    }

    async fn invalidate_prefix(&self, prefix: &str) {
        self.memory_tier.invalidate_prefix(prefix).await;
        let matching: Vec<(String, String)> = self
            .store
            .prefix_entries()
            .await
            .into_iter()
            .filter(|(stored, _)| stored.starts_with(prefix))
            .collect();
        for (_, dir) in &matching {
            self.store.remove_dir(dir).await;
        }
        if !matching.is_empty() {
            let prefixes: Vec<String> = matching.into_iter().map(|(stored, _)| stored).collect();
            self.store.remove_prefixes(&prefixes).await;
        }
    }

    async fn clear(&self) {
        self.memory_tier.clear().await;
        self.store.clear().await;
    }

    async fn num_entries(&self) -> usize {
        self.memory_tier.num_entries().await + self.store.approx_stats().1 as usize
    }

    async fn size_bytes(&self) -> usize {
        self.memory_tier.size_bytes().await + self.store.approx_stats().0 as usize
    }

    fn approx_num_entries(&self) -> usize {
        self.memory_tier.approx_num_entries() + self.store.approx_stats().1 as usize
    }

    fn approx_size_bytes(&self) -> usize {
        self.memory_tier.approx_size_bytes() + self.store.approx_stats().0 as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::disk_store::DiskEntryStore;
    use crate::cache::entry_store::fake::MemoryEntryStore;
    use lance_core::cache::CacheCodecImpl;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Toy serializable payload exercising the codec path.
    #[derive(Debug, PartialEq, Eq)]
    struct Payload(Vec<u8>);

    impl CacheCodecImpl for Payload {
        const TYPE_ID: &'static str = "search-api.test.Payload";
        const CURRENT_VERSION: u32 = 1;

        fn serialize(&self, writer: &mut lance_core::cache::CacheEntryWriter<'_>) -> LanceResult<()> {
            writer.write_raw(&self.0)
        }

        fn deserialize(reader: &mut lance_core::cache::CacheEntryReader<'_>) -> LanceResult<Self> {
            Ok(Payload(reader.read_raw()?.to_vec()))
        }
    }

    /// Builds a key under the given prefix.
    fn key(prefix: &str, name: &str) -> InternalCacheKey {
        InternalCacheKey::new(Arc::from(prefix), Arc::from(name), "Payload")
    }

    /// Opens a disk-backed hybrid backend over `root`, returning the shared store too.
    fn disk_backend(root: &Path, memory_bytes: usize) -> (HybridIndexCacheBackend, Arc<DiskEntryStore>) {
        let store = Arc::new(DiskEntryStore::open(root.to_path_buf()).unwrap());
        let backend = HybridIndexCacheBackend::new(store.clone(), memory_bytes, Arc::new(Metrics::disabled()));
        (backend, store)
    }

    /// Counts regular cache entry files under `root`, excluding the sidecar.
    fn entry_file_count(root: &Path) -> usize {
        let mut count = 0;
        for entry in walkdir(root) {
            if entry
                .file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with(".bin"))
            {
                count += 1;
            }
        }
        count
    }

    /// Minimal recursive file walk for assertions.
    fn walkdir(root: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        if let Ok(entries) = std::fs::read_dir(root) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    files.extend(walkdir(&path));
                } else {
                    files.push(path);
                }
            }
        }
        files
    }

    /// The on-disk path of one key's entry file under `root`.
    fn disk_entry_path(root: &Path, cache_key: &InternalCacheKey) -> PathBuf {
        let (dir, file) = entry_names(cache_key);
        root.join(dir).join(file)
    }

    #[tokio::test]
    async fn insert_get_round_trip_persists_and_survives_reopen() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (backend, _) = disk_backend(tmp.path(), 1024 * 1024);
        let codec = CacheCodec::from_impl::<Payload>();
        let cache_key = key("s3://bucket/ds.lance/", "page-0");
        let entry: CacheEntry = Arc::new(Payload(vec![7u8; 32]));
        backend.insert(&cache_key, entry, 32, Some(codec)).await;
        assert_eq!(entry_file_count(tmp.path()), 1);
        let fetched = backend.get(&cache_key, Some(codec)).await.unwrap();
        assert_eq!(fetched.downcast_ref::<Payload>().unwrap().0, vec![7u8; 32]);
        drop(backend);
        let (reopened, _) = disk_backend(tmp.path(), 1024 * 1024);
        let fetched = reopened.get(&cache_key, Some(codec)).await.unwrap();
        assert_eq!(fetched.downcast_ref::<Payload>().unwrap().0, vec![7u8; 32]);
        assert!(reopened.approx_num_entries() >= 1);
    }

    #[tokio::test]
    async fn codec_less_entries_stay_memory_only() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (backend, _) = disk_backend(tmp.path(), 1024 * 1024);
        let cache_key = key("s3://bucket/ds.lance/", "opened-index");
        backend
            .insert(&cache_key, Arc::new(Payload(vec![1, 2, 3])), 3, None)
            .await;
        assert_eq!(entry_file_count(tmp.path()), 0);
        assert!(backend.get(&cache_key, None).await.is_some());
    }

    #[tokio::test]
    async fn corrupt_file_is_a_miss_and_gets_deleted() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (backend, _) = disk_backend(tmp.path(), 1024 * 1024);
        let codec = CacheCodec::from_impl::<Payload>();
        let cache_key = key("s3://bucket/ds.lance/", "page-1");
        backend
            .insert(&cache_key, Arc::new(Payload(b"valid".to_vec())), 5, Some(codec))
            .await;
        let files = walkdir(tmp.path());
        let bin = files
            .iter()
            .find(|path| {
                path.file_name()
                    .is_some_and(|name| name.to_string_lossy().ends_with(".bin"))
            })
            .unwrap();
        std::fs::write(bin, b"garbage").unwrap();
        let (fresh, _) = disk_backend(tmp.path(), 1024 * 1024);
        assert!(fresh.get(&cache_key, Some(codec)).await.is_none());
        assert_eq!(entry_file_count(tmp.path()), 0);
    }

    #[tokio::test]
    async fn bit_flip_inside_payload_is_detected_by_the_frame() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (backend, _) = disk_backend(tmp.path(), 0);
        let codec = CacheCodec::from_impl::<Payload>();
        let cache_key = key("s3://bucket/ds.lance/", "page-flip");
        backend
            .insert(&cache_key, Arc::new(Payload(vec![7u8; 64])), 64, Some(codec))
            .await;
        let bin = walkdir(tmp.path())
            .into_iter()
            .find(|path| {
                path.file_name()
                    .is_some_and(|name| name.to_string_lossy().ends_with(".bin"))
            })
            .unwrap();
        let mut bytes = std::fs::read(&bin).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&bin, &bytes).unwrap();
        assert!(
            backend.get(&cache_key, Some(codec)).await.is_none(),
            "a flipped payload byte must fail the frame checksum and miss"
        );
        assert_eq!(entry_file_count(tmp.path()), 0, "the corrupt file must be deleted");
    }

    #[tokio::test]
    async fn invalidate_prefix_removes_dataset_and_index_scoped_entries() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (backend, _) = disk_backend(tmp.path(), 1024 * 1024);
        let codec = CacheCodec::from_impl::<Payload>();
        let dataset_key = key("s3://bucket/ds.lance/", "manifest/3");
        let index_key = key("s3://bucket/ds.lance/uuid-1/", "page-0");
        let other_key = key("s3://bucket/other.lance/", "manifest/3");
        for cache_key in [&dataset_key, &index_key, &other_key] {
            backend
                .insert(cache_key, Arc::new(Payload(vec![9u8; 8])), 8, Some(codec))
                .await;
        }
        assert_eq!(entry_file_count(tmp.path()), 3);
        backend.invalidate_prefix("s3://bucket/ds.lance/").await;
        assert_eq!(entry_file_count(tmp.path()), 1);
        assert!(backend.get(&dataset_key, Some(codec)).await.is_none());
        assert!(backend.get(&index_key, Some(codec)).await.is_none());
        assert!(backend.get(&other_key, Some(codec)).await.is_some());
    }

    #[tokio::test]
    async fn get_or_insert_runs_loader_at_most_once_under_concurrency() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (backend, _) = disk_backend(tmp.path(), 1024 * 1024);
        let backend = Arc::new(backend);
        let codec = CacheCodec::from_impl::<Payload>();
        let cache_key = key("s3://bucket/ds.lance/", "page-shared");
        let loader_runs = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let backend = backend.clone();
            let cache_key = cache_key.clone();
            let loader_runs = loader_runs.clone();
            tasks.push(tokio::spawn(async move {
                let loader = Box::pin(async move {
                    loader_runs.fetch_add(1, Ordering::SeqCst);
                    Ok((Arc::new(Payload(vec![5u8; 16])) as CacheEntry, 16usize))
                });
                backend.get_or_insert(&cache_key, loader, Some(codec)).await.unwrap()
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(loader_runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn accounting_matches_directory_walk() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (backend, store) = disk_backend(tmp.path(), 1024 * 1024);
        let codec = CacheCodec::from_impl::<Payload>();
        for index in 0..4 {
            backend
                .insert(
                    &key("s3://bucket/ds.lance/", &format!("page-{index}")),
                    Arc::new(Payload(vec![1u8; 100])),
                    100,
                    Some(codec),
                )
                .await;
        }
        let walked: u64 = walkdir(tmp.path())
            .iter()
            .filter(|path| {
                path.file_name()
                    .is_some_and(|name| name.to_string_lossy().ends_with(".bin"))
            })
            .map(|path| std::fs::metadata(path).unwrap().len())
            .sum();
        let (bytes, entries) = store.approx_stats();
        assert_eq!(bytes, walked);
        assert_eq!(entries, 4);
    }

    #[tokio::test]
    async fn sweep_respects_budget_and_subsequent_get_misses_cleanly() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (backend, store) = disk_backend(tmp.path(), 0);
        let codec = CacheCodec::from_impl::<Payload>();
        for index in 0..8 {
            backend
                .insert(
                    &key("s3://bucket/ds.lance/", &format!("page-{index}")),
                    Arc::new(Payload(vec![1u8; 200])),
                    200,
                    Some(codec),
                )
                .await;
        }
        store.sweep(Duration::from_secs(3600), 500);
        assert!(store.approx_stats().0 <= 500);
        let survivors = (0..8)
            .filter(|index| {
                std::fs::metadata(disk_entry_path(
                    tmp.path(),
                    &key("s3://bucket/ds.lance/", &format!("page-{index}")),
                ))
                .is_ok()
            })
            .count();
        assert!(survivors <= 2);
    }

    #[tokio::test]
    async fn store_hit_promotes_the_entry_into_the_memory_tier() {
        let store = Arc::new(MemoryEntryStore::default());
        let backend = HybridIndexCacheBackend::new(store.clone(), 1024 * 1024, Arc::new(Metrics::disabled()));
        let codec = CacheCodec::from_impl::<Payload>();
        let cache_key = key("s3://bucket/ds.lance/", "page-promote");
        backend
            .insert(&cache_key, Arc::new(Payload(vec![3u8; 16])), 16, Some(codec))
            .await;
        let cold = HybridIndexCacheBackend::new(store.clone(), 1024 * 1024, Arc::new(Metrics::disabled()));
        let store_gets_before = store.gets.load(Ordering::SeqCst);
        assert!(cold.get(&cache_key, Some(codec)).await.is_some());
        assert_eq!(store.gets.load(Ordering::SeqCst), store_gets_before + 1);
        assert!(cold.get(&cache_key, Some(codec)).await.is_some());
        assert_eq!(
            store.gets.load(Ordering::SeqCst),
            store_gets_before + 1,
            "the second get must be served by the promoted memory-tier entry"
        );
        assert!(store.touches.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn corrupt_store_value_is_purged_through_the_seam() {
        let store = Arc::new(MemoryEntryStore::default());
        let backend = HybridIndexCacheBackend::new(store.clone(), 0, Arc::new(Metrics::disabled()));
        let codec = CacheCodec::from_impl::<Payload>();
        let cache_key = key("s3://bucket/ds.lance/", "page-corrupt");
        let (dir, file) = entry_names(&cache_key);
        store.put(&dir, &file, b"garbage").await;
        assert!(backend.get(&cache_key, Some(codec)).await.is_none());
        assert!(
            store.get(&dir, &file).await.is_none(),
            "the corrupt value must be removed through remove_entry"
        );
    }

    #[tokio::test]
    async fn invalidate_prefix_works_through_the_registry_seam() {
        let store = Arc::new(MemoryEntryStore::default());
        let backend = HybridIndexCacheBackend::new(store.clone(), 1024 * 1024, Arc::new(Metrics::disabled()));
        let codec = CacheCodec::from_impl::<Payload>();
        let kept = key("s3://bucket/other.lance/", "page-0");
        let purged = key("s3://bucket/ds.lance/", "page-0");
        for cache_key in [&kept, &purged] {
            backend
                .insert(cache_key, Arc::new(Payload(vec![2u8; 8])), 8, Some(codec))
                .await;
        }
        backend.invalidate_prefix("s3://bucket/ds.lance/").await;
        assert_eq!(store.prefix_entries().await.len(), 1);
        assert_eq!(store.approx_stats().1, 1);
        let cold = HybridIndexCacheBackend::new(store.clone(), 1024 * 1024, Arc::new(Metrics::disabled()));
        assert!(cold.get(&purged, Some(codec)).await.is_none());
        assert!(cold.get(&kept, Some(codec)).await.is_some());
    }
}
