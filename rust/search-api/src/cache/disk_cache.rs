//! Disk-backed [`CacheBackend`] for the Lance index cache.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Future;
use lance_core::Result as LanceResult;
use lance_core::cache::{CacheBackend, CacheCodec, CacheEntry, InternalCacheKey, MokaCacheBackend};
use serde_json::Value;

use crate::cache::layout::{SweepStats, atomic_write, dir_stats, hash_hex, sweep_tier, touch_file};
use crate::telemetry::{CacheName, EvictionReason, Metrics, Tier};

/// Sidecar file mapping full cache-key prefixes to their hashed directory names, enabling
/// `invalidate_prefix` to find directories by string-prefix match across process restarts.
const PREFIXES_FILE: &str = "prefixes.json";

/// Hybrid disk + memory cache backend for the Lance index cache.
///
/// Entries whose key carries a [`CacheCodec`] are serialized to files under the cache root and
/// also kept in an in-memory hot tier. Codec-less entries are delegated entirely to the inner
/// Moka backend, as the [`CacheBackend`] contract requires. On-disk names bind the full
/// `(prefix, key, type_name)` triple via blake3 hashes, so 30k org datasets share one cache
/// without collision risk and per-dataset purges stay O(#prefixes-for-dataset).
pub struct DiskIndexCacheBackend {
    root: PathBuf,
    memory_tier: MokaCacheBackend,
    inflight: tokio::sync::Mutex<HashMap<InternalCacheKey, Arc<tokio::sync::Mutex<()>>>>,
    prefix_index: RwLock<HashMap<String, String>>,
    disk_bytes: AtomicU64,
    disk_entries: AtomicU64,
    metrics: Arc<Metrics>,
}

impl std::fmt::Debug for DiskIndexCacheBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskIndexCacheBackend")
            .field("root", &self.root)
            .field("disk_entries", &self.disk_entries.load(Ordering::Relaxed))
            .field("disk_bytes", &self.disk_bytes.load(Ordering::Relaxed))
            .finish()
    }
}

impl DiskIndexCacheBackend {
    /// Opens (or creates) the disk tier under `root`, sizing the in-memory hot tier to
    /// `memory_bytes`. Seeds size accounting from a directory walk and removes orphaned
    /// temp files left by a previous crash.
    pub fn open(root: PathBuf, memory_bytes: usize, metrics: Arc<Metrics>) -> std::io::Result<Self> {
        std::fs::create_dir_all(&root)?;
        let prefix_index = load_prefixes(&root.join(PREFIXES_FILE));
        let (bytes, entries) = dir_stats(&root);
        let entries = entries.saturating_sub(if root.join(PREFIXES_FILE).exists() { 1 } else { 0 });
        let bytes = bytes.saturating_sub(
            std::fs::metadata(root.join(PREFIXES_FILE))
                .map(|meta| meta.len())
                .unwrap_or(0),
        );
        Ok(Self {
            root,
            memory_tier: MokaCacheBackend::with_capacity(memory_bytes),
            inflight: tokio::sync::Mutex::new(HashMap::new()),
            prefix_index: RwLock::new(prefix_index),
            disk_bytes: AtomicU64::new(bytes),
            disk_entries: AtomicU64::new(entries),
            metrics,
        })
    }

    /// Returns the cache root directory (the `index/` tier under the stamp dir).
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Approximate bytes currently persisted on disk by this tier (excludes the memory tier).
    pub fn disk_size_bytes(&self) -> u64 {
        self.disk_bytes.load(Ordering::Relaxed)
    }

    /// Computes the on-disk file path for one cache key, registering its prefix directory.
    fn entry_path(&self, key: &InternalCacheKey, register: bool) -> PathBuf {
        let dir_name = hash_hex(key.prefix(), 32);
        if register {
            self.register_prefix(key.prefix(), &dir_name);
        }
        let file_name = format!("{}-{}.bin", hash_hex(key.key(), 32), hash_hex(key.type_name(), 16));
        self.root.join(dir_name).join(file_name)
    }

    /// Records a prefix → directory mapping, persisting the sidecar when the prefix is new.
    fn register_prefix(&self, prefix: &str, dir_name: &str) {
        {
            let map = self
                .prefix_index
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if map.contains_key(prefix) {
                return;
            }
        }
        let snapshot = {
            let mut map = self
                .prefix_index
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            map.insert(prefix.to_string(), dir_name.to_string());
            map.clone()
        };
        persist_prefixes(&self.root.join(PREFIXES_FILE), &snapshot);
    }

    /// Deletes one disk entry after a read or decode failure, adjusting accounting.
    async fn drop_corrupt_entry(&self, path: &Path) {
        if let Ok(meta) = tokio::fs::metadata(path).await {
            self.disk_bytes.fetch_sub(
                meta.len().min(self.disk_bytes.load(Ordering::Relaxed)),
                Ordering::Relaxed,
            );
            self.disk_entries
                .fetch_sub(1.min(self.disk_entries.load(Ordering::Relaxed)), Ordering::Relaxed);
        }
        let _ = tokio::fs::remove_file(path).await;
    }

    /// Reads and deserializes a disk entry. Any failure deletes the file and reports a miss.
    async fn read_disk_entry(&self, key: &InternalCacheKey, codec: &CacheCodec) -> Option<(CacheEntry, usize)> {
        let path = self.entry_path(key, false);
        let buf = match tokio::fs::read(&path).await {
            Ok(buf) => buf,
            Err(_) => return None,
        };
        let size = buf.len();
        match codec.deserialize(&Bytes::from(buf)) {
            Ok(entry) => {
                drop(tokio::task::spawn_blocking(move || touch_file(&path)));
                Some((entry, size))
            }
            Err(_) => {
                self.drop_corrupt_entry(&path).await;
                self.metrics
                    .cache_evictions(CacheName::Index, EvictionReason::Corrupt, 1);
                None
            }
        }
    }

    /// Serializes and persists one entry. Failures are swallowed so cache writes never fail loads.
    ///
    /// Size accounting re-stats the file after the rename rather than trusting the buffer length.
    /// On overwrite the old size is subtracted before the new size is added: the two atomics are
    /// not updated as one transaction, so the ordering bounds the transient error to an undercount
    /// (the janitor briefly under-evicts) instead of an overcount that could suppress eviction
    /// while the tier is over budget. The janitor sweep fully reconciles any residual drift.
    async fn write_disk_entry(&self, key: &InternalCacheKey, entry: &CacheEntry, codec: &CacheCodec) {
        let mut buf = Vec::new();
        if codec.serialize(entry, &mut buf).is_err() {
            tracing::warn!(
                cache.key_type = key.type_name(),
                "index cache entry failed to serialize, kept memory-only"
            );
            self.metrics.cache_serialize_error(CacheName::Index);
            return;
        }
        let path = self.entry_path(key, true);
        let old_len = tokio::fs::metadata(&path).await.map(|meta| meta.len()).ok();
        if atomic_write(&path, &buf).await.is_ok() {
            self.metrics.cache_insert_bytes(CacheName::Index, buf.len() as u64);
            let new_on_disk = tokio::fs::metadata(&path)
                .await
                .map(|meta| meta.len())
                .unwrap_or(buf.len() as u64);
            match old_len {
                Some(old) => {
                    self.disk_bytes
                        .fetch_sub(old.min(self.disk_bytes.load(Ordering::Relaxed)), Ordering::Relaxed);
                    self.disk_bytes.fetch_add(new_on_disk, Ordering::Relaxed);
                }
                None => {
                    self.disk_bytes.fetch_add(new_on_disk, Ordering::Relaxed);
                    self.disk_entries.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Sweeps the disk tier: TTL expiry plus oldest-first eviction down to `budget_bytes`,
    /// then reconciles accounting and rewrites the prefix sidecar dropping empty directories.
    pub fn sweep(&self, ttl: Duration, budget_bytes: u64) -> SweepStats {
        let prefixes_path = self.root.join(PREFIXES_FILE);
        let _ = std::fs::remove_file(&prefixes_path);
        let stats = sweep_tier(&self.root, ttl, budget_bytes, &self.disk_bytes, &self.disk_entries);
        let snapshot = {
            let mut map = self
                .prefix_index
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            map.retain(|_, dir_name| self.root.join(dir_name.as_str()).is_dir());
            map.clone()
        };
        persist_prefixes(&prefixes_path, &snapshot);
        stats
    }
}

#[async_trait]
impl CacheBackend for DiskIndexCacheBackend {
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
        span.record("cache.tier", "disk");
        let disk_entry = self.read_disk_entry(key, &codec).await;
        self.metrics
            .cache_lookup(CacheName::Index, Tier::Disk, disk_entry.is_some());
        span.record("cache.hit", disk_entry.is_some());
        let (entry, size) = disk_entry?;
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
            self.write_disk_entry(key, &entry, &codec).await;
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
        let matching: Vec<(String, String)> = {
            let map = self
                .prefix_index
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            map.iter()
                .filter(|(stored, _)| stored.starts_with(prefix))
                .map(|(stored, dir_name)| (stored.clone(), dir_name.clone()))
                .collect()
        };
        for (_, dir_name) in &matching {
            let dir = self.root.join(dir_name.as_str());
            let (bytes, entries) = dir_stats(&dir);
            let _ = tokio::fs::remove_dir_all(&dir).await;
            self.disk_bytes
                .fetch_sub(bytes.min(self.disk_bytes.load(Ordering::Relaxed)), Ordering::Relaxed);
            self.disk_entries.fetch_sub(
                entries.min(self.disk_entries.load(Ordering::Relaxed)),
                Ordering::Relaxed,
            );
        }
        if !matching.is_empty() {
            let snapshot = {
                let mut map = self
                    .prefix_index
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                for (stored, _) in &matching {
                    map.remove(stored);
                }
                map.clone()
            };
            persist_prefixes(&self.root.join(PREFIXES_FILE), &snapshot);
        }
    }

    async fn clear(&self) {
        self.memory_tier.clear().await;
        let _ = tokio::fs::remove_dir_all(&self.root).await;
        let _ = tokio::fs::create_dir_all(&self.root).await;
        self.prefix_index
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        self.disk_bytes.store(0, Ordering::Relaxed);
        self.disk_entries.store(0, Ordering::Relaxed);
    }

    async fn num_entries(&self) -> usize {
        self.memory_tier.num_entries().await + self.disk_entries.load(Ordering::Relaxed) as usize
    }

    async fn size_bytes(&self) -> usize {
        self.memory_tier.size_bytes().await + self.disk_bytes.load(Ordering::Relaxed) as usize
    }

    fn approx_num_entries(&self) -> usize {
        self.memory_tier.approx_num_entries() + self.disk_entries.load(Ordering::Relaxed) as usize
    }

    fn approx_size_bytes(&self) -> usize {
        self.memory_tier.approx_size_bytes() + self.disk_bytes.load(Ordering::Relaxed) as usize
    }
}

/// Loads the prefix sidecar. Missing or malformed files yield an empty map.
fn load_prefixes(path: &Path) -> HashMap<String, String> {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return HashMap::new();
    };
    let Ok(Value::Object(object)) = serde_json::from_str::<Value>(&raw) else {
        return HashMap::new();
    };
    object
        .into_iter()
        .filter_map(|(prefix, dir_name)| dir_name.as_str().map(|dir| (prefix, dir.to_string())))
        .collect()
}

/// Persists the prefix sidecar. Failures are swallowed (the map is rebuilt on demand).
fn persist_prefixes(path: &Path, map: &HashMap<String, String>) {
    let object: serde_json::Map<String, Value> = map
        .iter()
        .map(|(prefix, dir_name)| (prefix.clone(), Value::String(dir_name.clone())))
        .collect();
    let _ = std::fs::write(path, Value::Object(object).to_string());
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_core::cache::CacheCodecImpl;
    use std::sync::atomic::AtomicUsize;

    /// Toy serializable payload exercising the codec path.
    #[derive(Debug, PartialEq, Eq)]
    struct Payload(Vec<u8>);

    impl CacheCodecImpl for Payload {
        fn serialize(&self, writer: &mut dyn std::io::Write) -> LanceResult<()> {
            writer.write_all(&self.0)?;
            Ok(())
        }

        fn deserialize(data: &Bytes) -> LanceResult<Self> {
            Ok(Payload(data.to_vec()))
        }
    }

    /// Builds a key under the given prefix.
    fn key(prefix: &str, name: &str) -> InternalCacheKey {
        InternalCacheKey::new(Arc::from(prefix), Arc::from(name), "Payload")
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

    #[tokio::test]
    async fn insert_get_round_trip_persists_and_survives_reopen() {
        let tmp = tempfile::TempDir::new().unwrap();
        let backend =
            DiskIndexCacheBackend::open(tmp.path().to_path_buf(), 1024 * 1024, Arc::new(Metrics::disabled())).unwrap();
        let codec = CacheCodec::from_impl::<Payload>();
        let cache_key = key("s3://bucket/ds.lance/", "page-0");
        let entry: CacheEntry = Arc::new(Payload(vec![7u8; 32]));
        backend.insert(&cache_key, entry, 32, Some(codec)).await;
        assert_eq!(entry_file_count(tmp.path()), 1);
        let fetched = backend.get(&cache_key, Some(codec)).await.unwrap();
        assert_eq!(fetched.downcast_ref::<Payload>().unwrap().0, vec![7u8; 32]);
        drop(backend);
        let reopened =
            DiskIndexCacheBackend::open(tmp.path().to_path_buf(), 1024 * 1024, Arc::new(Metrics::disabled())).unwrap();
        let fetched = reopened.get(&cache_key, Some(codec)).await.unwrap();
        assert_eq!(fetched.downcast_ref::<Payload>().unwrap().0, vec![7u8; 32]);
        assert!(reopened.approx_num_entries() >= 1);
    }

    #[tokio::test]
    async fn codec_less_entries_stay_memory_only() {
        let tmp = tempfile::TempDir::new().unwrap();
        let backend =
            DiskIndexCacheBackend::open(tmp.path().to_path_buf(), 1024 * 1024, Arc::new(Metrics::disabled())).unwrap();
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
        let backend =
            DiskIndexCacheBackend::open(tmp.path().to_path_buf(), 1024 * 1024, Arc::new(Metrics::disabled())).unwrap();
        let codec = CacheCodec::new(
            |_, writer| {
                writer.write_all(b"valid")?;
                Ok(())
            },
            |data| {
                if data.as_ref() == b"valid" {
                    Ok(Arc::new(Payload(data.to_vec())))
                } else {
                    Err(lance_core::Error::internal("corrupt".to_string()))
                }
            },
        );
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
        let fresh =
            DiskIndexCacheBackend::open(tmp.path().to_path_buf(), 1024 * 1024, Arc::new(Metrics::disabled())).unwrap();
        assert!(fresh.get(&cache_key, Some(codec)).await.is_none());
        assert_eq!(entry_file_count(tmp.path()), 0);
    }

    #[tokio::test]
    async fn invalidate_prefix_removes_dataset_and_index_scoped_entries() {
        let tmp = tempfile::TempDir::new().unwrap();
        let backend =
            DiskIndexCacheBackend::open(tmp.path().to_path_buf(), 1024 * 1024, Arc::new(Metrics::disabled())).unwrap();
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
        let backend = Arc::new(
            DiskIndexCacheBackend::open(tmp.path().to_path_buf(), 1024 * 1024, Arc::new(Metrics::disabled())).unwrap(),
        );
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
        let backend =
            DiskIndexCacheBackend::open(tmp.path().to_path_buf(), 1024 * 1024, Arc::new(Metrics::disabled())).unwrap();
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
        assert_eq!(backend.disk_bytes.load(Ordering::Relaxed), walked);
        assert_eq!(backend.disk_entries.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn sweep_respects_budget_and_subsequent_get_misses_cleanly() {
        let tmp = tempfile::TempDir::new().unwrap();
        let backend = DiskIndexCacheBackend::open(tmp.path().to_path_buf(), 0, Arc::new(Metrics::disabled())).unwrap();
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
        backend.sweep(Duration::from_secs(3600), 500);
        assert!(backend.disk_bytes.load(Ordering::Relaxed) <= 500);
        let survivors = (0..8)
            .filter(|index| {
                std::fs::metadata(backend.entry_path(&key("s3://bucket/ds.lance/", &format!("page-{index}")), false))
                    .is_ok()
            })
            .count();
        assert!(survivors <= 2);
    }
}
