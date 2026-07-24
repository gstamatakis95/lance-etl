//! Local-disk [`EntryStore`]: the default persistent backend beneath both cache tiers.
//!
//! Keeps the exact on-disk layout the tiers used before the backend seam existed
//! (`{root}/{dir}/{file}` entry files, the `prefixes.json` registry sidecar, mtime recency),
//! so existing caches survive the refactor without a schema bump.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use crate::cache::entry_store::EntryStore;
use crate::cache::layout::{
    META_FILE, PREFIXES_FILE, SweepStats, atomic_write, dir_stats, gauge_sub, remove_dir_accounted, sweep_tier,
    touch_file,
};
use crate::telemetry::Tier;

/// Disk-backed [`EntryStore`] rooted at one tier's directory.
pub struct DiskEntryStore {
    root: PathBuf,
    prefix_index: Arc<RwLock<HashMap<String, String>>>,
    prefix_persist: Arc<Mutex<()>>,
    disk_bytes: AtomicU64,
    disk_entries: AtomicU64,
}

impl std::fmt::Debug for DiskEntryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskEntryStore")
            .field("root", &self.root)
            .field("disk_entries", &self.disk_entries.load(Ordering::Relaxed))
            .field("disk_bytes", &self.disk_bytes.load(Ordering::Relaxed))
            .finish()
    }
}

impl DiskEntryStore {
    /// Opens (or creates) the store under `root`. Seeds size accounting from a directory walk
    /// (excluding the registry sidecar) and removes orphaned temp files left by a prior crash.
    pub fn open(root: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&root)?;
        let prefix_index = load_prefixes(&root.join(PREFIXES_FILE));
        let (bytes, entries) = dir_stats(&root);
        Ok(Self {
            root,
            prefix_index: Arc::new(RwLock::new(prefix_index)),
            prefix_persist: Arc::new(Mutex::new(())),
            disk_bytes: AtomicU64::new(bytes),
            disk_entries: AtomicU64::new(entries),
        })
    }

    /// The absolute path of one entry file.
    fn entry_path(&self, dir: &str, file: &str) -> PathBuf {
        self.root.join(dir).join(file)
    }

    /// Whether accounting tracks this file. `META_FILE` sidecars are excluded to match
    /// [`dir_stats`] and the sweep, which never count or evict them directly.
    fn accounted(file: &str) -> bool {
        file != META_FILE
    }

    /// Sweeps the store: TTL expiry plus oldest-first eviction down to `budget_bytes`, then
    /// reconciles accounting and rewrites the prefix sidecar dropping empty directories.
    ///
    /// The sidecar is excluded from the sweep walk and every registry mutation is serialized with
    /// this reconciliation, so a concurrent registration cannot be clobbered by an older snapshot.
    pub fn sweep(&self, ttl: Duration, budget_bytes: u64) -> SweepStats {
        let persist_guard = self
            .prefix_persist
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let prefixes_path = self.root.join(PREFIXES_FILE);
        let had_sidecar = prefixes_path.exists();
        let stats = sweep_tier(&self.root, ttl, budget_bytes, &self.disk_bytes, &self.disk_entries);
        let snapshot = {
            let mut map = self
                .prefix_index
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            map.retain(|_, dir_name| self.root.join(dir_name.as_str()).is_dir());
            map.clone()
        };
        if had_sidecar || !snapshot.is_empty() {
            persist_prefixes(&prefixes_path, &snapshot);
        }
        drop(persist_guard);
        stats
    }
}

#[async_trait]
impl EntryStore for DiskEntryStore {
    async fn get(&self, dir: &str, file: &str) -> Option<Vec<u8>> {
        tokio::fs::read(self.entry_path(dir, file)).await.ok()
    }

    async fn get_pair(&self, dir: &str, first: &str, second: &str) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        let (first_read, second_read) = tokio::join!(
            tokio::fs::read(self.entry_path(dir, first)),
            tokio::fs::read(self.entry_path(dir, second))
        );
        (first_read.ok(), second_read.ok())
    }

    /// Size accounting re-stats the file after the rename rather than trusting the buffer
    /// length. On overwrite the old size is subtracted before the new size is added: the two
    /// atomics are not updated as one transaction, so the ordering bounds the transient error
    /// to an undercount (the janitor briefly under-evicts) instead of an overcount that could
    /// suppress eviction while the tier is over budget. The janitor sweep fully reconciles any
    /// residual drift.
    async fn put(&self, dir: &str, file: &str, bytes: &[u8]) {
        let path = self.entry_path(dir, file);
        let old_len = tokio::fs::metadata(&path).await.map(|meta| meta.len()).ok();
        if atomic_write(&path, bytes).await.is_ok() && Self::accounted(file) {
            let new_on_disk = tokio::fs::metadata(&path)
                .await
                .map(|meta| meta.len())
                .unwrap_or(bytes.len() as u64);
            match old_len {
                Some(old) => {
                    gauge_sub(&self.disk_bytes, old);
                    self.disk_bytes.fetch_add(new_on_disk, Ordering::Relaxed);
                }
                None => {
                    self.disk_bytes.fetch_add(new_on_disk, Ordering::Relaxed);
                    self.disk_entries.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    async fn put_if_absent(&self, dir: &str, file: &str, bytes: &[u8]) {
        let path = self.entry_path(dir, file);
        if tokio::fs::metadata(&path).await.is_ok() {
            return;
        }
        self.put(dir, file, bytes).await;
    }

    async fn remove_entry(&self, dir: &str, file: &str) {
        let path = self.entry_path(dir, file);
        if let Ok(meta) = tokio::fs::metadata(&path).await
            && Self::accounted(file)
        {
            gauge_sub(&self.disk_bytes, meta.len());
            gauge_sub(&self.disk_entries, 1);
        }
        let _ = tokio::fs::remove_file(&path).await;
    }

    async fn remove_dir(&self, dir: &str) {
        remove_dir_accounted(&self.root.join(dir), &self.disk_bytes, &self.disk_entries).await;
    }

    async fn clear(&self) {
        let root = self.root.clone();
        let prefix_index = self.prefix_index.clone();
        let prefix_persist = self.prefix_persist.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let persist_guard = prefix_persist.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let _ = std::fs::remove_dir_all(&root);
            let _ = std::fs::create_dir_all(&root);
            prefix_index
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clear();
            drop(persist_guard);
        })
        .await;
        self.disk_bytes.store(0, Ordering::Relaxed);
        self.disk_entries.store(0, Ordering::Relaxed);
    }

    /// Registry mutation and persistence run on the blocking pool under one serialization lock.
    /// This makes the returned future the durability boundary and prevents a janitor snapshot or
    /// sibling registration from clobbering the new row.
    async fn register_prefix(&self, prefix: &str, dir: &str) {
        let prefix = prefix.to_string();
        let dir = dir.to_string();
        let path = self.root.join(PREFIXES_FILE);
        let prefix_index = self.prefix_index.clone();
        let prefix_persist = self.prefix_persist.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let persist_guard = prefix_persist.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let snapshot = {
                let mut map = prefix_index.write().unwrap_or_else(|poisoned| poisoned.into_inner());
                if map.contains_key(&prefix) {
                    return;
                }
                map.insert(prefix, dir);
                map.clone()
            };
            persist_prefixes(&path, &snapshot);
            drop(persist_guard);
        })
        .await;
    }

    async fn prefix_entries(&self) -> HashMap<String, String> {
        self.prefix_index
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    async fn remove_prefixes(&self, prefixes: &[String]) {
        if prefixes.is_empty() {
            return;
        }
        let prefixes = prefixes.to_vec();
        let path = self.root.join(PREFIXES_FILE);
        let prefix_index = self.prefix_index.clone();
        let prefix_persist = self.prefix_persist.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let persist_guard = prefix_persist.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let snapshot = {
                let mut map = prefix_index.write().unwrap_or_else(|poisoned| poisoned.into_inner());
                for prefix in prefixes {
                    map.remove(&prefix);
                }
                map.clone()
            };
            persist_prefixes(&path, &snapshot);
            drop(persist_guard);
        })
        .await;
    }

    fn touch(&self, dir: &str, file: &str) {
        let path = self.entry_path(dir, file);
        drop(tokio::task::spawn_blocking(move || touch_file(&path)));
    }

    fn approx_stats(&self) -> (u64, u64) {
        (
            self.disk_bytes.load(Ordering::Relaxed),
            self.disk_entries.load(Ordering::Relaxed),
        )
    }

    fn tier(&self) -> Tier {
        Tier::Disk
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

/// Persists the prefix sidecar atomically (temp file plus rename), so a crash mid-write leaves
/// the previous sidecar intact instead of a torn JSON that would blank the map on restart.
/// Failures are swallowed (the map is rebuilt on demand).
fn persist_prefixes(path: &Path, map: &HashMap<String, String>) {
    let object: serde_json::Map<String, Value> = map
        .iter()
        .map(|(prefix, dir_name)| (prefix.clone(), Value::String(dir_name.clone())))
        .collect();
    let Some(parent) = path.parent() else {
        return;
    };
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let tmp = parent.join(format!("prefixes.json.tmp-{}-{nonce}", std::process::id()));
    if std::fs::write(&tmp, Value::Object(object).to_string()).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_registration_and_sweep_preserve_the_registry() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(DiskEntryStore::open(tmp.path().to_path_buf()).unwrap());
        let mut registrations = tokio::task::JoinSet::new();
        for index in 0..32 {
            let dir = format!("dir-{index}");
            store.put(&dir, "entry.bin", b"value").await;
            let store = store.clone();
            registrations.spawn(async move {
                store.register_prefix(&format!("prefix-{index}"), &dir).await;
            });
        }
        let sweeping_store = store.clone();
        let sweeper = tokio::task::spawn_blocking(move || {
            for _ in 0..8 {
                sweeping_store.sweep(Duration::from_secs(600), u64::MAX);
            }
        });
        while let Some(result) = registrations.join_next().await {
            result.unwrap();
        }
        sweeper.await.unwrap();
        let expected = store.prefix_entries().await;
        assert_eq!(expected.len(), 32);
        assert!(tmp.path().join(PREFIXES_FILE).exists());
        drop(store);
        let reopened = DiskEntryStore::open(tmp.path().to_path_buf()).unwrap();
        assert_eq!(reopened.prefix_entries().await, expected);
    }
}
