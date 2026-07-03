//! Local-disk [`EntryStore`]: the default persistent backend beneath both cache tiers.
//!
//! Keeps the exact on-disk layout the tiers used before the backend seam existed
//! (`{root}/{dir}/{file}` entry files, the `prefixes.json` registry sidecar, mtime recency),
//! so existing caches survive the refactor without a schema bump.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use crate::cache::entry_store::EntryStore;
use crate::cache::layout::{
    META_FILE, SweepStats, atomic_write, dir_stats, gauge_sub, remove_dir_accounted, sweep_tier, touch_file,
};
use crate::telemetry::Tier;

/// Sidecar file mapping full cache-key prefixes to their hashed directory names, enabling
/// prefix invalidation to find directories by string-prefix match across process restarts.
const PREFIXES_FILE: &str = "prefixes.json";

/// Disk-backed [`EntryStore`] rooted at one tier's directory.
pub struct DiskEntryStore {
    root: PathBuf,
    prefix_index: RwLock<HashMap<String, String>>,
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
        let entries = entries.saturating_sub(if root.join(PREFIXES_FILE).exists() { 1 } else { 0 });
        let bytes = bytes.saturating_sub(
            std::fs::metadata(root.join(PREFIXES_FILE))
                .map(|meta| meta.len())
                .unwrap_or(0),
        );
        Ok(Self {
            root,
            prefix_index: RwLock::new(prefix_index),
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
    /// The sidecar is removed before the sweep walk so it is never counted or evicted as an
    /// entry, and rewritten afterwards from the retained map. A store that never registered a
    /// prefix (the metadata byte tier) skips the sidecar handling entirely so no stray
    /// `prefixes.json` appears in its directory.
    pub fn sweep(&self, ttl: Duration, budget_bytes: u64) -> SweepStats {
        let prefixes_path = self.root.join(PREFIXES_FILE);
        let had_sidecar = prefixes_path.exists();
        if had_sidecar {
            let _ = std::fs::remove_file(&prefixes_path);
        }
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
        let _ = tokio::fs::remove_dir_all(&self.root).await;
        let _ = tokio::fs::create_dir_all(&self.root).await;
        self.prefix_index
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        self.disk_bytes.store(0, Ordering::Relaxed);
        self.disk_entries.store(0, Ordering::Relaxed);
    }

    /// The write lock is held only for the in-memory map update, then dropped before the
    /// synchronous filesystem write. Holding the lock across `std::fs::write` would block every
    /// concurrent insert for the entire disk-flush duration — exactly the mass-cold-open
    /// scenario where many inserts fire at once. The sidecar is a rebuildable hint: a lost
    /// write between the lock drop and the file write means prefix invalidation may miss some
    /// directories on the next process start, but the janitor sweep reconciles the map from
    /// the actual directory listing, so eventual consistency is acceptable.
    async fn register_prefix(&self, prefix: &str, dir: &str) {
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
            map.insert(prefix.to_string(), dir.to_string());
            map.clone()
        };
        let path = self.root.join(PREFIXES_FILE);
        tokio::task::spawn_blocking(move || persist_prefixes(&path, &snapshot));
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
        let snapshot = {
            let mut map = self
                .prefix_index
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for prefix in prefixes {
                map.remove(prefix);
            }
            map.clone()
        };
        persist_prefixes(&self.root.join(PREFIXES_FILE), &snapshot);
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
    let tmp = parent.join(format!("prefixes.json.tmp-{}", std::process::id()));
    if std::fs::write(&tmp, Value::Object(object).to_string()).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}
