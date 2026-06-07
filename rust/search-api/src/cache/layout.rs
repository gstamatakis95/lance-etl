//! Shared on-disk layout for the persistent caches: versioned stamp directory, key hashing,
//! atomic file writes, and the TTL/budget sweep used by the janitor.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

/// Version of our on-disk cache schema. Bump on any layout or format change.
pub const CACHE_SCHEMA_VERSION: u32 = 1;

/// Lance crate version baked into the stamp. Bump together with the `lance` path dependency
/// because the cache codec format is explicitly unstable across lance releases.
pub const LANCE_CACHE_STAMP: &str = "8.0.0-beta.6";

/// Substring marking in-progress write files which readers must ignore and sweeps may delete.
const TMP_MARKER: &str = ".tmp-";

/// File name of the per-object `ObjectMeta` sidecar written by the store cache.
///
/// Sidecars are excluded from residency accounting and from the sweep's eviction set: they are
/// never inserted through `record_insert`, so counting them in `dir_stats` would make the in-process
/// gauges diverge from the on-disk reality. Lone sidecars are reclaimed by `prune_empty_dirs`.
pub const META_FILE: &str = "meta.json";

/// Returns the stamp directory name combining our schema version and the lance version.
pub fn stamp_dir_name() -> String {
    format!("v{CACHE_SCHEMA_VERSION}-lance-{LANCE_CACHE_STAMP}")
}

/// Creates `{cache_dir}/{stamp}` and deletes any sibling directory with a different stamp
/// (stale layouts from lance upgrades or our own format changes). Returns the stamp path.
pub fn prepare_cache_root(cache_dir: &Path) -> std::io::Result<PathBuf> {
    let stamp = stamp_dir_name();
    let root = cache_dir.join(&stamp);
    std::fs::create_dir_all(&root)?;
    for entry in std::fs::read_dir(cache_dir)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy() != stamp.as_str() {
            let stale = entry.path();
            if stale.is_dir() {
                let _ = std::fs::remove_dir_all(&stale);
            } else {
                let _ = std::fs::remove_file(&stale);
            }
        }
    }
    Ok(root)
}

/// Hashes `input` with blake3 and returns the first `hex_len` hex characters.
pub fn hash_hex(input: &str, hex_len: usize) -> String {
    let mut hex = blake3::hash(input.as_bytes()).to_hex().to_string();
    hex.truncate(hex_len);
    hex
}

/// Writes `bytes` to `path` atomically: temp file in the same directory, then rename.
///
/// Concurrent writers of the same key race benignly (last rename wins, both contents are valid).
/// Readers never observe partial files.
pub async fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("cache path has no parent directory"))?;
    tokio::fs::create_dir_all(parent).await?;
    let nonce = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let file_name = path
        .file_name()
        .ok_or_else(|| std::io::Error::other("cache path has no file name"))?
        .to_string_lossy()
        .to_string();
    let tmp = parent.join(format!("{file_name}{TMP_MARKER}{}-{nonce}", std::process::id()));
    tokio::fs::write(&tmp, bytes).await?;
    tokio::fs::rename(&tmp, path).await
}

/// Decrements an atomic residency gauge, saturating at zero.
///
/// Uses a single `fetch_update` so the read-and-subtract is one atomic transaction. A plain
/// load-then-`fetch_sub` pair has a TOCTOU window: concurrent decrements can each clamp against the
/// same stale snapshot and drive the gauge below zero, wrapping it near `u64::MAX`. The saturating
/// update closes that window entirely.
pub fn gauge_sub(gauge: &AtomicU64, amount: u64) {
    let _ = gauge.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(amount))
    });
}

/// Best-effort mtime refresh so the sweep's LRU-by-mtime approximation tracks disk hits.
pub fn touch_file(path: &Path) {
    let _ = std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|file| file.set_modified(SystemTime::now()));
}

/// Recursively collects all regular files under `root`, deleting orphaned temp files on the way.
fn collect_files(root: &Path, files: &mut Vec<(PathBuf, u64, SystemTime)>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, files);
            continue;
        }
        if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().contains(TMP_MARKER))
        {
            let _ = std::fs::remove_file(&path);
            continue;
        }
        if path.file_name().is_some_and(|name| name == META_FILE) {
            continue;
        }
        if let Ok(meta) = entry.metadata() {
            let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            files.push((path, meta.len(), mtime));
        }
    }
}

/// Walks `root` and returns `(total_bytes, file_count)` of all cache entry files, removing
/// orphaned temp files as a side effect.
pub fn dir_stats(root: &Path) -> (u64, u64) {
    let mut files = Vec::new();
    collect_files(root, &mut files);
    let bytes = files.iter().map(|(_, len, _)| *len).sum();
    (bytes, files.len() as u64)
}

/// Outcome of one tier sweep: post-sweep residency plus how many entries each policy evicted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepStats {
    /// Bytes remaining in the tier after the sweep.
    pub remaining_bytes: u64,
    /// Entry files remaining in the tier after the sweep.
    pub remaining_entries: u64,
    /// Entries deleted because they outlived the TTL.
    pub ttl_evicted: u64,
    /// Entries deleted (oldest first) to fit the byte budget.
    pub size_evicted: u64,
}

/// Sweeps one cache tier: deletes entries older than `ttl`, then deletes oldest-mtime entries
/// until the tier fits in `budget_bytes`. Empty subdirectories are pruned. The given atomics are
/// reconciled to the post-sweep totals.
pub fn sweep_tier(
    root: &Path,
    ttl: Duration,
    budget_bytes: u64,
    bytes_gauge: &AtomicU64,
    entries_gauge: &AtomicU64,
) -> SweepStats {
    let mut files = Vec::new();
    collect_files(root, &mut files);
    let now = SystemTime::now();
    let mut stats = SweepStats::default();
    let mut survivors = Vec::new();
    for (path, len, mtime) in files {
        let expired = now.duration_since(mtime).map(|age| age > ttl).unwrap_or(false);
        if expired {
            let _ = std::fs::remove_file(&path);
            stats.ttl_evicted += 1;
        } else {
            survivors.push((path, len, mtime));
        }
    }
    survivors.sort_by_key(|(_, _, mtime)| *mtime);
    let mut total: u64 = survivors.iter().map(|(_, len, _)| *len).sum();
    for (path, len, _) in survivors {
        if total > budget_bytes {
            let _ = std::fs::remove_file(&path);
            total -= len;
            stats.size_evicted += 1;
        } else {
            stats.remaining_entries += 1;
        }
    }
    prune_empty_dirs(root, false);
    stats.remaining_bytes = total;
    bytes_gauge.store(stats.remaining_bytes, Ordering::Relaxed);
    entries_gauge.store(stats.remaining_entries, Ordering::Relaxed);
    stats
}

/// Removes empty directories below `root` (and `root` itself when `include_root` is set).
///
/// A directory whose only remaining files are `META_FILE` sidecars counts as empty: its data
/// entries have all been evicted, so the orphaned sidecars are deleted along with the directory.
/// This keeps sidecars from accumulating now that the sweep no longer evicts them directly.
fn prune_empty_dirs(root: &Path, include_root: bool) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            prune_empty_dirs(&path, true);
        }
    }
    if include_root && dir_holds_only_sidecars(root) {
        let _ = std::fs::remove_dir_all(root);
    }
}

/// Reports whether `root` contains nothing but `META_FILE` sidecars (and so holds no live entry).
fn dir_holds_only_sidecars(root: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(root) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() || path.file_name().is_none_or(|name| name != META_FILE) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamp_mismatch_dirs_are_wiped() {
        let tmp = tempfile::TempDir::new().unwrap();
        let stale = tmp.path().join("v0-lance-7.0.0");
        std::fs::create_dir_all(stale.join("index")).unwrap();
        std::fs::write(stale.join("index/x.bin"), b"old").unwrap();
        let root = prepare_cache_root(tmp.path()).unwrap();
        assert!(root.ends_with(stamp_dir_name()));
        assert!(!stale.exists());
        let root_again = prepare_cache_root(tmp.path()).unwrap();
        assert_eq!(root, root_again);
    }

    #[test]
    fn sweep_enforces_ttl_and_budget() {
        let tmp = tempfile::TempDir::new().unwrap();
        let root = tmp.path();
        let expired = root.join("a/expired.bin");
        std::fs::create_dir_all(expired.parent().unwrap()).unwrap();
        std::fs::write(&expired, vec![0u8; 10]).unwrap();
        let old_time = SystemTime::now() - Duration::from_secs(3600);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&expired)
            .unwrap()
            .set_modified(old_time)
            .unwrap();
        let older = root.join("b/older.bin");
        std::fs::create_dir_all(older.parent().unwrap()).unwrap();
        std::fs::write(&older, vec![0u8; 64]).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&older)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(60))
            .unwrap();
        let newer = root.join("b/newer.bin");
        std::fs::write(&newer, vec![0u8; 64]).unwrap();
        let bytes = AtomicU64::new(0);
        let entries = AtomicU64::new(0);
        let stats = sweep_tier(root, Duration::from_secs(600), 100, &bytes, &entries);
        assert!(!expired.exists(), "expired entry should be deleted by TTL");
        assert!(!older.exists(), "oldest entry should be deleted to fit the budget");
        assert!(newer.exists(), "newest entry should survive");
        assert_eq!(stats.remaining_bytes, 64);
        assert_eq!(stats.remaining_entries, 1);
        assert_eq!(stats.ttl_evicted, 1);
        assert_eq!(stats.size_evicted, 1);
        assert_eq!(bytes.load(Ordering::Relaxed), 64);
    }

    #[test]
    fn hash_hex_is_stable_and_truncated() {
        assert_eq!(hash_hex("abc", 16).len(), 16);
        assert_eq!(hash_hex("abc", 16), hash_hex("abc", 16));
        assert_ne!(hash_hex("abc", 16), hash_hex("abd", 16));
    }
}
