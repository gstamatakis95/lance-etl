//! Shared on-disk layout for the persistent caches: versioned stamp directory, key hashing,
//! atomic file writes, and the TTL/budget sweep used by the janitor.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

/// Version of our on-disk cache schema. Bump on any layout or format change.
///
/// v2 wraps every entry payload in a checksummed frame (see [`frame_bytes`]) so torn writes and
/// bit rot are detected on read instead of being served to lance readers verbatim.
pub const CACHE_SCHEMA_VERSION: u32 = 2;

/// Lance crate version baked into the stamp. Bump together with the `lance` dependency in
/// `Cargo.toml` because the cache codec format is explicitly unstable across lance releases.
pub const LANCE_CACHE_STAMP: &str = "8.0.0";

/// Magic prefix of a framed cache entry, versioned with the frame layout.
const FRAME_MAGIC: &[u8; 4] = b"LEC2";

/// Bytes a frame adds ahead of the payload: the magic plus a 32-byte blake3 checksum.
pub const FRAME_OVERHEAD: usize = 4 + 32;

/// Substring marking in-progress write files which readers must ignore and sweeps may delete.
const TMP_MARKER: &str = ".tmp-";

/// File name of the per-object `ObjectMeta` sidecar written by the store cache.
///
/// Sidecars are excluded from residency accounting and from the sweep's eviction set, so
/// counting them in `dir_stats` would make the in-process gauges diverge from the on-disk
/// reality. Lone sidecars are reclaimed by `prune_empty_dirs`.
pub const META_FILE: &str = "meta.json";

/// Returns the stamp directory name combining our schema version and the lance version.
pub fn stamp_dir_name() -> String {
    format!("v{CACHE_SCHEMA_VERSION}-lance-{LANCE_CACHE_STAMP}")
}

/// Reports whether `name` matches the versioned stamp naming pattern `v{N}-lance-{version}`.
///
/// Gates the stale-stamp wipe in [`prepare_cache_root`]: only entries this service itself named
/// are ever deleted, so a misconfigured `SEARCH_API_CACHE_DIR` pointed at a shared volume never
/// loses unrelated data.
fn is_stamp_dir_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix('v') else {
        return false;
    };
    let Some((schema_version, lance_version)) = rest.split_once("-lance-") else {
        return false;
    };
    !schema_version.is_empty() && schema_version.bytes().all(|byte| byte.is_ascii_digit()) && !lance_version.is_empty()
}

/// Creates `{cache_dir}/{stamp}` and deletes any sibling entry carrying a *different* stamp name
/// (stale layouts from lance upgrades or our own format changes). Returns the stamp path.
///
/// Deletion is restricted to entries matching the versioned stamp naming pattern
/// ([`is_stamp_dir_name`]). Anything else in the cache directory is left untouched, so pointing
/// `SEARCH_API_CACHE_DIR` at a directory that also holds unrelated data can never destroy it.
pub fn prepare_cache_root(cache_dir: &Path) -> std::io::Result<PathBuf> {
    let stamp = stamp_dir_name();
    let root = cache_dir.join(&stamp);
    std::fs::create_dir_all(&root)?;
    for entry in std::fs::read_dir(cache_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name == stamp || !is_stamp_dir_name(&name) {
            continue;
        }
        let stale = entry.path();
        if stale.is_dir() {
            let _ = std::fs::remove_dir_all(&stale);
        } else {
            let _ = std::fs::remove_file(&stale);
        }
    }
    Ok(root)
}

/// Wraps a payload in the checksummed on-disk frame: magic, blake3 of the payload, payload.
pub fn frame_bytes(payload: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(FRAME_OVERHEAD + payload.len());
    framed.extend_from_slice(FRAME_MAGIC);
    framed.extend_from_slice(blake3::hash(payload).as_bytes());
    framed.extend_from_slice(payload);
    framed
}

/// Verifies and strips the frame, returning the payload. `None` means the entry is corrupt:
/// too short, wrong magic (including pre-v2 unframed files), or a checksum mismatch from a torn
/// write or bit rot. Callers treat `None` as a cache miss and delete the file.
pub fn unframe_bytes(buf: Vec<u8>) -> Option<bytes::Bytes> {
    if buf.len() < FRAME_OVERHEAD || &buf[..4] != FRAME_MAGIC {
        return None;
    }
    let expected: [u8; 32] = buf[4..FRAME_OVERHEAD].try_into().ok()?;
    let payload = bytes::Bytes::from(buf).slice(FRAME_OVERHEAD..);
    if blake3::hash(&payload).as_bytes() != &expected {
        return None;
    }
    Some(payload)
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

/// Removes one cache directory and decrements both residency gauges by its contents.
///
/// Stats the directory first (the size and entry count it holds), removes it, then subtracts both
/// figures from the gauges with the saturating [`gauge_sub`]. Shared by the index tier's
/// prefix invalidation and the store tier's per-object invalidation, which previously each
/// inlined the same stat-remove-decrement sequence.
pub async fn remove_dir_accounted(dir: &Path, bytes_gauge: &AtomicU64, entries_gauge: &AtomicU64) {
    let (bytes, entries) = dir_stats(dir);
    let _ = tokio::fs::remove_dir_all(dir).await;
    gauge_sub(bytes_gauge, bytes);
    gauge_sub(entries_gauge, entries);
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
    fn stamp_wipe_never_touches_entries_outside_the_stamp_naming_pattern() {
        let tmp = tempfile::TempDir::new().unwrap();
        let foreign_dir = tmp.path().join("user-data");
        std::fs::create_dir_all(&foreign_dir).unwrap();
        std::fs::write(foreign_dir.join("keep.bin"), b"precious").unwrap();
        let foreign_file = tmp.path().join("notes.txt");
        std::fs::write(&foreign_file, b"also precious").unwrap();
        let near_miss = tmp.path().join("vX-lance-8.0.0");
        std::fs::create_dir_all(&near_miss).unwrap();
        let stale = tmp.path().join("v1-lance-7.0.0");
        std::fs::create_dir_all(&stale).unwrap();
        std::fs::write(stale.join("old.bin"), b"old").unwrap();
        prepare_cache_root(tmp.path()).unwrap();
        assert!(foreign_dir.join("keep.bin").exists(), "unrelated dirs must survive");
        assert!(foreign_file.exists(), "unrelated files must survive");
        assert!(near_miss.exists(), "names outside the stamp pattern must survive");
        assert!(!stale.exists(), "an old-stamp sibling must be removed");
    }

    #[test]
    fn stamp_name_pattern_accepts_stamps_and_rejects_everything_else() {
        assert!(is_stamp_dir_name(&stamp_dir_name()));
        assert!(is_stamp_dir_name("v0-lance-7.0.0"));
        assert!(is_stamp_dir_name("v12-lance-9.0.0-beta.1"));
        assert!(!is_stamp_dir_name("user-data"));
        assert!(!is_stamp_dir_name("v-lance-8.0.0"));
        assert!(!is_stamp_dir_name("vX-lance-8.0.0"));
        assert!(!is_stamp_dir_name("v2-lance-"));
        assert!(!is_stamp_dir_name("prefixes.json"));
        assert!(!is_stamp_dir_name(""));
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
    fn frame_round_trip_and_corruption_detection() {
        let payload = b"index page bytes".to_vec();
        let framed = frame_bytes(&payload);
        assert_eq!(framed.len(), FRAME_OVERHEAD + payload.len());
        assert_eq!(unframe_bytes(framed.clone()).unwrap().as_ref(), payload.as_slice());

        let mut flipped = framed.clone();
        let last = flipped.len() - 1;
        flipped[last] ^= 0xFF;
        assert!(unframe_bytes(flipped).is_none(), "bit flip must be detected");

        let truncated = framed[..framed.len() - 1].to_vec();
        assert!(unframe_bytes(truncated).is_none(), "torn write must be detected");

        assert!(unframe_bytes(b"raw pre-v2 content".to_vec()).is_none());
        assert!(unframe_bytes(Vec::new()).is_none());
        let empty = frame_bytes(b"");
        assert_eq!(unframe_bytes(empty).unwrap().len(), 0, "empty payload is representable");
    }

    #[test]
    fn hash_hex_is_stable_and_truncated() {
        assert_eq!(hash_hex("abc", 16).len(), 16);
        assert_eq!(hash_hex("abc", 16), hash_hex("abc", 16));
        assert_ne!(hash_hex("abc", 16), hash_hex("abd", 16));
    }
}
