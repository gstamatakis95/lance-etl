//! The persistent byte-store seam beneath both cache tiers.
//!
//! The compositions (the hybrid index cache backend and the metadata byte cache) own all
//! routing, framing, and memory-tier logic. This trait is only the persistence beneath them,
//! so a backend swap (local disk, shared Redis) never touches the cache semantics.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::telemetry::Tier;

/// Persistent byte store keyed by `(dir, file)`, the invalidation unit being the dir.
///
/// The index tier uses a hashed cache-key prefix as the dir and one file per
/// `(key, type_name)` pair. The store tier uses a hashed `(store_prefix, location)` pair as the
/// dir holding one `meta.json` sidecar plus one file per request shape. Values are opaque
/// framed bytes: corruption detection lives in the composition, which reacts to a bad frame by
/// calling [`EntryStore::remove_entry`] or [`EntryStore::remove_dir`].
///
/// Implementations never surface errors: a failed read is a miss, a failed write is dropped,
/// and failures are metered internally (see `Metrics::cache_backend_error`), so a degraded
/// backend can never fail a search.
#[async_trait]
pub trait EntryStore: Send + Sync + std::fmt::Debug + 'static {
    /// Reads one entry, or `None` on a miss or a backend failure.
    async fn get(&self, dir: &str, file: &str) -> Option<Vec<u8>>;

    /// Reads two entries of one dir, in a single round trip where the backend allows it.
    async fn get_pair(&self, dir: &str, first: &str, second: &str) -> (Option<Vec<u8>>, Option<Vec<u8>>);

    /// Writes one entry, overwriting any previous value.
    async fn put(&self, dir: &str, file: &str, bytes: &[u8]);

    /// Writes one entry only when absent. Concurrent writers of the same key race benignly.
    async fn put_if_absent(&self, dir: &str, file: &str, bytes: &[u8]);

    /// Removes one entry (corrupt-frame cleanup).
    async fn remove_entry(&self, dir: &str, file: &str);

    /// Removes one dir and every entry in it (the invalidation unit).
    async fn remove_dir(&self, dir: &str);

    /// Removes every entry in the store (admin path, rare).
    async fn clear(&self);

    /// Records an idempotent `prefix -> dir` row in this store's prefix registry.
    ///
    /// The registry lets the index tier resolve a string-prefix invalidation to the dirs it
    /// covers. The store tier never calls this.
    async fn register_prefix(&self, prefix: &str, dir: &str);

    /// Returns the full prefix registry as `prefix -> dir`.
    async fn prefix_entries(&self) -> HashMap<String, String>;

    /// Removes the given prefixes from the registry.
    async fn remove_prefixes(&self, prefixes: &[String]);

    /// Refreshes the recency signal of one entry (disk mtime touch, Redis TTL refresh).
    ///
    /// Best-effort and non-blocking: implementations spawn the refresh and return immediately.
    fn touch(&self, dir: &str, file: &str);

    /// Approximate resident `(bytes, entries)` of this store.
    fn approx_stats(&self) -> (u64, u64);

    /// Metric tier tag for lookups against this store.
    fn tier(&self) -> Tier;
}

#[cfg(test)]
pub(crate) mod fake {
    //! HashMap-backed [`EntryStore`] fake for exercising the compositions without disk or Redis.

    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    use async_trait::async_trait;

    use super::EntryStore;
    use crate::telemetry::Tier;

    /// In-memory [`EntryStore`] with operation counters for call-shape assertions.
    #[derive(Debug, Default)]
    pub struct MemoryEntryStore {
        entries: Mutex<HashMap<(String, String), Vec<u8>>>,
        prefixes: Mutex<HashMap<String, String>>,
        /// Number of `get` and `get_pair` calls served.
        pub gets: AtomicU64,
        /// Number of `put` and `put_if_absent` calls applied.
        pub puts: AtomicU64,
        /// Number of `touch` calls received.
        pub touches: AtomicU64,
    }

    #[async_trait]
    impl EntryStore for MemoryEntryStore {
        async fn get(&self, dir: &str, file: &str) -> Option<Vec<u8>> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.entries
                .lock()
                .unwrap()
                .get(&(dir.to_string(), file.to_string()))
                .cloned()
        }

        async fn get_pair(&self, dir: &str, first: &str, second: &str) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
            self.gets.fetch_add(1, Ordering::SeqCst);
            let entries = self.entries.lock().unwrap();
            (
                entries.get(&(dir.to_string(), first.to_string())).cloned(),
                entries.get(&(dir.to_string(), second.to_string())).cloned(),
            )
        }

        async fn put(&self, dir: &str, file: &str, bytes: &[u8]) {
            self.puts.fetch_add(1, Ordering::SeqCst);
            self.entries
                .lock()
                .unwrap()
                .insert((dir.to_string(), file.to_string()), bytes.to_vec());
        }

        async fn put_if_absent(&self, dir: &str, file: &str, bytes: &[u8]) {
            self.puts.fetch_add(1, Ordering::SeqCst);
            self.entries
                .lock()
                .unwrap()
                .entry((dir.to_string(), file.to_string()))
                .or_insert_with(|| bytes.to_vec());
        }

        async fn remove_entry(&self, dir: &str, file: &str) {
            self.entries
                .lock()
                .unwrap()
                .remove(&(dir.to_string(), file.to_string()));
        }

        async fn remove_dir(&self, dir: &str) {
            self.entries
                .lock()
                .unwrap()
                .retain(|(entry_dir, _), _| entry_dir != dir);
        }

        async fn clear(&self) {
            self.entries.lock().unwrap().clear();
            self.prefixes.lock().unwrap().clear();
        }

        async fn register_prefix(&self, prefix: &str, dir: &str) {
            self.prefixes
                .lock()
                .unwrap()
                .entry(prefix.to_string())
                .or_insert_with(|| dir.to_string());
        }

        async fn prefix_entries(&self) -> HashMap<String, String> {
            self.prefixes.lock().unwrap().clone()
        }

        async fn remove_prefixes(&self, prefixes: &[String]) {
            let mut map = self.prefixes.lock().unwrap();
            for prefix in prefixes {
                map.remove(prefix);
            }
        }

        fn touch(&self, _dir: &str, _file: &str) {
            self.touches.fetch_add(1, Ordering::SeqCst);
        }

        fn approx_stats(&self) -> (u64, u64) {
            let entries = self.entries.lock().unwrap();
            let bytes = entries.values().map(|value| value.len() as u64).sum();
            (bytes, entries.len() as u64)
        }

        fn tier(&self) -> Tier {
            Tier::Remote
        }
    }
}
