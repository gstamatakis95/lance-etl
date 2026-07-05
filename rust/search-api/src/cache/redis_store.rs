//! Shared-Redis [`EntryStore`]: one warm cache spanning every replica.
//!
//! Each dir is ONE Redis HASH (`{namespace}:{stamp}:{tier}:{dir}`) whose fields are the entry
//! file names, so dir invalidation is a single `DEL`, the store tier's entry-plus-sidecar read
//! is one `HMGET`, and an `allkeys-lru` `maxmemory` policy evicts at the same whole-object
//! granularity the disk sweep uses. Recency and expiry are native: every put and hit refreshes
//! a per-dir `EXPIRE`, so no local janitor sweep exists for this backend. Values keep the same
//! checksummed frame as disk entries, guarding torn or corrupted values in transit too.
//!
//! Every operation degrades instead of failing: an errored round trip is a miss or a dropped
//! write, metered through `cache.backend_errors`, so a down Redis never fails a search.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;

use crate::cache::entry_store::EntryStore;
use crate::cache::layout::stamp_dir_name;
use crate::telemetry::{CacheName, Metrics, StoreOp, Tier};

/// Wall-clock budget for establishing the initial Redis connection at startup.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Keys scanned or deleted per round trip on the admin `clear` path.
const SCAN_BATCH: usize = 512;

/// Dir keys probed per pipelined `EXISTS` round trip during registry hygiene.
const HYGIENE_BATCH: usize = 128;

/// Age after which a locally seen prefix is re-registered with `HSETNX`.
///
/// A sibling replica's hygiene pass may delete a shared registry row whose dir went cold while
/// this replica's local seen-set still claims it is registered. Re-registering warm prefixes on
/// this cadence (matching the hygiene interval) restores the shared row within one period, so a
/// dataset purge can miss a re-warmed prefix for at most one refresh window instead of forever.
const REGISTER_REFRESH: Duration = Duration::from_secs(crate::config::REDIS_REGISTRY_HYGIENE_SECS);

/// Redis-backed [`EntryStore`] for one cache tier.
pub struct RedisEntryStore {
    manager: ConnectionManager,
    key_prefix: String,
    ttl_secs: i64,
    cache: CacheName,
    registered: Mutex<HashMap<String, Instant>>,
    put_bytes: AtomicU64,
    put_entries: AtomicU64,
    metrics: Arc<Metrics>,
}

impl std::fmt::Debug for RedisEntryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisEntryStore")
            .field("key_prefix", &self.key_prefix)
            .field("ttl_secs", &self.ttl_secs)
            .finish()
    }
}

impl RedisEntryStore {
    /// Connects to Redis at `url` (`redis://` or `rediss://`) and namespaces every key under
    /// `{namespace}:{stamp}:{tier_label}`.
    ///
    /// The stamp segment (see [`stamp_dir_name`]) binds keys to the cache schema and lance
    /// versions: after an upgrade the new process reads and writes fresh keys while the old
    /// generation simply ages out through its TTLs, with no wipe. The initial connection is
    /// bounded by a short timeout so an unreachable server fails construction fast and the
    /// caller can fall back to memory-only caching.
    pub async fn connect(
        url: &str,
        namespace: &str,
        tier_label: &str,
        ttl: Duration,
        cache: CacheName,
        metrics: Arc<Metrics>,
    ) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(url)?;
        let manager = match tokio::time::timeout(CONNECT_TIMEOUT, ConnectionManager::new(client)).await {
            Ok(connected) => connected?,
            Err(_) => {
                return Err(redis::RedisError::from((
                    redis::ErrorKind::Io,
                    "timed out connecting to redis",
                )));
            }
        };
        Ok(Self {
            manager,
            key_prefix: format!("{namespace}:{}:{tier_label}", stamp_dir_name()),
            ttl_secs: ttl.as_secs() as i64,
            cache,
            registered: Mutex::new(HashMap::new()),
            put_bytes: AtomicU64::new(0),
            put_entries: AtomicU64::new(0),
            metrics,
        })
    }

    /// The Redis HASH key holding one dir's entries.
    fn dir_key(&self, dir: &str) -> String {
        format!("{}:{dir}", self.key_prefix)
    }

    /// The Redis HASH key of the prefix registry.
    ///
    /// The registry carries NO TTL: expiring it would silently break prefix invalidation, which
    /// is a correctness path for dataset purges. Rows whose dir key has expired are reclaimed by
    /// [`RedisEntryStore::spawn_registry_hygiene`].
    fn registry_key(&self) -> String {
        format!("{}:prefixes", self.key_prefix)
    }

    /// Meters and logs one failed round trip. The caller degrades to a miss or a dropped write.
    fn note_error(&self, op: StoreOp, error: &redis::RedisError) {
        self.metrics.cache_backend_error(self.cache, op);
        tracing::warn!(
            error = %error,
            cache = self.cache.as_tag(),
            op = op.as_tag(),
            "redis cache operation failed, degrading to a miss"
        );
    }

    /// Spawns the hourly registry hygiene loop: probes every registered dir key and deletes
    /// registry rows whose dir has expired or been evicted, so index-UUID churn cannot grow the
    /// registry unboundedly. Dropping the handle aborts nothing, callers `abort()` on shutdown
    /// if needed.
    pub fn spawn_registry_hygiene(self: &Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        let store = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                store.registry_hygiene_once().await;
            }
        })
    }

    /// One registry hygiene pass. Errors are metered and end the pass early: the next interval
    /// retries from scratch.
    async fn registry_hygiene_once(&self) {
        let mut conn = self.manager.clone();
        let entries: HashMap<String, String> = match conn.hgetall(self.registry_key()).await {
            Ok(entries) => entries,
            Err(error) => {
                self.note_error(StoreOp::Registry, &error);
                return;
            }
        };
        let rows: Vec<(String, String)> = entries.into_iter().collect();
        let mut dead: Vec<String> = Vec::new();
        for chunk in rows.chunks(HYGIENE_BATCH) {
            let mut pipe = redis::pipe();
            for (_, dir) in chunk {
                pipe.exists(self.dir_key(dir));
            }
            let exists: Vec<bool> = match pipe.query_async(&mut conn).await {
                Ok(exists) => exists,
                Err(error) => {
                    self.note_error(StoreOp::Registry, &error);
                    return;
                }
            };
            for ((prefix, _), alive) in chunk.iter().zip(exists) {
                if !alive {
                    dead.push(prefix.clone());
                }
            }
        }
        if dead.is_empty() {
            return;
        }
        if let Err(error) = conn.hdel::<_, _, u64>(self.registry_key(), &dead).await {
            self.note_error(StoreOp::Registry, &error);
            return;
        }
        let mut registered = self.registered.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        for prefix in &dead {
            registered.remove(prefix);
        }
        tracing::debug!(
            cache = self.cache.as_tag(),
            removed = dead.len(),
            "redis prefix-registry hygiene reclaimed dead rows"
        );
    }
}

#[async_trait]
impl EntryStore for RedisEntryStore {
    async fn get(&self, dir: &str, file: &str) -> Option<Vec<u8>> {
        let mut conn = self.manager.clone();
        match conn.hget::<_, _, Option<Vec<u8>>>(self.dir_key(dir), file).await {
            Ok(value) => value,
            Err(error) => {
                self.note_error(StoreOp::Get, &error);
                None
            }
        }
    }

    async fn get_pair(&self, dir: &str, first: &str, second: &str) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        let mut conn = self.manager.clone();
        match conn
            .hmget::<_, _, Vec<Option<Vec<u8>>>>(self.dir_key(dir), &[first, second])
            .await
        {
            Ok(mut values) if values.len() == 2 => {
                let second_value = values.pop().unwrap_or_default();
                let first_value = values.pop().unwrap_or_default();
                (first_value, second_value)
            }
            Ok(_) => (None, None),
            Err(error) => {
                self.note_error(StoreOp::Get, &error);
                (None, None)
            }
        }
    }

    async fn put(&self, dir: &str, file: &str, bytes: &[u8]) {
        let mut conn = self.manager.clone();
        let dir_key = self.dir_key(dir);
        let mut pipe = redis::pipe();
        pipe.atomic()
            .hset(&dir_key, file, bytes)
            .expire(&dir_key, self.ttl_secs);
        match pipe.query_async::<(u64, i64)>(&mut conn).await {
            Ok((added, _)) => {
                self.put_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                self.put_entries.fetch_add(added, Ordering::Relaxed);
            }
            Err(error) => self.note_error(StoreOp::Put, &error),
        }
    }

    async fn put_if_absent(&self, dir: &str, file: &str, bytes: &[u8]) {
        let mut conn = self.manager.clone();
        let dir_key = self.dir_key(dir);
        let mut pipe = redis::pipe();
        pipe.atomic()
            .hset_nx(&dir_key, file, bytes)
            .expire(&dir_key, self.ttl_secs);
        match pipe.query_async::<(u64, i64)>(&mut conn).await {
            Ok((added, _)) => {
                if added > 0 {
                    self.put_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    self.put_entries.fetch_add(added, Ordering::Relaxed);
                }
            }
            Err(error) => self.note_error(StoreOp::Put, &error),
        }
    }

    async fn remove_entry(&self, dir: &str, file: &str) {
        let mut conn = self.manager.clone();
        if let Err(error) = conn.hdel::<_, _, u64>(self.dir_key(dir), file).await {
            self.note_error(StoreOp::Remove, &error);
        }
    }

    async fn remove_dir(&self, dir: &str) {
        let mut conn = self.manager.clone();
        if let Err(error) = conn.del::<_, u64>(self.dir_key(dir)).await {
            self.note_error(StoreOp::Remove, &error);
        }
    }

    async fn clear(&self) {
        let mut conn = self.manager.clone();
        let pattern = format!("{}:*", self.key_prefix);
        let mut cursor: u64 = 0;
        loop {
            let scanned: Result<(u64, Vec<String>), redis::RedisError> = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(SCAN_BATCH)
                .query_async(&mut conn)
                .await;
            let (next, keys) = match scanned {
                Ok(scanned) => scanned,
                Err(error) => {
                    self.note_error(StoreOp::Clear, &error);
                    return;
                }
            };
            if !keys.is_empty()
                && let Err(error) = conn.del::<_, u64>(keys).await
            {
                self.note_error(StoreOp::Clear, &error);
                return;
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        self.registered
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        self.put_bytes.store(0, Ordering::Relaxed);
        self.put_entries.store(0, Ordering::Relaxed);
    }

    /// A process-local seen-set caps the cost at one `HSETNX` per prefix per
    /// [`REGISTER_REFRESH`] window instead of one per insert. The entries are time-bounded
    /// rather than kept for the process lifetime because a sibling replica's hygiene pass can
    /// delete the shared row while this replica still remembers registering it — re-issuing the
    /// idempotent `HSETNX` on each refresh restores the row for warm prefixes. On an errored
    /// write the prefix stays unrecorded locally so a later insert retries the registration.
    async fn register_prefix(&self, prefix: &str, dir: &str) {
        {
            let registered = self.registered.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(seen_at) = registered.get(prefix)
                && seen_at.elapsed() < REGISTER_REFRESH
            {
                return;
            }
        }
        let mut conn = self.manager.clone();
        match conn.hset_nx::<_, _, _, u64>(self.registry_key(), prefix, dir).await {
            Ok(_) => {
                self.registered
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(prefix.to_string(), Instant::now());
            }
            Err(error) => self.note_error(StoreOp::Registry, &error),
        }
    }

    /// Reads the registry live, so a prefix invalidation on this replica also covers dirs
    /// written by sibling replicas sharing the server — something the disk backend's
    /// process-local sidecar cannot do.
    async fn prefix_entries(&self) -> HashMap<String, String> {
        let mut conn = self.manager.clone();
        match conn.hgetall::<_, HashMap<String, String>>(self.registry_key()).await {
            Ok(entries) => entries,
            Err(error) => {
                self.note_error(StoreOp::Registry, &error);
                HashMap::new()
            }
        }
    }

    async fn remove_prefixes(&self, prefixes: &[String]) {
        if prefixes.is_empty() {
            return;
        }
        let mut conn = self.manager.clone();
        if let Err(error) = conn.hdel::<_, _, u64>(self.registry_key(), prefixes).await {
            self.note_error(StoreOp::Registry, &error);
        }
        let mut registered = self.registered.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        for prefix in prefixes {
            registered.remove(prefix);
        }
    }

    /// Refreshes the whole dir's TTL fire-and-forget: recency here is per dir, not per file,
    /// because a dir's entries have correlated lifetimes and hash fields carry no portable
    /// per-field expiry.
    fn touch(&self, dir: &str, _file: &str) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut conn = self.manager.clone();
        let dir_key = self.dir_key(dir);
        let ttl_secs = self.ttl_secs;
        drop(handle.spawn(async move {
            let _: Result<i64, redis::RedisError> = conn.expire(&dir_key, ttl_secs).await;
        }));
    }

    /// Net entries and bytes written by THIS process since start — a deliberately cheap local
    /// approximation. Removals and server-side expiry are not subtracted: the authoritative
    /// residency bound lives in the Redis server's `maxmemory` policy, and these figures only
    /// feed process introspection.
    fn approx_stats(&self) -> (u64, u64) {
        (
            self.put_bytes.load(Ordering::Relaxed),
            self.put_entries.load(Ordering::Relaxed),
        )
    }

    fn tier(&self) -> Tier {
        Tier::Remote
    }
}
