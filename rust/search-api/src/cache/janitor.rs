//! Background janitor enforcing TTL and disk budgets over both persistent cache tiers.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cache::disk_cache::DiskIndexCacheBackend;
use crate::cache::layout::SweepStats;
use crate::cache::store_cache::MetadataByteCache;
use crate::telemetry::{CacheName, EvictionReason, Metrics};

/// Sweeps the index and store cache tiers on a fixed interval.
///
/// Each sweep deletes entries older than the TTL, then deletes oldest-mtime entries until each
/// tier fits in its byte budget, reconciles the in-memory size accounting, and publishes the
/// post-sweep residency gauges and eviction counters to Datadog.
pub struct CacheJanitor {
    index_cache: Arc<DiskIndexCacheBackend>,
    store_cache: Arc<MetadataByteCache>,
    ttl: Duration,
    index_budget_bytes: u64,
    store_budget_bytes: u64,
    metrics: Arc<Metrics>,
}

impl CacheJanitor {
    /// Creates a janitor over the two disk tiers.
    pub fn new(
        index_cache: Arc<DiskIndexCacheBackend>,
        store_cache: Arc<MetadataByteCache>,
        ttl: Duration,
        index_budget_bytes: u64,
        store_budget_bytes: u64,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            index_cache,
            store_cache,
            ttl,
            index_budget_bytes,
            store_budget_bytes,
            metrics,
        }
    }

    /// Runs one sweep of both tiers on the blocking thread pool and emits the cache gauges.
    ///
    /// Each tier is timed individually so the `cache`-tagged sweep duration reflects the cost of
    /// that tier's own directory walk.
    pub async fn sweep_once(&self) {
        let index_cache = self.index_cache.clone();
        let store_cache = self.store_cache.clone();
        let ttl = self.ttl;
        let index_budget = self.index_budget_bytes;
        let store_budget = self.store_budget_bytes;
        let swept = tokio::task::spawn_blocking(move || {
            let index_started = Instant::now();
            let index_stats = index_cache.sweep(ttl, index_budget);
            let index_elapsed = index_started.elapsed();
            let store_started = Instant::now();
            let store_stats = store_cache.sweep(ttl, store_budget);
            let store_elapsed = store_started.elapsed();
            (index_stats, index_elapsed, store_stats, store_elapsed)
        })
        .await;
        if let Ok((index_stats, index_elapsed, store_stats, store_elapsed)) = swept {
            self.publish(CacheName::Index, index_stats, index_elapsed);
            self.publish(CacheName::Store, store_stats, store_elapsed);
        }
    }

    /// Publishes the post-sweep gauges, eviction counters, and sweep timing of one tier.
    fn publish(&self, cache: CacheName, stats: SweepStats, elapsed: Duration) {
        self.metrics
            .cache_disk_gauges(cache, stats.remaining_bytes, stats.remaining_entries);
        self.metrics
            .cache_sweep(cache, elapsed, stats.ttl_evicted + stats.size_evicted);
        self.metrics
            .cache_evictions(cache, EvictionReason::Ttl, stats.ttl_evicted);
        self.metrics
            .cache_evictions(cache, EvictionReason::Size, stats.size_evicted);
        if stats.ttl_evicted > 0 || stats.size_evicted > 0 {
            tracing::debug!(
                cache = cache.as_tag(),
                ttl_evicted = stats.ttl_evicted,
                size_evicted = stats.size_evicted,
                remaining_bytes = stats.remaining_bytes,
                "cache sweep evicted entries"
            );
        }
    }

    /// Spawns the periodic sweep loop. Dropping the returned handle aborts nothing, callers
    /// should `abort()` it on shutdown if needed.
    pub fn spawn(self, interval: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                self.sweep_once().await;
            }
        })
    }
}
