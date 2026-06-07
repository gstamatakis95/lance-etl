//! The typed DogStatsD metrics facade and its low-cardinality tag enums.

use std::fmt;
use std::time::Duration;

use cadence::{Counted, Distributed, Gauged, MetricSink, NopMetricSink, QueuingMetricSink, StatsdClient};

/// Bound on the number of metric packets queued for the DogStatsD sink. Overflow is dropped.
const METRICS_QUEUE_CAPACITY: usize = 8192;

/// RPC names used as the `rpc` metric tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rpc {
    /// `SearchService/VectorSearch`.
    VectorSearch,
    /// `SearchService/TextSearch`.
    TextSearch,
    /// `SearchService/HybridSearch`.
    HybridSearch,
    /// `SearchService/Prewarm`.
    Prewarm,
    /// `SearchService/Clusters`.
    Clusters,
}

impl Rpc {
    /// Tag value for this RPC.
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::VectorSearch => "vector_search",
            Self::TextSearch => "text_search",
            Self::HybridSearch => "hybrid_search",
            Self::Prewarm => "prewarm",
            Self::Clusters => "clusters",
        }
    }
}

/// Cache identities used as the `cache` metric tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheName {
    /// The serialized index cache (disk + memory hot tier).
    Index,
    /// The metadata byte cache wrapping object stores.
    Store,
    /// The open-dataset-handle LRU.
    Handles,
}

impl CacheName {
    /// Tag value for this cache.
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::Index => "index",
            Self::Store => "store",
            Self::Handles => "handles",
        }
    }
}

/// Cache tiers used as the `tier` metric tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// In-memory tier.
    Memory,
    /// Local-disk tier.
    Disk,
}

impl Tier {
    /// Tag value for this tier.
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Disk => "disk",
        }
    }
}

/// Why cache entries were evicted, used as the `reason` metric tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionReason {
    /// The entry outlived the configured TTL.
    Ttl,
    /// The tier exceeded its byte budget.
    Size,
    /// The entry failed to read or decode.
    Corrupt,
}

impl EvictionReason {
    /// Tag value for this reason.
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::Ttl => "ttl",
            Self::Size => "size",
            Self::Corrupt => "corrupt",
        }
    }
}

/// Overall outcome of one Prewarm call, used as the `status` metric tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrewarmStatus {
    /// Everything requested was warmed.
    Ok,
    /// The call succeeded but at least one index reported an error.
    Partial,
    /// The call failed as a whole.
    Error,
}

impl PrewarmStatus {
    /// Tag value for this status.
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Partial => "partial",
            Self::Error => "error",
        }
    }
}

/// Index families used as the `kind` metric tag on per-index prewarm timings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrewarmIndexKind {
    /// Vector (IVF/HNSW) indexes.
    Vector,
    /// Inverted full-text indexes.
    Fts,
    /// Scalar (BTree/bitmap/...) indexes.
    Scalar,
}

impl PrewarmIndexKind {
    /// Tag value for this kind.
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::Vector => "vector",
            Self::Fts => "fts",
            Self::Scalar => "scalar",
        }
    }
}

/// Search leg families used as the `leg` metric tag on fan-out timings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanoutLeg {
    /// Nearest-neighbor leg.
    Vector,
    /// Full-text leg.
    Text,
    /// Combined vector + text leg of one hybrid fan-out.
    Hybrid,
}

impl FanoutLeg {
    /// Tag value for this leg.
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::Vector => "vector",
            Self::Text => "text",
            Self::Hybrid => "hybrid",
        }
    }
}

/// Typed facade over the DogStatsD client so call sites cannot invent metric names or tags.
///
/// Tag policy: only `rpc`, `status`, `cold`, `cache`, `tier`, `outcome`, `reason`, `kind`,
/// `leg`, `filtered`, `warmed`, and `changed` — `org_id`/`tenant_id`/`version` never appear on
/// metrics (30k orgs would explode the timeseries count). Org-, tenant-, and version-level detail
/// lives on traces and logs instead.
pub struct Metrics {
    client: StatsdClient,
}

impl fmt::Debug for Metrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Metrics").finish()
    }
}

impl Metrics {
    /// Metric name prefix. cadence joins it to every key with a dot.
    const PREFIX: &'static str = "search_api";

    /// No-op metrics for tests, local runs, and the disabled mode.
    pub fn disabled() -> Self {
        Self {
            client: StatsdClient::builder(Self::PREFIX, NopMetricSink).build(),
        }
    }

    /// Metrics over an arbitrary sink. Used by unit tests with cadence's `SpyMetricSink`.
    pub fn from_sink<S: MetricSink + Send + Sync + std::panic::RefUnwindSafe + 'static>(sink: S) -> Self {
        Self {
            client: with_default_tags(StatsdClient::builder(Self::PREFIX, sink)).build(),
        }
    }

    /// DogStatsD metrics over buffered non-blocking UDP behind a bounded queue.
    ///
    /// Constant tags `env`, `service`, and `version` are taken from `DD_ENV`, `DD_SERVICE` (or
    /// `OTEL_SERVICE_NAME`), and `DD_VERSION` when set. Any socket or sink failure degrades to
    /// the no-op client with a warning. Metric emission never blocks and never fails requests.
    pub fn dogstatsd(addr: &str) -> Self {
        let sink = match build_udp_sink(addr) {
            Ok(sink) => sink,
            Err(error) => {
                eprintln!("search-api: telemetry: DogStatsD sink unavailable ({addr}), metrics disabled: {error}");
                return Self::disabled();
            }
        };
        Self {
            client: with_default_tags(StatsdClient::builder(Self::PREFIX, sink)).build(),
        }
    }

    /// One finished RPC: request count and latency distribution tagged by `rpc` and `status`,
    /// plus an error count for non-`ok` statuses.
    pub fn rpc(&self, rpc: Rpc, status: &'static str, duration: Duration) {
        self.client
            .count_with_tags("rpc.requests", 1)
            .with_tag("rpc", rpc.as_tag())
            .with_tag("status", status)
            .send();
        self.client
            .distribution_with_tags("rpc.duration_ms", millis(duration))
            .with_tag("rpc", rpc.as_tag())
            .with_tag("status", status)
            .send();
        if status != "ok" {
            self.client
                .count_with_tags("rpc.errors", 1)
                .with_tag("rpc", rpc.as_tag())
                .with_tag("status", status)
                .send();
        }
    }

    /// Per-query object-store execution stats from one Lance scan, tagged by `rpc`.
    ///
    /// Values come from Lance's execution-stats callback (`ExecutionSummaryCounts`): `iops` is the
    /// number of I/O operations after coalescing, `bytes_read` the bytes pulled from storage, and
    /// `parts_loaded` the number of index partitions loaded. Emitted as distributions so the
    /// per-query spread is preserved. No `org`/`tenant` tags: cardinality lives in traces.
    pub fn query_execution_stats(&self, rpc: Rpc, iops: u64, bytes_read: u64, parts_loaded: u64) {
        self.client
            .distribution_with_tags("query.iops", iops)
            .with_tag("rpc", rpc.as_tag())
            .send();
        self.client
            .distribution_with_tags("query.bytes_read", bytes_read)
            .with_tag("rpc", rpc.as_tag())
            .send();
        self.client
            .distribution_with_tags("query.parts_loaded", parts_loaded)
            .with_tag("rpc", rpc.as_tag())
            .send();
    }

    /// One Lance object-store throttle event from the AIMD rate limiter.
    ///
    /// `errored` counts a throttle error against `throttle.errors`. `new_rate`, when present,
    /// records the limiter's freshly reduced fill rate (requests per second) as the
    /// `throttle.new_rate` gauge. Both are untagged: throttle pressure is a per-process signal.
    pub fn throttle_event(&self, errored: bool, new_rate: Option<f64>) {
        if errored {
            self.client.count_with_tags("throttle.errors", 1).send();
        }
        if let Some(rate) = new_rate {
            self.client.gauge_with_tags("throttle.new_rate", rate).send();
        }
    }

    /// Latency of one dataset resolution. `cold` marks resolutions that actually opened the
    /// dataset instead of hitting the handle cache.
    pub fn dataset_open(&self, cold: bool, duration: Duration) {
        self.client
            .distribution_with_tags("dataset.open.duration_ms", millis(duration))
            .with_tag("cold", if cold { "true" } else { "false" })
            .send();
    }

    /// Current size of the open-dataset-handle LRU.
    pub fn dataset_handles(&self, entries: u64) {
        self.client.gauge_with_tags("cache.handles.entries", entries).send();
    }

    /// One serving cold open, tagged by whether the opened version had already been prewarmed on
    /// this replica.
    ///
    /// `warmed:false` is the flip-without-prewarm signal: serving reached a version that prewarm
    /// has not warmed, so the first queries on it pay the cold-cache cost. A healthy blue-green
    /// rollout keeps this at `warmed:true`. No version tag (cardinality lives on the span).
    pub fn serve_cold_open(&self, warmed: bool) {
        self.client
            .count_with_tags("serve.cold_open", 1)
            .with_tag("warmed", if warmed { "true" } else { "false" })
            .send();
    }

    /// One serve-tag re-resolution after the TTL lapsed, tagged by whether the resolved version
    /// changed from the previous resolution.
    ///
    /// `changed:true` marks the moment a replica observes a tag flip, so the spread of these
    /// across the fleet is the flip-propagation latency.
    pub fn serve_tag_resolved(&self, changed: bool) {
        self.client
            .count_with_tags("serve.tag_resolved", 1)
            .with_tag("changed", if changed { "true" } else { "false" })
            .send();
    }

    /// The most recently prewarmed committed version on this process (last writer wins).
    ///
    /// A process-wide gauge (no per-dataset tag, to stay low-cardinality) that, read together
    /// with the served version on traces, shows whether prewarm is keeping pace with the tag.
    pub fn prewarm_last_version(&self, version: u64) {
        self.client.gauge_with_tags("prewarm.last_version", version).send();
    }

    /// One cache lookup outcome.
    pub fn cache_lookup(&self, cache: CacheName, tier: Tier, hit: bool) {
        self.client
            .count_with_tags("cache.lookup", 1)
            .with_tag("cache", cache.as_tag())
            .with_tag("tier", tier.as_tag())
            .with_tag("outcome", if hit { "hit" } else { "miss" })
            .send();
    }

    /// Bytes persisted to a disk tier by one insert.
    pub fn cache_insert_bytes(&self, cache: CacheName, bytes: u64) {
        self.client
            .count_with_tags("cache.insert_bytes", bytes as i64)
            .with_tag("cache", cache.as_tag())
            .with_tag("tier", Tier::Disk.as_tag())
            .send();
    }

    /// Post-sweep disk residency gauges of one tier.
    pub fn cache_disk_gauges(&self, cache: CacheName, bytes: u64, entries: u64) {
        self.client
            .gauge_with_tags("cache.disk.bytes", bytes)
            .with_tag("cache", cache.as_tag())
            .send();
        self.client
            .gauge_with_tags("cache.disk.entries", entries)
            .with_tag("cache", cache.as_tag())
            .send();
    }

    /// Entries evicted from one tier for one reason.
    pub fn cache_evictions(&self, cache: CacheName, reason: EvictionReason, count: u64) {
        if count == 0 {
            return;
        }
        self.client
            .count_with_tags("cache.evictions", count as i64)
            .with_tag("cache", cache.as_tag())
            .with_tag("reason", reason.as_tag())
            .send();
    }

    /// One entry that failed to serialize for the disk tier (kept memory-only).
    pub fn cache_serialize_error(&self, cache: CacheName) {
        self.client
            .count_with_tags("cache.serialize_errors", 1)
            .with_tag("cache", cache.as_tag())
            .send();
    }

    /// Duration of one whole Prewarm call.
    pub fn prewarm(&self, status: PrewarmStatus, duration: Duration) {
        self.client
            .distribution_with_tags("prewarm.duration_ms", millis(duration))
            .with_tag("status", status.as_tag())
            .send();
    }

    /// Duration of prewarming one index.
    pub fn prewarm_index(&self, kind: PrewarmIndexKind, duration: Duration) {
        self.client
            .distribution_with_tags("prewarm.index.duration_ms", millis(duration))
            .with_tag("kind", kind.as_tag())
            .send();
    }

    /// Indexes successfully warmed by one Prewarm call.
    pub fn prewarm_indexes_warmed(&self, count: u64) {
        self.client
            .count_with_tags("prewarm.indexes_warmed", count as i64)
            .send();
    }

    /// Approximate bytes resident in the index cache after one Prewarm call.
    pub fn prewarm_warmed_bytes(&self, bytes: u64) {
        self.client.distribution_with_tags("prewarm.warmed_bytes", bytes).send();
    }

    /// Width of one date-range fan-out: how many per-day datasets actually served the query.
    pub fn fanout_legs(&self, leg: FanoutLeg, legs: u64) {
        self.client
            .distribution_with_tags("fanout.legs", legs)
            .with_tag("leg", leg.as_tag())
            .send();
    }

    /// Latency of one per-day leg of a fan-out search.
    pub fn fanout_leg_duration(&self, leg: FanoutLeg, duration: Duration) {
        self.client
            .distribution_with_tags("fanout.leg.duration_ms", millis(duration))
            .with_tag("leg", leg.as_tag())
            .send();
    }

    /// Duplicate hits folded into a surviving hit by the dedup merge of one fan-out search.
    pub fn fanout_dedup_dropped(&self, leg: FanoutLeg, count: u64) {
        if count == 0 {
            return;
        }
        self.client
            .count_with_tags("fanout.dedup.dropped", count as i64)
            .with_tag("leg", leg.as_tag())
            .send();
    }

    /// One search captured for recall scoring, tagged by query type and whether it carried a
    /// filter. `query_type` is one of `vector`, `text`, or `hybrid` (low cardinality).
    pub fn recall_sample(&self, query_type: &'static str, filtered: bool) {
        self.client
            .count_with_tags("recall.samples", 1)
            .with_tag("query_type", query_type)
            .with_tag("filtered", if filtered { "true" } else { "false" })
            .send();
    }

    /// One post-fusion reranking pass: candidate count and duration tagged by `rpc`.
    ///
    /// `candidates` is the number of hits fed to the reranker before any truncation. Emitted only
    /// when a request carries a rerank spec, so the default identity path stays metric-free.
    pub fn rerank(&self, rpc: Rpc, candidates: u64, duration: Duration) {
        self.client
            .distribution_with_tags("rerank.duration_ms", millis(duration))
            .with_tag("rpc", rpc.as_tag())
            .send();
        self.client
            .distribution_with_tags("rerank.candidates", candidates)
            .with_tag("rpc", rpc.as_tag())
            .send();
    }

    /// Duration of reading the IVF centroids for one Clusters call.
    pub fn clusters_read(&self, duration: Duration) {
        self.client
            .distribution_with_tags("clusters.read.duration_ms", millis(duration))
            .send();
    }

    /// Number of centroids returned by one Clusters call.
    pub fn clusters_centroids(&self, count: u64) {
        self.client.distribution_with_tags("clusters.centroids", count).send();
    }
}

/// Converts a duration to whole milliseconds for distributions.
fn millis(duration: Duration) -> u64 {
    duration.as_millis() as u64
}

/// Applies the constant Datadog unified-service tags from the environment.
fn with_default_tags(mut builder: cadence::StatsdClientBuilder) -> cadence::StatsdClientBuilder {
    if let Ok(env_name) = std::env::var("DD_ENV") {
        builder = builder.with_tag("env", env_name);
    }
    if let Ok(service) = std::env::var("DD_SERVICE").or_else(|_| std::env::var("OTEL_SERVICE_NAME")) {
        builder = builder.with_tag("service", service);
    }
    if let Ok(version) = std::env::var("DD_VERSION") {
        builder = builder.with_tag("version", version);
    }
    builder
}

/// Builds the buffered UDP sink behind a bounded queue. Emission never blocks the caller.
fn build_udp_sink(addr: &str) -> std::io::Result<QueuingMetricSink> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
    socket.set_nonblocking(true)?;
    let buffered = cadence::BufferedUdpMetricSink::from(addr, socket).map_err(std::io::Error::other)?;
    Ok(QueuingMetricSink::with_capacity(buffered, METRICS_QUEUE_CAPACITY))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence::SpyMetricSink;

    /// Builds a spy-backed metrics facade and a closure draining everything emitted so far.
    fn spy_metrics() -> (Metrics, impl Fn() -> Vec<String>) {
        let (receiver, sink) = SpyMetricSink::new();
        let metrics = Metrics::from_sink(sink);
        let drain = move || {
            let mut lines = Vec::new();
            while let Ok(packet) = receiver.try_recv() {
                lines.push(String::from_utf8(packet).unwrap());
            }
            lines
        };
        (metrics, drain)
    }

    #[test]
    fn disabled_metrics_never_panic() {
        let metrics = Metrics::disabled();
        metrics.rpc(Rpc::VectorSearch, "ok", Duration::from_millis(3));
        metrics.cache_lookup(CacheName::Index, Tier::Disk, true);
        metrics.cache_disk_gauges(CacheName::Store, 10, 2);
        metrics.prewarm(PrewarmStatus::Partial, Duration::from_millis(5));
        metrics.fanout_legs(FanoutLeg::Vector, 3);
        metrics.clusters_read(Duration::from_millis(2));
    }

    #[test]
    fn rpc_metrics_render_expected_names_and_tags() {
        let (metrics, drain) = spy_metrics();
        metrics.rpc(Rpc::HybridSearch, "ok", Duration::from_millis(12));
        let lines = drain();
        assert!(
            lines.iter().any(|line| line.starts_with("search_api.rpc.requests:1|c")
                && line.contains("rpc:hybrid_search")
                && line.contains("status:ok")),
            "unexpected lines: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("search_api.rpc.duration_ms:12|d") && line.contains("rpc:hybrid_search")),
            "unexpected lines: {lines:?}"
        );
        assert!(
            !lines.iter().any(|line| line.contains("rpc.errors")),
            "ok status must not count as an error: {lines:?}"
        );
    }

    #[test]
    fn rpc_errors_are_counted_for_non_ok_statuses() {
        let (metrics, drain) = spy_metrics();
        metrics.rpc(Rpc::TextSearch, "not_found", Duration::from_millis(1));
        let lines = drain();
        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("search_api.rpc.errors:1|c") && line.contains("status:not_found")),
            "unexpected lines: {lines:?}"
        );
    }

    #[test]
    fn cache_and_prewarm_metrics_render_expected_tags() {
        let (metrics, drain) = spy_metrics();
        metrics.cache_lookup(CacheName::Index, Tier::Memory, false);
        metrics.cache_insert_bytes(CacheName::Store, 256);
        metrics.cache_evictions(CacheName::Index, EvictionReason::Ttl, 3);
        metrics.cache_evictions(CacheName::Index, EvictionReason::Size, 0);
        metrics.cache_serialize_error(CacheName::Index);
        metrics.dataset_open(true, Duration::from_millis(40));
        metrics.dataset_handles(7);
        metrics.prewarm_index(PrewarmIndexKind::Fts, Duration::from_millis(8));
        metrics.prewarm_indexes_warmed(2);
        metrics.prewarm_warmed_bytes(1024);
        let lines = drain();
        let expect = [
            (
                "search_api.cache.lookup:1|c",
                vec!["cache:index", "tier:memory", "outcome:miss"],
            ),
            ("search_api.cache.insert_bytes:256|c", vec!["cache:store", "tier:disk"]),
            ("search_api.cache.evictions:3|c", vec!["cache:index", "reason:ttl"]),
            ("search_api.cache.serialize_errors:1|c", vec!["cache:index"]),
            ("search_api.dataset.open.duration_ms:40|d", vec!["cold:true"]),
            ("search_api.cache.handles.entries:7|g", vec![]),
            ("search_api.prewarm.index.duration_ms:8|d", vec!["kind:fts"]),
            ("search_api.prewarm.indexes_warmed:2|c", vec![]),
            ("search_api.prewarm.warmed_bytes:1024|d", vec![]),
        ];
        for (head, tags) in expect {
            assert!(
                lines
                    .iter()
                    .any(|line| line.starts_with(head) && tags.iter().all(|tag| line.contains(tag))),
                "missing {head} with {tags:?} in {lines:?}"
            );
        }
        assert!(
            !lines.iter().any(|line| line.contains("reason:size")),
            "zero-count evictions must not be emitted: {lines:?}"
        );
    }

    #[test]
    fn recall_sample_metric_renders_expected_tags() {
        let (metrics, drain) = spy_metrics();
        metrics.recall_sample("vector", true);
        metrics.recall_sample("text", false);
        metrics.recall_sample("hybrid", false);
        let lines = drain();
        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("search_api.recall.samples:1|c")
                    && line.contains("query_type:vector")
                    && line.contains("filtered:true")),
            "missing filtered vector sample count: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("search_api.recall.samples:1|c")
                    && line.contains("query_type:text")
                    && line.contains("filtered:false")),
            "missing text sample count: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("search_api.recall.samples:1|c") && line.contains("query_type:hybrid")),
            "missing hybrid sample count: {lines:?}"
        );
    }

    #[test]
    fn rerank_metric_renders_candidates_and_duration_tagged_by_rpc() {
        let (metrics, drain) = spy_metrics();
        metrics.rerank(Rpc::HybridSearch, 7, Duration::from_millis(4));
        let lines = drain();
        let expect = [
            ("search_api.rerank.duration_ms:4|d", "rpc:hybrid_search"),
            ("search_api.rerank.candidates:7|d", "rpc:hybrid_search"),
        ];
        for (head, tag) in expect {
            assert!(
                lines.iter().any(|line| line.starts_with(head) && line.contains(tag)),
                "missing {head} with {tag} in {lines:?}"
            );
        }
    }

    #[test]
    fn fanout_and_clusters_metrics_render_expected_tags() {
        let (metrics, drain) = spy_metrics();
        metrics.fanout_legs(FanoutLeg::Vector, 3);
        metrics.fanout_leg_duration(FanoutLeg::Hybrid, Duration::from_millis(6));
        metrics.fanout_dedup_dropped(FanoutLeg::Text, 4);
        metrics.fanout_dedup_dropped(FanoutLeg::Text, 0);
        metrics.clusters_read(Duration::from_millis(9));
        metrics.clusters_centroids(256);
        let lines = drain();
        let expect = [
            ("search_api.fanout.legs:3|d", vec!["leg:vector"]),
            ("search_api.fanout.leg.duration_ms:6|d", vec!["leg:hybrid"]),
            ("search_api.fanout.dedup.dropped:4|c", vec!["leg:text"]),
            ("search_api.clusters.read.duration_ms:9|d", vec![]),
            ("search_api.clusters.centroids:256|d", vec![]),
        ];
        for (head, tags) in expect {
            assert!(
                lines
                    .iter()
                    .any(|line| line.starts_with(head) && tags.iter().all(|tag| line.contains(tag))),
                "missing {head} with {tags:?} in {lines:?}"
            );
        }
        assert_eq!(
            lines
                .iter()
                .filter(|line| line.contains("fanout.dedup.dropped"))
                .count(),
            1,
            "zero-count dedup drops must not be emitted: {lines:?}"
        );
    }

    #[test]
    fn query_execution_stats_render_three_distributions_tagged_by_rpc() {
        let (metrics, drain) = spy_metrics();
        metrics.query_execution_stats(Rpc::VectorSearch, 7, 4096, 3);
        let lines = drain();
        let expect = [
            ("search_api.query.iops:7|d", "rpc:vector_search"),
            ("search_api.query.bytes_read:4096|d", "rpc:vector_search"),
            ("search_api.query.parts_loaded:3|d", "rpc:vector_search"),
        ];
        for (head, tag) in expect {
            assert!(
                lines.iter().any(|line| line.starts_with(head) && line.contains(tag)),
                "missing {head} with {tag} in {lines:?}"
            );
        }
        assert!(
            !lines.iter().any(|line| line.contains("org")),
            "execution stats must never carry org/tenant tags: {lines:?}"
        );
    }

    #[test]
    fn throttle_event_emits_error_counter_and_rate_gauge() {
        let (metrics, drain) = spy_metrics();
        metrics.throttle_event(true, Some(12.5));
        let lines = drain();
        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("search_api.throttle.errors:1|c")),
            "missing throttle error counter: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("search_api.throttle.new_rate:12.5|g")),
            "missing throttle rate gauge: {lines:?}"
        );
    }

    #[test]
    fn blue_green_metrics_render_expected_names_and_tags() {
        let (metrics, drain) = spy_metrics();
        metrics.serve_cold_open(true);
        metrics.serve_cold_open(false);
        metrics.serve_tag_resolved(true);
        metrics.serve_tag_resolved(false);
        metrics.prewarm_last_version(42);
        let lines = drain();
        let expect = [
            ("search_api.serve.cold_open:1|c", "warmed:true"),
            ("search_api.serve.cold_open:1|c", "warmed:false"),
            ("search_api.serve.tag_resolved:1|c", "changed:true"),
            ("search_api.serve.tag_resolved:1|c", "changed:false"),
            ("search_api.prewarm.last_version:42|g", ""),
        ];
        for (head, tag) in expect {
            assert!(
                lines.iter().any(|line| line.starts_with(head) && line.contains(tag)),
                "missing {head} with {tag} in {lines:?}"
            );
        }
        assert!(
            !lines.iter().any(|line| line.contains("org")),
            "blue-green metrics must never carry org/version tags: {lines:?}"
        );
    }

    #[test]
    fn throttle_event_without_error_or_rate_emits_nothing() {
        let (metrics, drain) = spy_metrics();
        metrics.throttle_event(false, None);
        assert!(drain().is_empty(), "no throttle signal must produce no metrics");
    }

    #[test]
    fn tag_enums_render_expected_strings() {
        assert_eq!(Rpc::VectorSearch.as_tag(), "vector_search");
        assert_eq!(Rpc::Prewarm.as_tag(), "prewarm");
        assert_eq!(Rpc::Clusters.as_tag(), "clusters");
        assert_eq!(CacheName::Handles.as_tag(), "handles");
        assert_eq!(Tier::Disk.as_tag(), "disk");
        assert_eq!(EvictionReason::Corrupt.as_tag(), "corrupt");
        assert_eq!(PrewarmStatus::Partial.as_tag(), "partial");
        assert_eq!(PrewarmIndexKind::Scalar.as_tag(), "scalar");
        assert_eq!(FanoutLeg::Hybrid.as_tag(), "hybrid");
    }
}
