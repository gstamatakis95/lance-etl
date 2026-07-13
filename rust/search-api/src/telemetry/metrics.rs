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
    /// Remote shared tier (the Redis backend).
    Remote,
}

impl Tier {
    /// Tag value for this tier.
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Disk => "disk",
            Self::Remote => "remote",
        }
    }
}

/// Persistent-store operations used as the `op` tag on backend error counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOp {
    /// A read of one entry (or entry pair).
    Get,
    /// A write of one entry.
    Put,
    /// A removal of one entry or one dir.
    Remove,
    /// A whole-tier clear.
    Clear,
    /// A prefix-registry read or write.
    Registry,
}

impl StoreOp {
    /// Tag value for this operation.
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Put => "put",
            Self::Remove => "remove",
            Self::Clear => "clear",
            Self::Registry => "registry",
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

/// Lance IO-event kinds used as the `io_type` metric tag on `lance.io_events`.
///
/// Mirrors the fixed `lance::io_events` enum from the Lance checkout
/// (`lance_core::utils::tracing::IO_TYPE_*`). Low cardinality: six fixed variants, no ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LanceIoType {
    /// A scalar (BTree/bitmap/inverted/ngram) index was opened.
    OpenScalarIndex,
    /// A vector (IVF/HNSW) index was opened.
    OpenVectorIndex,
    /// The fragment-reuse system index was opened.
    OpenFragReuseIndex,
    /// The memory-WAL system index was opened.
    OpenMemWalIndex,
    /// A vector index partition was loaded from storage.
    LoadVectorPart,
    /// A scalar index partition was loaded from storage.
    LoadScalarPart,
}

impl LanceIoType {
    /// Tag value for this IO type.
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::OpenScalarIndex => "open_scalar_index",
            Self::OpenVectorIndex => "open_vector_index",
            Self::OpenFragReuseIndex => "open_frag_reuse_index",
            Self::OpenMemWalIndex => "open_mem_wal_index",
            Self::LoadVectorPart => "load_vector_part",
            Self::LoadScalarPart => "load_scalar_part",
        }
    }

    /// Parses the Lance `type` field of a `lance::io_events` event into this enum.
    pub fn from_lance(value: &str) -> Option<Self> {
        match value {
            "open_scalar_index" => Some(Self::OpenScalarIndex),
            "open_vector_index" => Some(Self::OpenVectorIndex),
            "open_frag_reuse_index" => Some(Self::OpenFragReuseIndex),
            "open_mem_wal_index" => Some(Self::OpenMemWalIndex),
            "load_vector_part" => Some(Self::LoadVectorPart),
            "load_scalar_part" => Some(Self::LoadScalarPart),
            _ => None,
        }
    }
}

/// Lance dataset-lifecycle events used as the `event` metric tag on `lance.dataset_events`.
///
/// Mirrors the fixed `lance::dataset_events` enum from the Lance checkout
/// (`lance_core::utils::tracing::DATASET_*_EVENT`). `loading` fires on dataset open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetEvent {
    /// A dataset version was opened (loaded).
    Loading,
    /// A write transaction is in progress.
    Writing,
    /// A transaction was committed.
    Committed,
    /// A column is being dropped.
    DroppingColumn,
    /// Rows are being deleted.
    Deleting,
    /// Fragments are being compacted.
    Compacting,
    /// Old versions are being cleaned up.
    Cleaning,
}

impl DatasetEvent {
    /// Tag value for this event.
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::Loading => "loading",
            Self::Writing => "writing",
            Self::Committed => "committed",
            Self::DroppingColumn => "dropping_column",
            Self::Deleting => "deleting",
            Self::Compacting => "compacting",
            Self::Cleaning => "cleaning",
        }
    }

    /// Parses the Lance `event` field of a `lance::dataset_events` event into this enum.
    pub fn from_lance(value: &str) -> Option<Self> {
        match value {
            "loading" => Some(Self::Loading),
            "writing" => Some(Self::Writing),
            "committed" => Some(Self::Committed),
            "dropping_column" => Some(Self::DroppingColumn),
            "deleting" => Some(Self::Deleting),
            "compacting" => Some(Self::Compacting),
            "cleaning" => Some(Self::Cleaning),
            _ => None,
        }
    }
}

/// Lance file-audit modes used as the `mode` metric tag on `lance.file_audit`.
///
/// Mirrors `lance_core::utils::tracing::AUDIT_MODE_*` from the Lance checkout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileAuditMode {
    /// A file was created.
    Create,
    /// A file was deleted after verification.
    Delete,
    /// A file was deleted without verification.
    DeleteUnverified,
}

impl FileAuditMode {
    /// Tag value for this mode.
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Delete => "delete",
            Self::DeleteUnverified => "delete_unverified",
        }
    }

    /// Parses the Lance `mode` field of a `lance::file_audit` event into this enum.
    pub fn from_lance(value: &str) -> Option<Self> {
        match value {
            "create" => Some(Self::Create),
            "delete" => Some(Self::Delete),
            "delete_unverified" => Some(Self::DeleteUnverified),
            _ => None,
        }
    }
}

/// Lance file-audit file kinds used as the `type` metric tag on `lance.file_audit`.
///
/// Mirrors `lance_core::utils::tracing::AUDIT_TYPE_*` from the Lance checkout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileAuditType {
    /// A manifest file.
    Manifest,
    /// An index file.
    Index,
    /// A data file.
    Data,
    /// A deletion file.
    Deletion,
}

impl FileAuditType {
    /// Tag value for this file type.
    pub fn as_tag(self) -> &'static str {
        match self {
            Self::Manifest => "manifest",
            Self::Index => "index",
            Self::Data => "data",
            Self::Deletion => "deletion",
        }
    }

    /// Parses the Lance `type` field of a `lance::file_audit` event into this enum.
    pub fn from_lance(value: &str) -> Option<Self> {
        match value {
            "manifest" => Some(Self::Manifest),
            "index" => Some(Self::Index),
            "data" => Some(Self::Data),
            "deletion" => Some(Self::Deletion),
            _ => None,
        }
    }
}

/// Typed facade over the DogStatsD client so call sites cannot invent metric names or tags.
///
/// Tag policy: only `rpc`, `status`, `cold`, `cache`, `tier`, `outcome`, `op`, `reason`,
/// `kind`, `filtered`, `warmed`, and `changed` — `org_id`/`tenant_id`/`version` never appear on
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

    /// One Lance `io_events` event, counting an index open or partition load tagged by `io_type`.
    ///
    /// Sourced from Lance's `lance::io_events` tracing target. Counts only, tagged by the fixed
    /// IO-type enum: no uri, org, or tenant ever appears on the tag.
    pub fn lance_io_event(&self, io_type: LanceIoType) {
        self.client
            .count_with_tags("lance.io_events", 1)
            .with_tag("io_type", io_type.as_tag())
            .send();
    }

    /// One Lance `dataset_events` event, counting a dataset-lifecycle transition tagged by `event`.
    ///
    /// Sourced from Lance's `lance::dataset_events` tracing target. `event:loading` counts a
    /// dataset open. Counts only, tagged by the fixed lifecycle enum: no uri or tenant on the tag.
    pub fn lance_dataset_event(&self, event: DatasetEvent) {
        self.client
            .count_with_tags("lance.dataset_events", 1)
            .with_tag("event", event.as_tag())
            .send();
    }

    /// One Lance `file_audit` event, counting a file create/delete tagged by `mode` and `type`.
    ///
    /// Sourced from Lance's `lance::file_audit` tracing target. Counts only, tagged by the fixed
    /// mode and file-type enums: the audited path never appears on the tag.
    pub fn lance_file_audit(&self, mode: FileAuditMode, file_type: FileAuditType) {
        self.client
            .count_with_tags("lance.file_audit", 1)
            .with_tag("mode", mode.as_tag())
            .with_tag("type", file_type.as_tag())
            .send();
    }

    /// Latency of one dataset resolution. `cold` marks resolutions that actually opened the
    /// dataset instead of hitting the handle cache.
    pub fn dataset_open(&self, cold: bool, duration: Duration) {
        self.client
            .distribution_with_tags("dataset.open.duration_ms", millis(duration))
            .with_tag("cold", if cold { "true" } else { "false" })
            .send();
    }

    /// Current entry count of the open-dataset-handle LRU.
    pub fn dataset_handles(&self, entries: u64) {
        self.client.gauge_with_tags("cache.handles.entries", entries).send();
    }

    /// Current total weighted size of the open-dataset-handle LRU.
    ///
    /// The handle cache is bounded by total weight (clamped open fragment count per handle) rather
    /// than a flat count, so this gauge tracks budget utilization against the configured weighted
    /// capacity. Low cardinality: no org/tenant tags.
    pub fn dataset_handles_weighted(&self, weighted_size: u64) {
        self.client
            .gauge_with_tags("cache.handles.weighted_size", weighted_size)
            .send();
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

    /// Bytes persisted to one persistent tier by one insert.
    pub fn cache_insert_bytes(&self, cache: CacheName, tier: Tier, bytes: u64) {
        self.client
            .count_with_tags("cache.insert_bytes", bytes as i64)
            .with_tag("cache", cache.as_tag())
            .with_tag("tier", tier.as_tag())
            .send();
    }

    /// One persistent-backend operation that failed and degraded to a miss or a dropped write.
    ///
    /// Emitted by the Redis store on every errored round trip. A sustained non-zero rate means
    /// the cache server is unreachable or overloaded while searches keep succeeding memory-only.
    pub fn cache_backend_error(&self, cache: CacheName, op: StoreOp) {
        self.client
            .count_with_tags("cache.backend_errors", 1)
            .with_tag("cache", cache.as_tag())
            .with_tag("op", op.as_tag())
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

    /// One completed janitor sweep of one tier: wall-clock duration plus the total entries
    /// removed (TTL + budget evictions combined).
    ///
    /// The duration distribution tracks the cost of the directory walk the sweep performs over the
    /// tier. On a large fleet a climbing sweep duration is the early signal that a tier's on-disk
    /// entry count is outgrowing what a periodic full walk can service cheaply. Tagged by `cache`
    /// only: no per-dataset or per-key dimension.
    pub fn cache_sweep(&self, cache: CacheName, duration: Duration, removed: u64) {
        self.client
            .distribution_with_tags("cache.sweep.duration_ms", millis(duration))
            .with_tag("cache", cache.as_tag())
            .send();
        self.client
            .distribution_with_tags("cache.sweep.removed", removed)
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

    /// One search captured for recall scoring, tagged by query type and whether it carried a
    /// filter. `query_type` is one of `vector`, `text`, or `hybrid` (low cardinality).
    pub fn recall_sample(&self, query_type: &'static str, filtered: bool) {
        self.client
            .count_with_tags("recall.samples", 1)
            .with_tag("query_type", query_type)
            .with_tag("filtered", if filtered { "true" } else { "false" })
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
        metrics.cache_sweep(CacheName::Index, Duration::from_millis(7), 4);
        metrics.prewarm(PrewarmStatus::Partial, Duration::from_millis(5));
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
        metrics.cache_insert_bytes(CacheName::Store, Tier::Disk, 256);
        metrics.cache_backend_error(CacheName::Index, StoreOp::Put);
        metrics.cache_evictions(CacheName::Index, EvictionReason::Ttl, 3);
        metrics.cache_evictions(CacheName::Index, EvictionReason::Size, 0);
        metrics.cache_sweep(CacheName::Store, Duration::from_millis(11), 5);
        metrics.cache_serialize_error(CacheName::Index);
        metrics.dataset_open(true, Duration::from_millis(40));
        metrics.dataset_handles(7);
        metrics.dataset_handles_weighted(42);
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
            ("search_api.cache.backend_errors:1|c", vec!["cache:index", "op:put"]),
            ("search_api.cache.evictions:3|c", vec!["cache:index", "reason:ttl"]),
            ("search_api.cache.sweep.duration_ms:11|d", vec!["cache:store"]),
            ("search_api.cache.sweep.removed:5|d", vec!["cache:store"]),
            ("search_api.cache.serialize_errors:1|c", vec!["cache:index"]),
            ("search_api.dataset.open.duration_ms:40|d", vec!["cold:true"]),
            ("search_api.cache.handles.entries:7|g", vec![]),
            ("search_api.cache.handles.weighted_size:42|g", vec![]),
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
    fn clusters_metrics_render_expected_tags() {
        let (metrics, drain) = spy_metrics();
        metrics.clusters_read(Duration::from_millis(9));
        metrics.clusters_centroids(256);
        let lines = drain();
        let expect = [
            "search_api.clusters.read.duration_ms:9|d",
            "search_api.clusters.centroids:256|d",
        ];
        for head in expect {
            assert!(
                lines.iter().any(|line| line.starts_with(head)),
                "missing {head} in {lines:?}"
            );
        }
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
    fn lance_event_metrics_render_expected_names_and_tags() {
        let (metrics, drain) = spy_metrics();
        metrics.lance_io_event(LanceIoType::OpenVectorIndex);
        metrics.lance_dataset_event(DatasetEvent::Loading);
        metrics.lance_file_audit(FileAuditMode::Create, FileAuditType::Manifest);
        let lines = drain();
        let expect = [
            ("search_api.lance.io_events:1|c", vec!["io_type:open_vector_index"]),
            ("search_api.lance.dataset_events:1|c", vec!["event:loading"]),
            ("search_api.lance.file_audit:1|c", vec!["mode:create", "type:manifest"]),
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
            !lines.iter().any(|line| line.contains("org")),
            "lance event metrics must never carry org/tenant tags: {lines:?}"
        );
    }

    #[test]
    fn lance_event_tag_enums_parse_and_render_round_trip() {
        assert_eq!(
            LanceIoType::from_lance("load_scalar_part"),
            Some(LanceIoType::LoadScalarPart)
        );
        assert_eq!(LanceIoType::from_lance("nope"), None);
        assert_eq!(DatasetEvent::from_lance("committed"), Some(DatasetEvent::Committed));
        assert_eq!(DatasetEvent::from_lance("nope"), None);
        assert_eq!(
            FileAuditMode::from_lance("delete_unverified"),
            Some(FileAuditMode::DeleteUnverified)
        );
        assert_eq!(FileAuditType::from_lance("deletion"), Some(FileAuditType::Deletion));
        assert_eq!(LanceIoType::OpenScalarIndex.as_tag(), "open_scalar_index");
        assert_eq!(DatasetEvent::DroppingColumn.as_tag(), "dropping_column");
        assert_eq!(FileAuditMode::Create.as_tag(), "create");
        assert_eq!(FileAuditType::Data.as_tag(), "data");
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
    }
}
