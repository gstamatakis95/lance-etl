//! Datadog observability: OTLP trace export, structured JSON logs with trace correlation, a
//! typed DogStatsD metrics facade, and sampled-query recall capture.
//!
//! This module references neither Lance nor protobuf types (the [`recall`] submodule references
//! domain types only), so the `cache`, `lance`, and `grpc` layers may all depend on it without
//! violating the crate layering. `domain` stays telemetry-free.
//!
//! Every emitter here is infallible by construction: an unreachable Datadog Agent never panics
//! and never fails a request. Trace export uses the SDK batch processor (bounded queue, drops on
//! overflow, lazy gRPC connect with internal retries). Metrics use a bounded queuing DogStatsD
//! sink over non-blocking UDP. Setup failures degrade to no-op emitters with a warning.
//!
//! Submodules:
//! - [`traces`]: tracing subscriber setup, OTLP span export, and the Datadog JSON log format.
//! - [`metrics`]: the typed [`Metrics`] facade and its low-cardinality tag enums.
//! - [`recall`]: deterministic sampling of vector searches into `recall.*` span attributes.
//!
//! # Metric catalog
//!
//! All metrics flow through the typed [`Metrics`] facade (DogStatsD, prefix `search_api.`).
//! Cardinality policy: metrics carry only the low-cardinality tags listed below. Per-org,
//! per-tenant, and per-version detail lives on spans and logs, never on metrics. Every emitter is
//! infallible (an unreachable Agent never panics and never fails a request).
//!
//! RPC surface ([`metrics::Metrics::rpc`]):
//! - `rpc.requests` (count), `rpc.duration_ms` (distribution), `rpc.errors` (count, non-ok only),
//!   tagged `rpc` and `status`.
//!
//! Query execution, tapped from the Lance scan-stats callback
//! ([`metrics::Metrics::query_execution_stats`], [`metrics::Metrics::throttle_event`]):
//! - `query.iops`, `query.bytes_read`, `query.parts_loaded` (distributions, tagged `rpc`).
//! - `throttle.errors` (count), `throttle.new_rate` (gauge): untagged per-process throttle signal.
//!
//! Caches and handles ([`metrics::Metrics::cache_lookup`], `cache_insert_bytes`,
//! `cache_disk_gauges`, `cache_evictions`, `cache_serialize_error`, `dataset_open`,
//! `dataset_handles`):
//! - `cache.lookup` (count, tagged `cache`/`tier`/`outcome`), `cache.insert_bytes` (count),
//!   `cache.disk.bytes` + `cache.disk.entries` (gauges, tagged `cache`), `cache.evictions`
//!   (count, tagged `cache`/`reason`), `cache.serialize_errors` (count, tagged `cache`).
//! - `dataset.open.duration_ms` (distribution, tagged `cold`), `cache.handles.entries` (gauge:
//!   open-handle LRU size).
//!
//! Date-range fan-out ([`metrics::Metrics::fanout_legs`], `fanout_leg_duration`,
//! `fanout_dedup_dropped`):
//! - `fanout.legs` (distribution: fan-out width), `fanout.leg.duration_ms` (distribution: per-day
//!   leg latency), `fanout.dedup.dropped` (count), each tagged `leg`.
//!
//! Prewarm and blue-green ([`metrics::Metrics::prewarm`], `prewarm_index`,
//! `prewarm_indexes_warmed`, `prewarm_warmed_bytes`, `prewarm_last_version`, `serve_cold_open`,
//! `serve_tag_resolved`):
//! - `prewarm.duration_ms` (distribution, tagged `status`), `prewarm.index.duration_ms`
//!   (distribution, tagged `kind`), `prewarm.indexes_warmed` (count), `prewarm.warmed_bytes`
//!   (distribution), `prewarm.last_version` (gauge: most recently warmed version).
//! - `serve.cold_open` (count, tagged `warmed`): a serving cold open whose version had or had not
//!   been prewarmed. `warmed:false` is the flip-without-prewarm signal.
//! - `serve.tag_resolved` (count, tagged `changed`): a serve-tag re-resolution after the TTL
//!   lapsed. `changed:true` marks a replica observing a tag flip.
//!
//! Clusters, recall, and rerank ([`metrics::Metrics::clusters_read`], `clusters_centroids`,
//! `recall_sample`, `rerank`):
//! - `clusters.read.duration_ms` (distribution: centroid read duration), `clusters.centroids`
//!   (distribution: centroid count), `recall.samples` (count, tagged `query_type`/`filtered`).
//! - `rerank.duration_ms` (distribution) and `rerank.candidates` (distribution), both tagged
//!   `rpc`. Emitted only when a request carries a rerank spec.
//!
//! Spans (via the OpenTelemetry layer) carry the high-cardinality detail: `org_id`, `tenant_id`,
//! `namespace`, `dataset.version`, `prewarm.resolved_version`, `clusters.index`, the `fanout.*`
//! widths, and the gRPC status code.

pub mod metrics;
pub mod recall;
pub mod traces;

pub use metrics::{CacheName, EvictionReason, FanoutLeg, Metrics, PrewarmIndexKind, PrewarmStatus, Rpc, Tier};
pub use recall::{RecallCapture, RecallHook, RecallQueryType, RecallRecord};
pub use traces::{TelemetryGuard, init_tracing};
