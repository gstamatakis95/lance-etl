//! Datadog observability: OTLP trace export, structured JSON logs with trace correlation, and a
//! typed DogStatsD metrics facade.
//!
//! This module references neither Lance nor protobuf types, so the `cache`, `lance`, and `grpc`
//! layers may all depend on it without violating the crate layering. `domain` stays
//! telemetry-free.
//!
//! Every emitter here is infallible by construction: an unreachable Datadog Agent never panics
//! and never fails a request. Trace export uses the SDK batch processor (bounded queue, drops on
//! overflow, lazy gRPC connect with internal retries). Metrics use a bounded queuing DogStatsD
//! sink over non-blocking UDP. Setup failures degrade to no-op emitters with a warning.
//!
//! Submodules:
//! - [`traces`]: tracing subscriber setup, OTLP span export, and the Datadog JSON log format.
//! - [`metrics`]: the typed [`Metrics`] facade and its low-cardinality tag enums.

pub mod metrics;
pub mod traces;

pub use metrics::{CacheName, EvictionReason, FanoutLeg, Metrics, PrewarmIndexKind, PrewarmStatus, Rpc, Tier};
pub use traces::{DEFAULT_SERVICE_NAME, TelemetryGuard, init_tracing};
