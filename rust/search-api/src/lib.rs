//! Lightweight gRPC search service over per-org Lance datasets.
//!
//! Serves nearest-neighbor, full-text, and hybrid (fused) search across up to 30k organization
//! datasets through one shared Lance session and an LRU of open dataset handles.
//!
//! Layering, bottom up:
//! - [`domain`]: transport- and engine-agnostic request/response types, traits, and errors.
//! - [`lance`]: Lance-backed implementations of the domain traits.
//! - [`grpc`]: thin tonic transport mapping protobuf onto any [`domain::SearchBackend`].
//! - [`config`]: environment-driven runtime configuration.
//! - [`telemetry`]: Datadog tracing/metrics/logging facade, free of Lance and proto types, usable
//!   from both the `lance` and `grpc` layers.

pub mod config;
pub mod domain;
pub mod grpc;
pub mod lance;
pub mod telemetry;

/// Generated protobuf and gRPC types for `lance_etl.search.v1`.
pub mod pb {
    tonic::include_proto!("lance_etl.search.v1");
}
