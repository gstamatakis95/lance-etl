//! Lightweight gRPC search service over per-tenant Lance datasets.
//!
//! Serves nearest-neighbor, full-text, and hybrid (fused) search — plus cache prewarming and IVF
//! cluster introspection — across tens of thousands of `{org}/{tenant}/{namespace}` datasets
//! (optionally date-partitioned per day) through one shared Lance session and an LRU of open
//! dataset handles.
//!
//! Layering, bottom up:
//! - [`domain`]: transport- and engine-agnostic request/response types, traits, and errors. It
//!   never references protobuf, tonic, or Lance.
//! - [`cache`]: persistent disk caching plugged into Lance through its cache and object-store
//!   seams. It never references datasets, queries, or domain types.
//! - [`lance`]: Lance-backed implementations of the domain traits.
//! - [`grpc`]: thin tonic transport mapping protobuf onto the domain traits. It never references
//!   Lance types.
//! - [`config`]: environment-driven runtime configuration.
//! - [`telemetry`]: Datadog tracing/metrics/logging facade, free of Lance and proto types, usable
//!   from every layer above `domain`.

pub mod cache;
pub mod config;
pub mod domain;
pub mod grpc;
pub mod lance;
pub mod telemetry;

/// Generated protobuf and gRPC types for `lance_etl.search.v1`.
pub mod pb {
    tonic::include_proto!("lance_etl.search.v1");
}
