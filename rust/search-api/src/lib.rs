//! Lightweight gRPC search service over per-tenant Lance datasets.
//!
//! Serves nearest-neighbor, full-text, and hybrid (fused) search — plus cache prewarming and IVF
//! cluster introspection — across tens of thousands of `{org}/{tenant}/{namespace}` datasets
//! through one shared Lance session and an LRU of open dataset handles.
//!
//! Layering, bottom up:
//! - [`domain`]: transport- and engine-agnostic request/response types, traits, and errors. It
//!   never references protobuf, tonic, or Lance.
//! - [`cache`]: persistent caching (local disk or shared Redis) plugged into Lance through its
//!   cache and object-store seams. It never references datasets, queries, or domain types.
//! - [`lance`]: Lance-backed implementations of the domain traits.
//! - [`grpc`]: thin tonic transport mapping protobuf onto the domain traits. It never references
//!   Lance types.
//! - [`config`]: environment-driven runtime configuration.
//! - [`telemetry`]: Datadog tracing/metrics/logging facade, free of Lance and proto types, usable
//!   from every layer above `domain`.
//!
//! # Extension points
//!
//! Every cross-cutting behavior sits behind a trait (or, for fusion, a declarative enum) so a
//! second implementation drops in without editing the layers around it. The rule of thumb the
//! crate follows: domain traits speak only domain types, the `lance` module is the only place
//! Lance types appear, and the `grpc` module is the only place proto/tonic types appear. To add a
//! new implementation:
//!
//! - New search engine: implement [`domain::SearchBackend`] (and, to serve the full API,
//!   [`domain::Prewarmer`] + [`domain::clusters::ClusterReader`]) over your engine, expressed
//!   purely in domain types. The transport ([`grpc::SearchGrpc`]) is generic over these traits, so
//!   it needs no change. [`lance::LanceSearchBackend`] is the reference implementation.
//! - New dataset-resolution strategy (different URI layout, a catalog, a different blue-green
//!   scheme): implement [`lance::DatasetProvider`]. It owns version/tag resolution and the
//!   open-handle cache; the backend only states which version it wants via [`domain::DatasetRef`],
//!   with prewarm opens routed through `dataset_for_prewarm` so cold-open telemetry stays honest.
//!   [`lance::CachingDatasetProvider`] is the reference implementation.
//! - New cache persistence backend (a distributed KV, a different object store): implement
//!   [`cache::entry_store::EntryStore`]. The hybrid index cache and the metadata byte cache
//!   compose over it, so the cache semantics never change with the backend.
//!   [`cache::disk_store::DiskEntryStore`] and [`cache::redis_store::RedisEntryStore`] are the
//!   reference implementations, selected by `SEARCH_API_CACHE_BACKEND`.
//! - New hybrid fusion strategy: add a variant to [`domain::FusionSpec`] and a match arm to its
//!   `fuse` method. Fusion is a pure function of the leg lists, so it is unit-testable with no
//!   engine or server. Map it from proto in [`grpc::convert::fusion_from_proto`].
//! - New version selector: extend [`domain::DatasetRef`]; resolve it in
//!   [`lance::DatasetProvider`] implementations and map it from proto in `grpc::convert`.
//! - New transport (e.g. HTTP/JSON): add a sibling of [`grpc`] that converts its wire types to and
//!   from domain types and delegates to the same backend traits. The domain and engine layers are
//!   untouched.
//!
//! Single-use helpers are intentionally left as concrete functions: a seam is added only where a
//! second implementation would plausibly need one.

pub mod cache;
pub mod config;
pub mod domain;
pub mod grpc;
pub mod lance;
pub mod telemetry;

/// Generated protobuf and gRPC types for the `lance_etl.v1.SearchService` defined in
/// `lance_etl.proto`.
pub mod pb {
    tonic::include_proto!("lance_etl.v1");
}
