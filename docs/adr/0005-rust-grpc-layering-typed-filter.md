# 0005. Rust gRPC service layering and the typed filter AST

Status: Accepted

## Context

A small tokio gRPC service serves vector, full-text, and hybrid search over the per-org datasets. It must stay
maintainable and extensible as it grows (caching, prewarm, clusters, observability), and it must never accept
raw SQL from clients.

## Decision

Layer the crate with hard boundaries: `domain` holds engine- and transport-agnostic types and traits (the
typed `Filter` AST, query types, `SearchBackend`, `DatasetProvider`, fusion, errors) and references neither
proto nor tonic nor lance. `lance` holds the engine implementations. `grpc` is a thin tonic adapter that maps
proto to and from domain and never references lance types. `cache` and `telemetry` are their own module folders.
`lib.rs` re-exports a clean surface.

Filtering uses a typed `Filter` AST (comparison, in-list, is-null, between, and/or/not), never a SQL string.
Column names are validated against the dataset schema and an identifier allowlist, literals become typed
DataFusion `lit` expressions, and the AST translates to a DataFusion `Expr` fed to the scanner. Clients cannot
inject expressions.

## Consequences

The layering lets caching, prewarm, and clusters slot in without entangling transport and engine. The Clusters
RPC reads IVF centroids through `index_reader.rs` (open dataset, resolve index UUID and column from
`IndexMetadata`, `open_vector_index` with a `NoOpMetricsCollector`, walk `IvfModel` partitions, downcast
centroids to `Float32Array`) routed through a domain trait so grpc stays lance-free. Injection attempts such as
a column named `id; DROP TABLE users` are rejected at the allowlist and covered by tests. The proto is
pre-release, so breaking reshapes (the `DatasetTarget` message) were taken freely.

The `SearchService` and the later `IntakeService` ([0017](0017-rust-intake-service.md)) were
subsequently consolidated into one proto file, `proto/lance_etl/v1/lance_etl.proto` (package
`lance_etl.v1`), with a single shared `DatasetTarget` message referenced by both services. The
layering above is unchanged: `grpc` remains the only place proto and tonic types appear.
