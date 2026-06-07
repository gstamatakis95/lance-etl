# 0017. Rust intake service with a pluggable record sink

Status: Accepted

## Context

The search service ([0005](0005-rust-grpc-layering-typed-filter.md)) only reads. Clients also need a
way to push record mutations (create, update, delete) toward the datasets the ETL builds. The write
path will eventually publish to Kafka for the ETL to consume, but that infrastructure is not ready.
We want the gRPC contract and the service to land now without committing to a destination, and we
want the destination to be swappable without reshaping the wire API or the transport.

The same hard rules that govern the search side apply. No raw SQL ever crosses the boundary. The
domain layer stays free of tonic and Lance. The grpc layer is the only place proto and tonic types
appear. Telemetry emitters are infallible and metrics carry only low-cardinality tags.

## Decision

Add a second gRPC service, `IntakeService`, defined in `proto/lance_etl/intake/v1/intake.proto`. It
exposes a unary `Mutate(MutateRequest) returns (MutateResponse)` for one-shot batches and a
client-streaming `MutateStream(stream MutateRequest) returns (MutateResponse)` for high-throughput
batching. Every request carries an intake-local `DatasetTarget` (org, tenant, namespace) mirroring
the search addressing, so the intake proto stays independent of the search proto. A `Mutation` pairs
a `MutationOp` enum (`OP_UPSERT` for create and update, `OP_DELETE` for delete) with a `Record`.

The `Record` mirrors the ETL and search data model. It carries a string `id`, an
`event_timestamp_ms` source-event clock, a `metadata` map, an optional map of named vectors, and an
optional map of named text fields. The addressing stays on the request-level `DatasetTarget` and is
never duplicated onto each record. The `metadata` field is a `map<string, string>` that lands as an
Arrow `Map<Utf8, Utf8>` column downstream, replacing the earlier opaque JSON string. The `vectors`
field is a `map<string, FloatVector>` where `FloatVector` is a `repeated float values` message, so a
single record can carry several named fixed-dimension vectors, each keyed by its column name. The
`texts` field is a `map<string, string>` keyed by text column name. The key naming mirrors the
search side's full-text convention: a text field under key `body` lands in the `body` column, the
same column a `TextSearch` queries by naming it, so intake and search agree on column names without
any translation table. A delete reads only the id. The response reports accepted and rejected counts
plus per-item errors.

Validation rejects an upsert with an empty id, an upsert carrying no metadata, vectors, or texts at
all, and any named vector whose values list is empty. Dimension consistency across records is left
to the dataset schema downstream, not enforced here. A delete needs only a non-empty id.

Introduce a domain seam `RecordSink` with one method, `accept(IntakeBatch) -> IntakeReport`, written
in the same native `impl Future` async-trait style the `SearchBackend` and `Prewarmer` traits use.
The sink is the only place a write destination is named. The single implementation today is
`StdoutSink`, which prints each mutation as one structured line and counts it. A future `KafkaSink`
implements the same trait and replaces `StdoutSink` at the construction site in `main` without any
other change. The transport struct `IntakeGrpc<S>` is generic over the sink, exactly as
`SearchGrpc<B>` is generic over the backend. It validates protobuf into domain types (target on the
allowlist, non-empty id, a non-empty payload and non-empty named vectors on an upsert), routes valid
mutations to the sink, and records per-item rejections. The service is registered on the same `Server::builder` router as the
search service and reports health alongside it. Per-RPC spans plus `intake.*` metrics (request and
latency, upsert, delete, reject counts, and batch size, all tagged only by the low-cardinality RPC
name and status) follow the typed `Metrics` facade from [0008](0008-observability-and-recall-audit.md).

There is no `_ingested_at`. The record's `event_timestamp_ms` is the canonical clock, consistent
with the ETL.

## Consequences

The wire contract and the service exist now, so clients can integrate against a stable API while the
real destination is still being built. Swapping stdout for Kafka is a one-line change at the
construction site because the seam isolates the destination. The layering rules hold: the domain
intake types reference neither tonic nor Lance, the grpc layer owns all proto conversion and
validation, and intake never opens a dataset, so it carries no Lance dependency at all. Validation
rejects bad mutations per item rather than failing whole batches, which suits streaming ingestion.
A bad target or a whole-sink failure still fails the request. The intake service shares the search
binary, port, router, and telemetry pipeline, so operating it adds no new process. Revisiting the
destination (for example adding a transactional outbox or a direct Lance writer) is a new sink
implementation behind the same trait and does not require a proto change.
