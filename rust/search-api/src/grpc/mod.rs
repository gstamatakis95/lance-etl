//! Thin gRPC transport: proto <-> domain mapping over any [`SearchBackend`].
//!
//! This layer never references Lance types; it converts protobuf requests into domain queries,
//! delegates to the backend, and converts domain hits and errors back to protobuf. Per-RPC
//! observability lives here: the tower layer in `main` opens the server span, and the handlers
//! annotate it with `org_id` and the gRPC status, emit one request/latency metric per call, and
//! log failures with the org context.

pub mod convert;

use std::sync::Arc;
use std::time::Instant;

use tonic::{Code, Request, Response, Status};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::domain::{Prewarmer, SearchBackend, SearchError};
use crate::grpc::convert::{
    fused_hit_to_proto, hybrid_query_from_proto, prewarm_report_to_proto, prewarm_spec_from_proto, text_hit_to_proto,
    text_query_from_proto, vector_hit_to_proto, vector_query_from_proto,
};
use crate::pb::search_service_server::SearchService;
use crate::pb::{
    HybridSearchRequest, HybridSearchResponse, PrewarmRequest, PrewarmResponse, TextSearchRequest, TextSearchResponse,
    VectorSearchRequest, VectorSearchResponse,
};
use crate::telemetry::{Metrics, Rpc};

/// gRPC service adapter over any domain search backend.
pub struct SearchGrpc<B> {
    backend: Arc<B>,
    metrics: Arc<Metrics>,
}

impl<B> SearchGrpc<B> {
    /// Creates the adapter over a shared backend with telemetry disabled.
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            backend,
            metrics: Arc::new(Metrics::disabled()),
        }
    }

    /// Creates the adapter emitting per-RPC metrics through the given facade.
    pub fn with_metrics(backend: Arc<B>, metrics: Arc<Metrics>) -> Self {
        Self { backend, metrics }
    }
}

/// Maps a domain error onto the corresponding gRPC status.
pub fn status_from_error(err: SearchError) -> Status {
    match err {
        SearchError::InvalidArgument(message) => Status::invalid_argument(message),
        SearchError::NotFound(message) => Status::not_found(message),
        SearchError::Unavailable(message) => Status::unavailable(message),
        SearchError::Internal(message) => Status::internal(message),
    }
}

/// Low-cardinality metric tag for a gRPC status code.
fn code_tag(code: Code) -> &'static str {
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "cancelled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
    }
}

/// Annotates the current (server) span with the canonical request attributes.
///
/// `org_id` is allowed on traces and logs but never on metrics; `set_attribute` writes through
/// the OpenTelemetry layer, so it works even though the tower layer's span does not declare these
/// tracing fields, and degrades to a no-op when telemetry is disabled.
fn annotate_request_span(org_id: &str) {
    let span = tracing::Span::current();
    span.set_attribute("org_id", org_id.to_string());
}

/// Records the RPC outcome: gRPC status code on the span, request/latency/error metrics, and a
/// warn-level event with the org context on failure.
fn record_outcome<T>(metrics: &Metrics, rpc: Rpc, org_id: &str, started: Instant, result: &Result<T, Status>) {
    let code = match result {
        Ok(_) => Code::Ok,
        Err(status) => status.code(),
    };
    let span = tracing::Span::current();
    span.set_attribute("rpc.grpc.status_code", code as i64);
    metrics.rpc(rpc, code_tag(code), started.elapsed());
    if let Err(status) = result {
        tracing::warn!(
            org_id = %org_id,
            rpc = rpc.as_tag(),
            status = code_tag(code),
            message = status.message(),
            "rpc failed"
        );
    }
}

#[tonic::async_trait]
impl<B: SearchBackend + Prewarmer> SearchService for SearchGrpc<B> {
    /// Nearest-neighbor search on a vector column of the org dataset.
    async fn vector_search(
        &self,
        request: Request<VectorSearchRequest>,
    ) -> Result<Response<VectorSearchResponse>, Status> {
        let started = Instant::now();
        let request = request.into_inner();
        annotate_request_span(&request.org_id);
        let result = async {
            let query = vector_query_from_proto(request.query).map_err(status_from_error)?;
            tracing::Span::current().set_attribute("search.k", query.k as i64);
            let hits = self
                .backend
                .vector_search(&request.org_id, query)
                .await
                .map_err(status_from_error)?;
            Ok(Response::new(VectorSearchResponse {
                results: hits.into_iter().map(vector_hit_to_proto).collect(),
            }))
        }
        .await;
        record_outcome(&self.metrics, Rpc::VectorSearch, &request.org_id, started, &result);
        result
    }

    /// Full-text search via the INVERTED index.
    async fn text_search(&self, request: Request<TextSearchRequest>) -> Result<Response<TextSearchResponse>, Status> {
        let started = Instant::now();
        let request = request.into_inner();
        annotate_request_span(&request.org_id);
        let result = async {
            let query = text_query_from_proto(request.query).map_err(status_from_error)?;
            tracing::Span::current().set_attribute("search.k", query.k as i64);
            let hits = self
                .backend
                .text_search(&request.org_id, query)
                .await
                .map_err(status_from_error)?;
            Ok(Response::new(TextSearchResponse {
                results: hits.into_iter().map(text_hit_to_proto).collect(),
            }))
        }
        .await;
        record_outcome(&self.metrics, Rpc::TextSearch, &request.org_id, started, &result);
        result
    }

    /// Runs a vector leg and a text leg, then fuses them with the configured strategy.
    async fn hybrid_search(
        &self,
        request: Request<HybridSearchRequest>,
    ) -> Result<Response<HybridSearchResponse>, Status> {
        let started = Instant::now();
        let request = request.into_inner();
        let org_id = request.org_id.clone();
        annotate_request_span(&org_id);
        let span = tracing::Span::current();
        span.set_attribute("search.hybrid", true);
        let result = async {
            let query = hybrid_query_from_proto(request).map_err(status_from_error)?;
            span.set_attribute("search.k", query.k as i64);
            let hits = self
                .backend
                .hybrid_search(&org_id, query)
                .await
                .map_err(status_from_error)?;
            Ok(Response::new(HybridSearchResponse {
                results: hits.into_iter().map(fused_hit_to_proto).collect(),
            }))
        }
        .await;
        record_outcome(&self.metrics, Rpc::HybridSearch, &org_id, started, &result);
        result
    }

    /// Proactively pulls one org's metadata and index structures into the local caches.
    async fn prewarm(&self, request: Request<PrewarmRequest>) -> Result<Response<PrewarmResponse>, Status> {
        let started = Instant::now();
        let request = request.into_inner();
        annotate_request_span(&request.org_id);
        let result = async {
            let spec = prewarm_spec_from_proto(&request);
            let report = self
                .backend
                .prewarm(&request.org_id, spec)
                .await
                .map_err(status_from_error)?;
            tracing::Span::current().set_attribute("prewarm.index_count", report.indexes.len() as i64);
            Ok(Response::new(prewarm_report_to_proto(report)))
        }
        .await;
        record_outcome(&self.metrics, Rpc::Prewarm, &request.org_id, started, &result);
        result
    }
}
