//! Thin gRPC transport: proto <-> domain mapping over the domain service traits.
//!
//! This layer never references Lance types. It converts protobuf requests into domain queries,
//! delegates to the backend, and converts domain results and errors back to protobuf. Per-RPC
//! observability lives here: the tower layer in `main` opens the server span, and the handlers
//! annotate it with low-cardinality search state and the gRPC status, emit one request/latency
//! metric per call, and log bounded failures without target identity. Sampled vector, text, and hybrid requests
//! additionally get `recall.*` capture attributes on the server span (see
//! [`crate::telemetry::recall`]), and a request carrying a rerank spec with `top_n` set truncates
//! the results to that count after the backend returns.
//!
//! Submodules:
//! - [`convert`]: pure conversions between search protobuf messages and domain types.
//! - [`timeout`]: the per-route request-timeout tower layer applied by `main` (and by any test
//!   that mirrors the production server stack).

pub mod admin;
pub mod admission;
pub mod convert;
pub mod timeout;

pub use timeout::RouteTimeoutLayer;

use std::sync::Arc;
use std::time::Instant;

use tonic::{Code, Request, Response, Status};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::domain::{DatasetTarget, SearchBackend, SearchError};
use crate::grpc::admission::AdmissionController;
use crate::grpc::convert::{
    dataset_target_from_proto, fused_hit_to_proto, fused_to_hit, hybrid_query_from_proto, text_hit_to_proto,
    text_query_from_proto, time_range_from_proto, vector_hit_to_proto, vector_query_from_proto, warning_to_proto,
};
use crate::pb::search_service_server::SearchService;
use crate::pb::{
    HybridSearchRequest, HybridSearchResponse, TextSearchRequest, TextSearchResponse, VectorSearchRequest,
    VectorSearchResponse,
};
use crate::telemetry::{Metrics, RecallCapture, Rpc};

/// gRPC service adapter over any domain search backend.
pub struct SearchGrpc<B> {
    backend: Arc<B>,
    metrics: Arc<Metrics>,
    recall: RecallCapture,
    admission: Arc<AdmissionController>,
}

impl<B> SearchGrpc<B> {
    /// Creates the adapter emitting per-RPC metrics through the given facade, with recall
    /// capture disabled.
    pub fn with_metrics(backend: Arc<B>, metrics: Arc<Metrics>, admission: Arc<AdmissionController>) -> Self {
        Self {
            backend,
            metrics,
            recall: RecallCapture::disabled(),
            admission,
        }
    }

    /// Enables sampled-query recall capture for vector, text, and hybrid requests.
    pub fn with_recall(mut self, recall: RecallCapture) -> Self {
        self.recall = recall;
        self
    }

    /// Shared per-RPC scaffold: validates the target, admits the request, runs the handler body,
    /// and records the outcome (status span attribute, metrics, failure log).
    async fn handle<T>(
        &self,
        rpc: Rpc,
        target: Option<crate::pb::DatasetTarget>,
        run: impl AsyncFnOnce(&DatasetTarget) -> Result<T, Status>,
    ) -> Result<Response<T>, Status> {
        let started = Instant::now();
        let target = take_target(target);
        let result = match &target {
            Err(status) => Err(status.clone()),
            Ok(target) => match self.admission.admit(target) {
                Err(status) => Err(status),
                Ok(permit) => {
                    let result = run(target).await.map(Response::new);
                    drop(permit);
                    result
                }
            },
        };
        record_outcome(&self.metrics, rpc, started, &result);
        result
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
pub(crate) fn code_tag(code: Code) -> &'static str {
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

/// Records the RPC outcome: gRPC status code on the span, request/latency/error metrics, and a
/// warn-level bounded event on failure.
fn record_outcome<T>(metrics: &Metrics, rpc: Rpc, started: Instant, result: &Result<T, Status>) {
    let code = match result {
        Ok(_) => Code::Ok,
        Err(status) => status.code(),
    };
    let span = tracing::Span::current();
    span.set_attribute("rpc.grpc.status_code", code as i64);
    metrics.rpc(rpc, code_tag(code), started.elapsed());
    if result.is_err() {
        tracing::warn!(rpc = rpc.as_tag(), status = code_tag(code), "rpc failed");
    }
}

/// Converts and validates the request target without emitting its identity.
fn take_target(target: Option<crate::pb::DatasetTarget>) -> Result<DatasetTarget, Status> {
    dataset_target_from_proto(target).map_err(status_from_error)
}

#[tonic::async_trait]
impl<B: SearchBackend> SearchService for SearchGrpc<B> {
    /// Nearest-neighbor search on a vector column of the target dataset(s).
    async fn vector_search(
        &self,
        request: Request<VectorSearchRequest>,
    ) -> Result<Response<VectorSearchResponse>, Status> {
        let request = request.into_inner();
        self.handle(Rpc::VectorSearch, request.target, async |target| {
            let time_range = time_range_from_proto(request.time_range);
            let query =
                vector_query_from_proto(request.query, request.k, request.filter, request.projection, time_range)
                    .map_err(status_from_error)?;
            tracing::Span::current().set_attribute("search.k", query.k as i64);
            let pending = self.recall.begin(target, &query);
            let outcome = self
                .backend
                .vector_search(target, query)
                .await
                .map_err(status_from_error)?;
            if let Some(pending) = pending {
                self.recall.finish(pending, Some(outcome.served_version), &outcome.hits);
            }
            Ok(VectorSearchResponse {
                results: outcome
                    .hits
                    .into_iter()
                    .map(vector_hit_to_proto)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(status_from_error)?,
                served_version: outcome.served_version,
                partial: outcome.partial,
                warnings: outcome.warnings.into_iter().map(warning_to_proto).collect(),
            })
        })
        .await
    }

    /// Full-text search via the INVERTED index.
    async fn text_search(&self, request: Request<TextSearchRequest>) -> Result<Response<TextSearchResponse>, Status> {
        let request = request.into_inner();
        self.handle(Rpc::TextSearch, request.target, async |target| {
            let time_range = time_range_from_proto(request.time_range);
            let query = text_query_from_proto(request.query, request.k, request.filter, request.projection, time_range)
                .map_err(status_from_error)?;
            tracing::Span::current().set_attribute("search.k", query.k as i64);
            let pending = self.recall.begin_text(target, &query);
            let outcome = self
                .backend
                .text_search(target, query)
                .await
                .map_err(status_from_error)?;
            if let Some(pending) = pending {
                self.recall.finish(pending, Some(outcome.served_version), &outcome.hits);
            }
            Ok(TextSearchResponse {
                results: outcome
                    .hits
                    .into_iter()
                    .map(text_hit_to_proto)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(status_from_error)?,
                served_version: outcome.served_version,
                partial: outcome.partial,
                warnings: outcome.warnings.into_iter().map(warning_to_proto).collect(),
            })
        })
        .await
    }

    /// Runs a vector leg and a text leg, then fuses them with the configured strategy.
    async fn hybrid_search(
        &self,
        request: Request<HybridSearchRequest>,
    ) -> Result<Response<HybridSearchResponse>, Status> {
        let mut request = request.into_inner();
        let target = request.target.take();
        tracing::Span::current().set_attribute("search.hybrid", true);
        self.handle(Rpc::HybridSearch, target, async |target| {
            let query = hybrid_query_from_proto(request).map_err(status_from_error)?;
            tracing::Span::current().set_attribute("search.k", query.k as i64);
            let pending = self.recall.begin_hybrid(target, &query);
            let outcome = self
                .backend
                .hybrid_search(target, query)
                .await
                .map_err(status_from_error)?;
            if let Some(pending) = pending {
                let recall_hits: Vec<_> = outcome.hits.iter().cloned().map(fused_to_hit).collect();
                self.recall.finish(pending, Some(outcome.served_version), &recall_hits);
            }
            Ok(HybridSearchResponse {
                results: outcome
                    .hits
                    .into_iter()
                    .map(fused_hit_to_proto)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(status_from_error)?,
                served_version: outcome.served_version,
                partial: outcome.partial,
                warnings: outcome.warnings.into_iter().map(warning_to_proto).collect(),
            })
        })
        .await
    }
}
