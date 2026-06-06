//! Thin gRPC transport: proto <-> domain mapping over the domain service traits.
//!
//! This layer never references Lance types. It converts protobuf requests into domain queries,
//! delegates to the backend, and converts domain results and errors back to protobuf. Per-RPC
//! observability lives here: the tower layer in `main` opens the server span, and the handlers
//! annotate it with the dataset target and the gRPC status, emit one request/latency metric per
//! call, and log failures with the target context. Sampled VectorSearch requests additionally
//! get `recall.*` capture attributes on the server span (see [`crate::telemetry::recall`]).
//!
//! Submodules:
//! - [`convert`]: pure conversions between protobuf messages and domain types.

pub mod convert;

use std::sync::Arc;
use std::time::Instant;

use tonic::{Code, Request, Response, Status};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::domain::{ClusterReader, DatasetTarget, Prewarmer, SearchBackend, SearchError};
use crate::grpc::convert::{
    cluster_report_to_proto, cluster_spec_from_proto, dataset_target_from_proto, fused_hit_to_proto,
    hybrid_query_from_proto, prewarm_report_to_proto, prewarm_spec_from_proto, text_hit_to_proto,
    text_query_from_proto, vector_hit_to_proto, vector_query_from_proto,
};
use crate::pb::search_service_server::SearchService;
use crate::pb::{
    ClustersRequest, ClustersResponse, HybridSearchRequest, HybridSearchResponse, PrewarmRequest, PrewarmResponse,
    TextSearchRequest, TextSearchResponse, VectorSearchRequest, VectorSearchResponse,
};
use crate::telemetry::{Metrics, RecallCapture, Rpc};

/// gRPC service adapter over any domain search backend.
pub struct SearchGrpc<B> {
    backend: Arc<B>,
    metrics: Arc<Metrics>,
    recall: RecallCapture,
}

impl<B> SearchGrpc<B> {
    /// Creates the adapter emitting per-RPC metrics through the given facade, with recall
    /// capture disabled.
    pub fn with_metrics(backend: Arc<B>, metrics: Arc<Metrics>) -> Self {
        Self {
            backend,
            metrics,
            recall: RecallCapture::disabled(),
        }
    }

    /// Enables sampled-query recall capture for VectorSearch requests.
    pub fn with_recall(mut self, recall: RecallCapture) -> Self {
        self.recall = recall;
        self
    }

    /// Shared per-RPC scaffold: converts and annotates the target, runs the handler body, and
    /// records the outcome (status span attribute, metrics, failure log).
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
            Ok(target) => run(target).await.map(Response::new),
        };
        record_outcome(&self.metrics, rpc, target.as_ref().ok(), started, &result);
        result
    }
}

/// Maps a domain error onto the corresponding gRPC status.
pub fn status_from_error(err: SearchError) -> Status {
    match err {
        SearchError::InvalidArgument(message) => Status::invalid_argument(message),
        SearchError::NotFound(message) => Status::not_found(message),
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

/// Annotates the current (server) span with the canonical dataset-target attributes.
///
/// Target identifiers are allowed on traces and logs but never on metrics. `set_attribute`
/// writes through the OpenTelemetry layer, so it works even though the tower layer's span does
/// not declare these tracing fields, and degrades to a no-op when telemetry is disabled.
fn annotate_request_span(target: &DatasetTarget) {
    let span = tracing::Span::current();
    span.set_attribute("org_id", target.org_id.clone());
    span.set_attribute("tenant_id", target.tenant_id.clone());
    span.set_attribute("namespace", target.namespace.clone());
    if let Some(range) = &target.date_range {
        span.set_attribute("date_range.start", range.start.to_string());
        span.set_attribute("date_range.end", range.end.to_string());
        span.set_attribute("date_range.days", range.days().len() as i64);
    }
}

/// Renders the target for failure logs.
fn target_label(target: &DatasetTarget) -> String {
    match &target.date_range {
        None => format!("{}/{}/{}", target.org_id, target.tenant_id, target.namespace),
        Some(range) => format!(
            "{}/{}/{}@{}..{}",
            target.org_id, target.tenant_id, target.namespace, range.start, range.end
        ),
    }
}

/// Records the RPC outcome: gRPC status code on the span, request/latency/error metrics, and a
/// warn-level event with the target context on failure.
fn record_outcome<T>(
    metrics: &Metrics,
    rpc: Rpc,
    target: Option<&DatasetTarget>,
    started: Instant,
    result: &Result<T, Status>,
) {
    let code = match result {
        Ok(_) => Code::Ok,
        Err(status) => status.code(),
    };
    let span = tracing::Span::current();
    span.set_attribute("rpc.grpc.status_code", code as i64);
    metrics.rpc(rpc, code_tag(code), started.elapsed());
    if let Err(status) = result {
        tracing::warn!(
            target = target.map(target_label).unwrap_or_default(),
            rpc = rpc.as_tag(),
            status = code_tag(code),
            message = status.message(),
            "rpc failed"
        );
    }
}

/// Converts the request target, annotating the span on success.
fn take_target(target: Option<crate::pb::DatasetTarget>) -> Result<DatasetTarget, Status> {
    let target = dataset_target_from_proto(target).map_err(status_from_error)?;
    annotate_request_span(&target);
    Ok(target)
}

#[tonic::async_trait]
impl<B: SearchBackend + Prewarmer + ClusterReader> SearchService for SearchGrpc<B> {
    /// Nearest-neighbor search on a vector column of the target dataset(s).
    async fn vector_search(
        &self,
        request: Request<VectorSearchRequest>,
    ) -> Result<Response<VectorSearchResponse>, Status> {
        let request = request.into_inner();
        self.handle(Rpc::VectorSearch, request.target, async |target| {
            let query = vector_query_from_proto(request.query).map_err(status_from_error)?;
            tracing::Span::current().set_attribute("search.k", query.k as i64);
            let pending = self.recall.begin(target, &query);
            let outcome = self
                .backend
                .vector_search(target, query)
                .await
                .map_err(status_from_error)?;
            if let Some(pending) = pending {
                self.recall.finish(pending, outcome.dataset_version, &outcome.hits);
            }
            Ok(VectorSearchResponse {
                results: outcome.hits.into_iter().map(vector_hit_to_proto).collect(),
            })
        })
        .await
    }

    /// Full-text search via the INVERTED index.
    async fn text_search(&self, request: Request<TextSearchRequest>) -> Result<Response<TextSearchResponse>, Status> {
        let request = request.into_inner();
        self.handle(Rpc::TextSearch, request.target, async |target| {
            let query = text_query_from_proto(request.query).map_err(status_from_error)?;
            tracing::Span::current().set_attribute("search.k", query.k as i64);
            let hits = self
                .backend
                .text_search(target, query)
                .await
                .map_err(status_from_error)?;
            Ok(TextSearchResponse {
                results: hits.into_iter().map(text_hit_to_proto).collect(),
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
            let hits = self
                .backend
                .hybrid_search(target, query)
                .await
                .map_err(status_from_error)?;
            Ok(HybridSearchResponse {
                results: hits.into_iter().map(fused_hit_to_proto).collect(),
            })
        })
        .await
    }

    /// Proactively pulls one dataset's metadata and index structures into the local caches.
    async fn prewarm(&self, request: Request<PrewarmRequest>) -> Result<Response<PrewarmResponse>, Status> {
        let mut request = request.into_inner();
        let target = request.target.take();
        self.handle(Rpc::Prewarm, target, async |target| {
            let spec = prewarm_spec_from_proto(&request);
            let report = self.backend.prewarm(target, spec).await.map_err(status_from_error)?;
            tracing::Span::current().set_attribute("prewarm.index_count", report.indexes.len() as i64);
            Ok(prewarm_report_to_proto(report))
        })
        .await
    }

    /// Reads the IVF cluster centroids of a vector index from the target dataset.
    async fn clusters(&self, request: Request<ClustersRequest>) -> Result<Response<ClustersResponse>, Status> {
        let mut request = request.into_inner();
        let target = request.target.take();
        self.handle(Rpc::Clusters, target, async |target| {
            let spec = cluster_spec_from_proto(&request);
            let report = self.backend.clusters(target, spec).await.map_err(status_from_error)?;
            tracing::Span::current().set_attribute("clusters.count", report.num_partitions() as i64);
            Ok(cluster_report_to_proto(report))
        })
        .await
    }
}
