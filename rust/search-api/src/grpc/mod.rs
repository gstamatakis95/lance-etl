//! Thin gRPC transport: proto <-> domain mapping over any [`SearchBackend`].
//!
//! This layer never references Lance types; it converts protobuf requests into domain queries,
//! delegates to the backend, and converts domain hits and errors back to protobuf.

pub mod convert;

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::domain::{SearchBackend, SearchError};
use crate::grpc::convert::{
    fused_hit_to_proto, hybrid_query_from_proto, text_hit_to_proto, text_query_from_proto, vector_hit_to_proto,
    vector_query_from_proto,
};
use crate::pb::search_service_server::SearchService;
use crate::pb::{
    HybridSearchRequest, HybridSearchResponse, TextSearchRequest, TextSearchResponse, VectorSearchRequest,
    VectorSearchResponse,
};

/// gRPC service adapter over any domain search backend.
pub struct SearchGrpc<B> {
    backend: Arc<B>,
}

impl<B> SearchGrpc<B> {
    /// Creates the adapter over a shared backend.
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
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

#[tonic::async_trait]
impl<B: SearchBackend> SearchService for SearchGrpc<B> {
    /// Nearest-neighbor search on a vector column of the org dataset.
    async fn vector_search(
        &self,
        request: Request<VectorSearchRequest>,
    ) -> Result<Response<VectorSearchResponse>, Status> {
        let request = request.into_inner();
        let query = vector_query_from_proto(request.query).map_err(status_from_error)?;
        let hits = self
            .backend
            .vector_search(&request.org_id, query)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(VectorSearchResponse {
            results: hits.into_iter().map(vector_hit_to_proto).collect(),
        }))
    }

    /// Full-text search via the INVERTED index.
    async fn text_search(&self, request: Request<TextSearchRequest>) -> Result<Response<TextSearchResponse>, Status> {
        let request = request.into_inner();
        let query = text_query_from_proto(request.query).map_err(status_from_error)?;
        let hits = self
            .backend
            .text_search(&request.org_id, query)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(TextSearchResponse {
            results: hits.into_iter().map(text_hit_to_proto).collect(),
        }))
    }

    /// Runs a vector leg and a text leg, then fuses them with the configured strategy.
    async fn hybrid_search(
        &self,
        request: Request<HybridSearchRequest>,
    ) -> Result<Response<HybridSearchResponse>, Status> {
        let request = request.into_inner();
        let org_id = request.org_id.clone();
        let query = hybrid_query_from_proto(request).map_err(status_from_error)?;
        let hits = self
            .backend
            .hybrid_search(&org_id, query)
            .await
            .map_err(status_from_error)?;
        Ok(Response::new(HybridSearchResponse {
            results: hits.into_iter().map(fused_hit_to_proto).collect(),
        }))
    }
}
