//! Unauthenticated replica-local administration transport.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::domain::{ExactPrewarmer, ServingRoute};
use crate::grpc::convert::dataset_target_from_proto;
use crate::grpc::status_from_error;
use crate::internal_pb::admin_service_server::AdminService;
use crate::internal_pb::{PrewarmExactRequest, PrewarmExactResponse};

/// Internal service, reachable only on the loopback interface, that warms only the addressed
/// local process.
pub struct AdminGrpc<B> {
    backend: Arc<B>,
    replica_id: String,
    prewarm_admission: Arc<tokio::sync::Semaphore>,
}

impl<B> AdminGrpc<B> {
    /// Creates the local service with a stable deployment-provided replica identity.
    pub fn new(backend: Arc<B>, replica_id: String) -> Self {
        Self {
            backend,
            replica_id,
            prewarm_admission: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }
}

#[tonic::async_trait]
impl<B: ExactPrewarmer> AdminService for AdminGrpc<B> {
    /// Warms metadata and all committed indexes for one exact candidate on this replica.
    async fn prewarm_exact(
        &self,
        request: Request<PrewarmExactRequest>,
    ) -> Result<Response<PrewarmExactResponse>, Status> {
        let request = request.into_inner();
        let target = dataset_target_from_proto(request.target).map_err(status_from_error)?;
        if request.candidate_lance_uri.is_empty() || request.candidate_lance_version == 0 {
            return Err(Status::invalid_argument(
                "candidate URI and positive version are required",
            ));
        }
        let permit = self
            .prewarm_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("replica prewarm already in progress"))?;
        let route = ServingRoute {
            lance_uri: request.candidate_lance_uri.clone(),
            lance_version: request.candidate_lance_version,
        };
        let report = self
            .backend
            .prewarm_exact(&target, route)
            .await
            .map_err(status_from_error)?;
        drop(permit);
        if report.resolved_version != request.candidate_lance_version {
            return Err(Status::failed_precondition("prewarm resolved an unexpected version"));
        }
        if report.indexes.iter().any(|index| index.error.is_some()) {
            return Err(Status::failed_precondition(
                "one or more candidate indexes failed to prewarm",
            ));
        }
        let indexes_warmed = u32::try_from(report.indexes.len())
            .map_err(|_| Status::internal("warmed index count exceeds the response limit"))?;
        Ok(Response::new(PrewarmExactResponse {
            replica_id: self.replica_id.clone(),
            candidate_lance_uri: request.candidate_lance_uri,
            resolved_version: report.resolved_version,
            indexes_warmed,
        }))
    }
}
