//! Internal replica-local prewarm contract tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use search_api::domain::{DatasetTarget, ExactPrewarmer, PrewarmReport, PrewarmedIndex, SearchError, ServingRoute};
use search_api::grpc::admin::AdminGrpc;
use search_api::grpc::auth::{RequestAuthorizer, RequiredRole};
use search_api::internal_pb::PrewarmExactRequest;
use search_api::internal_pb::admin_service_client::AdminServiceClient;
use search_api::internal_pb::admin_service_server::AdminServiceServer;
use search_api::pb::DatasetTarget as ProtoTarget;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};

/// Fake exact prewarmer recording whether protected work was reached.
struct FakeExactPrewarmer {
    calls: Arc<AtomicU64>,
}

impl ExactPrewarmer for FakeExactPrewarmer {
    async fn prewarm_exact(&self, _target: &DatasetTarget, route: ServingRoute) -> Result<PrewarmReport, SearchError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(PrewarmReport {
            metadata_warmed: true,
            indexes: vec![PrewarmedIndex {
                name: "vector_idx".to_owned(),
                duration: Duration::from_millis(1),
                error: None,
            }],
            metadata_duration: Duration::from_millis(1),
            total_duration: Duration::from_millis(2),
            index_cache_size_bytes: 1024,
            resolved_version: route.lance_version,
        })
    }
}

/// Test authorizer with an explicit allow or deny decision.
struct AdminTestAuthorizer {
    allow: bool,
}

#[async_trait::async_trait]
impl RequestAuthorizer for AdminTestAuthorizer {
    async fn authorize(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
        _target: &DatasetTarget,
        required_role: RequiredRole,
    ) -> Result<(), tonic::Status> {
        assert_eq!(required_role, RequiredRole::Admin);
        if self.allow {
            Ok(())
        } else {
            Err(tonic::Status::permission_denied("denied by test policy"))
        }
    }
}

/// Starts an internal-only server and returns its client channel.
async fn serve(allow: bool, calls: Arc<AtomicU64>) -> Channel {
    let service = AdminGrpc::new(
        Arc::new(FakeExactPrewarmer { calls }),
        Arc::new(AdminTestAuthorizer { allow }),
        "search-api-2".to_owned(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(
        Server::builder()
            .add_service(AdminServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

/// Builds one exact candidate request.
fn request() -> PrewarmExactRequest {
    PrewarmExactRequest {
        target: Some(ProtoTarget {
            org_id: "org1".to_owned(),
            tenant_id: "tenant1".to_owned(),
            namespace: "namespace1".to_owned(),
        }),
        candidate_lance_uri: "s3://bucket/candidates/build-17.lance".to_owned(),
        candidate_lance_version: 17,
    }
}

#[tokio::test]
async fn exact_prewarm_returns_local_replica_and_candidate_proof() {
    let calls = Arc::new(AtomicU64::new(0));
    let response = AdminServiceClient::new(serve(true, calls.clone()).await)
        .prewarm_exact(request())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.replica_id, "search-api-2");
    assert_eq!(response.candidate_lance_uri, "s3://bucket/candidates/build-17.lance");
    assert_eq!(response.resolved_version, 17);
    assert_eq!(response.indexes_warmed, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn denied_admin_request_never_reaches_prewarm() {
    let calls = Arc::new(AtomicU64::new(0));
    let status = AdminServiceClient::new(serve(false, calls.clone()).await)
        .prewarm_exact(request())
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
