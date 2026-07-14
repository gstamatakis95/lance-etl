//! Integration tests for the per-route request timeouts: builds the REAL production server stack
//! (OpenTelemetry layer plus [`RouteTimeoutLayer::from_defaults`], the same chain `main.rs`
//! installs) over a controllable slow backend and asserts that a slow search is cut off at the
//! default budget while a fast search passes unchanged.

use std::sync::Arc;
use std::time::{Duration, Instant};

use search_api::domain::{
    DatasetTarget, HybridQuery, HybridSearchOutcome, SearchBackend, SearchError, TextQuery, TextSearchOutcome,
    VectorQuery, VectorSearchOutcome,
};
use search_api::grpc::{RouteTimeoutLayer, SearchGrpc};
use search_api::pb::search_service_client::SearchServiceClient;
use search_api::pb::search_service_server::SearchServiceServer;
use search_api::pb::{VectorQuery as VectorQueryProto, VectorSearchRequest};
use search_api::telemetry::{self, Metrics};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::transport::{Channel, Server};
use tonic_tracing_opentelemetry::middleware::filters::reject_healthcheck;
use tonic_tracing_opentelemetry::middleware::server::OtelGrpcLayer;

/// Backend whose handlers sleep for configured durations before answering, so tests can steer
/// each RPC past or under the server-side budgets.
struct SlowBackend {
    search_delay: Duration,
}

impl SearchBackend for SlowBackend {
    async fn vector_search(
        &self,
        _target: &DatasetTarget,
        _query: VectorQuery,
    ) -> Result<VectorSearchOutcome, SearchError> {
        tokio::time::sleep(self.search_delay).await;
        Ok(VectorSearchOutcome {
            served_version: 1,
            ..Default::default()
        })
    }

    async fn text_search(&self, _target: &DatasetTarget, _query: TextQuery) -> Result<TextSearchOutcome, SearchError> {
        tokio::time::sleep(self.search_delay).await;
        Ok(TextSearchOutcome::default())
    }

    async fn hybrid_search(
        &self,
        _target: &DatasetTarget,
        _query: HybridQuery,
    ) -> Result<HybridSearchOutcome, SearchError> {
        tokio::time::sleep(self.search_delay).await;
        Ok(HybridSearchOutcome::default())
    }
}

/// Serves the search API over the given slow backend with the production layer chain and returns
/// a connected channel.
async fn serve_slow(backend: SlowBackend) -> Channel {
    drop(telemetry::init_tracing(true, Arc::new(Metrics::disabled())));
    let service = SearchGrpc::with_metrics(Arc::new(backend), Arc::new(Metrics::disabled()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        Server::builder()
            .concurrency_limit_per_connection(search_api::config::DEFAULT_CONCURRENCY_LIMIT_PER_CONNECTION)
            .max_concurrent_streams(search_api::config::DEFAULT_MAX_CONCURRENT_STREAMS)
            .layer(OtelGrpcLayer::default().filter(reject_healthcheck))
            .layer(RouteTimeoutLayer::from_defaults(Arc::new(Metrics::disabled())))
            .add_service(SearchServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

/// Builds the proto target for `org1/tenant1/ns1`.
fn target() -> Option<search_api::pb::DatasetTarget> {
    Some(search_api::pb::DatasetTarget {
        org_id: "org1".to_string(),
        tenant_id: "tenant1".to_string(),
        namespace: "ns1".to_string(),
    })
}

#[tokio::test]
async fn slow_search_is_cut_off_at_the_search_budget() {
    let channel = serve_slow(SlowBackend {
        search_delay: Duration::from_secs(3),
    })
    .await;
    let mut client = SearchServiceClient::new(channel);

    let started = Instant::now();
    let status = client
        .vector_search(VectorSearchRequest {
            time_range: None,
            target: target(),
            query: Some(VectorQueryProto {
                vector: vec![1.0, 0.0, 0.0, 0.0],
            }),
            k: 1,
            filter: None,
            projection: Vec::new(),
        })
        .await
        .unwrap_err();
    let search_elapsed = started.elapsed();
    assert_eq!(
        status.code(),
        Code::DeadlineExceeded,
        "a search slower than the default budget must be cut off: {status}"
    );
    assert!(
        search_elapsed < Duration::from_millis(2_500),
        "the cutoff must fire at the 800 ms budget, not wait out the handler: {search_elapsed:?}"
    );
}

#[tokio::test]
async fn fast_search_passes_through_the_timeout_layer_untouched() {
    let channel = serve_slow(SlowBackend {
        search_delay: Duration::from_millis(10),
    })
    .await;
    let mut client = SearchServiceClient::new(channel);
    let response = client
        .vector_search(VectorSearchRequest {
            time_range: None,
            target: target(),
            query: Some(VectorQueryProto {
                vector: vec![1.0, 0.0, 0.0, 0.0],
            }),
            k: 1,
            filter: None,
            projection: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(response.results.is_empty(), "the fake backend returns no hits");
}
