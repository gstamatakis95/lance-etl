//! Binary entry point for the gRPC search service.

use std::net::SocketAddr;
use std::sync::Arc;

use search_api::config::Config;
use search_api::grpc::SearchGrpc;
use search_api::lance::{CachingDatasetProvider, LanceSearchBackend};
use search_api::pb::search_service_server::SearchServiceServer;
use search_api::telemetry::{self, Metrics};
use tonic::transport::Server;
use tonic_tracing_opentelemetry::middleware::filters::reject_healthcheck;
use tonic_tracing_opentelemetry::middleware::server::OtelGrpcLayer;

/// Backend type served by this binary: Lance over the caching base-URI-template provider.
type Backend = LanceSearchBackend<CachingDatasetProvider>;

/// Reads configuration from the environment, initializes Datadog telemetry (OTLP traces, JSON
/// logs, DogStatsD metrics), wires provider -> backend -> transport, spawns the disk-cache
/// janitor, and serves the gRPC API together with the standard gRPC health service.
///
/// Every RPC flows through the OpenTelemetry tower layer (health checks excluded), which extracts
/// inbound trace context and opens the per-request server span. Telemetry failures never block or
/// fail requests.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;
    let telemetry_guard = telemetry::init_tracing(config.telemetry_disabled);
    let metrics = Arc::new(if config.telemetry_disabled {
        Metrics::disabled()
    } else {
        Metrics::dogstatsd(&config.statsd_addr)
    });
    let addr: SocketAddr = ([0, 0, 0, 0], config.port).into();
    let provider = CachingDatasetProvider::with_telemetry(&config, metrics.clone());
    if let Some(janitor) = provider.janitor(&config) {
        janitor.spawn(std::time::Duration::from_secs(config.disk_cache_sweep_secs));
    }
    let backend = Arc::new(
        LanceSearchBackend::new(provider)
            .with_prewarm_concurrency(config.prewarm_concurrency)
            .with_fanout_concurrency(config.fanout_concurrency)
            .with_id_column(config.id_column.clone())
            .with_metrics(metrics.clone()),
    );
    let service = SearchGrpc::with_metrics(backend, metrics);
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<SearchServiceServer<SearchGrpc<Backend>>>()
        .await;
    tracing::info!(address = %addr, "search-api listening");
    Server::builder()
        .layer(OtelGrpcLayer::default().filter(reject_healthcheck))
        .add_service(health_service)
        .add_service(SearchServiceServer::new(service))
        .serve(addr)
        .await?;
    drop(telemetry_guard);
    Ok(())
}
