//! Binary entry point for the gRPC search service.

use std::net::SocketAddr;
use std::sync::Arc;

use search_api::config::Config;
use search_api::grpc::SearchGrpc;
use search_api::lance::{CachingDatasetProvider, LanceSearchBackend};
use search_api::pb::search_service_server::SearchServiceServer;
use tonic::transport::Server;

/// Backend type served by this binary: Lance over the caching base-URI-template provider.
type Backend = LanceSearchBackend<CachingDatasetProvider>;

/// Reads configuration from the environment, wires provider -> backend -> transport, spawns the
/// disk-cache janitor, and serves the gRPC API together with the standard gRPC health service.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;
    let addr: SocketAddr = ([0, 0, 0, 0], config.port).into();
    let provider = CachingDatasetProvider::new(&config);
    if let Some(janitor) = provider.janitor(&config) {
        janitor.spawn(std::time::Duration::from_secs(config.disk_cache_sweep_secs));
    }
    let backend = Arc::new(LanceSearchBackend::new(provider).with_prewarm_concurrency(config.prewarm_concurrency));
    let service = SearchGrpc::new(backend);
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<SearchServiceServer<SearchGrpc<Backend>>>()
        .await;
    println!("search-api listening on {addr}");
    Server::builder()
        .add_service(health_service)
        .add_service(SearchServiceServer::new(service))
        .serve(addr)
        .await?;
    Ok(())
}
