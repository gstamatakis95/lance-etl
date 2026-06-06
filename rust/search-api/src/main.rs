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

/// Reads configuration from the environment, wires provider -> backend -> transport, and serves
/// the gRPC API together with the standard gRPC health service.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;
    let addr: SocketAddr = ([0, 0, 0, 0], config.port).into();
    let provider = CachingDatasetProvider::new(&config);
    let backend = Arc::new(LanceSearchBackend::new(provider));
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
