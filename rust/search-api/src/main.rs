//! Binary entry point for the gRPC search service.

use std::net::SocketAddr;
use std::sync::Arc;

use search_api::config::Config;
use search_api::grpc::SearchGrpc;
use search_api::lance::{CachingDatasetProvider, LanceSearchBackend};
use search_api::pb::search_service_server::SearchServiceServer;
use search_api::telemetry::{self, Metrics, RecallCapture};
use tonic::transport::Server;
use tonic_tracing_opentelemetry::middleware::filters::reject_healthcheck;
use tonic_tracing_opentelemetry::middleware::server::OtelGrpcLayer;

/// Backend type served by this binary: Lance over the caching base-URI-template provider.
type Backend = LanceSearchBackend<CachingDatasetProvider>;

/// Stamps process-global Lance IO tuning knobs into the environment.
///
/// Lance reads `LANCE_IO_THREADS` lazily at every `ObjectStore::io_parallelism()` call and
/// `OBJECT_STORE_CLIENT_RETRY_TIMEOUT` when it builds S3/GCS/Azure clients.  Setting them
/// here, before any dataset opens or object-store construction, ensures every thread in the
/// process sees a consistent value sourced from the service's own config rather than whatever
/// the operator's shell happened to export.
///
/// This must run while the process is still single-threaded, before the tokio runtime spawns
/// any worker thread.  `set_var` is unsound once other threads exist, because a worker racing
/// on `getenv` against this `set_var` is a data race under the C11/POSIX memory model.  `main`
/// is therefore a synchronous entry point that calls this before building the runtime.
///
/// Knobs stamped here must not already be set in the environment; if they are (e.g. in a
/// Kubernetes pod spec that overrides the default), `set_var` would silently overwrite them.
/// The semantics are intentional: `SEARCH_API_*` vars take precedence over ambient env.
fn apply_lance_io_env(config: &Config) {
    unsafe {
        std::env::set_var("LANCE_IO_THREADS", config.io_concurrency.to_string());
        std::env::set_var(
            "OBJECT_STORE_CLIENT_RETRY_TIMEOUT",
            config.object_store_timeout_secs.to_string(),
        );
    }
}

/// Reads configuration from the environment, initializes Datadog telemetry (OTLP traces, JSON
/// logs, DogStatsD metrics), wires provider -> backend -> transport, spawns the disk-cache
/// janitor, and serves the gRPC API together with the standard gRPC health service.
///
/// IO tuning: three process-global Lance knobs are stamped into the environment before any
/// dataset opens, so that Lance reads them consistently across every thread.
///
/// - `LANCE_IO_THREADS` — read by `ObjectStore::io_parallelism()` on every scan; controls
///   the number of parallel in-flight object-store requests.  Sourced from
///   `SEARCH_API_IO_CONCURRENCY` (default 256).
/// - `OBJECT_STORE_CLIENT_RETRY_TIMEOUT` — picked up by S3/GCS/Azure client builders inside
///   Lance; the total retry-window budget in seconds.  Sourced from
///   `SEARCH_API_OBJECT_STORE_TIMEOUT_SECS` (default 120).
/// - `LANCE_DEFAULT_IO_BUFFER_SIZE` is intentionally left at its Lance default (2 GiB) because
///   the search service issues random index reads rather than full sequential scans, so the
///   per-process backpressure buffer does not need tuning here.
///
/// The per-open `block_size` (`SEARCH_API_IO_BLOCK_SIZE_BYTES`, default 256 KiB) is injected
/// into `ObjectStoreParams` by the provider on each dataset cache miss.
///
/// Every RPC flows through the OpenTelemetry tower layer (health checks excluded), which extracts
/// inbound trace context and opens the per-request server span. Telemetry failures never block or
/// fail requests.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;
    apply_lance_io_env(&config);
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    runtime.block_on(serve(config))
}

/// Runs the async service body on the already-built runtime: wires telemetry, provider, backend,
/// and transport, then serves the gRPC API together with the standard gRPC health service.
async fn serve(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Arc::new(if config.telemetry_disabled {
        Metrics::disabled()
    } else {
        Metrics::dogstatsd(&config.statsd_addr)
    });
    let telemetry_guard = telemetry::init_tracing(config.telemetry_disabled, metrics.clone());
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
    let recall = RecallCapture::new(config.recall_sample_rate, config.id_column.clone(), metrics.clone());
    let service = SearchGrpc::with_metrics(backend, metrics).with_recall(recall);
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
