//! Binary entry point for the gRPC search service.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use search_api::config::Config;
use search_api::domain::{DatasetRef, DatasetTarget, PrewarmSpec, Prewarmer, StdoutSink};
use search_api::grpc::{IntakeGrpc, SearchGrpc};
use search_api::lance::{AnnDefaults, CachingDatasetProvider, LanceSearchBackend};
use search_api::pb::intake_service_server::IntakeServiceServer;
use search_api::pb::search_service_server::SearchServiceServer;
use search_api::telemetry::{self, Metrics, RecallCapture};
use tokio::sync::Semaphore;
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
            search_api::config::DEFAULT_OBJECT_STORE_TIMEOUT_SECS.to_string(),
        );
    }
}

/// Reads configuration from the environment, initializes Datadog telemetry (OTLP traces, JSON
/// logs, DogStatsD metrics), wires provider -> backend -> transport, spawns the disk-cache
/// janitor, and serves the search and intake gRPC APIs together with the standard gRPC health
/// service. The intake service uses the placeholder [`StdoutSink`]; a future Kafka sink drops in
/// at this construction site without any other change.
///
/// IO tuning: three process-global Lance knobs are stamped into the environment before any
/// dataset opens, so that Lance reads them consistently across every thread.
///
/// - `LANCE_IO_THREADS` — read by `ObjectStore::io_parallelism()` on every scan; controls
///   the number of parallel in-flight object-store requests.  Sourced from
///   `SEARCH_API_IO_CONCURRENCY` (default 256).
/// - `OBJECT_STORE_CLIENT_RETRY_TIMEOUT` — picked up by S3/GCS/Azure client builders inside
///   Lance; the total retry-window budget in seconds.  Fixed at
///   [`search_api::config::DEFAULT_OBJECT_STORE_TIMEOUT_SECS`] (120).
/// - `LANCE_DEFAULT_IO_BUFFER_SIZE` is intentionally left at its Lance default (2 GiB) because
///   the search service issues random index reads rather than full sequential scans, so the
///   per-process backpressure buffer does not need tuning here.
///
/// The per-open `block_size` (fixed at [`search_api::config::DEFAULT_IO_BLOCK_SIZE_BYTES`],
/// 256 KiB) is injected into `ObjectStoreParams` by the provider on each dataset cache miss.
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

/// Parses a prewarm-targets file and returns the successfully parsed targets.
///
/// Each line is expected to be `{org_id}/{tenant_id}/{namespace}`. Blank lines and lines with
/// fewer than three slash-separated segments or invalid path segment characters are skipped with
/// a warning. A file that cannot be read at all is also warned and yields an empty list.
fn parse_prewarm_targets(path: &std::path::Path) -> Vec<DatasetTarget> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) => {
            tracing::warn!(path = %path.display(), error = %err, "failed to read prewarm targets file");
            return Vec::new();
        }
    };
    content
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            let parts: Vec<&str> = line.splitn(3, '/').collect();
            if parts.len() != 3 {
                tracing::warn!(line = %line, "prewarm targets: skipping malformed line (expected org/tenant/namespace)");
                return None;
            }
            let target = DatasetTarget::new(parts[0], parts[1], parts[2]);
            match target.validate() {
                Ok(()) => Some(target),
                Err(err) => {
                    tracing::warn!(line = %line, error = %err, "prewarm targets: skipping line with invalid path segment");
                    None
                }
            }
        })
        .collect()
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
        janitor.spawn(std::time::Duration::from_secs(
            search_api::config::DEFAULT_DISK_CACHE_SWEEP_SECS,
        ));
    }
    let ann_defaults = AnnDefaults::from_config(&config);
    let backend = Arc::new(
        LanceSearchBackend::new(provider)
            .with_prewarm_concurrency(config.prewarm_concurrency)
            .with_metrics(metrics.clone())
            .with_event_timestamp_column(config.event_timestamp_column.clone())
            .with_ann_defaults(ann_defaults),
    );

    if let Some(targets_path) = &config.prewarm_targets_path {
        let targets = parse_prewarm_targets(targets_path);
        if !targets.is_empty() {
            let backend_for_prewarm = backend.clone();
            let concurrency = config.prewarm_concurrency;
            tokio::spawn(async move {
                let semaphore = Arc::new(Semaphore::new(concurrency.max(1)));
                let mut tasks = tokio::task::JoinSet::new();
                for target in targets {
                    let backend = backend_for_prewarm.clone();
                    let semaphore = semaphore.clone();
                    tasks.spawn(async move {
                        let permit = semaphore.acquire_owned().await;
                        let spec = PrewarmSpec {
                            metadata: true,
                            all_indexes: true,
                            ..Default::default()
                        };
                        match backend.prewarm(&target, spec, DatasetRef::Latest).await {
                            Ok(report) => tracing::info!(
                                org_id = %target.org_id,
                                tenant_id = %target.tenant_id,
                                namespace = %target.namespace,
                                resolved_version = report.resolved_version,
                                indexes_warmed = report.indexes.len(),
                                "startup prewarm succeeded"
                            ),
                            Err(err) => tracing::warn!(
                                org_id = %target.org_id,
                                tenant_id = %target.tenant_id,
                                namespace = %target.namespace,
                                error = %err,
                                "startup prewarm failed"
                            ),
                        }
                        drop(permit);
                    });
                }
                while tasks.join_next().await.is_some() {}
            });
        }
    }

    let recall = RecallCapture::new(
        config.recall_sample_rate,
        search_api::config::DEFAULT_ID_COLUMN,
        metrics.clone(),
    );
    let service = SearchGrpc::with_metrics(backend, metrics.clone()).with_recall(recall);
    let intake = IntakeGrpc::with_metrics(Arc::new(StdoutSink), metrics);
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<SearchServiceServer<SearchGrpc<Backend>>>()
        .await;
    health_reporter
        .set_serving::<IntakeServiceServer<IntakeGrpc<StdoutSink>>>()
        .await;
    tracing::info!(address = %addr, "search-api listening");
    let mut server_builder = Server::builder()
        .concurrency_limit_per_connection(config.concurrency_limit_per_connection)
        .max_concurrent_streams(config.max_concurrent_streams);
    if config.request_timeout_ms > 0 {
        server_builder = server_builder.timeout(Duration::from_millis(config.request_timeout_ms));
    }
    server_builder
        .layer(OtelGrpcLayer::default().filter(reject_healthcheck))
        .add_service(health_service)
        .add_service(SearchServiceServer::new(service))
        .add_service(IntakeServiceServer::new(intake))
        .serve(addr)
        .await?;
    drop(telemetry_guard);
    Ok(())
}
