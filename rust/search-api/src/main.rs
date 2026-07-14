//! Binary entry point for the gRPC search service.

use std::ffi::OsStr;
use std::net::SocketAddr;
use std::sync::Arc;

use search_api::catalog::PostgresServingCatalog;
use search_api::config::Config;
use search_api::grpc::{RouteTimeoutLayer, SearchGrpc};
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
/// process sees a consistent value.  Both knobs are fixed constants (no longer env-configurable),
/// so no `Config` is needed to compute them.
///
/// This must run while the process is still single-threaded, before the tokio runtime spawns
/// any worker thread.  `set_var` is unsound once other threads exist, because a worker racing
/// on `getenv` against this `set_var` is a data race under the C11/POSIX memory model.  `main`
/// is therefore a synchronous entry point that calls this before building the runtime.
///
/// A matching deployment value is accepted. A conflicting pre-set value fails startup before the
/// runtime is built, so the service never silently replaces deployment state.
fn apply_fixed_env(name: &str, expected: &str) -> std::io::Result<()> {
    match std::env::var_os(name) {
        Some(actual) if actual == OsStr::new(expected) => Ok(()),
        Some(_) => Err(std::io::Error::other(format!(
            "{name} conflicts with the search service's fixed production value"
        ))),
        None => {
            unsafe { std::env::set_var(name, expected) };
            Ok(())
        }
    }
}

/// Applies every fixed Lance process-global environment value without overwriting a conflict.
fn apply_lance_io_env() -> std::io::Result<()> {
    apply_fixed_env(
        "LANCE_IO_THREADS",
        &search_api::config::DEFAULT_IO_CONCURRENCY.to_string(),
    )?;
    apply_fixed_env(
        "OBJECT_STORE_CLIENT_RETRY_TIMEOUT",
        &search_api::config::DEFAULT_OBJECT_STORE_TIMEOUT_SECS.to_string(),
    )
}

/// Reads configuration from the environment, initializes Datadog telemetry (OTLP traces, JSON
/// logs, DogStatsD metrics), wires provider -> backend -> transport, spawns the disk-cache
/// janitor, and serves the search gRPC API together with the standard gRPC health service.
///
/// IO tuning: two process-global Lance knobs are stamped into the environment before any
/// dataset opens, so that Lance reads them consistently across every thread.
///
/// - `LANCE_IO_THREADS` — read by `ObjectStore::io_parallelism()` on every scan; controls
///   the number of parallel in-flight object-store requests.  Fixed at
///   [`search_api::config::DEFAULT_IO_CONCURRENCY`] (256).
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
    apply_lance_io_env()?;
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
    let catalog = Arc::new(PostgresServingCatalog::connect(&config.database_url).await?);
    let provider = CachingDatasetProvider::with_catalog_and_telemetry(&config, catalog, metrics.clone()).await;
    if let Some(janitor) = provider.janitor(&config) {
        janitor.spawn(std::time::Duration::from_secs(
            search_api::config::DEFAULT_DISK_CACHE_SWEEP_SECS,
        ));
    }
    let backend = Arc::new(LanceSearchBackend::new(provider).with_metrics(metrics.clone()));

    let recall = RecallCapture::new(search_api::config::DEFAULT_RECALL_SAMPLE_RATE, metrics.clone());
    let service = SearchGrpc::with_metrics(backend, metrics.clone()).with_recall(recall);
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<SearchServiceServer<SearchGrpc<Backend>>>()
        .await;
    tracing::info!(address = %addr, "search-api listening");
    let server_builder = Server::builder()
        .concurrency_limit_per_connection(search_api::config::DEFAULT_CONCURRENCY_LIMIT_PER_CONNECTION)
        .max_concurrent_streams(search_api::config::DEFAULT_MAX_CONCURRENT_STREAMS);
    server_builder
        .layer(OtelGrpcLayer::default().filter(reject_healthcheck))
        .layer(RouteTimeoutLayer::from_defaults(metrics.clone()))
        .add_service(health_service)
        .add_service(SearchServiceServer::new(service))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;
    tracing::info!("in-flight requests drained, flushing telemetry and exiting");
    drop(telemetry_guard);
    Ok(())
}

/// Resolves when the process receives SIGTERM or ctrl-c (SIGINT), starting the graceful drain.
///
/// Kubernetes (and most process supervisors) deliver SIGTERM on deploy or scale-down. Wiring the
/// signal into `serve_with_shutdown` lets tonic stop accepting new requests while in-flight
/// requests complete, and the explicit `drop(telemetry_guard)` afterwards flushes the tracer
/// provider so drain-window spans are exported instead of lost. A SIGTERM handler that cannot be
/// installed degrades to ctrl-c handling alone with a warning, never a startup failure.
async fn shutdown_signal() {
    tokio::select! {
        _ = terminate_signal() => {},
        _ = interrupt_signal() => {},
    }
    tracing::info!("shutdown signal received, draining in-flight requests");
}

/// Resolves when ctrl-c (SIGINT) is delivered.
///
/// A ctrl-c handler that cannot be installed degrades to a never-resolving future with a warning,
/// symmetrically with [`terminate_signal`], so a failed install never fires the shutdown select at
/// boot — never a startup failure. `ctrl_c().await` returning `Ok(())` means the signal arrived.
async fn interrupt_signal() {
    degrade_on_install_error(
        tokio::signal::ctrl_c().await,
        "failed to install the ctrl-c handler, relying on SIGTERM only",
    )
    .await;
}

/// Resolves immediately on `Ok`, or logs `warning` and never resolves on `Err`.
///
/// Shared degrade path for a signal source whose readiness IS its install result (ctrl-c): a
/// successful install that has already fired resolves the shutdown select, and a failed install
/// warns once and awaits [`std::future::pending`] so it can never fire.
async fn degrade_on_install_error<T>(result: std::io::Result<T>, warning: &str) {
    if let Err(err) = result {
        tracing::warn!(error = %err, "{}", warning);
        std::future::pending::<()>().await;
    }
}

/// Resolves when SIGTERM is delivered (unix targets).
#[cfg(unix)]
async fn terminate_signal() {
    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(mut stream) => {
            stream.recv().await;
        }
        Err(err) => {
            tracing::warn!(error = %err, "failed to install the SIGTERM handler, relying on ctrl-c only");
            std::future::pending::<()>().await;
        }
    }
}

/// Never resolves on targets without unix signals, leaving ctrl-c as the only trigger.
#[cfg(not(unix))]
async fn terminate_signal() {
    std::future::pending::<()>().await;
}

#[cfg(test)]
mod tests {
    use super::{apply_fixed_env, degrade_on_install_error};
    use std::sync::Mutex;
    use std::time::Duration;

    /// Serializes tests that mutate the process environment.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Runs one assertion with an environment value and restores its prior state.
    fn with_env(name: &str, value: Option<&str>, body: impl FnOnce()) {
        let guard = ENV_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::env::var_os(name);
        match value {
            Some(value) => unsafe { std::env::set_var(name, value) },
            None => unsafe { std::env::remove_var(name) },
        }
        body();
        match previous {
            Some(value) => unsafe { std::env::set_var(name, value) },
            None => unsafe { std::env::remove_var(name) },
        }
        drop(guard);
    }

    #[test]
    fn fixed_environment_value_is_set_when_absent() {
        with_env("SEARCH_API_TEST_FIXED_ENV", None, || {
            apply_fixed_env("SEARCH_API_TEST_FIXED_ENV", "expected").unwrap();
            assert_eq!(std::env::var("SEARCH_API_TEST_FIXED_ENV").unwrap(), "expected");
        });
    }

    #[test]
    fn matching_fixed_environment_value_is_accepted() {
        with_env("SEARCH_API_TEST_FIXED_ENV", Some("expected"), || {
            apply_fixed_env("SEARCH_API_TEST_FIXED_ENV", "expected").unwrap();
            assert_eq!(std::env::var("SEARCH_API_TEST_FIXED_ENV").unwrap(), "expected");
        });
    }

    #[test]
    fn conflicting_fixed_environment_value_fails_without_overwrite() {
        with_env("SEARCH_API_TEST_FIXED_ENV", Some("conflict"), || {
            let error = apply_fixed_env("SEARCH_API_TEST_FIXED_ENV", "expected").unwrap_err();
            assert!(error.to_string().contains("SEARCH_API_TEST_FIXED_ENV"));
            assert_eq!(std::env::var("SEARCH_API_TEST_FIXED_ENV").unwrap(), "conflict");
        });
    }

    #[tokio::test]
    async fn install_success_resolves_immediately() {
        let outcome =
            tokio::time::timeout(Duration::from_millis(100), degrade_on_install_error(Ok(()), "unused")).await;
        assert!(outcome.is_ok(), "a successful install must resolve the shutdown arm");
    }

    #[tokio::test]
    async fn install_failure_never_resolves() {
        let err = std::io::Error::other("handler install failed");
        let outcome = tokio::time::timeout(
            Duration::from_millis(100),
            degrade_on_install_error::<()>(Err(err), "degraded"),
        )
        .await;
        assert!(
            outcome.is_err(),
            "a failed install must never fire the shutdown select, only warn and pend"
        );
    }
}
