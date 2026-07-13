//! Per-route request timeouts for the gRPC server.
//!
//! A single global tonic timeout cannot serve this API: search RPCs need a tight sub-second
//! budget while `SearchService/Prewarm` legitimately loads every requested IVF partition and
//! BTree page over the object store. [`RouteTimeoutLayer`] therefore matches the gRPC method path
//! of each request and applies the default search budget to everything except the routes in
//! [`LONG_TIMEOUT_ROUTES`], which get the long budget instead. A request that exceeds its budget
//! is answered with a `DEADLINE_EXCEEDED` gRPC status, never a hung connection.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tonic::Status;
use tower::{Layer, Service};

use crate::config::{DEFAULT_LONG_REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT_MS};

/// gRPC method paths that get the long timeout budget instead of the search default.
///
/// `Prewarm` walks every requested index over the object store, so it would be killed mid-flight
/// by the sub-second search budget.
pub const LONG_TIMEOUT_ROUTES: [&str; 1] = ["/lance_etl.v1.SearchService/Prewarm"];

/// Tower layer applying a per-route server-side timeout to every request.
///
/// Built from the fixed config constants via [`RouteTimeoutLayer::from_defaults`] in production.
/// Explicit budgets are constructible for tests.
#[derive(Debug, Clone)]
pub struct RouteTimeoutLayer {
    default_budget: Duration,
    long_budget: Duration,
}

impl RouteTimeoutLayer {
    /// Builds a layer with explicit budgets: `default_budget` for every route except the
    /// [`LONG_TIMEOUT_ROUTES`], which get `long_budget`.
    pub fn new(default_budget: Duration, long_budget: Duration) -> Self {
        Self {
            default_budget,
            long_budget,
        }
    }

    /// Builds the production layer from [`DEFAULT_REQUEST_TIMEOUT_MS`] and
    /// [`DEFAULT_LONG_REQUEST_TIMEOUT_MS`].
    pub fn from_defaults() -> Self {
        Self::new(
            Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
            Duration::from_millis(DEFAULT_LONG_REQUEST_TIMEOUT_MS),
        )
    }

    /// Returns the timeout budget applied to one gRPC method path.
    pub fn budget_for(&self, path: &str) -> Duration {
        if LONG_TIMEOUT_ROUTES.contains(&path) {
            self.long_budget
        } else {
            self.default_budget
        }
    }
}

impl<S> Layer<S> for RouteTimeoutLayer {
    type Service = RouteTimeout<S>;

    /// Wraps `inner` with the per-route timeout.
    fn layer(&self, inner: S) -> Self::Service {
        RouteTimeout {
            inner,
            budgets: self.clone(),
        }
    }
}

/// Service wrapper enforcing the per-route budget of a [`RouteTimeoutLayer`].
///
/// The budget covers the inner service future, i.e. everything up to the response being produced
/// (the entire handler for unary RPCs, the whole request stream for client-streaming RPCs). On
/// expiry the request is answered with a `DEADLINE_EXCEEDED` trailers-only gRPC response.
#[derive(Debug, Clone)]
pub struct RouteTimeout<S> {
    inner: S,
    budgets: RouteTimeoutLayer,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for RouteTimeout<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    /// Delegates readiness to the inner service.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    /// Runs the inner service under the budget selected by the request's gRPC method path.
    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        let budget = self.budgets.budget_for(request.uri().path());
        let future = self.inner.call(request);
        Box::pin(async move {
            match tokio::time::timeout(budget, future).await {
                Ok(result) => result,
                Err(_) => Ok(Status::deadline_exceeded(format!(
                    "request exceeded the server-side timeout of {} ms",
                    budget.as_millis()
                ))
                .into_http()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use super::*;

    /// Inner service producing an empty OK response after a fixed delay.
    #[derive(Clone)]
    struct SleepyService {
        delay: Duration,
    }

    impl Service<http::Request<()>> for SleepyService {
        type Response = http::Response<tonic::body::Body>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: http::Request<()>) -> Self::Future {
            let delay = self.delay;
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                Ok(http::Response::new(tonic::body::Body::default()))
            })
        }
    }

    /// Builds a request against the given gRPC method path.
    fn request_for(path: &str) -> http::Request<()> {
        http::Request::builder()
            .uri(format!("http://127.0.0.1{path}"))
            .body(())
            .unwrap()
    }

    /// The `grpc-status` header of a trailers-only response, if any.
    fn grpc_status_header(response: &http::Response<tonic::body::Body>) -> Option<&str> {
        response.headers().get("grpc-status").and_then(|raw| raw.to_str().ok())
    }

    #[test]
    fn long_budget_applies_only_to_the_listed_routes() {
        let layer = RouteTimeoutLayer::from_defaults();
        let default = Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS);
        let long = Duration::from_millis(DEFAULT_LONG_REQUEST_TIMEOUT_MS);
        assert_eq!(layer.budget_for("/lance_etl.v1.SearchService/Prewarm"), long);
        assert_eq!(layer.budget_for("/lance_etl.v1.SearchService/VectorSearch"), default);
        assert_eq!(layer.budget_for("/lance_etl.v1.SearchService/TextSearch"), default);
        assert_eq!(layer.budget_for("/lance_etl.v1.SearchService/HybridSearch"), default);
        assert_eq!(layer.budget_for("/lance_etl.v1.SearchService/Clusters"), default);
        assert_eq!(layer.budget_for("/grpc.health.v1.Health/Check"), default);
    }

    #[tokio::test(start_paused = true)]
    async fn default_route_over_budget_gets_deadline_exceeded() {
        let layer = RouteTimeoutLayer::new(Duration::from_millis(800), Duration::from_secs(600));
        let mut service = layer.layer(SleepyService {
            delay: Duration::from_secs(5),
        });
        let response = service
            .call(request_for("/lance_etl.v1.SearchService/VectorSearch"))
            .await
            .unwrap();
        assert_eq!(
            grpc_status_header(&response),
            Some((tonic::Code::DeadlineExceeded as i32).to_string().as_str()),
            "a slow search must be answered with DEADLINE_EXCEEDED"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn long_route_survives_past_the_default_budget() {
        let layer = RouteTimeoutLayer::new(Duration::from_millis(800), Duration::from_secs(600));
        let mut service = layer.layer(SleepyService {
            delay: Duration::from_secs(5),
        });
        let response = service
            .call(request_for("/lance_etl.v1.SearchService/Prewarm"))
            .await
            .unwrap();
        assert_eq!(
            grpc_status_header(&response),
            None,
            "a slow prewarm must complete normally under the long budget"
        );
    }
}
