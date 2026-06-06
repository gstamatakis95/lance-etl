//! Tracing setup: OTLP span export to the Datadog Agent and JSON stdout logs with trace
//! correlation.

use std::fmt;

use opentelemetry::trace::{TraceContextExt, TracerProvider};
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::fmt::format::{JsonFields, Writer};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormattedFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

/// Service name reported when neither `OTEL_SERVICE_NAME` nor `DD_SERVICE` is set.
pub const DEFAULT_SERVICE_NAME: &str = "search-api";

/// Resolves the service name from `OTEL_SERVICE_NAME`, then `DD_SERVICE`, then the default.
fn service_name() -> String {
    std::env::var("OTEL_SERVICE_NAME")
        .or_else(|_| std::env::var("DD_SERVICE"))
        .unwrap_or_else(|_| DEFAULT_SERVICE_NAME.to_string())
}

/// Owns the tracer provider so spans are flushed to the agent on shutdown.
///
/// Dropping the guard shuts the provider down (best effort). Keep it alive for the process
/// lifetime in `main`.
#[derive(Debug, Default)]
pub struct TelemetryGuard {
    tracer_provider: Option<SdkTracerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take() {
            let _ = provider.shutdown();
        }
    }
}

/// Installs the global tracing subscriber: `RUST_LOG`-driven filtering, JSON logs on stdout with
/// Datadog trace/span correlation fields, and (unless disabled) an OTLP gRPC span exporter
/// pointed at the Datadog Agent.
///
/// Endpoint resolution honors `OTEL_EXPORTER_OTLP_ENDPOINT` first and falls back to
/// `http://{DD_AGENT_HOST}:4317`. Sampling honors `OTEL_TRACES_SAMPLER` /
/// `OTEL_TRACES_SAMPLER_ARG`. The function never panics and never fails: exporter setup errors
/// degrade to log-only mode, and calling it when a subscriber is already installed (tests) is a
/// no-op.
pub fn init_tracing(telemetry_disabled: bool) -> TelemetryGuard {
    let tracer_provider = if telemetry_disabled {
        None
    } else {
        build_tracer_provider()
    };
    match &tracer_provider {
        Some(provider) => {
            global::set_text_map_propagator(TraceContextPropagator::new());
            let tracer = provider.tracer(DEFAULT_SERVICE_NAME);
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
            let _ = tracing_subscriber::registry()
                .with(env_filter())
                .with(otel_layer)
                .with(json_fmt_layer())
                .try_init();
        }
        None => {
            let _ = tracing_subscriber::registry()
                .with(env_filter())
                .with(json_fmt_layer())
                .try_init();
        }
    }
    TelemetryGuard { tracer_provider }
}

/// `RUST_LOG`-driven filter, defaulting to `info` when the variable is unset or invalid.
fn env_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
}

/// JSON stdout log layer with Datadog trace correlation, generic over the subscriber stack.
fn json_fmt_layer<S>() -> tracing_subscriber::fmt::Layer<S, JsonFields, DatadogJsonFormat>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    tracing_subscriber::fmt::layer()
        .fmt_fields(JsonFields::new())
        .event_format(DatadogJsonFormat)
}

/// Builds the OTLP tonic tracer provider. Any failure returns `None` (log-only mode).
///
/// Must run inside a tokio runtime because the lazy tonic channel spawns its background task at
/// creation time. Outside a runtime this degrades instead of panicking.
fn build_tracer_provider() -> Option<SdkTracerProvider> {
    if tokio::runtime::Handle::try_current().is_err() {
        eprintln!("search-api: telemetry: no tokio runtime available, traces disabled");
        return None;
    }
    let mut exporter_builder = opentelemetry_otlp::SpanExporter::builder().with_tonic();
    let endpoint_from_env = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok()
        || std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_ok();
    if !endpoint_from_env && let Ok(agent_host) = std::env::var("DD_AGENT_HOST") {
        exporter_builder = exporter_builder.with_endpoint(format!("http://{agent_host}:4317"));
    }
    let exporter = match exporter_builder.build() {
        Ok(exporter) => exporter,
        Err(error) => {
            eprintln!("search-api: telemetry: failed to build OTLP exporter, traces disabled: {error}");
            return None;
        }
    };
    let mut attributes = vec![KeyValue::new("service.name", service_name())];
    if let Ok(env_name) = std::env::var("DD_ENV") {
        attributes.push(KeyValue::new("deployment.environment.name", env_name));
    }
    if let Ok(version) = std::env::var("DD_VERSION") {
        attributes.push(KeyValue::new("service.version", version));
    }
    let resource = Resource::builder().with_attributes(attributes).build();
    Some(
        SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build(),
    )
}

/// Flat JSON log formatter with Datadog trace correlation.
///
/// Each line carries `timestamp`, `level`, `target`, `message`, the event's fields, the fields of
/// every span in scope (inner spans override outer, the event overrides both), and — when a
/// sampled OpenTelemetry span is active — `trace_id` (32 hex chars) and `span_id` (16 hex chars)
/// in the OTel convention Datadog ingests directly.
struct DatadogJsonFormat;

impl<S> FormatEvent<S, JsonFields> for DatadogJsonFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, JsonFields>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut fields = Map::new();
        fields.insert(
            "timestamp".to_string(),
            Value::String(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true)),
        );
        let metadata = event.metadata();
        fields.insert("level".to_string(), Value::String(metadata.level().to_string()));
        fields.insert("target".to_string(), Value::String(metadata.target().to_string()));
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                let extensions = span.extensions();
                if let Some(formatted) = extensions.get::<FormattedFields<JsonFields>>()
                    && let Ok(Value::Object(span_fields)) = serde_json::from_str::<Value>(formatted.fields.as_str())
                {
                    for (key, value) in span_fields {
                        fields.insert(key, value);
                    }
                }
            }
        }
        let mut visitor = JsonEventVisitor { fields: &mut fields };
        event.record(&mut visitor);
        let otel_context = tracing::Span::current().context();
        let span_context = otel_context.span().span_context().clone();
        if span_context.is_valid() {
            fields.insert(
                "trace_id".to_string(),
                Value::String(format!("{:032x}", span_context.trace_id())),
            );
            fields.insert(
                "span_id".to_string(),
                Value::String(format!("{:016x}", span_context.span_id())),
            );
        }
        writeln!(writer, "{}", Value::Object(fields))
    }
}

/// Records the fields of one event into a JSON map. `message` lands under the `message` key.
struct JsonEventVisitor<'a> {
    fields: &'a mut Map<String, Value>,
}

impl Visit for JsonEventVisitor<'_> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.fields.insert(field.name().to_string(), Value::from(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields.insert(field.name().to_string(), Value::from(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields.insert(field.name().to_string(), Value::from(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields.insert(field.name().to_string(), Value::from(value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields.insert(field.name().to_string(), Value::from(value));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.fields
            .insert(field.name().to_string(), Value::from(value.to_string()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.fields
            .insert(field.name().to_string(), Value::from(format!("{value:?}")));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_tracing_disabled_is_idempotent_and_panic_free() {
        let first = init_tracing(true);
        let second = init_tracing(true);
        tracing::info!(org_id = "org-test", "telemetry smoke event");
        drop(second);
        drop(first);
    }
}
