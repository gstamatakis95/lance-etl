//! Tracing setup: OTLP span export to the Datadog Agent and JSON stdout logs with trace
//! correlation.

use std::fmt;
use std::sync::Arc;

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
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

use super::metrics::Metrics;

/// Service name reported when neither `OTEL_SERVICE_NAME` nor `DD_SERVICE` is set.
pub const DEFAULT_SERVICE_NAME: &str = "search-api";

/// Tracing target of Lance's AIMD object-store rate limiter.
///
/// Verified against the Lance checkout at
/// `rust/lance-io/src/object_store/throttle.rs:402-471` (constant
/// `lance_core::utils::tracing::TRACE_OBJECT_STORE_THROTTLE`). Kept as a local literal so the
/// telemetry layer stays free of Lance type dependencies.
const THROTTLE_TARGET: &str = "lance::object_store::throttle";

/// Tracing target of Lance's index-open and partition-load IO events
/// (`lance_core::utils::tracing::TRACE_IO_EVENTS`). The `type` field carries the IO kind.
const IO_EVENTS_TARGET: &str = "lance::io_events";

/// Tracing target of Lance's dataset-lifecycle events
/// (`lance_core::utils::tracing::TRACE_DATASET_EVENTS`). The `event` field carries the transition.
/// `loading` fires on dataset open.
const DATASET_EVENTS_TARGET: &str = "lance::dataset_events";

/// Tracing target of Lance's file-audit events
/// (`lance_core::utils::tracing::TRACE_FILE_AUDIT`). The `mode` and `type` fields carry the
/// create/delete action and the file kind.
const FILE_AUDIT_TARGET: &str = "lance::file_audit";

/// `EnvFilter` directives forcing the Lance event targets through at `info` severity regardless of
/// `RUST_LOG`, so the [`LanceEventMetricsLayer`] and OTLP layer always see them.
///
/// The throttle target's AIMD rate-reduction event is emitted at `warn` (which `info` admits). The
/// io/dataset/file targets emit at `info`. Forcing each target keeps the bridge live even when
/// `RUST_LOG` narrows the Lance crate's own logs below these levels.
const LANCE_EVENT_TARGET_DIRECTIVES: [&str; 4] = [
    "lance::object_store::throttle=info",
    "lance::io_events=info",
    "lance::dataset_events=info",
    "lance::file_audit=info",
];

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
///
/// `metrics` backs the [`LanceEventMetricsLayer`] so Lance throttle, IO, dataset-lifecycle, and
/// file-audit events become DogStatsD metrics. Pass [`Metrics::disabled`](super::Metrics::disabled)
/// to suppress them.
pub fn init_tracing(telemetry_disabled: bool, metrics: Arc<Metrics>) -> TelemetryGuard {
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
                .with(LanceEventMetricsLayer::new(metrics))
                .try_init();
        }
        None => {
            let _ = tracing_subscriber::registry()
                .with(env_filter())
                .with(json_fmt_layer())
                .with(LanceEventMetricsLayer::new(metrics))
                .try_init();
        }
    }
    TelemetryGuard { tracer_provider }
}

/// `RUST_LOG`-driven filter, defaulting to `info` when the variable is unset or invalid.
///
/// The Lance event targets are always admitted at `info` (see [`LANCE_EVENT_TARGET_DIRECTIVES`]) so
/// the [`LanceEventMetricsLayer`] and the OTLP span-event layer still see throttle, IO, dataset, and
/// file-audit events when `RUST_LOG` narrows the crate's logs.
fn env_filter() -> tracing_subscriber::EnvFilter {
    let mut filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    for directive in LANCE_EVENT_TARGET_DIRECTIVES {
        if let Ok(parsed) = directive.parse() {
            filter = filter.add_directive(parsed);
        }
    }
    filter
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

/// Tracing layer that promotes Lance trace events into DogStatsD metrics.
///
/// It matches events on four Lance targets and forwards each through the [`Metrics`] facade:
/// - [`THROTTLE_TARGET`]: a throttle-error count and the AIMD limiter's freshly reduced fill rate.
/// - [`IO_EVENTS_TARGET`]: an IO-event counter tagged by the `type` field (index open or part load).
/// - [`DATASET_EVENTS_TARGET`]: a dataset-lifecycle counter tagged by the `event` field
///   (`loading` on open, plus writing/committed/dropping_column/deleting/compacting/cleaning).
/// - [`FILE_AUDIT_TARGET`]: a file-audit counter tagged by the `mode` and `type` fields.
///
/// The layer only reads event fields and emits metrics, so it never panics, never blocks the traced
/// task, and adds nothing for events on any other target.
pub struct LanceEventMetricsLayer {
    metrics: Arc<Metrics>,
}

impl LanceEventMetricsLayer {
    /// Builds a Lance-event metrics layer emitting through the given facade.
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }
}

impl<S> Layer<S> for LanceEventMetricsLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let target = event.metadata().target();
        if target != THROTTLE_TARGET
            && target != IO_EVENTS_TARGET
            && target != DATASET_EVENTS_TARGET
            && target != FILE_AUDIT_TARGET
        {
            return;
        }
        let mut visitor = LanceEventVisitor::default();
        event.record(&mut visitor);
        if target == THROTTLE_TARGET {
            self.metrics.throttle_event(visitor.errored, visitor.new_rate);
        } else if target == IO_EVENTS_TARGET {
            if let Some(io_type) = visitor.type_field.as_deref().and_then(super::LanceIoType::from_lance) {
                self.metrics.lance_io_event(io_type);
            }
        } else if target == DATASET_EVENTS_TARGET {
            if let Some(dataset_event) = visitor.event.as_deref().and_then(super::DatasetEvent::from_lance) {
                self.metrics.lance_dataset_event(dataset_event);
            }
        } else if target == FILE_AUDIT_TARGET
            && let (Some(mode), Some(file_type)) = (
                visitor.mode.as_deref().and_then(super::FileAuditMode::from_lance),
                visitor.type_field.as_deref().and_then(super::FileAuditType::from_lance),
            )
        {
            self.metrics.lance_file_audit(mode, file_type);
        }
    }
}

/// Extracts the Lance event fields the metrics layer reads, by name.
///
/// `errored`/`new_rate` back the throttle metric (Lance emits `new_rate` as a formatted string and
/// `error` via a `Display` wrapper, so both string and debug records are handled). `event` carries
/// the dataset-lifecycle transition, `type_field` the io/file-audit kind (Lance's `r#type` field),
/// and `mode` the file-audit action.
#[derive(Default)]
struct LanceEventVisitor {
    errored: bool,
    new_rate: Option<f64>,
    event: Option<String>,
    type_field: Option<String>,
    mode: Option<String>,
}

impl Visit for LanceEventVisitor {
    fn record_f64(&mut self, field: &Field, value: f64) {
        if field.name() == "new_rate" {
            self.new_rate = Some(value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "new_rate" => self.new_rate = value.trim().parse().ok(),
            "error" => self.errored = true,
            "event" => self.event = Some(value.to_string()),
            "type" => self.type_field = Some(value.to_string()),
            "mode" => self.mode = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        match field.name() {
            "error" => self.errored = true,
            "new_rate" if self.new_rate.is_none() => {
                self.new_rate = format!("{value:?}").trim_matches('"').trim().parse().ok();
            }
            "event" if self.event.is_none() => {
                self.event = Some(format!("{value:?}").trim_matches('"').to_string());
            }
            "type" if self.type_field.is_none() => {
                self.type_field = Some(format!("{value:?}").trim_matches('"').to_string());
            }
            "mode" if self.mode.is_none() => {
                self.mode = Some(format!("{value:?}").trim_matches('"').to_string());
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence::SpyMetricSink;
    use tracing_subscriber::layer::SubscriberExt;

    #[test]
    fn init_tracing_disabled_is_idempotent_and_panic_free() {
        let first = init_tracing(true, Arc::new(Metrics::disabled()));
        let second = init_tracing(true, Arc::new(Metrics::disabled()));
        tracing::info!(org_id = "org-test", "telemetry smoke event");
        drop(second);
        drop(first);
    }

    #[test]
    fn lance_event_layer_emits_throttle_error_counter_and_rate_gauge_on_matching_events() {
        let (receiver, sink) = SpyMetricSink::new();
        let metrics = Arc::new(Metrics::from_sink(sink));
        let subscriber = tracing_subscriber::registry().with(LanceEventMetricsLayer::new(metrics));
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(
                target: THROTTLE_TARGET,
                previous_rate = "20.0",
                new_rate = "12.5",
                error = "503 Slow Down",
                "AIMD throttle"
            );
            tracing::info!(target: "search_api::other", new_rate = "99.0", "unrelated event");
        });
        let mut lines = Vec::new();
        while let Ok(packet) = receiver.try_recv() {
            lines.push(String::from_utf8(packet).unwrap());
        }
        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("search_api.throttle.errors:1|c")),
            "missing throttle error counter: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("search_api.throttle.new_rate:12.5|g")),
            "missing throttle rate gauge: {lines:?}"
        );
        assert_eq!(
            lines.iter().filter(|line| line.contains("throttle")).count(),
            2,
            "only the matching throttle event must produce metrics: {lines:?}"
        );
    }

    #[test]
    fn lance_event_layer_maps_io_dataset_and_file_audit_events_to_metrics() {
        let (receiver, sink) = SpyMetricSink::new();
        let metrics = Arc::new(Metrics::from_sink(sink));
        let subscriber = tracing_subscriber::registry().with(LanceEventMetricsLayer::new(metrics));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: IO_EVENTS_TARGET, index_uuid = "uuid-1", r#type = "open_vector_index", index_type = "IVF_PQ");
            tracing::info!(target: DATASET_EVENTS_TARGET, event = "loading", uri = "memory://ds", status = "success");
            tracing::info!(target: FILE_AUDIT_TARGET, mode = "create", r#type = "manifest", path = "_versions/3.manifest");
            tracing::info!(target: IO_EVENTS_TARGET, r#type = "not_a_real_io_type");
            tracing::info!(target: "search_api::other", event = "loading", "unrelated event");
        });
        let mut lines = Vec::new();
        while let Ok(packet) = receiver.try_recv() {
            lines.push(String::from_utf8(packet).unwrap());
        }
        let expect = [
            ("search_api.lance.io_events:1|c", vec!["io_type:open_vector_index"]),
            ("search_api.lance.dataset_events:1|c", vec!["event:loading"]),
            ("search_api.lance.file_audit:1|c", vec!["mode:create", "type:manifest"]),
        ];
        for (head, tags) in expect {
            assert!(
                lines
                    .iter()
                    .any(|line| line.starts_with(head) && tags.iter().all(|tag| line.contains(tag))),
                "missing {head} with {tags:?} in {lines:?}"
            );
        }
        assert_eq!(
            lines.iter().filter(|line| line.contains("lance.io_events")).count(),
            1,
            "an unrecognized io type and a non-Lance target must not emit io metrics: {lines:?}"
        );
        assert_eq!(
            lines
                .iter()
                .filter(|line| line.contains("lance.dataset_events"))
                .count(),
            1,
            "only the Lance dataset target must emit dataset metrics: {lines:?}"
        );
    }
}
