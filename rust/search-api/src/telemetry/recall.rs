//! Sampled-query recall capture: records a deterministic sample of vector searches as span
//! attributes so an offline job can replay them and score recall.
//!
//! # What is sampled
//!
//! Only `VectorSearch` requests without a `date_range` are eligible (fan-out replay is out of
//! v1 scope). Requests carrying a typed filter are eligible and the capture records the filter,
//! so the offline scorer can replay it. Text and hybrid requests are never sampled.
//!
//! # Sampler design
//!
//! Sampling is deterministic and allocation-free: a per-process atomic counter `n` is bumped per
//! eligible request and request `n` is sampled exactly when `floor((n + 1) * rate)` exceeds
//! `floor(n * rate)`. Over `N` eligible requests exactly `floor(N * rate)` are sampled, with
//! rate 0 sampling none and rate 1 sampling all. No RNG state is involved.
//!
//! # Capture-record schema
//!
//! A sampled request attaches one flat group of `recall.*` attributes to the current tracing
//! span — the per-RPC server span opened by the OpenTelemetry tower layer, which the Datadog
//! Spans API can retrieve. Every JSON-valued attribute is a single compact string:
//!
//! - `recall.sample` (bool) — always `true` on sampled spans, the retrieval filter key.
//! - `recall.sample_id` (string) — UUIDv4 identifying this capture.
//! - `recall.captured_at_unix_ms` (int) — capture wall-clock time in Unix milliseconds.
//! - `recall.org_id` / `recall.tenant_id` / `recall.namespace` (string) — the dataset target.
//! - `recall.dataset_version` (int) — the committed Lance dataset version that served the query.
//! - `recall.k` (int) — requested neighbor count.
//! - `recall.nprobes_min` / `recall.nprobes_max` (int) — probed-partition bounds as recorded on
//!   the request (`nprobes` sets both). Absent when the request left them to the index defaults.
//! - `recall.refine_factor` (int) — re-rank factor. Absent when unset.
//! - `recall.distance_type` (string) — `l2`, `cosine`, `dot`, or `hamming`. Absent when the
//!   request kept the index metric.
//! - `recall.query_vector` (string) — the full query vector as a compact JSON array of numbers,
//!   e.g. `[1.0,0.0,0.5]`. Sized for vectors up to ~1536 dims (one attribute string).
//! - `recall.filter` (string) — the typed filter AST as the stable JSON documented in
//!   [`crate::domain::filter`]. Absent when the request had no filter.
//! - `recall.result_ids` (string) — JSON array of the served id-column values in rank order
//!   (`null` for rows whose projection omitted the id column).
//! - `recall.result_distances` (string) — JSON array of the served distances in rank order.
//!
//! # Datadog retention
//!
//! These spans are only useful if they outlive live search: configure a Datadog retention filter
//! on `recall.sample:true` so sampled spans are indexed and retained for the offline job.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::domain::{DatasetTarget, DistanceKind, Hit, VectorQuery};
use crate::telemetry::Metrics;

/// Observer invoked with every finished capture record. Used by tests to assert captures.
pub type RecallHook = Arc<dyn Fn(&RecallRecord) + Send + Sync>;

/// Deterministic counter-based sampler: over `N` calls exactly `floor(N * rate)` return true.
#[derive(Debug)]
pub struct RecallSampler {
    rate: f64,
    counter: AtomicU64,
}

impl RecallSampler {
    /// Creates a sampler for the given rate, clamped into `[0, 1]`.
    pub fn new(rate: f64) -> Self {
        Self {
            rate: rate.clamp(0.0, 1.0),
            counter: AtomicU64::new(0),
        }
    }

    /// Decides whether the next eligible request is sampled, advancing the counter.
    pub fn should_sample(&self) -> bool {
        if self.rate <= 0.0 {
            return false;
        }
        if self.rate >= 1.0 {
            return true;
        }
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        ((n + 1) as f64 * self.rate).floor() > (n as f64 * self.rate).floor()
    }
}

/// One finished capture: every field that lands on the span as a `recall.*` attribute.
#[derive(Debug, Clone)]
pub struct RecallRecord {
    /// UUIDv4 identifying this capture (`recall.sample_id`).
    pub sample_id: String,
    /// Capture time in Unix milliseconds (`recall.captured_at_unix_ms`).
    pub captured_at_unix_ms: i64,
    /// Target organization (`recall.org_id`).
    pub org_id: String,
    /// Target tenant (`recall.tenant_id`).
    pub tenant_id: String,
    /// Target namespace (`recall.namespace`).
    pub namespace: String,
    /// Committed dataset version that served the query (`recall.dataset_version`).
    pub dataset_version: Option<u64>,
    /// Requested neighbor count (`recall.k`).
    pub k: usize,
    /// Minimum probed partitions as recorded on the request (`recall.nprobes_min`).
    pub nprobes_min: Option<usize>,
    /// Maximum probed partitions as recorded on the request (`recall.nprobes_max`).
    pub nprobes_max: Option<usize>,
    /// Re-rank factor (`recall.refine_factor`).
    pub refine_factor: Option<u32>,
    /// Distance metric override (`recall.distance_type`). `None` keeps the index metric.
    pub distance_type: Option<&'static str>,
    /// The full query vector as a compact JSON number array (`recall.query_vector`).
    pub query_vector_json: String,
    /// The typed filter AST as stable JSON (`recall.filter`). `None` when unfiltered.
    pub filter_json: Option<String>,
    /// Served id-column values in rank order as a JSON array (`recall.result_ids`).
    pub result_ids_json: String,
    /// Served distances in rank order as a JSON array (`recall.result_distances`).
    pub result_distances_json: String,
}

impl RecallRecord {
    /// Attaches every field as a `recall.*` attribute on the current tracing span.
    pub fn attach_to_current_span(&self) {
        let span = tracing::Span::current();
        span.set_attribute("recall.sample", true);
        span.set_attribute("recall.sample_id", self.sample_id.clone());
        span.set_attribute("recall.captured_at_unix_ms", self.captured_at_unix_ms);
        span.set_attribute("recall.org_id", self.org_id.clone());
        span.set_attribute("recall.tenant_id", self.tenant_id.clone());
        span.set_attribute("recall.namespace", self.namespace.clone());
        if let Some(version) = self.dataset_version {
            span.set_attribute("recall.dataset_version", version as i64);
        }
        span.set_attribute("recall.k", self.k as i64);
        if let Some(min) = self.nprobes_min {
            span.set_attribute("recall.nprobes_min", min as i64);
        }
        if let Some(max) = self.nprobes_max {
            span.set_attribute("recall.nprobes_max", max as i64);
        }
        if let Some(refine) = self.refine_factor {
            span.set_attribute("recall.refine_factor", refine as i64);
        }
        if let Some(distance) = self.distance_type {
            span.set_attribute("recall.distance_type", distance);
        }
        span.set_attribute("recall.query_vector", self.query_vector_json.clone());
        if let Some(filter) = &self.filter_json {
            span.set_attribute("recall.filter", filter.clone());
        }
        span.set_attribute("recall.result_ids", self.result_ids_json.clone());
        span.set_attribute("recall.result_distances", self.result_distances_json.clone());
    }
}

/// A sampled request awaiting its results: the query context cloned at decision time.
#[derive(Debug)]
pub struct PendingRecall {
    org_id: String,
    tenant_id: String,
    namespace: String,
    k: usize,
    nprobes_min: Option<usize>,
    nprobes_max: Option<usize>,
    refine_factor: Option<u32>,
    distance_type: Option<&'static str>,
    query_vector_json: String,
    filter_json: Option<String>,
    filtered: bool,
}

/// The capture facade owned by the transport: sampling decision, record assembly, span
/// attachment, and the `recall.samples` metric.
pub struct RecallCapture {
    sampler: RecallSampler,
    id_column: String,
    metrics: Arc<Metrics>,
    hook: Option<RecallHook>,
}

impl std::fmt::Debug for RecallCapture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecallCapture").field("sampler", &self.sampler).finish()
    }
}

impl RecallCapture {
    /// Creates a capture facade with the given sample rate and result id column.
    pub fn new(rate: f64, id_column: impl Into<String>, metrics: Arc<Metrics>) -> Self {
        Self {
            sampler: RecallSampler::new(rate),
            id_column: id_column.into(),
            metrics,
            hook: None,
        }
    }

    /// A capture facade that never samples, for constructors without recall wiring.
    pub fn disabled() -> Self {
        Self::new(0.0, crate::config::DEFAULT_ID_COLUMN, Arc::new(Metrics::disabled()))
    }

    /// Installs an observer invoked with every finished record. Test seam.
    pub fn with_hook(mut self, hook: RecallHook) -> Self {
        self.hook = Some(hook);
        self
    }

    /// Decides whether this vector search is sampled, snapshotting the query when it is.
    ///
    /// Requests whose target carries a date range are never eligible and never advance the
    /// sampler counter.
    pub fn begin(&self, target: &DatasetTarget, query: &VectorQuery) -> Option<PendingRecall> {
        if target.date_range.is_some() || !self.sampler.should_sample() {
            return None;
        }
        let (nprobes_min, nprobes_max) = match query.nprobes {
            Some(nprobes) => (Some(nprobes), Some(nprobes)),
            None => (query.minimum_nprobes, query.maximum_nprobes),
        };
        Some(PendingRecall {
            org_id: target.org_id.clone(),
            tenant_id: target.tenant_id.clone(),
            namespace: target.namespace.clone(),
            k: query.k,
            nprobes_min,
            nprobes_max,
            refine_factor: query.refine_factor,
            distance_type: query.distance.map(distance_tag),
            query_vector_json: serde_json::to_string(&query.vector).unwrap_or_else(|_| "[]".to_string()),
            filter_json: query
                .filter
                .as_ref()
                .and_then(|filter| serde_json::to_string(filter).ok()),
            filtered: query.filter.is_some(),
        })
    }

    /// Finishes one sampled request: assembles the record from the served hits, attaches it to
    /// the current span, emits the `recall.samples` counter, and notifies the test hook.
    pub fn finish(&self, pending: PendingRecall, dataset_version: Option<u64>, hits: &[Hit]) {
        let ids: Vec<Value> = hits
            .iter()
            .map(|hit| hit.row.get(&self.id_column).cloned().unwrap_or(Value::Null))
            .collect();
        let distances: Vec<f64> = hits.iter().map(|hit| hit.score).collect();
        let record = RecallRecord {
            sample_id: uuid::Uuid::new_v4().to_string(),
            captured_at_unix_ms: unix_millis(),
            org_id: pending.org_id,
            tenant_id: pending.tenant_id,
            namespace: pending.namespace,
            dataset_version,
            k: pending.k,
            nprobes_min: pending.nprobes_min,
            nprobes_max: pending.nprobes_max,
            refine_factor: pending.refine_factor,
            distance_type: pending.distance_type,
            query_vector_json: pending.query_vector_json,
            filter_json: pending.filter_json,
            result_ids_json: serde_json::to_string(&ids).unwrap_or_else(|_| "[]".to_string()),
            result_distances_json: serde_json::to_string(&distances).unwrap_or_else(|_| "[]".to_string()),
        };
        record.attach_to_current_span();
        self.metrics.recall_sample(pending.filtered);
        if let Some(hook) = &self.hook {
            hook(&record);
        }
    }
}

/// Tag value for a distance metric.
fn distance_tag(distance: DistanceKind) -> &'static str {
    match distance {
        DistanceKind::L2 => "l2",
        DistanceKind::Cosine => "cosine",
        DistanceKind::Dot => "dot",
        DistanceKind::Hamming => "hamming",
    }
}

/// Current wall-clock time in Unix milliseconds.
fn unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{CompareOp, DateRange, Filter, Literal};
    use serde_json::Map;
    use std::sync::Mutex;

    /// Counts how many of `n` consecutive decisions sample.
    fn samples(rate: f64, n: usize) -> usize {
        let sampler = RecallSampler::new(rate);
        (0..n).filter(|_| sampler.should_sample()).count()
    }

    #[test]
    fn sampler_rate_zero_never_samples() {
        assert_eq!(samples(0.0, 1000), 0);
    }

    #[test]
    fn sampler_rate_one_always_samples() {
        assert_eq!(samples(1.0, 1000), 1000);
    }

    #[test]
    fn sampler_fractional_rate_is_deterministic_and_proportional() {
        let count = samples(0.1, 1000);
        assert!((90..=110).contains(&count), "expected ~100 samples, got {count}");
        assert_eq!(samples(0.1, 1000), count, "the scheme must be deterministic");
        assert_eq!(samples(0.5, 10), 5);
        assert_eq!(samples(0.001, 999), 0);
        assert_eq!(samples(0.001, 1000), 1);
    }

    /// Builds a no-range target and a filtered query for capture fixtures.
    fn fixture() -> (DatasetTarget, VectorQuery) {
        let target = DatasetTarget::new("org1", "tenant1", "ns1");
        let query = VectorQuery {
            vector: vec![1.0, 0.5, 0.0],
            k: 2,
            nprobes: Some(20),
            refine_factor: Some(2),
            distance: Some(DistanceKind::Cosine),
            filter: Some(Filter::Compare {
                column: "id".to_string(),
                op: CompareOp::Gt,
                value: Literal::Int(1),
            }),
            ..Default::default()
        };
        (target, query)
    }

    #[test]
    fn begin_skips_date_ranges_and_finish_builds_the_record() {
        let captured: Arc<Mutex<Vec<RecallRecord>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = captured.clone();
        let capture = RecallCapture::new(1.0, "vector_id", Arc::new(Metrics::disabled()))
            .with_hook(Arc::new(move |record| sink.lock().unwrap().push(record.clone())));
        let (mut target, query) = fixture();
        target.date_range = Some(
            DateRange::new(
                chrono::NaiveDate::from_ymd_opt(2026, 6, 1).unwrap(),
                chrono::NaiveDate::from_ymd_opt(2026, 6, 2).unwrap(),
            )
            .unwrap(),
        );
        assert!(
            capture.begin(&target, &query).is_none(),
            "date-range targets must never be sampled"
        );
        target.date_range = None;
        let pending = capture.begin(&target, &query).expect("rate 1.0 must sample");
        let mut row = Map::new();
        row.insert("vector_id".to_string(), Value::from(7));
        let hits = vec![Hit {
            row_id: 1,
            score: 0.25,
            row,
        }];
        capture.finish(pending, Some(42), &hits);
        let records = captured.lock().unwrap();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.org_id, "org1");
        assert_eq!(record.dataset_version, Some(42));
        assert_eq!(record.k, 2);
        assert_eq!(record.nprobes_min, Some(20));
        assert_eq!(record.nprobes_max, Some(20));
        assert_eq!(record.refine_factor, Some(2));
        assert_eq!(record.distance_type, Some("cosine"));
        assert_eq!(record.query_vector_json, "[1.0,0.5,0.0]");
        assert_eq!(
            record.filter_json.as_deref(),
            Some(r#"{"compare":{"column":"id","op":"gt","value":{"int":1}}}"#)
        );
        assert_eq!(record.result_ids_json, "[7]");
        assert_eq!(record.result_distances_json, "[0.25]");
        assert!(!record.sample_id.is_empty());
        assert!(record.captured_at_unix_ms > 0);
    }

    #[test]
    fn missing_id_column_values_become_json_nulls() {
        let captured: Arc<Mutex<Vec<RecallRecord>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = captured.clone();
        let capture = RecallCapture::new(1.0, "vector_id", Arc::new(Metrics::disabled()))
            .with_hook(Arc::new(move |record| sink.lock().unwrap().push(record.clone())));
        let (target, mut query) = fixture();
        query.filter = None;
        query.nprobes = None;
        query.minimum_nprobes = Some(4);
        let pending = capture.begin(&target, &query).unwrap();
        let hits = vec![Hit {
            row_id: 9,
            score: 1.5,
            row: Map::new(),
        }];
        capture.finish(pending, Some(1), &hits);
        let records = captured.lock().unwrap();
        assert_eq!(records[0].result_ids_json, "[null]");
        assert_eq!(records[0].filter_json, None);
        assert_eq!(records[0].nprobes_min, Some(4));
        assert_eq!(records[0].nprobes_max, None);
    }

    #[test]
    fn disabled_capture_never_begins() {
        let capture = RecallCapture::disabled();
        let (target, query) = fixture();
        assert!(capture.begin(&target, &query).is_none());
    }
}
