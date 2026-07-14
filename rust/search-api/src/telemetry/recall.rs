//! Sampled-query recall capture: records a deterministic sample of vector, text, and hybrid
//! searches as span attributes so an offline job can replay them and score recall.
//!
//! # What is sampled
//!
//! `VectorSearch`, `TextSearch`, and `HybridSearch` requests are eligible. Requests carrying a
//! typed filter are eligible and the vector capture records the filter, so the offline scorer can
//! replay it. Each query type has its own deterministic sampler counter.
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
//! Spans API can retrieve. Every JSON-valued attribute is a single compact string. Rust writes
//! these and the Python recall job reads them, so the two sides must agree on this schema:
//!
//! Shared (every query type):
//! - `recall.sample` (bool) — always `true` on sampled spans, the retrieval filter key.
//! - `recall.sample_id` (string) — UUIDv4 identifying this capture.
//! - `recall.captured_at_unix_ms` (int) — capture wall-clock time in Unix milliseconds.
//! - `recall.org_id` / `recall.tenant_id` / `recall.namespace` (string) — the dataset target.
//! - `recall.dataset_version` (int) — the committed Lance dataset version that served the query.
//! - `recall.k` (int) — requested result count (the fused `k` for hybrid).
//! - `recall.query_type` (string) — `vector`, `text`, or `hybrid`.
//! - `recall.result_ids` (string) — JSON array of the served id-column values in rank order
//!   (`null` for rows whose projection omitted the id column).
//!
//! Vector (and the vector knobs of a query that carries them):
//! - `recall.nprobes_min` / `recall.nprobes_max` (int) — probed-partition bounds as recorded on
//!   the request (`nprobes` sets both). Absent when left to the index defaults.
//! - `recall.refine_factor` (int) — re-rank factor. Absent when unset.
//! - `recall.distance_type` (string) — `l2`, `cosine`, `dot`, or `hamming`. Absent when the
//!   request kept the index metric.
//! - `recall.query_vector` (string) — the full query vector as a compact JSON array of numbers.
//!   Present for `vector` and `hybrid`.
//! - `recall.filter` (string) — the typed filter AST as the stable JSON documented in
//!   [`crate::domain::filter`]. Present for `vector` only, and absent when unfiltered.
//! - `recall.result_distances` (string) — JSON array of the served distances in rank order.
//!   Present for `vector` only.
//!
//! Text and hybrid:
//! - `recall.text_query` (string) — the [`crate::domain::query::TextQueryNode`] AST serialized as
//!   the stable JSON documented in [`crate::domain::query`].
//! - `recall.text_columns` (string) — JSON array of the text query columns.
//! - `recall.result_scores` (string) — JSON array of the served relevance scores (BM25 for text,
//!   fused score for hybrid) in rank order.
//!
//! Hybrid only:
//! - `recall.fusion` (string) — the fusion strategy as JSON, e.g. `{"rrf":{"k":60.0}}` or
//!   `{"weighted":{"vector_weight":0.7}}`.
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

use crate::domain::{DatasetTarget, DistanceKind, Hit, HybridQuery, TextQuery, VectorQuery};
use crate::telemetry::Metrics;

/// Observer invoked with every finished capture record. Used by tests to assert captures.
pub type RecallHook = Arc<dyn Fn(&RecallRecord) + Send + Sync>;

/// The query family a recall capture was taken from (`recall.query_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecallQueryType {
    /// Nearest-neighbor search.
    Vector,
    /// Full-text search.
    Text,
    /// Hybrid (fused vector + text) search.
    Hybrid,
}

impl RecallQueryType {
    /// The `recall.query_type` attribute value and metric tag for this query type.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Vector => "vector",
            Self::Text => "text",
            Self::Hybrid => "hybrid",
        }
    }
}

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
    /// The query family this capture came from (`recall.query_type`).
    pub query_type: RecallQueryType,
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
    /// Requested result count (`recall.k`).
    pub k: usize,
    /// Minimum probed partitions as recorded on the request (`recall.nprobes_min`).
    pub nprobes_min: Option<usize>,
    /// Maximum probed partitions as recorded on the request (`recall.nprobes_max`).
    pub nprobes_max: Option<usize>,
    /// Re-rank factor (`recall.refine_factor`).
    pub refine_factor: Option<u32>,
    /// Distance metric override (`recall.distance_type`). `None` keeps the index metric.
    pub distance_type: Option<&'static str>,
    /// The full query vector as a compact JSON number array (`recall.query_vector`). `None` for
    /// pure text searches.
    pub query_vector_json: Option<String>,
    /// The text query node AST as stable JSON (`recall.text_query`). `None` for pure vector.
    pub text_query_json: Option<String>,
    /// The text query columns as a JSON array (`recall.text_columns`). `None` for pure vector.
    pub text_columns_json: Option<String>,
    /// The fusion strategy as JSON (`recall.fusion`). `Some` for hybrid only.
    pub fusion_json: Option<String>,
    /// The typed filter AST as stable JSON (`recall.filter`). `Some` for filtered vector searches.
    pub filter_json: Option<String>,
    /// Served id-column values in rank order as a JSON array (`recall.result_ids`).
    pub result_ids_json: String,
    /// Served distances in rank order as a JSON array (`recall.result_distances`). `Some` for
    /// vector searches.
    pub result_distances_json: Option<String>,
    /// Served relevance scores in rank order as a JSON array (`recall.result_scores`). `Some` for
    /// text and hybrid searches.
    pub result_scores_json: Option<String>,
}

impl RecallRecord {
    /// Attaches every present field as a `recall.*` attribute on the current tracing span.
    pub fn attach_to_current_span(&self) {
        let span = tracing::Span::current();
        span.set_attribute("recall.sample", true);
        span.set_attribute("recall.query_type", self.query_type.as_str());
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
        if let Some(vector) = &self.query_vector_json {
            span.set_attribute("recall.query_vector", vector.clone());
        }
        if let Some(text_query) = &self.text_query_json {
            span.set_attribute("recall.text_query", text_query.clone());
        }
        if let Some(text_columns) = &self.text_columns_json {
            span.set_attribute("recall.text_columns", text_columns.clone());
        }
        if let Some(fusion) = &self.fusion_json {
            span.set_attribute("recall.fusion", fusion.clone());
        }
        if let Some(filter) = &self.filter_json {
            span.set_attribute("recall.filter", filter.clone());
        }
        span.set_attribute("recall.result_ids", self.result_ids_json.clone());
        if let Some(distances) = &self.result_distances_json {
            span.set_attribute("recall.result_distances", distances.clone());
        }
        if let Some(scores) = &self.result_scores_json {
            span.set_attribute("recall.result_scores", scores.clone());
        }
    }
}

/// A sampled request awaiting its results: the query context cloned at decision time.
#[derive(Debug)]
pub struct PendingRecall {
    query_type: RecallQueryType,
    org_id: String,
    tenant_id: String,
    namespace: String,
    k: usize,
    nprobes_min: Option<usize>,
    nprobes_max: Option<usize>,
    refine_factor: Option<u32>,
    distance_type: Option<&'static str>,
    query_vector_json: Option<String>,
    text_query_json: Option<String>,
    text_columns_json: Option<String>,
    fusion_json: Option<String>,
    filter_json: Option<String>,
    filtered: bool,
}

/// The capture facade owned by the transport: sampling decision, record assembly, span
/// attachment, and the `recall.samples` metric.
pub struct RecallCapture {
    vector_sampler: RecallSampler,
    text_sampler: RecallSampler,
    hybrid_sampler: RecallSampler,
    metrics: Arc<Metrics>,
    hook: Option<RecallHook>,
}

impl std::fmt::Debug for RecallCapture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecallCapture")
            .field("vector_sampler", &self.vector_sampler)
            .field("text_sampler", &self.text_sampler)
            .field("hybrid_sampler", &self.hybrid_sampler)
            .finish()
    }
}

impl RecallCapture {
    /// Creates a capture facade with the given sample rate and result id column.
    pub fn new(rate: f64, metrics: Arc<Metrics>) -> Self {
        Self {
            vector_sampler: RecallSampler::new(rate),
            text_sampler: RecallSampler::new(rate),
            hybrid_sampler: RecallSampler::new(rate),
            metrics,
            hook: None,
        }
    }

    /// A capture facade that never samples, for constructors without recall wiring.
    pub fn disabled() -> Self {
        Self::new(0.0, Arc::new(Metrics::disabled()))
    }

    /// Installs an observer invoked with every finished record. Test seam.
    pub fn with_hook(mut self, hook: RecallHook) -> Self {
        self.hook = Some(hook);
        self
    }

    /// Decides whether this vector search is sampled, snapshotting the query when it is.
    pub fn begin(&self, target: &DatasetTarget, query: &VectorQuery) -> Option<PendingRecall> {
        if !self.vector_sampler.should_sample() {
            return None;
        }
        let (nprobes_min, nprobes_max) = nprobes_bounds(query);
        Some(PendingRecall {
            query_type: RecallQueryType::Vector,
            org_id: target.org_id.clone(),
            tenant_id: target.tenant_id.clone(),
            namespace: target.namespace.clone(),
            k: query.k,
            nprobes_min,
            nprobes_max,
            refine_factor: query.refine_factor,
            distance_type: query.distance.map(distance_tag),
            query_vector_json: Some(vector_to_json(&query.vector)),
            text_query_json: None,
            text_columns_json: None,
            fusion_json: None,
            filter_json: query
                .filter
                .as_ref()
                .and_then(|filter| serde_json::to_string(filter).ok()),
            filtered: query.filter.is_some(),
        })
    }

    /// Decides whether this text search is sampled, snapshotting the query when it is.
    pub fn begin_text(&self, target: &DatasetTarget, query: &TextQuery) -> Option<PendingRecall> {
        if !self.text_sampler.should_sample() {
            return None;
        }
        Some(PendingRecall {
            query_type: RecallQueryType::Text,
            org_id: target.org_id.clone(),
            tenant_id: target.tenant_id.clone(),
            namespace: target.namespace.clone(),
            k: query.k,
            nprobes_min: None,
            nprobes_max: None,
            refine_factor: None,
            distance_type: None,
            query_vector_json: None,
            text_query_json: serde_json::to_string(&query.node).ok(),
            text_columns_json: serde_json::to_string(&query.columns).ok(),
            fusion_json: None,
            filter_json: None,
            filtered: query.filter.is_some(),
        })
    }

    /// Decides whether this hybrid search is sampled, snapshotting the query when it is.
    ///
    /// `k` is the fused result count.
    pub fn begin_hybrid(&self, target: &DatasetTarget, query: &HybridQuery) -> Option<PendingRecall> {
        if !self.hybrid_sampler.should_sample() {
            return None;
        }
        Some(PendingRecall {
            query_type: RecallQueryType::Hybrid,
            org_id: target.org_id.clone(),
            tenant_id: target.tenant_id.clone(),
            namespace: target.namespace.clone(),
            k: query.k,
            nprobes_min: None,
            nprobes_max: None,
            refine_factor: None,
            distance_type: None,
            query_vector_json: Some(vector_to_json(&query.vector.vector)),
            text_query_json: serde_json::to_string(&query.text.node).ok(),
            text_columns_json: serde_json::to_string(&query.text.columns).ok(),
            fusion_json: Some(query.fusion.to_recall_json().to_string()),
            filter_json: None,
            filtered: query.vector.filter.is_some() || query.text.filter.is_some(),
        })
    }

    /// Finishes one sampled request: assembles the record from the served hits, attaches it to
    /// the current span, emits the `recall.samples` counter, and notifies the test hook.
    ///
    /// Vector captures record the served scores as `recall.result_distances`; text and hybrid
    /// captures record them as `recall.result_scores`.
    pub fn finish(&self, pending: PendingRecall, dataset_version: Option<u64>, hits: &[Hit]) {
        let ids: Vec<Value> = hits.iter().map(|hit| Value::from(hit.vector_id.clone())).collect();
        let scores: Vec<f64> = hits.iter().map(|hit| hit.score).collect();
        let scores_json = serde_json::to_string(&scores).unwrap_or_else(|_| "[]".to_string());
        let (result_distances_json, result_scores_json) = match pending.query_type {
            RecallQueryType::Vector => (Some(scores_json), None),
            RecallQueryType::Text | RecallQueryType::Hybrid => (None, Some(scores_json)),
        };
        let record = RecallRecord {
            query_type: pending.query_type,
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
            text_query_json: pending.text_query_json,
            text_columns_json: pending.text_columns_json,
            fusion_json: pending.fusion_json,
            filter_json: pending.filter_json,
            result_ids_json: serde_json::to_string(&ids).unwrap_or_else(|_| "[]".to_string()),
            result_distances_json,
            result_scores_json,
        };
        record.attach_to_current_span();
        self.metrics
            .recall_sample(pending.query_type.as_str(), pending.filtered);
        if let Some(hook) = &self.hook {
            hook(&record);
        }
    }
}

/// The probed-partition bounds recorded for a vector query (`nprobes` sets both).
fn nprobes_bounds(query: &VectorQuery) -> (Option<usize>, Option<usize>) {
    match query.nprobes {
        Some(nprobes) => (Some(nprobes), Some(nprobes)),
        None => (query.minimum_nprobes, query.maximum_nprobes),
    }
}

/// Serializes a query vector as a compact JSON number array.
fn vector_to_json(vector: &[f32]) -> String {
    serde_json::to_string(vector).unwrap_or_else(|_| "[]".to_string())
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
    use crate::domain::{CompareOp, Filter, Literal, MatchSpec, TextQueryNode};
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

    /// Builds a capture that records every finished record into `sink`.
    fn capturing(sink: Arc<Mutex<Vec<RecallRecord>>>) -> RecallCapture {
        RecallCapture::new(1.0, Arc::new(Metrics::disabled()))
            .with_hook(Arc::new(move |record| sink.lock().unwrap().push(record.clone())))
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
    fn begin_samples_and_finish_builds_the_record() {
        let captured: Arc<Mutex<Vec<RecallRecord>>> = Arc::new(Mutex::new(Vec::new()));
        let capture = capturing(captured.clone());
        let (target, query) = fixture();
        let pending = capture.begin(&target, &query).expect("rate 1.0 must sample");
        let mut row = Map::new();
        row.insert("vector_id".to_string(), Value::from(7));
        let hits = vec![Hit {
            vector_id: "7".to_string(),
            score: 0.25,
            row,
        }];
        capture.finish(pending, Some(42), &hits);
        let records = captured.lock().unwrap();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.query_type, RecallQueryType::Vector);
        assert_eq!(record.org_id, "org1");
        assert_eq!(record.dataset_version, Some(42));
        assert_eq!(record.k, 2);
        assert_eq!(record.nprobes_min, Some(20));
        assert_eq!(record.nprobes_max, Some(20));
        assert_eq!(record.refine_factor, Some(2));
        assert_eq!(record.distance_type, Some("cosine"));
        assert_eq!(record.query_vector_json.as_deref(), Some("[1.0,0.5,0.0]"));
        assert_eq!(
            record.filter_json.as_deref(),
            Some(r#"{"compare":{"column":"id","op":"gt","value":{"int":1}}}"#)
        );
        assert_eq!(record.result_ids_json, r#"["7"]"#);
        assert_eq!(record.result_distances_json.as_deref(), Some("[0.25]"));
        assert_eq!(record.result_scores_json, None);
        assert!(!record.sample_id.is_empty());
        assert!(record.captured_at_unix_ms > 0);
    }

    #[test]
    fn logical_vector_id_is_always_recorded() {
        let captured: Arc<Mutex<Vec<RecallRecord>>> = Arc::new(Mutex::new(Vec::new()));
        let capture = capturing(captured.clone());
        let (target, mut query) = fixture();
        query.filter = None;
        query.nprobes = None;
        query.minimum_nprobes = Some(4);
        let pending = capture.begin(&target, &query).unwrap();
        let hits = vec![Hit {
            vector_id: "9".to_string(),
            score: 1.5,
            row: Map::new(),
        }];
        capture.finish(pending, Some(1), &hits);
        let records = captured.lock().unwrap();
        assert_eq!(records[0].result_ids_json, r#"["9"]"#);
        assert_eq!(records[0].filter_json, None);
        assert_eq!(records[0].nprobes_min, Some(4));
        assert_eq!(records[0].nprobes_max, None);
    }

    #[test]
    fn text_capture_records_query_type_text_query_and_scores() {
        let captured: Arc<Mutex<Vec<RecallRecord>>> = Arc::new(Mutex::new(Vec::new()));
        let capture = capturing(captured.clone());
        let target = DatasetTarget::new("org1", "tenant1", "ns1");
        let mut query = TextQuery::simple("lemon", 3);
        query.columns = vec!["text".to_string()];
        let pending = capture.begin_text(&target, &query).expect("rate 1.0 must sample");
        let mut row = Map::new();
        row.insert("vector_id".to_string(), Value::from(4));
        let hits = vec![Hit {
            vector_id: "4".to_string(),
            score: 2.5,
            row,
        }];
        capture.finish(pending, Some(7), &hits);
        let records = captured.lock().unwrap();
        let record = &records[0];
        assert_eq!(record.query_type, RecallQueryType::Text);
        assert_eq!(record.query_vector_json, None);
        assert_eq!(
            record.text_query_json.as_deref(),
            Some(
                r#"{"match":{"terms":"lemon","column":null,"boost":1.0,"operator":"or","fuzziness":"exact","max_expansions":null,"prefix_length":0}}"#
            )
        );
        assert_eq!(record.text_columns_json.as_deref(), Some(r#"["text"]"#));
        assert_eq!(record.result_ids_json, r#"["4"]"#);
        assert_eq!(record.result_scores_json.as_deref(), Some("[2.5]"));
        assert_eq!(record.result_distances_json, None);
        assert_eq!(record.fusion_json, None);
    }

    #[test]
    fn hybrid_capture_records_vector_text_and_fusion() {
        use crate::domain::FusionSpec;
        let captured: Arc<Mutex<Vec<RecallRecord>>> = Arc::new(Mutex::new(Vec::new()));
        let capture = capturing(captured.clone());
        let target = DatasetTarget::new("org1", "tenant1", "ns1");
        let query = HybridQuery {
            vector: VectorQuery {
                vector: vec![0.0, 1.0],
                k: 2,
                ..Default::default()
            },
            text: TextQuery {
                node: TextQueryNode::Match(MatchSpec::new("pear")),
                columns: vec!["text".to_string()],
                k: 2,
                ..TextQuery::simple("pear", 2)
            },
            k: 2,
            fusion: FusionSpec::Weighted { vector_weight: 0.7 },
        };
        let pending = capture.begin_hybrid(&target, &query).expect("rate 1.0 must sample");
        let mut row = Map::new();
        row.insert("vector_id".to_string(), Value::from(2));
        let hits = vec![Hit {
            vector_id: "2".to_string(),
            score: 0.42,
            row,
        }];
        capture.finish(pending, Some(9), &hits);
        let records = captured.lock().unwrap();
        let record = &records[0];
        assert_eq!(record.query_type, RecallQueryType::Hybrid);
        assert_eq!(record.query_vector_json.as_deref(), Some("[0.0,1.0]"));
        assert_eq!(record.text_columns_json.as_deref(), Some(r#"["text"]"#));
        assert_eq!(
            record.fusion_json.as_deref(),
            Some(r#"{"weighted":{"vector_weight":0.7}}"#)
        );
        assert_eq!(record.result_scores_json.as_deref(), Some("[0.42]"));
        assert_eq!(record.result_distances_json, None);
    }

    #[test]
    fn disabled_capture_never_begins() {
        let capture = RecallCapture::disabled();
        let (target, query) = fixture();
        assert!(capture.begin(&target, &query).is_none());
        assert!(capture.begin_text(&target, &TextQuery::simple("x", 1)).is_none());
    }
}
