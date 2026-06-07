//! Lance-backed implementation of the [`SearchBackend`] trait over single-dataset targets.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::Float32Array;
use arrow_schema::DataType;
use lance::Dataset;
use lance::dataset::scanner::{ExecutionStatsCallback, ExecutionSummaryCounts, Scanner};
use lance::deps::datafusion::logical_expr::Expr;
use lance_core::ROW_ID;
use lance_linalg::distance::DistanceType;
use serde_json::{Map, Value};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::domain::{
    DatasetRef, DatasetTarget, DistanceKind, FilterMode, Hit, HybridQuery, HybridSearchOutcome, SearchBackend,
    SearchError, TextQuery, TextSearchOutcome, TimeRange, VectorQuery, VectorSearchOutcome,
};
use crate::lance::error::classify_lance_error;
use crate::lance::filter::{filter_to_expr, time_range_to_expr};
use crate::lance::provider::DatasetProvider;
use crate::lance::rows::batch_to_json_rows;
use crate::lance::text::text_query_to_fts;
use crate::telemetry::{Metrics, Rpc};

/// Column key under which Lance reports vector distances.
const DISTANCE_KEY: &str = "_distance";

/// Column key under which Lance reports BM25 scores.
const SCORE_KEY: &str = "_score";

/// Object-store IO statistics captured from one Lance scan and attached to the per-query-leg span
/// as `s3.*` attributes, so a slow query can be drilled into by its object-store request volume.
///
/// Values are sourced from Lance's execution-stats callback ([`ExecutionSummaryCounts`]). Lance
/// exposes aggregate counts only: a precise GET/HEAD/LIST breakdown is not available outside the
/// `test-util` build, so this records the object-store request count and the IO totals that are.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanIoStats {
    /// Object-store requests made to the storage layer (`s3.requests`).
    pub requests: u64,
    /// I/O operations after coalescing (`s3.iops`).
    pub iops: u64,
    /// Bytes pulled from storage (`s3.bytes_read`).
    pub bytes_read: u64,
    /// Index partitions loaded from storage (`s3.parts_loaded`).
    pub parts_loaded: u64,
    /// Top-level indices loaded from storage (`s3.indices_loaded`).
    pub indices_loaded: u64,
}

impl ScanIoStats {
    /// Builds the stats from one Lance execution-summary count snapshot.
    fn from_counts(counts: &ExecutionSummaryCounts) -> Self {
        Self {
            requests: counts.requests as u64,
            iops: counts.iops as u64,
            bytes_read: counts.bytes_read as u64,
            parts_loaded: counts.parts_loaded as u64,
            indices_loaded: counts.indices_loaded as u64,
        }
    }

    /// Attaches every count to `span` as an `s3.*` attribute. Low cardinality: counts only, never
    /// org/tenant ids.
    fn attach_to_span(&self, span: &tracing::Span) {
        span.set_attribute("s3.requests", self.requests as i64);
        span.set_attribute("s3.iops", self.iops as i64);
        span.set_attribute("s3.bytes_read", self.bytes_read as i64);
        span.set_attribute("s3.parts_loaded", self.parts_loaded as i64);
        span.set_attribute("s3.indices_loaded", self.indices_loaded as i64);
    }
}

/// Observer invoked with the [`ScanIoStats`] captured from each scan. Test seam for asserting the
/// object-store stat-capture path runs without inspecting exported spans.
pub type ScanStatsHook = Arc<dyn Fn(&ScanIoStats) + Send + Sync>;

/// Search backend executing queries with Lance scanners over datasets from a [`DatasetProvider`].
pub struct LanceSearchBackend<P: DatasetProvider> {
    pub(crate) provider: P,
    pub(crate) prewarm_concurrency: usize,
    pub(crate) metrics: Arc<crate::telemetry::Metrics>,
    pub(crate) event_timestamp_column: String,
    pub(crate) scan_stats_hook: Option<ScanStatsHook>,
}

impl<P: DatasetProvider> LanceSearchBackend<P> {
    /// Creates a backend over the given dataset provider with the default prewarm concurrency, the
    /// default event-timestamp column, and telemetry disabled.
    pub fn new(provider: P) -> Self {
        Self {
            provider,
            prewarm_concurrency: crate::config::DEFAULT_PREWARM_CONCURRENCY,
            metrics: Arc::new(crate::telemetry::Metrics::disabled()),
            event_timestamp_column: crate::config::DEFAULT_EVENT_TIMESTAMP_COLUMN.to_string(),
            scan_stats_hook: None,
        }
    }

    /// Sets how many indexes one Prewarm call loads concurrently.
    pub fn with_prewarm_concurrency(mut self, prewarm_concurrency: usize) -> Self {
        self.prewarm_concurrency = prewarm_concurrency.max(1);
        self
    }

    /// Emits backend metrics (prewarm and clusters timings) through the given facade.
    pub fn with_metrics(mut self, metrics: Arc<crate::telemetry::Metrics>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Sets the column a request time range is applied to.
    pub fn with_event_timestamp_column(mut self, column: impl Into<String>) -> Self {
        self.event_timestamp_column = column.into();
        self
    }

    /// Installs an observer invoked with the IO stats captured from each scan. Test seam.
    pub fn with_scan_stats_hook(mut self, hook: ScanStatsHook) -> Self {
        self.scan_stats_hook = Some(hook);
        self
    }

    /// Bundles the per-query execution context (metrics facade, RPC tag, event-timestamp column,
    /// and scan-stats hook) borrowed for one search leg.
    fn context(&self, rpc: Rpc) -> QueryContext<'_> {
        QueryContext {
            metrics: &self.metrics,
            rpc,
            event_timestamp_column: &self.event_timestamp_column,
            scan_stats_hook: self.scan_stats_hook.as_ref(),
        }
    }
}

/// Borrowed per-query execution context shared by a search leg.
struct QueryContext<'a> {
    /// Metrics facade for per-query execution stats.
    metrics: &'a Arc<Metrics>,
    /// RPC tag for metrics and the scan-stats span.
    rpc: Rpc,
    /// Column a request time range is applied to.
    event_timestamp_column: &'a str,
    /// Optional observer of the captured scan IO stats (test seam).
    scan_stats_hook: Option<&'a ScanStatsHook>,
}

impl<P: DatasetProvider> SearchBackend for LanceSearchBackend<P> {
    #[tracing::instrument(
        name = "backend.vector_search",
        skip_all,
        fields(org_id = %target.org_id, search.k = query.k)
    )]
    async fn vector_search(
        &self,
        target: &DatasetTarget,
        query: VectorQuery,
    ) -> Result<VectorSearchOutcome, SearchError> {
        validate_k(query.k)?;
        let dataset = self.provider.dataset(target, DatasetRef::Serve).await?;
        let hits = run_vector_query(&dataset, &query, &self.context(Rpc::VectorSearch)).await?;
        Ok(VectorSearchOutcome {
            hits,
            dataset_version: Some(dataset.version_id()),
        })
    }

    #[tracing::instrument(
        name = "backend.text_search",
        skip_all,
        fields(org_id = %target.org_id, search.k = query.k)
    )]
    async fn text_search(&self, target: &DatasetTarget, query: TextQuery) -> Result<TextSearchOutcome, SearchError> {
        validate_k(query.k)?;
        let dataset = self.provider.dataset(target, DatasetRef::Serve).await?;
        let hits = run_text_query(&dataset, &query, &self.context(Rpc::TextSearch)).await?;
        Ok(TextSearchOutcome {
            hits,
            dataset_version: Some(dataset.version_id()),
        })
    }

    #[tracing::instrument(
        name = "backend.hybrid_search",
        skip_all,
        fields(org_id = %target.org_id, search.k = query.k)
    )]
    async fn hybrid_search(
        &self,
        target: &DatasetTarget,
        query: HybridQuery,
    ) -> Result<HybridSearchOutcome, SearchError> {
        validate_k(query.k)?;
        let mut vector_query = query.vector;
        if vector_query.k == 0 {
            vector_query.k = query.k;
        }
        let mut text_query = query.text;
        if text_query.k == 0 {
            text_query.k = query.k;
        }
        let fusion = query.fusion;
        let dataset = self.provider.dataset(target, DatasetRef::Serve).await?;
        let context = self.context(Rpc::HybridSearch);
        let (vector_hits, text_hits) = tokio::join!(
            run_vector_query(&dataset, &vector_query, &context),
            run_text_query(&dataset, &text_query, &context),
        );
        let (vector_hits, text_hits) = (vector_hits?, text_hits?);
        let fuse_span = tracing::info_span!("fusion.fuse", search.k = query.k);
        Ok(HybridSearchOutcome {
            hits: fuse_span.in_scope(|| fusion.fuse(vec![vector_hits, text_hits], query.k)),
            dataset_version: Some(dataset.version_id()),
        })
    }
}

/// Returns the name of the first fixed-size-list vector column in the dataset schema.
fn default_vector_column(dataset: &Dataset) -> Result<String, SearchError> {
    dataset
        .schema()
        .fields
        .iter()
        .find(|field| matches!(field.data_type(), DataType::FixedSizeList(_, _)))
        .map(|field| field.name.clone())
        .ok_or_else(|| SearchError::invalid_argument("dataset has no fixed-size-list vector column"))
}

/// Lists the scalar (non-vector) columns returned by default in search results.
fn scalar_output_columns(dataset: &Dataset) -> Vec<String> {
    dataset
        .schema()
        .fields
        .iter()
        .filter(|field| !matches!(field.data_type(), DataType::FixedSizeList(_, _)))
        .map(|field| field.name.clone())
        .collect()
}

/// Collects the top-level column names of the dataset schema for filter validation.
fn schema_columns(dataset: &Dataset) -> HashSet<String> {
    dataset.schema().fields.iter().map(|field| field.name.clone()).collect()
}

/// Validates that `k` is positive.
fn validate_k(k: usize) -> Result<(), SearchError> {
    if k == 0 {
        return Err(SearchError::invalid_argument("k must be a positive integer"));
    }
    Ok(())
}

/// Applies projection, typed filter, optional event-time range, and limit/offset to a scanner.
///
/// The caller-provided filter and the event-time range predicate are ANDed into a single filter
/// expression through the typed [`filter_to_expr`] / [`time_range_to_expr`] path, so no raw SQL is
/// ever constructed. When only a time range is present it still drives the scan filter, naturally
/// pruned by a BTREE or zone-map on the event-timestamp column.
///
/// The physical `_rowid` column is not requested here: each query path enables it conditionally via
/// [`Scanner::with_row_id`] before calling this helper, so pure single-leg searches that neither
/// return the row id nor feed cross-leg fusion dedup skip the extra object-store read.
fn apply_common_options(
    scanner: &mut Scanner,
    dataset: &Dataset,
    projection: &[String],
    predicate: ScanPredicate<'_>,
    k: usize,
    offset: Option<usize>,
) -> Result<(), SearchError> {
    if projection.is_empty() {
        scanner
            .project(&scalar_output_columns(dataset))
            .map_err(|err| classify_lance_error(&err))?;
    } else {
        scanner.project(projection).map_err(|err| classify_lance_error(&err))?;
    }
    if let Some(expr) = predicate.combined_expr(dataset)? {
        scanner.filter_expr(expr);
        scanner.prefilter(predicate.filter_mode == FilterMode::Prefilter);
    }
    scanner
        .limit(Some(k as i64), offset.map(|skip| skip as i64))
        .map_err(|err| classify_lance_error(&err))?;
    Ok(())
}

/// The scan predicate inputs: the caller's typed filter, its prefilter/postfilter mode, the
/// optional event-time window, and the column the window applies to.
struct ScanPredicate<'a> {
    /// Caller-provided typed predicate, if any.
    filter: Option<&'a crate::domain::Filter>,
    /// Whether the predicate runs before or after the index search.
    filter_mode: FilterMode,
    /// Optional event-time window, ANDed with `filter`.
    time_range: Option<&'a TimeRange>,
    /// Column the event-time window is applied to.
    event_timestamp_column: &'a str,
}

impl ScanPredicate<'_> {
    /// Combines the caller filter and the event-time range into one DataFusion expression, ANDing
    /// them when both are present. Returns `None` when neither restricts the scan.
    fn combined_expr(&self, dataset: &Dataset) -> Result<Option<Expr>, SearchError> {
        let filter_expr = match self.filter {
            Some(filter) => Some(filter_to_expr(filter, &schema_columns(dataset))?),
            None => None,
        };
        let range_expr = match self.time_range {
            Some(range) if range.is_bounded() => {
                let data_type = event_timestamp_data_type(dataset, self.event_timestamp_column)?;
                time_range_to_expr(range, self.event_timestamp_column, &data_type)?
            }
            _ => None,
        };
        Ok(match (filter_expr, range_expr) {
            (Some(filter), Some(range)) => Some(filter.and(range)),
            (Some(filter), None) => Some(filter),
            (None, range) => range,
        })
    }
}

/// Resolves the Arrow data type of the event-timestamp column, rejecting an absent column.
fn event_timestamp_data_type(dataset: &Dataset, column: &str) -> Result<DataType, SearchError> {
    dataset
        .schema()
        .fields
        .iter()
        .find(|field| field.name == column)
        .map(|field| field.data_type())
        .ok_or_else(|| {
            SearchError::invalid_argument(format!("event-timestamp column {column:?} not found in dataset schema"))
        })
}

/// Builds a Lance scan-stats callback that reports per-query object-store stats both as `query.*`
/// metric distributions tagged by `rpc` and as `s3.*` attributes on `span`.
///
/// Lance invokes the callback once, after the scan's plan finishes, with the aggregated
/// [`ExecutionSummaryCounts`]. The callback emits metrics through the infallible facade and writes
/// span attributes through the OpenTelemetry layer (a no-op when telemetry is disabled), so it
/// never panics and never blocks the scan. `span` is the per-query-leg span captured at scan setup,
/// a child of the per-RPC server span, so the RPC trace shows the object-store volume of each leg.
fn execution_stats_callback(
    metrics: Arc<Metrics>,
    rpc: Rpc,
    span: tracing::Span,
    hook: Option<ScanStatsHook>,
) -> ExecutionStatsCallback {
    Arc::new(move |counts: &ExecutionSummaryCounts| {
        let stats = ScanIoStats::from_counts(counts);
        metrics.query_execution_stats(rpc, stats.iops, stats.bytes_read, stats.parts_loaded);
        stats.attach_to_span(&span);
        if let Some(hook) = &hook {
            hook(&stats);
        }
    })
}

/// Runs one nearest-neighbor query against an open dataset.
#[tracing::instrument(name = "lance.vector_query", skip_all, fields(search.k = query.k))]
async fn run_vector_query(
    dataset: &Dataset,
    query: &VectorQuery,
    context: &QueryContext<'_>,
) -> Result<Vec<Hit>, SearchError> {
    validate_k(query.k)?;
    if query.vector.is_empty() {
        return Err(SearchError::invalid_argument("vector must be non-empty"));
    }
    let column_name = match &query.column {
        Some(name) => name.clone(),
        None => default_vector_column(dataset)?,
    };
    let key = Float32Array::from(query.vector.clone());
    let fetch = query.k + query.offset.unwrap_or(0);
    let mut scanner = dataset.scan();
    scanner.scan_stats_callback(execution_stats_callback(
        context.metrics.clone(),
        context.rpc,
        tracing::Span::current(),
        context.scan_stats_hook.cloned(),
    ));
    scanner
        .nearest(&column_name, &key, fetch)
        .map_err(|err| classify_lance_error(&err))?;
    if let Some(distance) = query.distance {
        scanner.distance_metric(distance_to_lance(distance));
    }
    if let Some(nprobes) = query.nprobes {
        scanner.nprobes(nprobes);
    } else {
        if let Some(minimum) = query.minimum_nprobes {
            scanner.minimum_nprobes(minimum);
        }
        if let Some(maximum) = query.maximum_nprobes {
            scanner.maximum_nprobes(maximum);
        }
    }
    if let Some(refine_factor) = query.refine_factor {
        scanner.refine(refine_factor);
    }
    if let Some(ef) = query.ef {
        scanner.ef(ef);
    }
    if query.fast_search {
        scanner.fast_search();
    }
    if query.bypass_vector_index {
        scanner.use_index(false);
    }
    let needs_row_id = query.with_row_id || context.rpc == Rpc::HybridSearch;
    if needs_row_id {
        scanner.with_row_id();
    }
    apply_common_options(
        &mut scanner,
        dataset,
        &query.projection,
        ScanPredicate {
            filter: query.filter.as_ref(),
            filter_mode: query.filter_mode,
            time_range: query.time_range.as_ref(),
            event_timestamp_column: context.event_timestamp_column,
        },
        query.k,
        query.offset,
    )?;
    let batch = scanner
        .try_into_batch()
        .await
        .map_err(|err| classify_lance_error(&err))?;
    rows_to_hits(
        batch_to_json_rows(&batch)?,
        DISTANCE_KEY,
        query.with_row_id,
        needs_row_id,
    )
}

/// Runs one full-text query against an open dataset.
#[tracing::instrument(name = "lance.text_query", skip_all, fields(search.k = query.k))]
async fn run_text_query(
    dataset: &Dataset,
    query: &TextQuery,
    context: &QueryContext<'_>,
) -> Result<Vec<Hit>, SearchError> {
    validate_k(query.k)?;
    let fetch = query.k + query.offset.unwrap_or(0);
    let fts = text_query_to_fts(query, fetch)?;
    let mut scanner = dataset.scan();
    scanner.scan_stats_callback(execution_stats_callback(
        context.metrics.clone(),
        context.rpc,
        tracing::Span::current(),
        context.scan_stats_hook.cloned(),
    ));
    scanner
        .full_text_search(fts)
        .map_err(|err| classify_lance_error(&err))?;
    let needs_row_id = query.with_row_id || context.rpc == Rpc::HybridSearch;
    if needs_row_id {
        scanner.with_row_id();
    }
    apply_common_options(
        &mut scanner,
        dataset,
        &query.projection,
        ScanPredicate {
            filter: query.filter.as_ref(),
            filter_mode: query.filter_mode,
            time_range: query.time_range.as_ref(),
            event_timestamp_column: context.event_timestamp_column,
        },
        query.k,
        query.offset,
    )?;
    let batch = scanner
        .try_into_batch()
        .await
        .map_err(|err| classify_lance_error(&err))?;
    rows_to_hits(batch_to_json_rows(&batch)?, SCORE_KEY, query.with_row_id, needs_row_id)
}

/// Converts JSON result rows into hits, extracting the score column and the physical row id.
///
/// The score column is always removed from the row (it travels in a dedicated response field).
/// When `has_row_id` is set the physical `_rowid` column is read from the row; it is kept in the row
/// only when `keep_row_id` is also set, otherwise it is stripped after capture. When `has_row_id` is
/// false the scanner did not fetch the column and the hit carries a placeholder row id of 0, which is
/// never consulted because such hits never feed fusion dedup.
fn rows_to_hits(
    rows: Vec<Map<String, Value>>,
    score_key: &str,
    keep_row_id: bool,
    has_row_id: bool,
) -> Result<Vec<Hit>, SearchError> {
    rows.into_iter()
        .map(|mut row| {
            let row_id = if has_row_id {
                let captured = row
                    .get(ROW_ID)
                    .and_then(Value::as_u64)
                    .ok_or_else(|| SearchError::internal("search result row is missing its row id"))?;
                if !keep_row_id {
                    row.remove(ROW_ID);
                }
                captured
            } else {
                0
            };
            let score = row.remove(score_key).and_then(|value| value.as_f64()).unwrap_or(0.0);
            Ok(Hit { row_id, score, row })
        })
        .collect()
}

/// Maps the domain distance metric onto the Lance distance type.
fn distance_to_lance(distance: DistanceKind) -> DistanceType {
    match distance {
        DistanceKind::L2 => DistanceType::L2,
        DistanceKind::Cosine => DistanceType::Cosine,
        DistanceKind::Dot => DistanceType::Dot,
        DistanceKind::Hamming => DistanceType::Hamming,
    }
}
