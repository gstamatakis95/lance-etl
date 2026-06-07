//! Lance-backed implementation of the [`SearchBackend`] trait over single-dataset targets.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::Float32Array;
use arrow_schema::DataType;
use lance::Dataset;
use lance::dataset::scanner::{ExecutionStatsCallback, ExecutionSummaryCounts, Scanner};
use lance_core::ROW_ID;
use lance_linalg::distance::DistanceType;
use serde_json::{Map, Value};

use crate::domain::{
    DatasetRef, DatasetTarget, DistanceKind, FilterMode, Hit, HybridQuery, HybridSearchOutcome, SearchBackend,
    SearchError, TextQuery, TextSearchOutcome, VectorQuery, VectorSearchOutcome,
};
use crate::lance::error::classify_lance_error;
use crate::lance::filter::filter_to_expr;
use crate::lance::provider::DatasetProvider;
use crate::lance::rows::batch_to_json_rows;
use crate::lance::text::text_query_to_fts;
use crate::telemetry::{Metrics, Rpc};

/// Column key under which Lance reports vector distances.
const DISTANCE_KEY: &str = "_distance";

/// Column key under which Lance reports BM25 scores.
const SCORE_KEY: &str = "_score";

/// Search backend executing queries with Lance scanners over datasets from a [`DatasetProvider`].
pub struct LanceSearchBackend<P: DatasetProvider> {
    pub(crate) provider: P,
    pub(crate) prewarm_concurrency: usize,
    pub(crate) metrics: Arc<crate::telemetry::Metrics>,
}

impl<P: DatasetProvider> LanceSearchBackend<P> {
    /// Creates a backend over the given dataset provider with the default prewarm concurrency and
    /// telemetry disabled.
    pub fn new(provider: P) -> Self {
        Self {
            provider,
            prewarm_concurrency: crate::config::DEFAULT_PREWARM_CONCURRENCY,
            metrics: Arc::new(crate::telemetry::Metrics::disabled()),
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
        let hits = run_vector_query(&dataset, &query, &self.metrics, Rpc::VectorSearch).await?;
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
        let hits = run_text_query(&dataset, &query, &self.metrics, Rpc::TextSearch).await?;
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
        let (vector_hits, text_hits) = tokio::join!(
            run_vector_query(&dataset, &vector_query, &self.metrics, Rpc::HybridSearch),
            run_text_query(&dataset, &text_query, &self.metrics, Rpc::HybridSearch),
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

/// Applies projection, typed filter, and limit/offset to a scanner.
///
/// The physical `_rowid` column is not requested here: each query path enables it conditionally via
/// [`Scanner::with_row_id`] before calling this helper, so pure single-leg searches that neither
/// return the row id nor feed cross-leg fusion dedup skip the extra object-store read.
fn apply_common_options(
    scanner: &mut Scanner,
    dataset: &Dataset,
    projection: &[String],
    filter: Option<&crate::domain::Filter>,
    filter_mode: FilterMode,
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
    if let Some(filter) = filter {
        let expr = filter_to_expr(filter, &schema_columns(dataset))?;
        scanner.filter_expr(expr);
        scanner.prefilter(filter_mode == FilterMode::Prefilter);
    }
    scanner
        .limit(Some(k as i64), offset.map(|skip| skip as i64))
        .map_err(|err| classify_lance_error(&err))?;
    Ok(())
}

/// Builds a Lance scan-stats callback that reports per-query object-store stats as `query.*`
/// distributions tagged by `rpc`.
///
/// Lance invokes the callback once, after the scan's plan finishes, with the aggregated
/// [`ExecutionSummaryCounts`]. The callback only emits metrics through the infallible facade, so it
/// never panics and never blocks the scan.
fn execution_stats_callback(metrics: Arc<Metrics>, rpc: Rpc) -> ExecutionStatsCallback {
    Arc::new(move |counts: &ExecutionSummaryCounts| {
        metrics.query_execution_stats(
            rpc,
            counts.iops as u64,
            counts.bytes_read as u64,
            counts.parts_loaded as u64,
        );
    })
}

/// Runs one nearest-neighbor query against an open dataset.
#[tracing::instrument(name = "lance.vector_query", skip_all, fields(search.k = query.k))]
async fn run_vector_query(
    dataset: &Dataset,
    query: &VectorQuery,
    metrics: &Arc<Metrics>,
    rpc: Rpc,
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
    scanner.scan_stats_callback(execution_stats_callback(metrics.clone(), rpc));
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
    let needs_row_id = query.with_row_id || rpc == Rpc::HybridSearch;
    if needs_row_id {
        scanner.with_row_id();
    }
    apply_common_options(
        &mut scanner,
        dataset,
        &query.projection,
        query.filter.as_ref(),
        query.filter_mode,
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
    metrics: &Arc<Metrics>,
    rpc: Rpc,
) -> Result<Vec<Hit>, SearchError> {
    validate_k(query.k)?;
    let fetch = query.k + query.offset.unwrap_or(0);
    let fts = text_query_to_fts(query, fetch)?;
    let mut scanner = dataset.scan();
    scanner.scan_stats_callback(execution_stats_callback(metrics.clone(), rpc));
    scanner
        .full_text_search(fts)
        .map_err(|err| classify_lance_error(&err))?;
    let needs_row_id = query.with_row_id || rpc == Rpc::HybridSearch;
    if needs_row_id {
        scanner.with_row_id();
    }
    apply_common_options(
        &mut scanner,
        dataset,
        &query.projection,
        query.filter.as_ref(),
        query.filter_mode,
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
