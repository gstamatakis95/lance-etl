//! Lance-backed implementation of the [`SearchBackend`] trait, including date-range fan-out.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::Float32Array;
use arrow_schema::DataType;
use chrono::NaiveDate;
use futures::StreamExt;
use lance::Dataset;
use lance::dataset::scanner::Scanner;
use lance_core::ROW_ID;
use lance_linalg::distance::DistanceType;
use serde_json::{Map, Value};
use tracing::Instrument;

use crate::domain::{
    DatasetTarget, DistanceKind, FilterMode, FusedHit, Hit, HybridQuery, ScoreOrder, SearchBackend, SearchError,
    TextQuery, VectorQuery, VectorSearchOutcome, merge_hits,
};
use crate::lance::error::classify_lance_error;
use crate::lance::filter::filter_to_expr;
use crate::lance::provider::DatasetProvider;
use crate::lance::rows::batch_to_json_rows;
use crate::lance::text::text_query_to_fts;
use crate::telemetry::FanoutLeg;

/// Column key under which Lance reports vector distances.
const DISTANCE_KEY: &str = "_distance";

/// Column key under which Lance reports BM25 scores.
const SCORE_KEY: &str = "_score";

/// Search backend executing queries with Lance scanners over datasets from a [`DatasetProvider`].
pub struct LanceSearchBackend<P: DatasetProvider> {
    pub(crate) provider: P,
    pub(crate) prewarm_concurrency: usize,
    pub(crate) fanout_concurrency: usize,
    pub(crate) id_column: String,
    pub(crate) metrics: Arc<crate::telemetry::Metrics>,
}

impl<P: DatasetProvider> LanceSearchBackend<P> {
    /// Creates a backend over the given dataset provider with default concurrency knobs, the
    /// default dedup id column, and telemetry disabled.
    pub fn new(provider: P) -> Self {
        Self {
            provider,
            prewarm_concurrency: crate::config::DEFAULT_PREWARM_CONCURRENCY,
            fanout_concurrency: crate::config::DEFAULT_FANOUT_CONCURRENCY,
            id_column: crate::config::DEFAULT_ID_COLUMN.to_string(),
            metrics: Arc::new(crate::telemetry::Metrics::disabled()),
        }
    }

    /// Sets how many indexes one Prewarm call loads concurrently.
    pub fn with_prewarm_concurrency(mut self, prewarm_concurrency: usize) -> Self {
        self.prewarm_concurrency = prewarm_concurrency.max(1);
        self
    }

    /// Sets how many per-day datasets one date-range fan-out queries concurrently.
    pub fn with_fanout_concurrency(mut self, fanout_concurrency: usize) -> Self {
        self.fanout_concurrency = fanout_concurrency.max(1);
        self
    }

    /// Sets the logical id column used to deduplicate fan-out results across date partitions.
    pub fn with_id_column(mut self, id_column: impl Into<String>) -> Self {
        self.id_column = id_column.into();
        self
    }

    /// Emits backend metrics (prewarm, fan-out, and clusters timings) through the given facade.
    pub fn with_metrics(mut self, metrics: Arc<crate::telemetry::Metrics>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Runs `run` against every existing per-day dataset of the range with bounded concurrency.
    ///
    /// Days whose dataset does not exist are skipped. Returns `NotFound` only when zero datasets
    /// exist in the whole range. Any other per-leg failure fails the call. Each leg runs inside a
    /// `fanout.leg` span and reports its latency to the `fanout.leg.duration_ms` distribution,
    /// on failure as well as on success, so error storms stay visible in the latency breakdown.
    async fn fan_out<T, F, Fut>(
        &self,
        target: &DatasetTarget,
        days: Vec<NaiveDate>,
        leg: FanoutLeg,
        run: F,
    ) -> Result<Vec<T>, SearchError>
    where
        F: Fn(Arc<Dataset>) -> Fut,
        Fut: Future<Output = Result<T, SearchError>>,
        T: Send,
    {
        let days_requested = days.len();
        let run = &run;
        let outcomes: Vec<Result<Option<T>, SearchError>> = futures::stream::iter(days.into_iter().map(|day| {
            let span = tracing::info_span!("fanout.leg", leg.date = %day, leg.kind = leg.as_tag());
            async move {
                let started = Instant::now();
                let dataset = match self.provider.dataset(target, Some(day)).await {
                    Ok(dataset) => dataset,
                    Err(SearchError::NotFound(_)) => {
                        tracing::debug!(leg.date = %day, "fan-out leg skipped, dataset missing");
                        return Ok(None);
                    }
                    Err(error) => return Err(error),
                };
                let result = run(dataset).await;
                self.metrics.fanout_leg_duration(leg, started.elapsed());
                Ok(Some(result?))
            }
            .instrument(span)
        }))
        .buffer_unordered(self.fanout_concurrency.max(1))
        .collect()
        .await;
        let mut legs = Vec::new();
        for outcome in outcomes {
            if let Some(result) = outcome? {
                legs.push(result);
            }
        }
        if legs.is_empty() {
            return Err(SearchError::not_found("no datasets exist in the requested date range"));
        }
        let span = tracing::Span::current();
        span.record("fanout.days", days_requested as u64);
        span.record("fanout.legs", legs.len() as u64);
        self.metrics.fanout_legs(leg, legs.len() as u64);
        Ok(legs)
    }

    /// Merges fan-out legs, recording dedup metrics and span attributes, then strips the id
    /// column from the merged rows when it was added only for deduplication.
    fn merge_fanout_legs(
        &self,
        legs: Vec<Vec<Hit>>,
        order: ScoreOrder,
        k: usize,
        leg: FanoutLeg,
        strip_id: bool,
    ) -> Vec<Hit> {
        let outcome = merge_hits(legs, &self.id_column, order, k);
        tracing::Span::current().record("fanout.dedup_dropped", outcome.duplicates_dropped);
        self.metrics.fanout_dedup_dropped(leg, outcome.duplicates_dropped);
        let mut hits = outcome.hits;
        if strip_id {
            for hit in &mut hits {
                hit.row.remove(&self.id_column);
            }
        }
        hits
    }

    /// Ensures the dedup id column is part of an explicit projection during fan-out.
    ///
    /// Returns true when the column was added (and must be stripped from merged rows). Empty
    /// projections already include every scalar column and are left untouched.
    fn ensure_id_projected(&self, projection: &mut Vec<String>) -> bool {
        if projection.is_empty() || projection.iter().any(|column| column == &self.id_column) {
            return false;
        }
        projection.push(self.id_column.clone());
        true
    }
}

impl<P: DatasetProvider> SearchBackend for LanceSearchBackend<P> {
    #[tracing::instrument(
        name = "backend.vector_search",
        skip_all,
        fields(
            org_id = %target.org_id,
            search.k = query.k,
            fanout.days = tracing::field::Empty,
            fanout.legs = tracing::field::Empty,
            fanout.dedup_dropped = tracing::field::Empty,
        )
    )]
    async fn vector_search(
        &self,
        target: &DatasetTarget,
        mut query: VectorQuery,
    ) -> Result<VectorSearchOutcome, SearchError> {
        let Some(range) = target.date_range else {
            let dataset = self.provider.dataset(target, None).await?;
            let hits = run_vector_query(&dataset, &query).await?;
            return Ok(VectorSearchOutcome {
                hits,
                dataset_version: Some(dataset.version_id()),
            });
        };
        validate_k(query.k)?;
        let strip_id = self.ensure_id_projected(&mut query.projection);
        let query = &query;
        let legs = self
            .fan_out(target, range.days(), FanoutLeg::Vector, |dataset| async move {
                run_vector_query(&dataset, query).await
            })
            .await?;
        Ok(VectorSearchOutcome {
            hits: self.merge_fanout_legs(legs, ScoreOrder::LowerIsBetter, query.k, FanoutLeg::Vector, strip_id),
            dataset_version: None,
        })
    }

    #[tracing::instrument(
        name = "backend.text_search",
        skip_all,
        fields(
            org_id = %target.org_id,
            search.k = query.k,
            fanout.days = tracing::field::Empty,
            fanout.legs = tracing::field::Empty,
            fanout.dedup_dropped = tracing::field::Empty,
        )
    )]
    async fn text_search(&self, target: &DatasetTarget, mut query: TextQuery) -> Result<Vec<Hit>, SearchError> {
        let Some(range) = target.date_range else {
            let dataset = self.provider.dataset(target, None).await?;
            return run_text_query(&dataset, &query).await;
        };
        validate_k(query.k)?;
        let strip_id = self.ensure_id_projected(&mut query.projection);
        let query = &query;
        let legs = self
            .fan_out(target, range.days(), FanoutLeg::Text, |dataset| async move {
                run_text_query(&dataset, query).await
            })
            .await?;
        Ok(self.merge_fanout_legs(legs, ScoreOrder::HigherIsBetter, query.k, FanoutLeg::Text, strip_id))
    }

    #[tracing::instrument(
        name = "backend.hybrid_search",
        skip_all,
        fields(
            org_id = %target.org_id,
            search.k = query.k,
            fanout.days = tracing::field::Empty,
            fanout.legs = tracing::field::Empty,
            fanout.dedup_dropped = tracing::field::Empty,
        )
    )]
    async fn hybrid_search(&self, target: &DatasetTarget, query: HybridQuery) -> Result<Vec<FusedHit>, SearchError> {
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
        let Some(range) = target.date_range else {
            let dataset = self.provider.dataset(target, None).await?;
            let (vector_hits, text_hits) = tokio::join!(
                run_vector_query(&dataset, &vector_query),
                run_text_query(&dataset, &text_query),
            );
            let (vector_hits, text_hits) = (vector_hits?, text_hits?);
            let fuse_span = tracing::info_span!("fusion.fuse", search.k = query.k);
            return Ok(fuse_span.in_scope(|| fusion.fuse(vec![vector_hits, text_hits], query.k)));
        };
        let strip_vector_id = self.ensure_id_projected(&mut vector_query.projection);
        let strip_text_id = self.ensure_id_projected(&mut text_query.projection);
        let strip_id = strip_vector_id || strip_text_id;
        let (vector_query, text_query) = (&vector_query, &text_query);
        let legs: Vec<(Vec<Hit>, Vec<Hit>)> = self
            .fan_out(target, range.days(), FanoutLeg::Hybrid, |dataset| async move {
                let (vector_hits, text_hits) = tokio::join!(
                    run_vector_query(&dataset, vector_query),
                    run_text_query(&dataset, text_query),
                );
                Ok((vector_hits?, text_hits?))
            })
            .await?;
        let (vector_legs, text_legs): (Vec<Vec<Hit>>, Vec<Vec<Hit>>) = legs.into_iter().unzip();
        let merged_vector = self.merge_fanout_legs(
            vector_legs,
            ScoreOrder::LowerIsBetter,
            vector_query.k,
            FanoutLeg::Hybrid,
            false,
        );
        let merged_text = self.merge_fanout_legs(
            text_legs,
            ScoreOrder::HigherIsBetter,
            text_query.k,
            FanoutLeg::Hybrid,
            false,
        );
        let fuse_span = tracing::info_span!("fusion.fuse", search.k = query.k);
        let mut fused = fuse_span.in_scope(|| fusion.fuse(vec![merged_vector, merged_text], query.k));
        if strip_id {
            for hit in &mut fused {
                hit.row.remove(&self.id_column);
            }
        }
        Ok(fused)
    }
}

/// Returns the name of the first fixed-size-list vector column in the dataset schema.
pub fn default_vector_column(dataset: &Dataset) -> Result<String, SearchError> {
    dataset
        .schema()
        .fields
        .iter()
        .find(|field| matches!(field.data_type(), DataType::FixedSizeList(_, _)))
        .map(|field| field.name.clone())
        .ok_or_else(|| SearchError::invalid_argument("dataset has no fixed-size-list vector column"))
}

/// Lists the scalar (non-vector) columns returned by default in search results.
pub fn scalar_output_columns(dataset: &Dataset) -> Vec<String> {
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

/// Applies projection, row-id, typed filter, and limit/offset to a scanner.
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
    scanner.with_row_id();
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

/// Runs one nearest-neighbor query against an open dataset.
#[tracing::instrument(name = "lance.vector_query", skip_all, fields(search.k = query.k))]
async fn run_vector_query(dataset: &Dataset, query: &VectorQuery) -> Result<Vec<Hit>, SearchError> {
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
    rows_to_hits(batch_to_json_rows(&batch)?, DISTANCE_KEY, query.with_row_id)
}

/// Runs one full-text query against an open dataset.
#[tracing::instrument(name = "lance.text_query", skip_all, fields(search.k = query.k))]
async fn run_text_query(dataset: &Dataset, query: &TextQuery) -> Result<Vec<Hit>, SearchError> {
    validate_k(query.k)?;
    let fetch = query.k + query.offset.unwrap_or(0);
    let fts = text_query_to_fts(query, fetch)?;
    let mut scanner = dataset.scan();
    scanner
        .full_text_search(fts)
        .map_err(|err| classify_lance_error(&err))?;
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
    rows_to_hits(batch_to_json_rows(&batch)?, SCORE_KEY, query.with_row_id)
}

/// Converts JSON result rows into hits, extracting the score column and the stable row id.
///
/// The score column is always removed from the row (it travels in a dedicated response field).
/// The row id is kept in the row only when `keep_row_id` is set.
fn rows_to_hits(rows: Vec<Map<String, Value>>, score_key: &str, keep_row_id: bool) -> Result<Vec<Hit>, SearchError> {
    rows.into_iter()
        .map(|mut row| {
            let row_id = row
                .get(ROW_ID)
                .and_then(Value::as_u64)
                .ok_or_else(|| SearchError::internal("search result row is missing its row id"))?;
            if !keep_row_id {
                row.remove(ROW_ID);
            }
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
