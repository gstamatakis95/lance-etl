//! Lance implementation of the domain [`Prewarmer`] trait.

use std::sync::Arc;
use std::time::Instant;

use lance::Dataset;
use lance::index::DatasetIndexExt;
use lance_index::{FtsPrewarmOptions, PrewarmOptions, is_system_index};
use tracing::Instrument;

use crate::domain::{PrewarmReport, PrewarmSpec, PrewarmedIndex, Prewarmer, SearchError};
use crate::lance::backend::LanceSearchBackend;
use crate::lance::error::classify_lance_error;
use crate::lance::provider::DatasetProvider;
use crate::telemetry::{Metrics, PrewarmIndexKind, PrewarmStatus};

/// Type-URL suffix marking inverted (FTS) index segments in the manifest.
const INVERTED_DETAILS_SUFFIX: &str = "InvertedIndexDetails";

/// Type-URL suffix marking vector index segments in the manifest.
const VECTOR_DETAILS_SUFFIX: &str = "VectorIndexDetails";

/// Warms one org's caches by opening the dataset through the shared session (manifest,
/// transaction, and index-listing metadata) and then prewarming the requested indexes.
///
/// Memory budget note: BTree/IVF prewarm loads every page/partition. With the disk index cache
/// backend the in-memory hot tier evicts under its Moka budget while the serialized copies stay
/// on disk, which is exactly the desired outcome for cold-process warmups.
impl<P: DatasetProvider> Prewarmer for LanceSearchBackend<P> {
    #[tracing::instrument(name = "backend.prewarm", skip_all, fields(org_id = %org_id))]
    async fn prewarm(&self, org_id: &str, spec: PrewarmSpec) -> Result<PrewarmReport, SearchError> {
        let total_start = Instant::now();
        let dataset = match self.provider.dataset(org_id).await {
            Ok(dataset) => dataset,
            Err(error) => {
                self.metrics.prewarm(PrewarmStatus::Error, total_start.elapsed());
                return Err(error);
            }
        };
        let metadata_duration = total_start.elapsed();
        let mut indexes = Vec::new();
        if spec.wants_indexes() {
            indexes = match prewarm_indexes(&dataset, &spec, self.prewarm_concurrency, &self.metrics).await {
                Ok(indexes) => indexes,
                Err(error) => {
                    self.metrics.prewarm(PrewarmStatus::Error, total_start.elapsed());
                    return Err(error);
                }
            };
        }
        let report = PrewarmReport {
            metadata_warmed: true,
            indexes,
            metadata_duration,
            total_duration: total_start.elapsed(),
            index_cache_size_bytes: self.provider.index_cache_size_bytes(),
        };
        let status = if report.indexes.iter().any(|index| index.error.is_some()) {
            PrewarmStatus::Partial
        } else {
            PrewarmStatus::Ok
        };
        let warmed = report.indexes.iter().filter(|index| index.error.is_none()).count() as u64;
        self.metrics.prewarm(status, report.total_duration);
        self.metrics.prewarm_indexes_warmed(warmed);
        self.metrics.prewarm_warmed_bytes(report.index_cache_size_bytes);
        tracing::info!(
            org_id = %org_id,
            status = status.as_tag(),
            indexes_warmed = warmed,
            duration_ms = report.total_duration.as_millis() as u64,
            "prewarm finished"
        );
        Ok(report)
    }
}

/// Classifies one listed index by its manifest details type URL for the `kind` metric tag.
fn index_kind(details_type_url: Option<&str>) -> PrewarmIndexKind {
    match details_type_url {
        Some(url) if url.ends_with(INVERTED_DETAILS_SUFFIX) => PrewarmIndexKind::Fts,
        Some(url) if url.ends_with(VECTOR_DETAILS_SUFFIX) => PrewarmIndexKind::Vector,
        _ => PrewarmIndexKind::Scalar,
    }
}

/// Prewarms the indexes selected by the spec with bounded concurrency; per-index failures are
/// reported in the result instead of failing the whole org.
async fn prewarm_indexes(
    dataset: &Arc<Dataset>,
    spec: &PrewarmSpec,
    concurrency: usize,
    metrics: &Arc<Metrics>,
) -> Result<Vec<PrewarmedIndex>, SearchError> {
    let listed = dataset.load_indices().await.map_err(|err| classify_lance_error(&err))?;
    let user_indexes: Vec<_> = listed.iter().filter(|meta| !is_system_index(meta)).collect();
    let available: Vec<String> = user_indexes.iter().map(|meta| meta.name.clone()).collect();
    let targets = spec.resolve_targets(&available);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let mut tasks = tokio::task::JoinSet::new();
    for name in targets {
        let details_url = user_indexes
            .iter()
            .find(|meta| meta.name == name)
            .and_then(|meta| meta.index_details.as_ref())
            .map(|details| details.type_url.clone());
        let kind = index_kind(details_url.as_deref());
        let with_position = spec.fts_with_position && kind == PrewarmIndexKind::Fts;
        let dataset = dataset.clone();
        let semaphore = semaphore.clone();
        let metrics = metrics.clone();
        let span = tracing::info_span!("prewarm.index", index.name = %name, index.kind = kind.as_tag());
        tasks.spawn(
            async move {
                let permit = semaphore.acquire_owned().await;
                let start = Instant::now();
                let outcome = if with_position {
                    dataset
                        .prewarm_index_with_options(
                            &name,
                            &PrewarmOptions::Fts(FtsPrewarmOptions::new().with_position(true)),
                        )
                        .await
                } else {
                    dataset.prewarm_index(&name).await
                };
                drop(permit);
                let duration = start.elapsed();
                metrics.prewarm_index(kind, duration);
                PrewarmedIndex {
                    name,
                    duration,
                    error: outcome
                        .err()
                        .map(|err| classify_lance_error(&err).message().to_string()),
                }
            }
            .instrument(span),
        );
    }
    let mut results = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(outcome) => results.push(outcome),
            Err(join_error) => return Err(SearchError::internal(format!("prewarm task failed: {join_error}"))),
        }
    }
    Ok(results)
}
