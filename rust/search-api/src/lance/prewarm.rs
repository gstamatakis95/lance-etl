//! Lance implementation of the domain [`Prewarmer`] trait.

use std::sync::Arc;
use std::time::{Duration, Instant};

use lance::Dataset;
use lance::index::DatasetIndexExt;
use lance_index::{FtsPrewarmOptions, PrewarmOptions, is_system_index};
use tracing::Instrument;

use crate::domain::{DatasetRef, DatasetTarget, PrewarmReport, PrewarmSpec, PrewarmedIndex, Prewarmer, SearchError};
use crate::lance::backend::LanceSearchBackend;
use crate::lance::error::classify_lance_error;
use crate::lance::provider::DatasetProvider;
use crate::telemetry::{Metrics, PrewarmIndexKind, PrewarmStatus};

/// Type-URL suffix marking inverted (FTS) index segments in the manifest.
pub(crate) const INVERTED_DETAILS_SUFFIX: &str = "InvertedIndexDetails";

/// Type-URL suffix marking vector index segments in the manifest.
pub(crate) const VECTOR_DETAILS_SUFFIX: &str = "VectorIndexDetails";

/// Warms one dataset's caches by opening it through the shared session (manifest, transaction,
/// and index-listing metadata) and then prewarming the requested indexes.
///
/// The target must address exactly one dataset: a date range, when present, has to cover a
/// single day.
///
/// A spec that requests neither metadata nor any index ([`PrewarmSpec::is_noop`]) short-circuits
/// into an empty report without opening the dataset, since opening is the only thing that warms
/// metadata.
///
/// Memory budget note: BTree/IVF prewarm loads every page/partition. With the disk index cache
/// backend the in-memory hot tier evicts under its Moka budget while the serialized copies stay
/// on disk, which is exactly the desired outcome for cold-process warmups.
impl<P: DatasetProvider> Prewarmer for LanceSearchBackend<P> {
    #[tracing::instrument(
        name = "backend.prewarm",
        skip_all,
        fields(org_id = %target.org_id, prewarm.resolved_version = tracing::field::Empty)
    )]
    async fn prewarm(
        &self,
        target: &DatasetTarget,
        spec: PrewarmSpec,
        reference: DatasetRef,
    ) -> Result<PrewarmReport, SearchError> {
        let total_start = Instant::now();
        if spec.is_noop() {
            self.metrics.prewarm(PrewarmStatus::Ok, total_start.elapsed());
            return Ok(PrewarmReport {
                metadata_warmed: false,
                indexes: Vec::new(),
                metadata_duration: Duration::ZERO,
                total_duration: total_start.elapsed(),
                index_cache_size_bytes: self.provider.index_cache_size_bytes(),
                resolved_version: 0,
            });
        }
        let date = target.single_date()?;
        let dataset = match self.provider.dataset(target, date, reference).await {
            Ok(dataset) => dataset,
            Err(error) => {
                self.metrics.prewarm(PrewarmStatus::Error, total_start.elapsed());
                return Err(error);
            }
        };
        let resolved_version = dataset.version_id();
        tracing::Span::current().record("prewarm.resolved_version", resolved_version);
        self.metrics.prewarm_last_version(resolved_version);
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
            resolved_version,
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
            org_id = %target.org_id,
            status = status.as_tag(),
            indexes_warmed = warmed,
            resolved_version,
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

/// Prewarms the indexes selected by the spec with bounded concurrency. Per-index failures are
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
                let permit = match semaphore.acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => {
                        return PrewarmedIndex {
                            name,
                            duration: Duration::ZERO,
                            error: Some("prewarm semaphore closed before the index could be warmed".to_string()),
                        };
                    }
                };
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
