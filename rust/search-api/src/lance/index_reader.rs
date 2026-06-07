//! Read structural data (IVF centroids, partition metadata) from committed Lance vector indices,
//! and the [`ClusterReader`] implementation of the backend.

use std::time::Instant;

use arrow_array::{Array, Float32Array};
use lance::Dataset;
use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};
use lance_index::is_system_index;
use lance_index::metrics::NoOpMetricsCollector;

use crate::domain::{ClusterReader, ClusterReport, ClusterSpec, DatasetRef, DatasetTarget, SearchError};
use crate::lance::backend::LanceSearchBackend;
use crate::lance::error::classify_lance_error;
use crate::lance::prewarm::VECTOR_DETAILS_SUFFIX;
use crate::lance::provider::DatasetProvider;

/// Extract the IVF centroid vectors from a vector index of an open dataset.
///
/// Locates the committed index named `index_name` (or the dataset's only vector index when
/// `None`), opens it through the shared session caches, and returns one centroid per IVF
/// partition in partition order. Each centroid has length equal to the vector dimension.
///
/// # Errors
///
/// Returns `NotFound` when the index does not exist (or no vector index exists for the default),
/// `InvalidArgument` when the name resolves to a non-vector index, the default is ambiguous, or
/// the index carries no centroid data, and an engine-classified error for everything else.
async fn ivf_centroids(dataset: &Dataset, index_name: Option<&str>) -> Result<ClusterReport, SearchError> {
    let name = match index_name {
        Some(name) => name.to_string(),
        None => default_vector_index(dataset).await?,
    };
    let metas = dataset
        .load_indices_by_name(&name)
        .await
        .map_err(|err| classify_lance_error(&err))?;
    let meta = metas
        .into_iter()
        .next()
        .ok_or_else(|| SearchError::not_found(format!("vector index {name:?} not found in dataset")))?;
    let is_vector = meta
        .index_details
        .as_ref()
        .is_some_and(|details| details.type_url.ends_with(VECTOR_DETAILS_SUFFIX));
    if !is_vector {
        return Err(SearchError::invalid_argument(format!(
            "index {name:?} is not a vector index"
        )));
    }
    let field_id = *meta
        .fields
        .first()
        .ok_or_else(|| SearchError::invalid_argument(format!("index {name:?} has no indexed fields")))?;
    let column = dataset
        .schema()
        .field_path(field_id)
        .map_err(|err| classify_lance_error(&err))?;
    let uuid = meta.uuid.to_string();
    let index = dataset
        .open_vector_index(&column, &uuid, &NoOpMetricsCollector)
        .await
        .map_err(|err| classify_lance_error(&err))?;
    let ivf = index.ivf_model();
    if ivf.centroids_array().is_none() {
        return Err(SearchError::invalid_argument(format!(
            "index {name:?} has no centroid data (index may be incomplete)"
        )));
    }
    let num_partitions = ivf.num_partitions();
    let dimension = ivf.dimension();
    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(num_partitions);
    for partition_id in 0..num_partitions {
        let centroid = ivf.centroid(partition_id).ok_or_else(|| {
            SearchError::internal(format!(
                "partition {partition_id} centroid is missing in index {name:?}"
            ))
        })?;
        let floats = centroid.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
            SearchError::internal(format!(
                "partition {partition_id} centroid is not a Float32Array (got {})",
                centroid.data_type()
            ))
        })?;
        centroids.push(floats.values().to_vec());
    }
    Ok(ClusterReport {
        centroids,
        dimension,
        index_name: name,
    })
}

/// Resolves the dataset's vector index name when the request did not name one.
///
/// Exactly one vector index (by distinct name, deltas collapse) must exist for the default to
/// apply. Zero is `NotFound`, several are `InvalidArgument`.
async fn default_vector_index(dataset: &Dataset) -> Result<String, SearchError> {
    let listed = dataset.load_indices().await.map_err(|err| classify_lance_error(&err))?;
    let mut names: Vec<String> = listed
        .iter()
        .filter(|meta| !is_system_index(meta))
        .filter(|meta| {
            meta.index_details
                .as_ref()
                .is_some_and(|details| details.type_url.ends_with(VECTOR_DETAILS_SUFFIX))
        })
        .map(|meta| meta.name.clone())
        .collect();
    names.sort();
    names.dedup();
    match names.len() {
        0 => Err(SearchError::not_found("dataset has no vector index")),
        1 => Ok(names.remove(0)),
        _ => Err(SearchError::invalid_argument(format!(
            "dataset has several vector indexes ({}), name one explicitly",
            names.join(", ")
        ))),
    }
}

impl<P: DatasetProvider> ClusterReader for LanceSearchBackend<P> {
    #[tracing::instrument(
        name = "backend.clusters",
        skip_all,
        fields(
            org_id = %target.org_id,
            clusters.index = tracing::field::Empty,
            clusters.count = tracing::field::Empty,
        )
    )]
    async fn clusters(&self, target: &DatasetTarget, spec: ClusterSpec) -> Result<ClusterReport, SearchError> {
        let dataset = self.provider.dataset(target, DatasetRef::Serve).await?;
        let started = Instant::now();
        let report = ivf_centroids(&dataset, spec.index_name.as_deref()).await?;
        self.metrics.clusters_read(started.elapsed());
        self.metrics.clusters_centroids(report.num_partitions() as u64);
        let span = tracing::Span::current();
        span.record("clusters.index", report.index_name.as_str());
        span.record("clusters.count", report.num_partitions() as u64);
        Ok(report)
    }
}
