//! Read structural data (centroids, partition metadata) from committed Lance vector indices.

use arrow_array::{Array, Float32Array};
use lance::Dataset;
use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};
use lance_core::{Error, Result};
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::vector::VectorIndex;

/// Extract the IVF centroid vectors from a named IVF-RQ vector index.
///
/// Opens `dataset_uri` as a Lance dataset, locates the committed index named
/// `index_name`, and returns one `Vec<f32>` per IVF partition in partition order.
/// Each inner `Vec` has length equal to the vector dimension.
///
/// # Errors
///
/// Returns an error if the dataset cannot be opened, the index is not found,
/// the index carries no centroid data, or any centroid is not a `Float32Array`.
pub async fn ivf_rq_centroids(dataset_uri: &str, index_name: &str) -> Result<Vec<Vec<f32>>> {
    let dataset = Dataset::open(dataset_uri).await?;

    let metas = dataset.load_indices_by_name(index_name).await?;
    let meta = metas.into_iter().next().ok_or_else(|| {
        Error::invalid_input(format!("vector index '{index_name}' not found in dataset"))
    })?;

    let field_id = *meta.fields.first().ok_or_else(|| {
        Error::invalid_input(format!("index '{index_name}' has no indexed fields"))
    })?;

    let column = dataset.schema().field_path(field_id)?;
    let uuid = meta.uuid.to_string();

    let index = dataset
        .open_vector_index(&column, &uuid, &NoOpMetricsCollector)
        .await?;

    let ivf = index.ivf_model();

    let centroids_array = ivf.centroids_array().ok_or_else(|| {
        Error::invalid_input(format!(
            "index '{index_name}' has no centroid data (index may be incomplete)"
        ))
    })?;

    let num_partitions = ivf.num_partitions();
    let dim = centroids_array.value_length() as usize;
    let mut result: Vec<Vec<f32>> = Vec::with_capacity(num_partitions);

    for partition_id in 0..num_partitions {
        let centroid_ref = ivf.centroid(partition_id).ok_or_else(|| {
            Error::invalid_input(format!(
                "partition {partition_id} centroid is missing in index '{index_name}'"
            ))
        })?;

        let floats = centroid_ref
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "partition {partition_id} centroid is not a Float32Array (got {})",
                    centroid_ref.data_type()
                ))
            })?;

        debug_assert_eq!(floats.len(), dim, "centroid length mismatch at partition {partition_id}");
        result.push(floats.values().to_vec());
    }

    Ok(result)
}
