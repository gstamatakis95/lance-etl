//! IVF cluster introspection: domain types and the trait transports call to read centroids.

use crate::domain::error::SearchError;
use crate::domain::target::DatasetTarget;

/// What a Clusters call should read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterSpec {
    /// Vector index to read. `None` selects the dataset's vector index (there must be exactly
    /// one candidate for the default to apply).
    pub index_name: Option<String>,
}

/// Centroids of one IVF vector index.
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterReport {
    /// One centroid per IVF partition, in partition order.
    pub centroids: Vec<Vec<f32>>,
    /// Vector dimension of every centroid.
    pub dimension: usize,
    /// Name of the index the centroids were read from.
    pub index_name: String,
}

impl ClusterReport {
    /// Number of IVF partitions (equals the number of centroids).
    pub fn num_partitions(&self) -> usize {
        self.centroids.len()
    }
}

/// Cluster reading abstraction. Transports stay generic over this trait next to `SearchBackend`,
/// so the gRPC layer never references engine types.
pub trait ClusterReader: Send + Sync + 'static {
    /// Reads the IVF centroids of the targeted dataset's vector index.
    ///
    /// The target must address exactly one dataset: a date range, when present, has to cover a
    /// single day.
    fn clusters(
        &self,
        target: &DatasetTarget,
        spec: ClusterSpec,
    ) -> impl Future<Output = Result<ClusterReport, SearchError>> + Send;
}
