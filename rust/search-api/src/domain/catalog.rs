//! Exact serving-catalog types and lookup abstraction.

use async_trait::async_trait;

use crate::domain::{DatasetTarget, SearchError};

/// Exact immutable Lance location selected for one logical target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServingRoute {
    /// Exact physical Lance dataset URI.
    pub lance_uri: String,
    /// Exact committed Lance version that may be served.
    pub lance_version: u64,
}

/// Resolves a validated logical target to its exact published serving tuple.
#[async_trait]
pub trait ServingCatalog: Send + Sync + 'static {
    /// Returns the exact URI and version currently published for `target`.
    async fn resolve(&self, target: &DatasetTarget) -> Result<ServingRoute, SearchError>;
}
