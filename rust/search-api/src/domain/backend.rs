//! The engine seam: a trait any search backend implements over domain types only.

use crate::domain::error::SearchError;
use crate::domain::query::{FusedHit, Hit, HybridQuery, TextQuery, VectorQuery};

/// Search engine abstraction over per-organization datasets.
///
/// Implementations execute vector, full-text, and hybrid queries expressed purely in domain
/// types; transports stay generic over this trait and never see engine types.
pub trait SearchBackend: Send + Sync + 'static {
    /// Runs a nearest-neighbor query, returning hits ordered nearest-first.
    fn vector_search(
        &self,
        org_id: &str,
        query: VectorQuery,
    ) -> impl Future<Output = Result<Vec<Hit>, SearchError>> + Send;

    /// Runs a full-text query, returning hits ordered best-first.
    fn text_search(&self, org_id: &str, query: TextQuery)
    -> impl Future<Output = Result<Vec<Hit>, SearchError>> + Send;

    /// Runs both legs of a hybrid query and fuses them, returning fused hits ordered best-first.
    fn hybrid_search(
        &self,
        org_id: &str,
        query: HybridQuery,
    ) -> impl Future<Output = Result<Vec<FusedHit>, SearchError>> + Send;
}
