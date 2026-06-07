//! Post-fusion reranking: the declarative rerank configuration, the request context, and the
//! [`Reranker`] seam any reranking engine implements over domain types only.
//!
//! Reranking runs after a vector, text, or hybrid result is produced and reorders (or truncates)
//! the top candidates. The default [`IdentityReranker`] preserves the incoming order, so behavior
//! is unchanged unless a request carries a [`RerankSpec`]. The trait is async and fallible so a
//! cross-encoder or LLM reranker can slot in later without touching the layers around it; its
//! errors map onto a transport status at the boundary that invokes it.

use async_trait::async_trait;

use crate::domain::error::SearchError;
use crate::domain::query::FusedHit;

/// Declarative reranking configuration carried by a search request.
///
/// New strategies (cross-encoder, LLM judge, ...) slot in as variants. The transport maps the
/// additive proto `Rerank` message onto this enum, and an engine implementing [`Reranker`]
/// interprets it.
#[derive(Debug, Clone, PartialEq)]
pub enum RerankSpec {
    /// Identity reranking: keep the incoming candidate order, optionally truncating to `top_n`.
    Identity {
        /// Number of leading candidates to keep. `None` keeps all.
        top_n: Option<usize>,
    },
}

/// The query-side context handed to a [`Reranker`] alongside the candidate hits.
///
/// A cross-encoder or LLM reranker scores `(query_text, document)` pairs: the document text travels
/// in each candidate's projected row, and the query text and target size travel here.
#[derive(Debug, Clone, PartialEq)]
pub struct RerankRequest {
    /// The chosen reranking strategy.
    pub spec: RerankSpec,
    /// The query text, when the originating search carried one (text and hybrid legs). `None` for
    /// pure vector search.
    pub query_text: Option<String>,
    /// The number of results the caller ultimately wants.
    pub k: usize,
}

/// Reranking engine seam: reorders post-fusion candidates expressed purely in domain types.
///
/// Implementations may be asynchronous (a network call to a model server) and may fail; the
/// transport maps [`SearchError`] onto its status. The default [`IdentityReranker`] is infallible
/// and synchronous in effect.
#[async_trait]
pub trait Reranker: Send + Sync + 'static {
    /// Reranks `hits` (ordered best-first) under `request`, returning the reordered candidates.
    async fn rerank(&self, request: &RerankRequest, hits: Vec<FusedHit>) -> Result<Vec<FusedHit>, SearchError>;
}

/// The default no-op reranker: preserves the incoming order, honoring only `Identity { top_n }`.
#[derive(Debug, Clone, Copy, Default)]
pub struct IdentityReranker;

#[async_trait]
impl Reranker for IdentityReranker {
    /// Returns the candidates unchanged, truncating to `top_n` when the identity spec sets one.
    async fn rerank(&self, request: &RerankRequest, mut hits: Vec<FusedHit>) -> Result<Vec<FusedHit>, SearchError> {
        let RerankSpec::Identity { top_n } = request.spec;
        if let Some(top_n) = top_n {
            hits.truncate(top_n);
        }
        Ok(hits)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value};

    use super::*;

    /// Builds a fused hit with the given row id and fused score.
    fn fused(row_id: u64, score: f64) -> FusedHit {
        let mut row = Map::new();
        row.insert("id".to_string(), Value::from(row_id));
        FusedHit { row_id, score, row }
    }

    #[tokio::test]
    async fn identity_preserves_order() {
        let hits = vec![fused(1, 0.9), fused(2, 0.5), fused(3, 0.1)];
        let request = RerankRequest {
            spec: RerankSpec::Identity { top_n: None },
            query_text: None,
            k: 3,
        };
        let reranked = IdentityReranker.rerank(&request, hits).await.unwrap();
        let ids: Vec<u64> = reranked.iter().map(|hit| hit.row_id).collect();
        assert_eq!(ids, vec![1, 2, 3], "identity must not reorder candidates");
    }

    #[tokio::test]
    async fn identity_truncates_to_top_n() {
        let hits = vec![fused(1, 0.9), fused(2, 0.5), fused(3, 0.1)];
        let request = RerankRequest {
            spec: RerankSpec::Identity { top_n: Some(2) },
            query_text: Some("apple".to_string()),
            k: 3,
        };
        let reranked = IdentityReranker.rerank(&request, hits).await.unwrap();
        let ids: Vec<u64> = reranked.iter().map(|hit| hit.row_id).collect();
        assert_eq!(ids, vec![1, 2], "top_n must keep the leading candidates in order");
    }

    /// A reranker that always fails, exercising the fallible seam.
    struct FailingReranker;

    #[async_trait]
    impl Reranker for FailingReranker {
        async fn rerank(&self, _request: &RerankRequest, _hits: Vec<FusedHit>) -> Result<Vec<FusedHit>, SearchError> {
            Err(SearchError::internal("reranker model unavailable"))
        }
    }

    #[tokio::test]
    async fn fallible_reranker_surfaces_its_error() {
        let request = RerankRequest {
            spec: RerankSpec::Identity { top_n: None },
            query_text: None,
            k: 1,
        };
        let error = FailingReranker.rerank(&request, vec![fused(1, 0.1)]).await.unwrap_err();
        assert_eq!(error, SearchError::internal("reranker model unavailable"));
    }
}
