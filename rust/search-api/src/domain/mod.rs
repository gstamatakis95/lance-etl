//! Transport- and engine-agnostic core: request/response types, the search traits, and errors.
//!
//! Nothing in this module references protobuf, tonic, or Lance types, so alternative transports
//! and backends can be wired in without touching it.

pub mod backend;
pub mod error;
pub mod filter;
pub mod fusion;
pub mod prewarm;
pub mod query;

pub use backend::SearchBackend;
pub use error::SearchError;
pub use filter::{CompareOp, Filter, Literal};
pub use fusion::{Fusion, FusionSpec, RrfFusion};
pub use prewarm::{PrewarmReport, PrewarmSpec, PrewarmedIndex, Prewarmer};
pub use query::{
    DistanceKind, FilterMode, FusedHit, Fuzziness, Hit, HybridQuery, MatchSpec, PhraseSpec, TextOperator, TextQuery,
    TextQueryNode, VectorQuery,
};
