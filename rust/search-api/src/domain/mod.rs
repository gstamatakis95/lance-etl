//! Transport- and engine-agnostic core: request/response types, the service traits, and errors.
//!
//! Nothing in this module references protobuf, tonic, or Lance types, so alternative transports
//! and backends can be wired in without touching it.
//!
//! Submodules:
//! - [`target`]: dataset addressing (org/tenant/namespace plus the optional day range).
//! - [`query`]: vector, full-text, and hybrid request/result types.
//! - [`filter`]: the typed predicate AST replacing raw SQL strings.
//! - [`fusion`]: hybrid leg fusion strategies (RRF).
//! - [`merge`]: dedup-by-id merging of date-range fan-out legs.
//! - [`backend`]: the [`SearchBackend`] trait every engine implements.
//! - [`prewarm`]: cache prewarming types and the [`Prewarmer`] trait.
//! - [`clusters`]: IVF centroid introspection types and the [`ClusterReader`] trait.
//! - [`error`]: the single domain error type shared below the transport.

pub mod backend;
pub mod clusters;
pub mod error;
pub mod filter;
pub mod fusion;
pub mod merge;
pub mod prewarm;
pub mod query;
pub mod target;

pub use backend::SearchBackend;
pub use clusters::{ClusterReader, ClusterReport, ClusterSpec};
pub use error::SearchError;
pub use filter::{CompareOp, Filter, Literal};
pub use fusion::{Fusion, FusionSpec, RrfFusion};
pub use merge::{MergeOutcome, ScoreOrder, merge_hits};
pub use prewarm::{PrewarmReport, PrewarmSpec, PrewarmedIndex, Prewarmer};
pub use query::{
    DistanceKind, FilterMode, FusedHit, Fuzziness, Hit, HybridQuery, MatchSpec, PhraseSpec, TextOperator, TextQuery,
    TextQueryNode, VectorQuery,
};
pub use target::{DatasetTarget, DateRange, MAX_DATE_RANGE_DAYS};
