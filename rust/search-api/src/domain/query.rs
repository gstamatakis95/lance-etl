//! Domain request and result types for vector, full-text, and hybrid search.
//!
//! The full-text query tree ([`TextQueryNode`] and its leaf specs) carries a serde serialization
//! that is part of the recall-capture contract: the `recall.text_query` span attribute holds
//! exactly this JSON and the offline recall job parses it. Enums use external tagging with
//! `snake_case` variant names, mirroring [`crate::domain::filter`].

use serde::Serialize;
use serde_json::{Map, Value};

use crate::domain::filter::Filter;
use crate::domain::fusion::FusionSpec;

/// Distance metric for nearest-neighbor search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceKind {
    /// Euclidean (L2) distance.
    L2,
    /// Cosine distance.
    Cosine,
    /// Negative dot-product distance.
    Dot,
    /// Hamming distance for binary vectors.
    Hamming,
}

/// An event-time window in epoch milliseconds, always applied to the event-timestamp column.
///
/// The window is start-inclusive and end-exclusive. Either bound may be `None` to leave that side
/// unbounded. A backend translates it into a typed range predicate on its event-timestamp column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TimeRange {
    /// Inclusive lower bound in epoch milliseconds. `None` leaves the window open on the low side.
    pub start_ms: Option<i64>,
    /// Exclusive upper bound in epoch milliseconds. `None` leaves the window open on the high side.
    pub end_ms: Option<i64>,
}

impl TimeRange {
    /// Returns true when at least one bound is set, so the window restricts the scan.
    pub fn is_bounded(&self) -> bool {
        self.start_ms.is_some() || self.end_ms.is_some()
    }
}

/// Whether a filter runs before or after the index search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FilterMode {
    /// Apply the filter before the index search. Exact but potentially more expensive.
    #[default]
    Prefilter,
    /// Apply the filter to the index results. May return fewer rows than requested.
    Postfilter,
}

/// One nearest-neighbor query.
#[derive(Debug, Clone, Default)]
pub struct VectorQuery {
    /// Query vector. Its length must match the vector column dimension.
    pub vector: Vec<f32>,
    /// Number of nearest neighbors to return.
    pub k: usize,
    /// Vector column name. `None` selects the first fixed-size-list column in the schema.
    pub column: Option<String>,
    /// Distance metric override. `None` keeps the index metric.
    pub distance: Option<DistanceKind>,
    /// Sets minimum and maximum probed partitions to the same value (IVF indexes).
    pub nprobes: Option<usize>,
    /// Minimum number of index partitions to probe. Ignored when `nprobes` is set.
    pub minimum_nprobes: Option<usize>,
    /// Maximum number of index partitions to probe. Only effective with a prefilter.
    pub maximum_nprobes: Option<usize>,
    /// Read `refine_factor * k` candidates and re-rank them with the raw vectors.
    pub refine_factor: Option<u32>,
    /// HNSW ef-search parameter.
    pub ef: Option<usize>,
    /// Search only indexed data, skipping fragments added after the last index build (weak
    /// consistency, lower latency). `Some(true)` forces fast search on; `Some(false)` forces it
    /// off (needed for read-after-write freshness guarantees); `None` lets the server apply its
    /// configured default, gated on whether the dataset has a vector index for the queried column.
    pub fast_search: Option<bool>,
    /// Skip the vector index and do a flat (exact) scan.
    pub bypass_vector_index: bool,
    /// Optional typed predicate applied to the search.
    pub filter: Option<Filter>,
    /// Whether the filter runs before or after the index search.
    pub filter_mode: FilterMode,
    /// Optional event-time window applied to the backend's event-timestamp column, ANDed with
    /// `filter`. `None` searches all event times.
    pub time_range: Option<TimeRange>,
    /// Columns to return. Empty selects all non-vector columns.
    pub projection: Vec<String>,
}

/// How match-query terms combine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TextOperator {
    /// At least one term must match.
    #[default]
    Or,
    /// All terms must match.
    And,
}

/// Fuzzy-matching behavior for a match query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Fuzziness {
    /// Exact term matching (edit distance 0).
    #[default]
    Exact,
    /// Pick the edit distance automatically from each term length.
    Auto,
    /// Maximum edit distance for fuzzy matching.
    Distance(u32),
}

/// Terms query against one column.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MatchSpec {
    /// Query terms, tokenized by the index tokenizer.
    pub terms: String,
    /// Column to search. `None` defers to the enclosing query columns or the index.
    pub column: Option<String>,
    /// Score multiplier for this query.
    pub boost: f32,
    /// How terms combine.
    pub operator: TextOperator,
    /// Fuzzy-matching behavior.
    pub fuzziness: Fuzziness,
    /// Maximum number of expanded terms for fuzzy matching. `None` keeps the engine default.
    pub max_expansions: Option<usize>,
    /// Number of beginning characters kept unchanged for fuzzy matching.
    pub prefix_length: u32,
}

impl MatchSpec {
    /// Builds a match query over `terms` with default knobs.
    pub fn new(terms: impl Into<String>) -> Self {
        Self {
            terms: terms.into(),
            column: None,
            boost: 1.0,
            operator: TextOperator::default(),
            fuzziness: Fuzziness::default(),
            max_expansions: None,
            prefix_length: 0,
        }
    }
}

/// Exact phrase query. The index must store positions.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PhraseSpec {
    /// Phrase terms in order.
    pub terms: String,
    /// Column to search. `None` defers to the enclosing query columns or the index.
    pub column: Option<String>,
    /// Maximum number of intervening unmatched positions allowed.
    pub slop: u32,
}

/// Full-text query node tree.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TextQueryNode {
    /// Terms matching with OR/AND semantics and optional fuzziness.
    Match(MatchSpec),
    /// Exact phrase matching.
    Phrase(PhraseSpec),
    /// Re-weights hits of a negative query against a positive query.
    Boost {
        /// Query whose hits keep full score.
        positive: Box<TextQueryNode>,
        /// Query whose hits are down-weighted.
        negative: Box<TextQueryNode>,
        /// Multiplier applied to negative hits.
        negative_boost: f32,
    },
    /// The same terms matched over several columns with per-column boosts.
    MultiMatch {
        /// Query terms.
        terms: String,
        /// Columns to search. Must be non-empty.
        columns: Vec<String>,
        /// Per-column boosts. Empty means 1.0 everywhere, otherwise one boost per column.
        boosts: Vec<f32>,
        /// How terms combine within each column.
        operator: TextOperator,
    },
    /// Boolean combination of sub-queries.
    Boolean {
        /// Optional clauses. Matching them increases the score.
        should: Vec<TextQueryNode>,
        /// Required clauses. Every hit must match all of them.
        must: Vec<TextQueryNode>,
        /// Excluding clauses. Hits matching any of them are dropped.
        must_not: Vec<TextQueryNode>,
    },
}

/// One full-text query with execution parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct TextQuery {
    /// The query node tree.
    pub node: TextQueryNode,
    /// Columns filled into query nodes that do not name a column themselves.
    pub columns: Vec<String>,
    /// Maximum number of hits to return.
    pub k: usize,
    /// WAND ranking factor. `None` keeps the engine default.
    pub wand_factor: Option<f32>,
    /// Optional typed predicate applied to the search.
    pub filter: Option<Filter>,
    /// Whether the filter runs before or after the index search.
    pub filter_mode: FilterMode,
    /// Optional event-time window applied to the backend's event-timestamp column, ANDed with
    /// `filter`. `None` searches all event times.
    pub time_range: Option<TimeRange>,
    /// Columns to return. Empty selects all non-vector columns.
    pub projection: Vec<String>,
    /// Search only indexed (INVERTED) data, skipping fragments appended after the last FTS index
    /// build. `Some(true)` forces fast search on; `Some(false)` forces it off; `None` lets the
    /// server apply its configured default, gated on whether the dataset has an FTS index for the
    /// queried column. Fragments appended after the last index build are silently excluded when
    /// `true` — callers that need read-after-write freshness must set `Some(false)` or leave it
    /// `None` on datasets where the server default is off.
    pub fast_search: Option<bool>,
}

impl TextQuery {
    /// Builds a simple match-string query with default knobs.
    pub fn simple(terms: impl Into<String>, k: usize) -> Self {
        Self {
            node: TextQueryNode::Match(MatchSpec::new(terms)),
            columns: Vec::new(),
            k,
            wand_factor: None,
            filter: None,
            filter_mode: FilterMode::default(),
            time_range: None,
            projection: Vec::new(),
            fast_search: None,
        }
    }
}

/// One hybrid query: a vector leg, a text leg, and a fusion strategy.
#[derive(Debug, Clone)]
pub struct HybridQuery {
    /// Vector leg parameters. `k == 0` inherits the fused `k`.
    pub vector: VectorQuery,
    /// Text leg parameters. `k == 0` inherits the fused `k`.
    pub text: TextQuery,
    /// Number of fused results to return.
    pub k: usize,
    /// Fusion strategy for merging the legs.
    pub fusion: FusionSpec,
}

/// Closed reason code accompanying a permitted partial response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchWarning {
    /// Deduplication left fewer than the requested number of logical results.
    ResultsUnderfilled,
}

/// The result of one vector search: ranked hits plus dataset provenance for recall capture.
#[derive(Debug, Clone, Default)]
pub struct VectorSearchOutcome {
    /// Hits ordered nearest-first.
    pub hits: Vec<Hit>,
    /// Exact committed Lance version selected by the serving catalog.
    pub served_version: u64,
    /// True only when `warnings` describes a permitted degraded result.
    pub partial: bool,
    /// Bounded closed warning codes.
    pub warnings: Vec<SearchWarning>,
}

/// The result of one full-text search: ranked hits plus dataset provenance for recall capture.
#[derive(Debug, Clone, Default)]
pub struct TextSearchOutcome {
    /// Hits ordered best-first.
    pub hits: Vec<Hit>,
    /// Exact committed Lance version selected by the serving catalog.
    pub served_version: u64,
    /// True only when `warnings` describes a permitted degraded result.
    pub partial: bool,
    /// Bounded closed warning codes.
    pub warnings: Vec<SearchWarning>,
}

/// The result of one hybrid search: fused hits plus dataset provenance for recall capture.
#[derive(Debug, Clone, Default)]
pub struct HybridSearchOutcome {
    /// Fused hits ordered best-first.
    pub hits: Vec<FusedHit>,
    /// Exact committed Lance version selected by the serving catalog.
    pub served_version: u64,
    /// True only when `warnings` describes a permitted degraded result.
    pub partial: bool,
    /// Bounded closed warning codes.
    pub warnings: Vec<SearchWarning>,
}

/// One ranked hit from a single search leg.
#[derive(Debug, Clone)]
pub struct Hit {
    /// Stable logical record identifier.
    pub record_id: String,
    /// Leg-specific score: distance for vector legs, BM25 score for text legs.
    pub score: f64,
    /// Projected columns of the row as a JSON object.
    pub row: Map<String, Value>,
}

/// One fused hit produced by a [`crate::domain::fusion::FusionSpec`] strategy.
#[derive(Debug, Clone)]
pub struct FusedHit {
    /// Stable logical record identifier.
    pub record_id: String,
    /// Fused score (larger is better).
    pub score: f64,
    /// Union of the projected columns of the legs that contained the row.
    pub row: Map<String, Value>,
}
