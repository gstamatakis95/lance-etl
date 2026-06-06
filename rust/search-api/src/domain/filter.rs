//! Typed filter predicate AST.
//!
//! Clients send structured predicates instead of expression strings, so no request can inject
//! expression text. Backends translate this AST into whatever their engine accepts.

/// Comparison operator for a column-vs-literal predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    /// Equal.
    Eq,
    /// Not equal.
    Ne,
    /// Less than.
    Lt,
    /// Less than or equal.
    Le,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    Ge,
}

/// A typed literal used in filter predicates. Never interpreted as an expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// Boolean literal.
    Bool(bool),
    /// 64-bit integer literal.
    Int(i64),
    /// Double literal.
    Float(f64),
    /// String literal, transported verbatim.
    String(String),
}

/// Typed predicate AST evaluated server-side.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// `column <op> value`.
    Compare {
        /// Column name. Validated against the dataset schema at translation time.
        column: String,
        /// Comparison operator.
        op: CompareOp,
        /// Right-hand literal.
        value: Literal,
    },
    /// `column IN (values)` or, when negated, `column NOT IN (values)`.
    InList {
        /// Column name. Validated against the dataset schema at translation time.
        column: String,
        /// Candidate values. Must be non-empty.
        values: Vec<Literal>,
        /// When true, the predicate is `NOT IN`.
        negated: bool,
    },
    /// `column IS NULL`.
    IsNull {
        /// Column name. Validated against the dataset schema at translation time.
        column: String,
    },
    /// `column IS NOT NULL`.
    IsNotNull {
        /// Column name. Validated against the dataset schema at translation time.
        column: String,
    },
    /// `low <= column <= high` (inclusive bounds).
    Between {
        /// Column name. Validated against the dataset schema at translation time.
        column: String,
        /// Inclusive lower bound.
        low: Literal,
        /// Inclusive upper bound.
        high: Literal,
    },
    /// All child filters must hold. Must be non-empty.
    And(Vec<Filter>),
    /// At least one child filter must hold. Must be non-empty.
    Or(Vec<Filter>),
    /// The child filter must not hold.
    Not(Box<Filter>),
}
