//! Typed filter predicate AST.
//!
//! Clients send structured predicates instead of expression strings, so no request can inject
//! expression text. Backends translate this AST into whatever their engine accepts.
//!
//! # Stable JSON serialization
//!
//! The AST carries a serde serialization that is part of the recall-capture contract: the
//! `recall.filter` span attribute holds exactly this JSON and the offline retrieval job parses
//! it. Enums use external tagging with `snake_case` variant names. The shapes are:
//!
//! - `Filter::Compare` — `{"compare": {"column": "id", "op": "gt", "value": {"int": 2}}}`
//! - `Filter::InList` — `{"in_list": {"column": "id", "values": [{"int": 1}], "negated": false}}`
//! - `Filter::IsNull` — `{"is_null": {"column": "id"}}`
//! - `Filter::IsNotNull` — `{"is_not_null": {"column": "id"}}`
//! - `Filter::Between` — `{"between": {"column": "id", "low": {"int": 1}, "high": {"int": 9}}}`
//! - `Filter::And` / `Filter::Or` — `{"and": [<filter>, ...]}` / `{"or": [<filter>, ...]}`
//! - `Filter::Not` — `{"not": <filter>}`
//! - [`Literal`] — `{"bool": true}`, `{"int": 5}`, `{"float": 1.5}`, or `{"string": "a"}`
//! - [`CompareOp`] — one of `"eq"`, `"ne"`, `"lt"`, `"le"`, `"gt"`, `"ge"`

use serde::{Deserialize, Serialize};

/// Comparison operator for a column-vs-literal predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds one filter exercising every AST node, literal type, and a nested composition.
    fn full_ast() -> Filter {
        Filter::And(vec![
            Filter::Compare {
                column: "id".to_string(),
                op: CompareOp::Gt,
                value: Literal::Int(2),
            },
            Filter::Or(vec![
                Filter::InList {
                    column: "label".to_string(),
                    values: vec![Literal::String("a".to_string()), Literal::Bool(true)],
                    negated: true,
                },
                Filter::Between {
                    column: "score".to_string(),
                    low: Literal::Float(0.5),
                    high: Literal::Float(1.5),
                },
            ]),
            Filter::Not(Box::new(Filter::IsNull {
                column: "ts".to_string(),
            })),
            Filter::IsNotNull {
                column: "org".to_string(),
            },
        ])
    }

    #[test]
    fn filter_json_round_trips() {
        let original = full_ast();
        let json = serde_json::to_string(&original).unwrap();
        let parsed: Filter = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn filter_json_shapes_are_stable() {
        let filter = Filter::Compare {
            column: "id".to_string(),
            op: CompareOp::Gt,
            value: Literal::Int(2),
        };
        assert_eq!(
            serde_json::to_string(&filter).unwrap(),
            r#"{"compare":{"column":"id","op":"gt","value":{"int":2}}}"#
        );
        let filter = Filter::Not(Box::new(Filter::IsNull {
            column: "ts".to_string(),
        }));
        assert_eq!(
            serde_json::to_string(&filter).unwrap(),
            r#"{"not":{"is_null":{"column":"ts"}}}"#
        );
        let filter = Filter::And(vec![Filter::IsNotNull {
            column: "org".to_string(),
        }]);
        assert_eq!(
            serde_json::to_string(&filter).unwrap(),
            r#"{"and":[{"is_not_null":{"column":"org"}}]}"#
        );
    }
}
