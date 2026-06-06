//! Translation of the domain filter AST into a DataFusion logical expression.
//!
//! Column names are validated with a strict identifier allowlist and against the dataset schema,
//! and literals become typed `lit` expressions, so no client input is ever parsed as SQL.

use std::collections::HashSet;

use lance::deps::datafusion::common::Column;
use lance::deps::datafusion::logical_expr::{Expr, lit, not};

use crate::domain::{CompareOp, Filter, Literal, SearchError};

/// Maximum nesting depth accepted for a filter AST.
const MAX_FILTER_DEPTH: usize = 32;

/// Translates a domain filter into a DataFusion expression against the given schema columns.
///
/// `allowed_columns` is the set of top-level column names of the dataset schema; any reference
/// outside it (or outside the `[A-Za-z_][A-Za-z0-9_]*` identifier shape) is rejected.
pub fn filter_to_expr(filter: &Filter, allowed_columns: &HashSet<String>) -> Result<Expr, SearchError> {
    translate(filter, allowed_columns, 0)
}

/// Recursive worker for [`filter_to_expr`] tracking nesting depth.
fn translate(filter: &Filter, allowed_columns: &HashSet<String>, depth: usize) -> Result<Expr, SearchError> {
    if depth > MAX_FILTER_DEPTH {
        return Err(SearchError::invalid_argument(format!(
            "filter nesting exceeds the maximum depth of {MAX_FILTER_DEPTH}"
        )));
    }
    match filter {
        Filter::Compare { column, op, value } => {
            let column_expr = column_ref(column, allowed_columns)?;
            let value_expr = literal_to_expr(value);
            Ok(match op {
                CompareOp::Eq => column_expr.eq(value_expr),
                CompareOp::Ne => column_expr.not_eq(value_expr),
                CompareOp::Lt => column_expr.lt(value_expr),
                CompareOp::Le => column_expr.lt_eq(value_expr),
                CompareOp::Gt => column_expr.gt(value_expr),
                CompareOp::Ge => column_expr.gt_eq(value_expr),
            })
        }
        Filter::InList {
            column,
            values,
            negated,
        } => {
            if values.is_empty() {
                return Err(SearchError::invalid_argument("in_list requires at least one value"));
            }
            let column_expr = column_ref(column, allowed_columns)?;
            let value_exprs = values.iter().map(literal_to_expr).collect();
            Ok(column_expr.in_list(value_exprs, *negated))
        }
        Filter::IsNull { column } => Ok(column_ref(column, allowed_columns)?.is_null()),
        Filter::IsNotNull { column } => Ok(column_ref(column, allowed_columns)?.is_not_null()),
        Filter::Between { column, low, high } => {
            let column_expr = column_ref(column, allowed_columns)?;
            Ok(column_expr.between(literal_to_expr(low), literal_to_expr(high)))
        }
        Filter::And(children) => combine(children, allowed_columns, depth, "and", Expr::and),
        Filter::Or(children) => combine(children, allowed_columns, depth, "or", Expr::or),
        Filter::Not(child) => Ok(not(translate(child, allowed_columns, depth + 1)?)),
    }
}

/// Folds the children of an AND/OR node with the given combinator.
fn combine(
    children: &[Filter],
    allowed_columns: &HashSet<String>,
    depth: usize,
    name: &str,
    join: fn(Expr, Expr) -> Expr,
) -> Result<Expr, SearchError> {
    let mut translated = children
        .iter()
        .map(|child| translate(child, allowed_columns, depth + 1))
        .collect::<Result<Vec<Expr>, SearchError>>()?
        .into_iter();
    let first = translated
        .next()
        .ok_or_else(|| SearchError::invalid_argument(format!("{name} requires at least one child filter")))?;
    Ok(translated.fold(first, join))
}

/// Builds a validated column reference expression.
///
/// The name must look like a plain identifier and exist in the dataset schema; the reference is
/// constructed as an unqualified [`Column`] so the name is never parsed as an expression.
fn column_ref(name: &str, allowed_columns: &HashSet<String>) -> Result<Expr, SearchError> {
    if !is_plain_identifier(name) {
        return Err(SearchError::invalid_argument(format!(
            "invalid filter column name: {name:?}"
        )));
    }
    if !allowed_columns.contains(name) {
        return Err(SearchError::invalid_argument(format!(
            "unknown filter column: {name:?}"
        )));
    }
    Ok(Expr::Column(Column::from_name(name)))
}

/// Returns true when `name` matches `[A-Za-z_][A-Za-z0-9_]*`.
fn is_plain_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Converts a domain literal into a typed DataFusion literal expression.
fn literal_to_expr(value: &Literal) -> Expr {
    match value {
        Literal::Bool(flag) => lit(*flag),
        Literal::Int(number) => lit(*number),
        Literal::Float(number) => lit(*number),
        Literal::String(text) => lit(text.clone()),
    }
}

#[cfg(test)]
mod tests {
    use lance::deps::datafusion::logical_expr::col;

    use super::*;

    /// Builds the allowlist used by the tests.
    fn columns() -> HashSet<String> {
        ["id", "text", "score"].iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn comparison_translates_to_a_binary_expression() {
        let filter = Filter::Compare {
            column: "id".to_string(),
            op: CompareOp::Gt,
            value: Literal::Int(2),
        };
        let expr = filter_to_expr(&filter, &columns()).unwrap();
        assert_eq!(expr, col("id").gt(lit(2_i64)));
    }

    #[test]
    fn boolean_combinators_fold_left() {
        let filter = Filter::And(vec![
            Filter::Compare {
                column: "id".to_string(),
                op: CompareOp::Ge,
                value: Literal::Int(1),
            },
            Filter::Or(vec![
                Filter::IsNull {
                    column: "text".to_string(),
                },
                Filter::Compare {
                    column: "score".to_string(),
                    op: CompareOp::Lt,
                    value: Literal::Float(0.5),
                },
            ]),
        ]);
        let expr = filter_to_expr(&filter, &columns()).unwrap();
        let expected = col("id")
            .gt_eq(lit(1_i64))
            .and(col("text").is_null().or(col("score").lt(lit(0.5_f64))));
        assert_eq!(expr, expected);
    }

    #[test]
    fn in_list_between_and_not_translate() {
        let filter = Filter::Not(Box::new(Filter::InList {
            column: "id".to_string(),
            values: vec![Literal::Int(1), Literal::Int(2)],
            negated: false,
        }));
        let expr = filter_to_expr(&filter, &columns()).unwrap();
        assert_eq!(expr, not(col("id").in_list(vec![lit(1_i64), lit(2_i64)], false)));

        let filter = Filter::Between {
            column: "score".to_string(),
            low: Literal::Float(0.1),
            high: Literal::Float(0.9),
        };
        let expr = filter_to_expr(&filter, &columns()).unwrap();
        assert_eq!(expr, col("score").between(lit(0.1_f64), lit(0.9_f64)));
    }

    #[test]
    fn malicious_column_names_are_rejected() {
        for name in [
            "id; DROP TABLE users",
            "id OR 1=1",
            "id\"",
            "id'",
            "a.b",
            "id--",
            "",
            " id",
        ] {
            let filter = Filter::Compare {
                column: name.to_string(),
                op: CompareOp::Eq,
                value: Literal::Int(1),
            };
            let err = filter_to_expr(&filter, &columns()).unwrap_err();
            assert!(matches!(err, SearchError::InvalidArgument(_)), "accepted {name:?}");
        }
    }

    #[test]
    fn unknown_columns_are_rejected_even_when_well_formed() {
        let filter = Filter::IsNotNull {
            column: "missing".to_string(),
        };
        let err = filter_to_expr(&filter, &columns()).unwrap_err();
        assert!(matches!(err, SearchError::InvalidArgument(_)));
    }

    #[test]
    fn string_literals_are_carried_as_values_not_expressions() {
        let filter = Filter::Compare {
            column: "text".to_string(),
            op: CompareOp::Eq,
            value: Literal::String("x' OR '1'='1".to_string()),
        };
        let expr = filter_to_expr(&filter, &columns()).unwrap();
        assert_eq!(expr, col("text").eq(lit("x' OR '1'='1")));
    }

    #[test]
    fn empty_combinators_and_empty_in_list_are_rejected() {
        let err = filter_to_expr(&Filter::And(Vec::new()), &columns()).unwrap_err();
        assert!(matches!(err, SearchError::InvalidArgument(_)));
        let err = filter_to_expr(&Filter::Or(Vec::new()), &columns()).unwrap_err();
        assert!(matches!(err, SearchError::InvalidArgument(_)));
        let err = filter_to_expr(
            &Filter::InList {
                column: "id".to_string(),
                values: Vec::new(),
                negated: false,
            },
            &columns(),
        )
        .unwrap_err();
        assert!(matches!(err, SearchError::InvalidArgument(_)));
    }

    #[test]
    fn excessive_nesting_is_rejected() {
        let mut filter = Filter::IsNull {
            column: "id".to_string(),
        };
        for _ in 0..40 {
            filter = Filter::Not(Box::new(filter));
        }
        let err = filter_to_expr(&filter, &columns()).unwrap_err();
        assert!(matches!(err, SearchError::InvalidArgument(_)));
    }
}
