//! Translation of the domain filter AST into a DataFusion logical expression.
//!
//! Column names are validated with a strict identifier allowlist and against the dataset schema,
//! and literals become typed `lit` expressions, so no client input is ever parsed as SQL.

use std::collections::HashSet;

use arrow_schema::{DataType, TimeUnit};
use lance::deps::datafusion::common::{Column, ScalarValue};
use lance::deps::datafusion::logical_expr::{Expr, lit, not};

use crate::domain::{CompareOp, Filter, Literal, SearchError, TimeRange};

/// Milliseconds per second, used to scale an epoch-millis bound to a second-resolution column.
const MILLIS_PER_SECOND: i64 = 1_000;

/// Microseconds per millisecond, used to scale an epoch-millis bound to a micro-resolution column.
const MICROS_PER_MILLI: i64 = 1_000;

/// Nanoseconds per millisecond, used to scale an epoch-millis bound to a nano-resolution column.
const NANOS_PER_MILLI: i64 = 1_000_000;

/// Maximum nesting depth accepted for a filter AST.
const MAX_FILTER_DEPTH: usize = 32;

/// Translates a domain filter into a DataFusion expression against the given schema columns.
///
/// `allowed_columns` is the set of top-level column names of the dataset schema. Any reference
/// outside it (or outside the `[A-Za-z_][A-Za-z0-9_]*` identifier shape) is rejected.
pub fn filter_to_expr(filter: &Filter, allowed_columns: &HashSet<String>) -> Result<Expr, SearchError> {
    translate(filter, allowed_columns, 0)
}

/// Translates an event-time window into a typed range predicate on `column` of type `data_type`.
///
/// The predicate is `column >= start` and/or `column < end` (start inclusive, end exclusive),
/// depending on which bounds are present. Each epoch-millisecond bound becomes a typed literal of
/// the column's own type (a [`ScalarValue`] timestamp scaled to the column unit and carrying the
/// column timezone, or a plain integer literal for integer columns), so no client text is ever
/// parsed as SQL and DataFusion needs no cross-type coercion. Returns `Ok(None)` when the window
/// has no bounds. The caller ANDs the result with any caller-provided filter.
pub fn time_range_to_expr(range: &TimeRange, column: &str, data_type: &DataType) -> Result<Option<Expr>, SearchError> {
    let mut bounds: Vec<Expr> = Vec::new();
    if let Some(start) = range.start_ms {
        let column_expr = Expr::Column(Column::from_name(column));
        bounds.push(column_expr.gt_eq(time_literal(start, data_type, TimeBound::Start)?));
    }
    if let Some(end) = range.end_ms {
        let column_expr = Expr::Column(Column::from_name(column));
        bounds.push(column_expr.lt(time_literal(end, data_type, TimeBound::End)?));
    }
    Ok(bounds.into_iter().reduce(Expr::and))
}

/// Which side of the half-open `[start, end)` window a bound literal sits on.
///
/// Needed by resolutions coarser than a millisecond: both bounds round up. A second-resolution
/// timestamp is an exact instant at a whole second, so this preserves the millisecond half-open
/// interval without admitting a row from the second before a non-aligned start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeBound {
    /// The inclusive lower bound (`column >= start`).
    Start,
    /// The exclusive upper bound (`column < end`).
    End,
}

/// Builds a typed literal for an epoch-millisecond bound matching the event-timestamp column type.
///
/// A timestamp column yields a [`ScalarValue`] timestamp scaled to the column's [`TimeUnit`] and
/// carrying the column's timezone, so the comparison is exact with no coercion. An `Int64` column
/// (epoch milliseconds stored as an integer) yields a plain integer literal, while an `Int32`
/// column cannot represent realistic epoch-millisecond values at all, so a bound outside the
/// `i32` range is rejected instead of being silently wrapped into an arbitrary predicate. Any
/// other column type is rejected as an invalid argument. Scaling to microsecond or nanosecond
/// resolution goes through `checked_mul`: an epoch-millisecond bound near `i64::MAX` would
/// otherwise wrap in release builds and silently produce the wrong time predicate, so an
/// out-of-range bound is rejected instead. Scaling down to second resolution rounds both bounds
/// up, preserving exact membership for whole-second instants.
fn time_literal(epoch_ms: i64, data_type: &DataType, bound: TimeBound) -> Result<Expr, SearchError> {
    match data_type {
        DataType::Timestamp(unit, tz) => {
            let scalar = match unit {
                TimeUnit::Second => {
                    ScalarValue::TimestampSecond(Some(epoch_ms_to_seconds(epoch_ms, bound)), tz.clone())
                }
                TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(epoch_ms), tz.clone()),
                TimeUnit::Microsecond => {
                    ScalarValue::TimestampMicrosecond(Some(scaled_epoch(epoch_ms, MICROS_PER_MILLI)?), tz.clone())
                }
                TimeUnit::Nanosecond => {
                    ScalarValue::TimestampNanosecond(Some(scaled_epoch(epoch_ms, NANOS_PER_MILLI)?), tz.clone())
                }
            };
            Ok(lit(scalar))
        }
        DataType::Int64 => Ok(lit(epoch_ms)),
        DataType::Int32 => i32::try_from(epoch_ms).map(lit).map_err(|_| {
            SearchError::invalid_argument(format!(
                "event-timestamp bound {epoch_ms} does not fit the dataset's Int32 event-timestamp column"
            ))
        }),
        other => Err(SearchError::invalid_argument(format!(
            "event-timestamp column has unsupported type for a time range: {other:?}"
        ))),
    }
}

/// Converts an epoch-millisecond bound to whole seconds with bound-aware rounding.
///
/// Both bounds ceil, so the second-resolution window `[ceil(start), ceil(end))` contains exactly
/// the whole-second instants in the requested millisecond window. Euclidean division keeps
/// negative pre-epoch bounds correct.
fn epoch_ms_to_seconds(epoch_ms: i64, bound: TimeBound) -> i64 {
    let floor = epoch_ms.div_euclid(MILLIS_PER_SECOND);
    match (bound, epoch_ms.rem_euclid(MILLIS_PER_SECOND)) {
        (_, 0) => floor,
        (TimeBound::Start | TimeBound::End, _) => floor + 1,
    }
}

/// Scales an epoch-millisecond bound by `factor`, rejecting the bound when the multiplication
/// would overflow `i64` instead of silently wrapping to an unrelated time.
fn scaled_epoch(epoch_ms: i64, factor: i64) -> Result<i64, SearchError> {
    epoch_ms.checked_mul(factor).ok_or_else(|| {
        SearchError::invalid_argument(format!(
            "event-timestamp bound {epoch_ms} overflows at the column's time resolution"
        ))
    })
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
/// The name must look like a plain identifier and exist in the dataset schema. The reference is
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
    fn time_range_builds_inclusive_start_exclusive_end_on_a_timestamp_column() {
        let data_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let range = TimeRange {
            start_ms: Some(1_000),
            end_ms: Some(2_000),
        };
        let expr = time_range_to_expr(&range, "ts", &data_type)
            .unwrap()
            .expect("a bounded window must produce an expression");
        let tz: Option<std::sync::Arc<str>> = Some("UTC".into());
        let expected = col("ts")
            .gt_eq(lit(ScalarValue::TimestampMicrosecond(Some(1_000_000), tz.clone())))
            .and(col("ts").lt(lit(ScalarValue::TimestampMicrosecond(Some(2_000_000), tz))));
        assert_eq!(expr, expected);
    }

    #[test]
    fn time_range_single_bounds_and_units_scale_correctly() {
        let millis = DataType::Timestamp(TimeUnit::Millisecond, None);
        let start_only = time_range_to_expr(
            &TimeRange {
                start_ms: Some(5),
                end_ms: None,
            },
            "ts",
            &millis,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            start_only,
            col("ts").gt_eq(lit(ScalarValue::TimestampMillisecond(Some(5), None)))
        );

        let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let end_only = time_range_to_expr(
            &TimeRange {
                start_ms: None,
                end_ms: Some(3),
            },
            "ts",
            &nanos,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            end_only,
            col("ts").lt(lit(ScalarValue::TimestampNanosecond(Some(3_000_000), None)))
        );
    }

    #[test]
    fn time_range_on_an_integer_column_uses_an_integer_literal() {
        let expr = time_range_to_expr(
            &TimeRange {
                start_ms: Some(42),
                end_ms: None,
            },
            "ts",
            &DataType::Int64,
        )
        .unwrap()
        .unwrap();
        assert_eq!(expr, col("ts").gt_eq(lit(42_i64)));
    }

    #[test]
    fn time_range_without_bounds_produces_no_expression() {
        let none = time_range_to_expr(
            &TimeRange::default(),
            "ts",
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        assert!(none.is_none(), "an unbounded window must not produce a predicate");
    }

    #[test]
    fn time_range_on_an_unsupported_column_type_is_rejected() {
        let err = time_range_to_expr(
            &TimeRange {
                start_ms: Some(1),
                end_ms: None,
            },
            "ts",
            &DataType::Utf8,
        )
        .unwrap_err();
        assert!(matches!(err, SearchError::InvalidArgument(_)));
    }

    #[test]
    fn time_range_bound_overflowing_the_column_resolution_is_rejected() {
        let err = time_range_to_expr(
            &TimeRange {
                start_ms: Some(i64::MAX),
                end_ms: None,
            },
            "ts",
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap_err();
        assert!(matches!(err, SearchError::InvalidArgument(_)));

        let err = time_range_to_expr(
            &TimeRange {
                start_ms: Some(i64::MIN),
                end_ms: None,
            },
            "ts",
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap_err();
        assert!(matches!(err, SearchError::InvalidArgument(_)));
    }

    #[test]
    fn time_range_on_a_second_resolution_column_preserves_millisecond_semantics() {
        let seconds = DataType::Timestamp(TimeUnit::Second, None);
        let expr = time_range_to_expr(
            &TimeRange {
                start_ms: Some(1_500),
                end_ms: Some(2_500),
            },
            "ts",
            &seconds,
        )
        .unwrap()
        .unwrap();
        let expected = col("ts")
            .gt_eq(lit(ScalarValue::TimestampSecond(Some(2), None)))
            .and(col("ts").lt(lit(ScalarValue::TimestampSecond(Some(3), None))));
        assert_eq!(
            expr, expected,
            "both bounds must ceil so coarse storage never admits values outside the millisecond window"
        );

        let exact = time_range_to_expr(
            &TimeRange {
                start_ms: Some(2_000),
                end_ms: Some(3_000),
            },
            "ts",
            &seconds,
        )
        .unwrap()
        .unwrap();
        let expected_exact = col("ts")
            .gt_eq(lit(ScalarValue::TimestampSecond(Some(2), None)))
            .and(col("ts").lt(lit(ScalarValue::TimestampSecond(Some(3), None))));
        assert_eq!(exact, expected_exact, "exact-second bounds must not be widened");

        let negative = time_range_to_expr(
            &TimeRange {
                start_ms: Some(-1_500),
                end_ms: Some(-500),
            },
            "ts",
            &seconds,
        )
        .unwrap()
        .unwrap();
        let expected_negative = col("ts")
            .gt_eq(lit(ScalarValue::TimestampSecond(Some(-1), None)))
            .and(col("ts").lt(lit(ScalarValue::TimestampSecond(Some(0), None))));
        assert_eq!(
            negative, expected_negative,
            "pre-epoch bounds must preserve the same millisecond semantics"
        );
    }

    #[test]
    fn time_range_int32_bounds_are_range_checked_instead_of_wrapping() {
        let in_range = time_range_to_expr(
            &TimeRange {
                start_ms: Some(42),
                end_ms: None,
            },
            "ts",
            &DataType::Int32,
        )
        .unwrap()
        .unwrap();
        assert_eq!(in_range, col("ts").gt_eq(lit(42_i32)));

        let realistic_epoch_ms = 1_770_000_000_000_i64;
        let err = time_range_to_expr(
            &TimeRange {
                start_ms: Some(realistic_epoch_ms),
                end_ms: None,
            },
            "ts",
            &DataType::Int32,
        )
        .unwrap_err();
        assert!(
            matches!(err, SearchError::InvalidArgument(_)),
            "an epoch-ms bound beyond i32 must be rejected, not wrapped: {err:?}"
        );

        let err = time_range_to_expr(
            &TimeRange {
                start_ms: None,
                end_ms: Some(i64::MIN),
            },
            "ts",
            &DataType::Int32,
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
