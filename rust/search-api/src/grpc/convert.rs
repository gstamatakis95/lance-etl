//! Conversions between the production protobuf surface and domain types.

use serde_json::{Map, Value};

use crate::config::DEFAULT_MAX_PROJECTION_COLUMNS;
use crate::domain::{
    CompareOp, DatasetTarget, Filter, FilterMode, FusedHit, FusionSpec, Fuzziness, Hit, HybridQuery, Literal,
    MatchSpec, PhraseSpec, SearchError, SearchWarning, TextOperator, TextQuery, TextQueryNode, TimeRange, VectorQuery,
};
use crate::pb;

/// Maximum accepted nesting depth for a full-text query tree.
const MAX_FTS_DEPTH: usize = 32;

/// Converts and validates a logical dataset target before any catalog access.
pub fn dataset_target_from_proto(target: Option<pb::DatasetTarget>) -> Result<DatasetTarget, SearchError> {
    let target = target.ok_or_else(|| SearchError::invalid_argument("target is required"))?;
    let target = DatasetTarget {
        org_id: target.org_id,
        tenant_id: target.tenant_id,
        namespace: target.namespace,
    };
    target.validate()?;
    Ok(target)
}

/// Converts an optional half-open epoch-millisecond window.
pub fn time_range_from_proto(range: Option<pb::TimeRange>) -> Option<TimeRange> {
    range.map(|range| TimeRange {
        start_ms: range.start_ms,
        end_ms: range.end_ms,
    })
}

/// Builds a vector query using only server-owned execution policy.
pub fn vector_query_from_proto(
    query: Option<pb::VectorQuery>,
    k: u32,
    filter: Option<pb::Filter>,
    projection: Vec<String>,
    time_range: Option<TimeRange>,
) -> Result<VectorQuery, SearchError> {
    let query = query.ok_or_else(|| SearchError::invalid_argument("query is required"))?;
    validate_projection(&projection)?;
    Ok(VectorQuery {
        vector: query.vector,
        k: k as usize,
        column: None,
        distance: None,
        nprobes: None,
        minimum_nprobes: None,
        maximum_nprobes: None,
        refine_factor: None,
        ef: None,
        fast_search: None,
        bypass_vector_index: false,
        filter: filter.map(filter_from_proto).transpose()?,
        filter_mode: FilterMode::Prefilter,
        time_range,
        projection,
    })
}

/// Builds a text query using only server-owned execution policy.
pub fn text_query_from_proto(
    query: Option<pb::TextQuery>,
    k: u32,
    filter: Option<pb::Filter>,
    projection: Vec<String>,
    time_range: Option<TimeRange>,
) -> Result<TextQuery, SearchError> {
    let query = query.ok_or_else(|| SearchError::invalid_argument("query is required"))?;
    validate_projection(&projection)?;
    let node = match query.input {
        Some(pb::text_query::Input::Simple(terms)) if !terms.is_empty() => TextQueryNode::Match(MatchSpec::new(terms)),
        Some(pb::text_query::Input::Simple(_)) => {
            return Err(SearchError::invalid_argument("query must be non-empty"));
        }
        Some(pb::text_query::Input::Fts(fts)) => fts_node_from_proto(fts, 0)?,
        None => return Err(SearchError::invalid_argument("text query input is required")),
    };
    Ok(TextQuery {
        node,
        columns: query.columns,
        k: k as usize,
        wand_factor: None,
        filter: filter.map(filter_from_proto).transpose()?,
        filter_mode: FilterMode::Prefilter,
        time_range,
        projection,
        fast_search: None,
    })
}

/// Enforces the fixed public projection-width bound before opening a dataset.
fn validate_projection(projection: &[String]) -> Result<(), SearchError> {
    if projection.len() > DEFAULT_MAX_PROJECTION_COLUMNS {
        return Err(SearchError::invalid_argument(format!(
            "projection must not exceed {DEFAULT_MAX_PROJECTION_COLUMNS} fields"
        )));
    }
    Ok(())
}

/// Converts a hybrid request and applies its filter, projection, and time range to both legs.
pub fn hybrid_query_from_proto(request: pb::HybridSearchRequest) -> Result<HybridQuery, SearchError> {
    let time_range = time_range_from_proto(request.time_range);
    let vector = vector_query_from_proto(
        request.vector,
        request.k,
        request.filter.clone(),
        request.projection.clone(),
        time_range,
    )?;
    let text = text_query_from_proto(request.text, request.k, request.filter, request.projection, time_range)?;
    Ok(HybridQuery {
        vector,
        text,
        k: request.k as usize,
        fusion: fusion_mode_from_proto(request.fusion_mode)?,
    })
}

/// Maps a small product fusion mode onto code-owned numeric policy.
pub fn fusion_mode_from_proto(mode: i32) -> Result<FusionSpec, SearchError> {
    match pb::HybridFusionMode::try_from(mode) {
        Ok(pb::HybridFusionMode::Unspecified | pb::HybridFusionMode::Balanced) => Ok(FusionSpec::default()),
        Ok(pb::HybridFusionMode::SemanticPriority) => Ok(FusionSpec::Weighted { vector_weight: 0.8 }),
        Ok(pb::HybridFusionMode::LexicalPriority) => Ok(FusionSpec::Weighted { vector_weight: 0.2 }),
        Err(_) => Err(SearchError::invalid_argument("unknown hybrid fusion mode")),
    }
}

/// Converts one protobuf full-text query node.
fn fts_node_from_proto(node: pb::FtsQuery, depth: usize) -> Result<TextQueryNode, SearchError> {
    if depth > MAX_FTS_DEPTH {
        return Err(SearchError::invalid_argument(format!(
            "fts query nesting exceeds the maximum depth of {MAX_FTS_DEPTH}"
        )));
    }
    match node.query {
        Some(pb::fts_query::Query::Match(query)) => Ok(TextQueryNode::Match(MatchSpec {
            terms: query.terms,
            column: query.column,
            boost: query.boost.unwrap_or(1.0),
            operator: text_operator_from_proto(query.operator)?,
            fuzziness: fuzziness_from_proto(query.fuzziness),
            max_expansions: None,
            prefix_length: query.prefix_length.unwrap_or(0),
        })),
        Some(pb::fts_query::Query::Phrase(query)) => Ok(TextQueryNode::Phrase(PhraseSpec {
            terms: query.terms,
            column: query.column,
            slop: query.slop,
        })),
        Some(pb::fts_query::Query::Boost(query)) => {
            let positive = query
                .positive
                .ok_or_else(|| SearchError::invalid_argument("boost query requires a positive query"))?;
            let negative = query
                .negative
                .ok_or_else(|| SearchError::invalid_argument("boost query requires a negative query"))?;
            Ok(TextQueryNode::Boost {
                positive: Box::new(fts_node_from_proto(*positive, depth + 1)?),
                negative: Box::new(fts_node_from_proto(*negative, depth + 1)?),
                negative_boost: query.negative_boost.unwrap_or(0.5),
            })
        }
        Some(pb::fts_query::Query::MultiMatch(query)) => Ok(TextQueryNode::MultiMatch {
            terms: query.terms,
            columns: query.columns,
            boosts: query.boosts,
            operator: text_operator_from_proto(query.operator)?,
        }),
        Some(pb::fts_query::Query::Boolean(query)) => Ok(TextQueryNode::Boolean {
            should: fts_nodes_from_proto(query.should, depth + 1)?,
            must: fts_nodes_from_proto(query.must, depth + 1)?,
            must_not: fts_nodes_from_proto(query.must_not, depth + 1)?,
        }),
        None => Err(SearchError::invalid_argument("fts query node is missing its kind")),
    }
}

/// Converts a list of protobuf full-text query nodes.
fn fts_nodes_from_proto(nodes: Vec<pb::FtsQuery>, depth: usize) -> Result<Vec<TextQueryNode>, SearchError> {
    nodes.into_iter().map(|node| fts_node_from_proto(node, depth)).collect()
}

/// Converts a protobuf typed filter AST.
pub fn filter_from_proto(filter: pb::Filter) -> Result<Filter, SearchError> {
    match filter.predicate {
        Some(pb::filter::Predicate::Comparison(comparison)) => Ok(Filter::Compare {
            column: comparison.column,
            op: compare_op_from_proto(comparison.op)?,
            value: literal_from_proto(comparison.value)?,
        }),
        Some(pb::filter::Predicate::InList(in_list)) => Ok(Filter::InList {
            column: in_list.column,
            values: in_list
                .values
                .into_iter()
                .map(|value| literal_from_proto(Some(value)))
                .collect::<Result<Vec<Literal>, SearchError>>()?,
            negated: in_list.negated,
        }),
        Some(pb::filter::Predicate::IsNull(is_null)) => Ok(Filter::IsNull { column: is_null.column }),
        Some(pb::filter::Predicate::IsNotNull(is_not_null)) => Ok(Filter::IsNotNull {
            column: is_not_null.column,
        }),
        Some(pb::filter::Predicate::Between(between)) => Ok(Filter::Between {
            column: between.column,
            low: literal_from_proto(between.low)?,
            high: literal_from_proto(between.high)?,
        }),
        Some(pb::filter::Predicate::And(list)) => Ok(Filter::And(filters_from_proto(list.filters)?)),
        Some(pb::filter::Predicate::Or(list)) => Ok(Filter::Or(filters_from_proto(list.filters)?)),
        Some(pb::filter::Predicate::Not(inner)) => Ok(Filter::Not(Box::new(filter_from_proto(*inner)?))),
        None => Err(SearchError::invalid_argument("filter is missing its predicate")),
    }
}

/// Converts a list of protobuf filters.
fn filters_from_proto(filters: Vec<pb::Filter>) -> Result<Vec<Filter>, SearchError> {
    filters.into_iter().map(filter_from_proto).collect()
}

/// Converts one protobuf literal.
fn literal_from_proto(value: Option<pb::LiteralValue>) -> Result<Literal, SearchError> {
    let kind = value
        .and_then(|literal| literal.kind)
        .ok_or_else(|| SearchError::invalid_argument("filter literal is missing its value"))?;
    Ok(match kind {
        pb::literal_value::Kind::BoolValue(flag) => Literal::Bool(flag),
        pb::literal_value::Kind::Int64Value(number) => Literal::Int(number),
        pb::literal_value::Kind::DoubleValue(number) => Literal::Float(number),
        pb::literal_value::Kind::StringValue(text) => Literal::String(text),
    })
}

/// Converts a comparison operator.
fn compare_op_from_proto(op: i32) -> Result<CompareOp, SearchError> {
    match pb::CompareOp::try_from(op) {
        Ok(pb::CompareOp::Eq) => Ok(CompareOp::Eq),
        Ok(pb::CompareOp::Ne) => Ok(CompareOp::Ne),
        Ok(pb::CompareOp::Lt) => Ok(CompareOp::Lt),
        Ok(pb::CompareOp::Le) => Ok(CompareOp::Le),
        Ok(pb::CompareOp::Gt) => Ok(CompareOp::Gt),
        Ok(pb::CompareOp::Ge) => Ok(CompareOp::Ge),
        Ok(pb::CompareOp::Unspecified) | Err(_) => {
            Err(SearchError::invalid_argument("comparison requires a valid operator"))
        }
    }
}

/// Converts a text operator, defaulting to OR.
fn text_operator_from_proto(operator: i32) -> Result<TextOperator, SearchError> {
    match pb::TextOperator::try_from(operator) {
        Ok(pb::TextOperator::Unspecified | pb::TextOperator::Or) => Ok(TextOperator::Or),
        Ok(pb::TextOperator::And) => Ok(TextOperator::And),
        Err(_) => Err(SearchError::invalid_argument("unknown text operator")),
    }
}

/// Converts fuzzy-match semantics.
fn fuzziness_from_proto(fuzziness: Option<pb::match_query::Fuzziness>) -> Fuzziness {
    match fuzziness {
        Some(pb::match_query::Fuzziness::AutoFuzziness(true)) => Fuzziness::Auto,
        Some(pb::match_query::Fuzziness::AutoFuzziness(false)) | None => Fuzziness::Exact,
        Some(pb::match_query::Fuzziness::MaxDistance(distance)) => Fuzziness::Distance(distance),
    }
}

/// Converts one vector hit into a typed result.
pub fn vector_hit_to_proto(hit: Hit) -> Result<pb::VectorSearchResult, SearchError> {
    Ok(pb::VectorSearchResult {
        distance: hit.score as f32,
        vector_id: hit.vector_id,
        projection: json_map_to_projection(hit.row)?,
    })
}

/// Converts one text hit into a typed result.
pub fn text_hit_to_proto(hit: Hit) -> Result<pb::TextSearchResult, SearchError> {
    Ok(pb::TextSearchResult {
        score: hit.score as f32,
        vector_id: hit.vector_id,
        projection: json_map_to_projection(hit.row)?,
    })
}

/// Lowers a fused hit into a single-leg hit for recall capture.
pub fn fused_to_hit(hit: FusedHit) -> Hit {
    Hit {
        vector_id: hit.vector_id,
        score: hit.score,
        row: hit.row,
    }
}

/// Converts one fused hit into a typed result.
pub fn fused_hit_to_proto(hit: FusedHit) -> Result<pb::HybridSearchResult, SearchError> {
    Ok(pb::HybridSearchResult {
        fused_score: hit.score,
        vector_id: hit.vector_id,
        projection: json_map_to_projection(hit.row)?,
    })
}

/// Converts a domain warning into its closed protobuf code.
pub fn warning_to_proto(warning: SearchWarning) -> i32 {
    match warning {
        SearchWarning::ResultsUnderfilled => pb::SearchWarning::ResultsUnderfilled as i32,
        SearchWarning::VectorLegUnavailable => pb::SearchWarning::VectorLegUnavailable as i32,
        SearchWarning::TextLegUnavailable => pb::SearchWarning::TextLegUnavailable as i32,
    }
}

/// Converts a JSON scalar map into an ordered typed projection.
pub fn json_map_to_projection(map: Map<String, Value>) -> Result<Vec<pb::ProjectedField>, SearchError> {
    map.into_iter()
        .map(|(name, value)| {
            Ok(pb::ProjectedField {
                name,
                value: Some(json_value_to_projection(value)?),
            })
        })
        .collect()
}

/// Converts one JSON scalar without lossy number coercion.
fn json_value_to_projection(value: Value) -> Result<pb::ProjectionValue, SearchError> {
    use pb::projection_value::Kind;
    let kind = match value {
        Value::Null => Kind::NullValue(pb::NullProjectionValue {}),
        Value::Bool(flag) => Kind::BoolValue(flag),
        Value::Number(number) if number.is_u64() => Kind::Uint64Value(number.as_u64().unwrap_or_default()),
        Value::Number(number) if number.is_i64() => Kind::Int64Value(number.as_i64().unwrap_or_default()),
        Value::Number(number) => {
            let value = number
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or_else(|| SearchError::internal("projection contains a non-finite number"))?;
            Kind::DoubleValue(value)
        }
        Value::String(text) => Kind::StringValue(text),
        Value::Array(_) | Value::Object(_) => {
            return Err(SearchError::invalid_argument(
                "projection contains an unsupported nested value",
            ));
        }
    };
    Ok(pb::ProjectionValue { kind: Some(kind) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn product_fusion_modes_use_fixed_policy() {
        assert_eq!(fusion_mode_from_proto(0).unwrap(), FusionSpec::default());
        assert_eq!(
            fusion_mode_from_proto(pb::HybridFusionMode::SemanticPriority as i32).unwrap(),
            FusionSpec::Weighted { vector_weight: 0.8 }
        );
        assert!(fusion_mode_from_proto(99).is_err());
    }

    #[test]
    fn typed_projection_preserves_large_integers_and_rejects_nested_values() {
        let mut map = Map::new();
        map.insert("large".to_string(), Value::from(9_007_199_254_740_993_u64));
        let fields = json_map_to_projection(map).unwrap();
        assert!(matches!(
            fields[0].value.as_ref().and_then(|value| value.kind.as_ref()),
            Some(pb::projection_value::Kind::Uint64Value(9_007_199_254_740_993))
        ));
        let mut nested = Map::new();
        nested.insert("items".to_string(), Value::Array(Vec::new()));
        assert!(json_map_to_projection(nested).is_err());
    }
}
