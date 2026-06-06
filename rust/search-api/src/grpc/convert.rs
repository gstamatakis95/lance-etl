//! Conversions between protobuf messages and domain types.

use prost_types::value::Kind;
use serde_json::{Map, Value};

use crate::domain::{
    CompareOp, DistanceKind, Filter, FilterMode, FusedHit, FusionSpec, Fuzziness, Hit, HybridQuery, Literal, MatchSpec,
    PhraseSpec, SearchError, TextOperator, TextQuery, TextQueryNode, VectorQuery,
};
use crate::pb;

/// Converts an optional proto vector query into the domain query.
pub fn vector_query_from_proto(query: Option<pb::VectorQuery>) -> Result<VectorQuery, SearchError> {
    let query = query.ok_or_else(|| SearchError::invalid_argument("query is required"))?;
    Ok(VectorQuery {
        vector: query.vector,
        k: query.k as usize,
        column: query.column,
        distance: distance_from_proto(query.distance_type)?,
        nprobes: query.nprobes.map(|n| n as usize),
        minimum_nprobes: query.minimum_nprobes.map(|n| n as usize),
        maximum_nprobes: query.maximum_nprobes.map(|n| n as usize),
        refine_factor: query.refine_factor,
        ef: query.ef.map(|n| n as usize),
        fast_search: query.fast_search,
        bypass_vector_index: query.bypass_vector_index,
        filter: query.filter.map(filter_from_proto).transpose()?,
        filter_mode: filter_mode_from_proto(query.filter_mode)?,
        projection: query.projection,
        with_row_id: query.with_row_id,
        offset: query.offset.map(|n| n as usize),
    })
}

/// Converts an optional proto text query into the domain query.
pub fn text_query_from_proto(query: Option<pb::TextQuery>) -> Result<TextQuery, SearchError> {
    let query = query.ok_or_else(|| SearchError::invalid_argument("query is required"))?;
    let node = match query.input {
        Some(pb::text_query::Input::Simple(terms)) => {
            if terms.is_empty() {
                return Err(SearchError::invalid_argument("query must be non-empty"));
            }
            TextQueryNode::Match(MatchSpec::new(terms))
        }
        Some(pb::text_query::Input::Fts(fts)) => fts_node_from_proto(fts)?,
        None => return Err(SearchError::invalid_argument("text query input is required")),
    };
    Ok(TextQuery {
        node,
        columns: query.columns,
        k: query.k as usize,
        wand_factor: query.wand_factor,
        filter: query.filter.map(filter_from_proto).transpose()?,
        filter_mode: filter_mode_from_proto(query.filter_mode)?,
        projection: query.projection,
        with_row_id: query.with_row_id,
        offset: query.offset.map(|n| n as usize),
    })
}

/// Converts a proto hybrid request into the domain query.
pub fn hybrid_query_from_proto(request: pb::HybridSearchRequest) -> Result<HybridQuery, SearchError> {
    Ok(HybridQuery {
        vector: vector_query_from_proto(request.vector)?,
        text: text_query_from_proto(request.text)?,
        k: request.k as usize,
        fusion: fusion_from_proto(request.fusion)?,
    })
}

/// Converts a proto fusion config into the domain spec, defaulting to RRF with `rrf_k = 60`.
pub fn fusion_from_proto(fusion: Option<pb::Fusion>) -> Result<FusionSpec, SearchError> {
    let Some(fusion) = fusion else {
        return Ok(FusionSpec::default());
    };
    match fusion.strategy {
        Some(pb::fusion::Strategy::Rrf(rrf)) => {
            let rrf_k = rrf.rrf_k.unwrap_or(crate::domain::fusion::DEFAULT_RRF_K);
            if !rrf_k.is_finite() || rrf_k <= 0.0 {
                return Err(SearchError::invalid_argument("rrf_k must be a positive finite number"));
            }
            Ok(FusionSpec::Rrf { rrf_k })
        }
        None => Ok(FusionSpec::default()),
    }
}

/// Converts a proto FTS query node tree into the domain tree.
fn fts_node_from_proto(node: pb::FtsQuery) -> Result<TextQueryNode, SearchError> {
    match node.query {
        Some(pb::fts_query::Query::Match(query)) => Ok(TextQueryNode::Match(MatchSpec {
            terms: query.terms,
            column: query.column,
            boost: query.boost.unwrap_or(1.0),
            operator: text_operator_from_proto(query.operator)?,
            fuzziness: fuzziness_from_proto(query.fuzziness),
            max_expansions: query.max_expansions.map(|n| n as usize),
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
                positive: Box::new(fts_node_from_proto(*positive)?),
                negative: Box::new(fts_node_from_proto(*negative)?),
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
            should: fts_nodes_from_proto(query.should)?,
            must: fts_nodes_from_proto(query.must)?,
            must_not: fts_nodes_from_proto(query.must_not)?,
        }),
        None => Err(SearchError::invalid_argument("fts query node is missing its kind")),
    }
}

/// Converts a list of proto FTS query nodes.
fn fts_nodes_from_proto(nodes: Vec<pb::FtsQuery>) -> Result<Vec<TextQueryNode>, SearchError> {
    nodes.into_iter().map(fts_node_from_proto).collect()
}

/// Converts a proto filter AST into the domain filter AST.
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

/// Converts a list of proto filters.
fn filters_from_proto(filters: Vec<pb::Filter>) -> Result<Vec<Filter>, SearchError> {
    filters.into_iter().map(filter_from_proto).collect()
}

/// Converts an optional proto literal into the domain literal.
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

/// Converts the proto comparison operator enum.
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

/// Converts the proto distance type enum; unspecified keeps the index metric.
fn distance_from_proto(distance: i32) -> Result<Option<DistanceKind>, SearchError> {
    match pb::DistanceType::try_from(distance) {
        Ok(pb::DistanceType::Unspecified) => Ok(None),
        Ok(pb::DistanceType::L2) => Ok(Some(DistanceKind::L2)),
        Ok(pb::DistanceType::Cosine) => Ok(Some(DistanceKind::Cosine)),
        Ok(pb::DistanceType::Dot) => Ok(Some(DistanceKind::Dot)),
        Ok(pb::DistanceType::Hamming) => Ok(Some(DistanceKind::Hamming)),
        Err(_) => Err(SearchError::invalid_argument("unknown distance type")),
    }
}

/// Converts the proto filter mode enum; unspecified defaults to prefilter.
fn filter_mode_from_proto(mode: i32) -> Result<FilterMode, SearchError> {
    match pb::FilterMode::try_from(mode) {
        Ok(pb::FilterMode::Unspecified) | Ok(pb::FilterMode::Prefilter) => Ok(FilterMode::Prefilter),
        Ok(pb::FilterMode::Postfilter) => Ok(FilterMode::Postfilter),
        Err(_) => Err(SearchError::invalid_argument("unknown filter mode")),
    }
}

/// Converts the proto text operator enum; unspecified defaults to OR.
fn text_operator_from_proto(operator: i32) -> Result<TextOperator, SearchError> {
    match pb::TextOperator::try_from(operator) {
        Ok(pb::TextOperator::Unspecified) | Ok(pb::TextOperator::Or) => Ok(TextOperator::Or),
        Ok(pb::TextOperator::And) => Ok(TextOperator::And),
        Err(_) => Err(SearchError::invalid_argument("unknown text operator")),
    }
}

/// Converts the proto fuzziness oneof; absent means exact matching.
fn fuzziness_from_proto(fuzziness: Option<pb::match_query::Fuzziness>) -> Fuzziness {
    match fuzziness {
        Some(pb::match_query::Fuzziness::AutoFuzziness(true)) => Fuzziness::Auto,
        Some(pb::match_query::Fuzziness::AutoFuzziness(false)) | None => Fuzziness::Exact,
        Some(pb::match_query::Fuzziness::MaxDistance(distance)) => Fuzziness::Distance(distance),
    }
}

/// Converts a vector hit into its proto result message.
pub fn vector_hit_to_proto(hit: Hit) -> pb::VectorSearchResult {
    pb::VectorSearchResult {
        distance: hit.score as f32,
        row: Some(json_map_to_struct(hit.row)),
    }
}

/// Converts a text hit into its proto result message.
pub fn text_hit_to_proto(hit: Hit) -> pb::TextSearchResult {
    pb::TextSearchResult {
        score: hit.score as f32,
        row: Some(json_map_to_struct(hit.row)),
    }
}

/// Converts a fused hit into its proto result message.
pub fn fused_hit_to_proto(hit: FusedHit) -> pb::HybridSearchResult {
    pb::HybridSearchResult {
        fused_score: hit.score,
        row: Some(json_map_to_struct(hit.row)),
    }
}

/// Converts a JSON object into a `google.protobuf.Struct` for transport in gRPC responses.
pub fn json_map_to_struct(map: Map<String, Value>) -> prost_types::Struct {
    prost_types::Struct {
        fields: map
            .into_iter()
            .map(|(key, value)| (key, json_value_to_prost(value)))
            .collect(),
    }
}

/// Converts one JSON value into the equivalent `google.protobuf.Value`.
fn json_value_to_prost(value: Value) -> prost_types::Value {
    let kind = match value {
        Value::Null => Kind::NullValue(0),
        Value::Bool(flag) => Kind::BoolValue(flag),
        Value::Number(number) => Kind::NumberValue(number.as_f64().unwrap_or(f64::NAN)),
        Value::String(text) => Kind::StringValue(text),
        Value::Array(items) => Kind::ListValue(prost_types::ListValue {
            values: items.into_iter().map(json_value_to_prost).collect(),
        }),
        Value::Object(map) => Kind::StructValue(json_map_to_struct(map)),
    };
    prost_types::Value { kind: Some(kind) }
}
