//! Conversions between protobuf messages and domain types.

use prost_types::value::Kind;
use serde_json::{Map, Value};

use crate::domain::{
    ClusterReport, ClusterSpec, CompareOp, DatasetRef, DatasetTarget, DistanceKind, Filter, FilterMode, FusedHit,
    FusionSpec, Fuzziness, Hit, HybridQuery, Literal, MatchSpec, PhraseSpec, PrewarmReport, PrewarmSpec, RerankSpec,
    SearchError, TextOperator, TextQuery, TextQueryNode, TimeRange, VectorQuery,
};
use crate::pb;

/// Converts an optional proto dataset target into the validated domain target.
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

/// Converts a proto prewarm request into the domain spec (the target travels separately).
pub fn prewarm_spec_from_proto(request: &pb::PrewarmRequest) -> PrewarmSpec {
    PrewarmSpec {
        metadata: request.metadata,
        all_indexes: request.all_indexes,
        index_names: request.index_names.clone(),
        fts_with_position: request.fts_with_position,
    }
}

/// Maps the additive prewarm `version_ref` oneof onto the version selector.
///
/// An unset selector means [`DatasetRef::Latest`] (warm the latest version, the original
/// behavior). An explicit version or tag pins the version to warm, which is what lets an operator
/// warm a green build before flipping the serve tag onto it.
pub fn prewarm_ref_from_proto(request: &pb::PrewarmRequest) -> DatasetRef {
    match &request.version_ref {
        Some(pb::prewarm_request::VersionRef::Version(version)) => DatasetRef::Version(*version),
        Some(pb::prewarm_request::VersionRef::Tag(tag)) => DatasetRef::Tag(tag.clone()),
        None => DatasetRef::Latest,
    }
}

/// Defines one `version_ref` oneof mapper per search request type.
///
/// The three generated oneof types are structurally identical but distinct Rust types, so one
/// macro emits the identical `Version`/`Tag`/unset mapping for each. An unset selector defaults
/// to [`DatasetRef::Serve`] (follow the serve policy), preserving the behavior of existing
/// clients that never set the field.
macro_rules! search_ref_from_proto {
    ($(#[$doc:meta])* $name:ident, $oneof:path) => {
        $(#[$doc])*
        pub fn $name(version_ref: &Option<$oneof>) -> DatasetRef {
            use $oneof as Oneof;
            match version_ref {
                Some(Oneof::Version(version)) => DatasetRef::Version(*version),
                Some(Oneof::Tag(tag)) => DatasetRef::Tag(tag.clone()),
                None => DatasetRef::Serve,
            }
        }
    };
}

search_ref_from_proto!(
    /// Maps the `version_ref` oneof of a [`pb::VectorSearchRequest`] onto a [`DatasetRef`].
    vector_search_ref_from_proto,
    pb::vector_search_request::VersionRef
);

search_ref_from_proto!(
    /// Maps the `version_ref` oneof of a [`pb::TextSearchRequest`] onto a [`DatasetRef`].
    text_search_ref_from_proto,
    pb::text_search_request::VersionRef
);

search_ref_from_proto!(
    /// Maps the `version_ref` oneof of a [`pb::HybridSearchRequest`] onto a [`DatasetRef`].
    hybrid_search_ref_from_proto,
    pb::hybrid_search_request::VersionRef
);

/// Converts a proto clusters request into the domain spec (the target travels separately).
pub fn cluster_spec_from_proto(request: &pb::ClustersRequest) -> ClusterSpec {
    ClusterSpec {
        index_name: request.index_name.clone().filter(|name| !name.is_empty()),
    }
}

/// Converts a domain cluster report into the proto response.
pub fn cluster_report_to_proto(report: ClusterReport) -> pb::ClustersResponse {
    let num_partitions = report.num_partitions() as u32;
    pb::ClustersResponse {
        clusters: report
            .centroids
            .into_iter()
            .enumerate()
            .map(|(id, centroid)| pb::Cluster {
                id: id as u32,
                centroid,
            })
            .collect(),
        dimension: report.dimension as u32,
        index_name: report.index_name,
        num_partitions,
    }
}

/// Converts a domain prewarm report into the proto response.
pub fn prewarm_report_to_proto(report: PrewarmReport) -> pb::PrewarmResponse {
    pb::PrewarmResponse {
        metadata_warmed: report.metadata_warmed,
        indexes: report
            .indexes
            .into_iter()
            .map(|index| pb::PrewarmedIndex {
                name: index.name,
                duration_ms: index.duration.as_millis() as u64,
                error: index.error.unwrap_or_default(),
            })
            .collect(),
        metadata_duration_ms: report.metadata_duration.as_millis() as u64,
        total_duration_ms: report.total_duration.as_millis() as u64,
        index_cache_size_bytes: report.index_cache_size_bytes,
        resolved_version: report.resolved_version,
    }
}

/// Converts an optional proto time range into the domain window.
///
/// An absent message means no window (search all event times). A present message with both bounds
/// unset is carried through as an unbounded window, which the backend treats as a no-op.
pub fn time_range_from_proto(range: Option<pb::TimeRange>) -> Option<TimeRange> {
    range.map(|range| TimeRange {
        start_ms: range.start_ms,
        end_ms: range.end_ms,
    })
}

/// Converts an optional proto vector query into the domain query, attaching the request time range
/// and the resolved dataset reference.
pub fn vector_query_from_proto(
    query: Option<pb::VectorQuery>,
    time_range: Option<TimeRange>,
    reference: DatasetRef,
) -> Result<VectorQuery, SearchError> {
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
        time_range,
        projection: query.projection,
        with_row_id: query.with_row_id,
        offset: query.offset.map(|n| n as usize),
        reference,
    })
}

/// Converts an optional proto text query into the domain query, attaching the request time range
/// and the resolved dataset reference.
pub fn text_query_from_proto(
    query: Option<pb::TextQuery>,
    time_range: Option<TimeRange>,
    reference: DatasetRef,
) -> Result<TextQuery, SearchError> {
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
        time_range,
        projection: query.projection,
        with_row_id: query.with_row_id,
        offset: query.offset.map(|n| n as usize),
        fast_search: query.fast_search,
        reference,
    })
}

/// Converts a proto hybrid request into the domain query.
///
/// The request-level time range is applied to both legs, so the vector and text legs filter the
/// same event-time window. When a request-level filter is present it is ANDed into both legs:
/// if a leg already has its own filter the two are combined with [`Filter::And`]; if only one
/// side is present that side is used alone. The request-level `filter_mode` is applied to both
/// legs when a request-level filter is present, leaving each leg's own mode unchanged otherwise.
/// The request-level `version_ref` is resolved once and set on the hybrid query and both legs so
/// fusion dedup is consistent: both legs always open the same dataset snapshot.
pub fn hybrid_query_from_proto(request: pb::HybridSearchRequest) -> Result<HybridQuery, SearchError> {
    let time_range = time_range_from_proto(request.time_range);
    let request_filter = request.filter.map(filter_from_proto).transpose()?;
    let request_filter_mode = filter_mode_from_proto(request.filter_mode)?;
    let reference = hybrid_search_ref_from_proto(&request.version_ref);
    let mut vector = vector_query_from_proto(request.vector, time_range, reference.clone())?;
    let mut text = text_query_from_proto(request.text, time_range, reference.clone())?;
    if let Some(req_filter) = request_filter {
        vector.filter = Some(combine_filters(vector.filter, req_filter.clone()));
        vector.filter_mode = request_filter_mode;
        text.filter = Some(combine_filters(text.filter, req_filter));
        text.filter_mode = request_filter_mode;
    }
    Ok(HybridQuery {
        vector,
        text,
        k: request.k as usize,
        fusion: fusion_from_proto(request.fusion)?,
        reference,
    })
}

/// ANDs a request-level filter with an optional per-leg filter.
///
/// When both are present the result is `Filter::And([leg_filter, request_filter])`. When only
/// one side is present it is returned unchanged. The caller guarantees at least `request_filter`
/// is `Some` before calling this helper.
fn combine_filters(
    leg_filter: Option<crate::domain::Filter>,
    request_filter: crate::domain::Filter,
) -> crate::domain::Filter {
    match leg_filter {
        Some(leg) => crate::domain::Filter::And(vec![leg, request_filter]),
        None => request_filter,
    }
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
        Some(pb::fusion::Strategy::Weighted(weighted)) => {
            let vector_weight = weighted
                .vector_weight
                .unwrap_or(crate::domain::fusion::DEFAULT_WEIGHTED_VECTOR_WEIGHT);
            if !vector_weight.is_finite() || !(0.0..=1.0).contains(&vector_weight) {
                return Err(SearchError::invalid_argument(
                    "vector_weight must be a finite number in [0, 1]",
                ));
            }
            Ok(FusionSpec::Weighted { vector_weight })
        }
        None => Ok(FusionSpec::default()),
    }
}

/// Converts a proto rerank config into the optional domain spec.
///
/// An absent message or an unset strategy means no reranking (the result order is returned
/// unchanged), so existing clients that never set the field keep their behavior.
pub fn rerank_from_proto(rerank: Option<pb::Rerank>) -> Result<Option<RerankSpec>, SearchError> {
    let Some(rerank) = rerank else {
        return Ok(None);
    };
    match rerank.strategy {
        Some(pb::rerank::Strategy::Identity(identity)) => Ok(Some(RerankSpec::Identity {
            top_n: identity.top_n.map(|n| n as usize),
        })),
        None => Ok(None),
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

/// Converts the proto distance type enum. Unspecified keeps the index metric.
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

/// Converts the proto filter mode enum. Unspecified defaults to prefilter.
fn filter_mode_from_proto(mode: i32) -> Result<FilterMode, SearchError> {
    match pb::FilterMode::try_from(mode) {
        Ok(pb::FilterMode::Unspecified) | Ok(pb::FilterMode::Prefilter) => Ok(FilterMode::Prefilter),
        Ok(pb::FilterMode::Postfilter) => Ok(FilterMode::Postfilter),
        Err(_) => Err(SearchError::invalid_argument("unknown filter mode")),
    }
}

/// Converts the proto text operator enum. Unspecified defaults to OR.
fn text_operator_from_proto(operator: i32) -> Result<TextOperator, SearchError> {
    match pb::TextOperator::try_from(operator) {
        Ok(pb::TextOperator::Unspecified) | Ok(pb::TextOperator::Or) => Ok(TextOperator::Or),
        Ok(pb::TextOperator::And) => Ok(TextOperator::And),
        Err(_) => Err(SearchError::invalid_argument("unknown text operator")),
    }
}

/// Converts the proto fuzziness oneof. Absent means exact matching.
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

/// Lifts a single-leg hit into a fused hit so the reranker seam can treat every result family
/// uniformly. The leg score (distance or BM25) carries over unchanged.
pub fn hit_to_fused(hit: Hit) -> FusedHit {
    FusedHit {
        row_id: hit.row_id,
        score: hit.score,
        row: hit.row,
    }
}

/// Lowers a fused hit back into a single-leg hit after reranking, preserving the score.
pub fn fused_to_hit(hit: FusedHit) -> Hit {
    Hit {
        row_id: hit.row_id,
        score: hit.score,
        row: hit.row,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_search_ref_unset_is_serve() {
        assert_eq!(vector_search_ref_from_proto(&None), DatasetRef::Serve);
    }

    #[test]
    fn vector_search_ref_version_maps_through() {
        let version_ref = Some(pb::vector_search_request::VersionRef::Version(42));
        assert_eq!(vector_search_ref_from_proto(&version_ref), DatasetRef::Version(42));
    }

    #[test]
    fn vector_search_ref_tag_maps_through() {
        let version_ref = Some(pb::vector_search_request::VersionRef::Tag("green".to_string()));
        assert_eq!(
            vector_search_ref_from_proto(&version_ref),
            DatasetRef::Tag("green".to_string())
        );
    }

    #[test]
    fn text_search_ref_unset_is_serve() {
        assert_eq!(text_search_ref_from_proto(&None), DatasetRef::Serve);
    }

    #[test]
    fn text_search_ref_version_maps_through() {
        let version_ref = Some(pb::text_search_request::VersionRef::Version(7));
        assert_eq!(text_search_ref_from_proto(&version_ref), DatasetRef::Version(7));
    }

    #[test]
    fn text_search_ref_tag_maps_through() {
        let version_ref = Some(pb::text_search_request::VersionRef::Tag("stable".to_string()));
        assert_eq!(
            text_search_ref_from_proto(&version_ref),
            DatasetRef::Tag("stable".to_string())
        );
    }

    #[test]
    fn hybrid_search_ref_unset_is_serve() {
        assert_eq!(hybrid_search_ref_from_proto(&None), DatasetRef::Serve);
    }

    #[test]
    fn hybrid_search_ref_version_maps_through() {
        let version_ref = Some(pb::hybrid_search_request::VersionRef::Version(3));
        assert_eq!(hybrid_search_ref_from_proto(&version_ref), DatasetRef::Version(3));
    }

    #[test]
    fn hybrid_search_ref_tag_maps_through() {
        let version_ref = Some(pb::hybrid_search_request::VersionRef::Tag("prod".to_string()));
        assert_eq!(
            hybrid_search_ref_from_proto(&version_ref),
            DatasetRef::Tag("prod".to_string())
        );
    }

    #[test]
    fn hybrid_query_reference_fans_out_to_both_legs() {
        let request = pb::HybridSearchRequest {
            target: None,
            vector: Some(pb::VectorQuery {
                vector: vec![1.0, 0.0, 0.0, 0.0],
                k: 2,
                ..Default::default()
            }),
            text: Some(pb::TextQuery {
                input: Some(pb::text_query::Input::Simple("hello".to_string())),
                k: 2,
                ..Default::default()
            }),
            k: 2,
            fusion: None,
            rerank: None,
            time_range: None,
            filter: None,
            filter_mode: 0,
            version_ref: Some(pb::hybrid_search_request::VersionRef::Tag("v2".to_string())),
        };
        let query = hybrid_query_from_proto(request).unwrap();
        assert_eq!(query.reference, DatasetRef::Tag("v2".to_string()));
        assert_eq!(query.vector.reference, DatasetRef::Tag("v2".to_string()));
        assert_eq!(query.text.reference, DatasetRef::Tag("v2".to_string()));
    }

    #[test]
    fn vector_query_from_proto_sets_reference() {
        let proto_query = Some(pb::VectorQuery {
            vector: vec![1.0, 0.0],
            k: 1,
            ..Default::default()
        });
        let reference = DatasetRef::Version(5);
        let query = vector_query_from_proto(proto_query, None, reference.clone()).unwrap();
        assert_eq!(query.reference, reference);
    }

    #[test]
    fn text_query_from_proto_sets_reference() {
        let proto_query = Some(pb::TextQuery {
            input: Some(pb::text_query::Input::Simple("cats".to_string())),
            k: 3,
            ..Default::default()
        });
        let reference = DatasetRef::Tag("latest-prod".to_string());
        let query = text_query_from_proto(proto_query, None, reference.clone()).unwrap();
        assert_eq!(query.reference, reference);
    }
}
