//! Translation of the domain full-text query tree into the Lance FTS query model.

use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::inverted::query::{
    BooleanQuery, BoostQuery, FtsQuery, MatchQuery, MultiMatchQuery, Operator, PhraseQuery,
};

use crate::domain::{Fuzziness, SearchError, TextOperator, TextQuery, TextQueryNode};

/// Builds a Lance [`FullTextSearchQuery`] from a domain text query, including limit, wand factor,
/// and default-column fill-in.
///
/// `limit` is the number of hits the FTS stage must produce (already including any offset).
pub fn text_query_to_fts(query: &TextQuery, limit: usize) -> Result<FullTextSearchQuery, SearchError> {
    let node = node_to_fts(&query.node)?;
    let mut fts = FullTextSearchQuery::new_query(node)
        .limit(Some(limit as i64))
        .wand_factor(query.wand_factor);
    if !query.columns.is_empty() {
        fts = fts
            .with_columns(&query.columns)
            .map_err(|err| SearchError::invalid_argument(err.to_string()))?;
    }
    Ok(fts)
}

/// Recursively converts a domain query node into a Lance [`FtsQuery`].
fn node_to_fts(node: &TextQueryNode) -> Result<FtsQuery, SearchError> {
    match node {
        TextQueryNode::Match(spec) => {
            if spec.terms.is_empty() {
                return Err(SearchError::invalid_argument("match terms must be non-empty"));
            }
            let mut query = MatchQuery::new(spec.terms.clone())
                .with_column(spec.column.clone())
                .with_boost(spec.boost)
                .with_operator(operator_to_lance(spec.operator))
                .with_fuzziness(fuzziness_to_lance(spec.fuzziness))
                .with_prefix_length(spec.prefix_length);
            if let Some(max_expansions) = spec.max_expansions {
                query = query.with_max_expansions(max_expansions);
            }
            Ok(FtsQuery::Match(query))
        }
        TextQueryNode::Phrase(spec) => {
            if spec.terms.is_empty() {
                return Err(SearchError::invalid_argument("phrase terms must be non-empty"));
            }
            let query = PhraseQuery::new(spec.terms.clone())
                .with_column(spec.column.clone())
                .with_slop(spec.slop);
            Ok(FtsQuery::Phrase(query))
        }
        TextQueryNode::Boost {
            positive,
            negative,
            negative_boost,
        } => {
            let positive = node_to_fts(positive)?;
            let negative = node_to_fts(negative)?;
            Ok(FtsQuery::Boost(BoostQuery::new(
                positive,
                negative,
                Some(*negative_boost),
            )))
        }
        TextQueryNode::MultiMatch {
            terms,
            columns,
            boosts,
            operator,
        } => {
            if terms.is_empty() {
                return Err(SearchError::invalid_argument("multi_match terms must be non-empty"));
            }
            let mut query = MultiMatchQuery::try_new(terms.clone(), columns.clone())
                .map_err(|err| SearchError::invalid_argument(err.to_string()))?;
            if !boosts.is_empty() {
                query = query
                    .try_with_boosts(boosts.clone())
                    .map_err(|err| SearchError::invalid_argument(err.to_string()))?;
            }
            Ok(FtsQuery::MultiMatch(query.with_operator(operator_to_lance(*operator))))
        }
        TextQueryNode::Boolean { should, must, must_not } => {
            if should.is_empty() && must.is_empty() && must_not.is_empty() {
                return Err(SearchError::invalid_argument(
                    "boolean query must have at least one clause",
                ));
            }
            Ok(FtsQuery::Boolean(BooleanQuery {
                should: nodes_to_fts(should)?,
                must: nodes_to_fts(must)?,
                must_not: nodes_to_fts(must_not)?,
            }))
        }
    }
}

/// Converts a list of domain query nodes.
fn nodes_to_fts(nodes: &[TextQueryNode]) -> Result<Vec<FtsQuery>, SearchError> {
    nodes.iter().map(node_to_fts).collect()
}

/// Maps the domain term operator onto the Lance operator.
fn operator_to_lance(operator: TextOperator) -> Operator {
    match operator {
        TextOperator::Or => Operator::Or,
        TextOperator::And => Operator::And,
    }
}

/// Maps the domain fuzziness onto the Lance encoding (`None` means auto, `Some(0)` exact).
fn fuzziness_to_lance(fuzziness: Fuzziness) -> Option<u32> {
    match fuzziness {
        Fuzziness::Exact => Some(0),
        Fuzziness::Auto => None,
        Fuzziness::Distance(distance) => Some(distance),
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::{MatchSpec, PhraseSpec};

    use super::*;

    #[test]
    fn simple_match_translates_with_defaults() {
        let query = TextQuery::simple("hello world", 5);
        let fts = text_query_to_fts(&query, 5).unwrap();
        assert_eq!(fts.limit, Some(5));
        match fts.query {
            FtsQuery::Match(inner) => {
                assert_eq!(inner.terms, "hello world");
                assert_eq!(inner.operator, Operator::Or);
                assert_eq!(inner.fuzziness, Some(0));
            }
            other => panic!("expected match query, got {other:?}"),
        }
    }

    #[test]
    fn boolean_and_phrase_nodes_translate() {
        let node = TextQueryNode::Boolean {
            should: vec![TextQueryNode::Match(MatchSpec::new("pear"))],
            must: vec![TextQueryNode::Phrase(PhraseSpec {
                terms: "green pear".to_string(),
                column: Some("text".to_string()),
                slop: 1,
            })],
            must_not: Vec::new(),
        };
        let fts = node_to_fts(&node).unwrap();
        match fts {
            FtsQuery::Boolean(inner) => {
                assert_eq!(inner.should.len(), 1);
                assert_eq!(inner.must.len(), 1);
                match &inner.must[0] {
                    FtsQuery::Phrase(phrase) => {
                        assert_eq!(phrase.slop, 1);
                        assert_eq!(phrase.column.as_deref(), Some("text"));
                    }
                    other => panic!("expected phrase query, got {other:?}"),
                }
            }
            other => panic!("expected boolean query, got {other:?}"),
        }
    }

    #[test]
    fn empty_terms_and_empty_boolean_are_rejected() {
        let err = node_to_fts(&TextQueryNode::Match(MatchSpec::new(""))).unwrap_err();
        assert!(matches!(err, SearchError::InvalidArgument(_)));
        let err = node_to_fts(&TextQueryNode::Boolean {
            should: Vec::new(),
            must: Vec::new(),
            must_not: Vec::new(),
        })
        .unwrap_err();
        assert!(matches!(err, SearchError::InvalidArgument(_)));
    }
}
