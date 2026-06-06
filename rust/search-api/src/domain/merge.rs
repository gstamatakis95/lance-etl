//! Merging of fan-out search legs: dedup-by-id keeping the best score, then a global re-rank.

use std::collections::HashMap;

use crate::domain::query::Hit;

/// Which direction of the leg score is better when deduplicating and ranking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScoreOrder {
    /// Smaller scores win (vector distance).
    LowerIsBetter,
    /// Larger scores win (BM25 / fused relevance).
    HigherIsBetter,
}

impl ScoreOrder {
    /// Returns true when `candidate` strictly beats `incumbent` under this order.
    fn beats(self, candidate: f64, incumbent: f64) -> bool {
        match self {
            Self::LowerIsBetter => candidate < incumbent,
            Self::HigherIsBetter => candidate > incumbent,
        }
    }
}

/// Outcome of merging several fan-out legs.
#[derive(Debug, Clone)]
pub struct MergeOutcome {
    /// Merged hits ordered best-first under the score order, truncated to `k`.
    pub hits: Vec<Hit>,
    /// Duplicate hits folded into a surviving hit (same id seen again across legs).
    pub duplicates_dropped: u64,
}

/// Merges per-day result legs into one ranking.
///
/// Hits sharing the same non-null value in `id_column` are deduplicated, keeping the hit with the
/// best score under `order`. Ties keep the first hit encountered (leg order, then rank order).
/// Hits whose row lacks `id_column` (or carries a null) are never deduplicated. The merged list
/// is sorted best-first under `order` (stable, so surviving ties keep their encounter order) and
/// truncated to `k`.
pub fn merge_hits(legs: Vec<Vec<Hit>>, id_column: &str, order: ScoreOrder, k: usize) -> MergeOutcome {
    let mut keyed: HashMap<String, Hit> = HashMap::new();
    let mut keyed_order: Vec<String> = Vec::new();
    let mut unkeyed: Vec<Hit> = Vec::new();
    let mut duplicates_dropped = 0u64;
    for leg in legs {
        for hit in leg {
            let key = hit
                .row
                .get(id_column)
                .filter(|value| !value.is_null())
                .map(|value| value.to_string());
            match key {
                Some(key) => match keyed.get_mut(&key) {
                    Some(incumbent) => {
                        duplicates_dropped += 1;
                        if order.beats(hit.score, incumbent.score) {
                            *incumbent = hit;
                        }
                    }
                    None => {
                        keyed.insert(key.clone(), hit);
                        keyed_order.push(key);
                    }
                },
                None => unkeyed.push(hit),
            }
        }
    }
    let mut hits: Vec<Hit> = keyed_order
        .into_iter()
        .filter_map(|key| keyed.remove(&key))
        .chain(unkeyed)
        .collect();
    hits.sort_by(|left, right| match order {
        ScoreOrder::LowerIsBetter => left
            .score
            .partial_cmp(&right.score)
            .unwrap_or(std::cmp::Ordering::Equal),
        ScoreOrder::HigherIsBetter => right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal),
    });
    hits.truncate(k);
    MergeOutcome {
        hits,
        duplicates_dropped,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value};

    use super::*;

    /// Builds a hit carrying a `vector_id` column and a marker column naming its leg.
    fn hit(vector_id: i64, score: f64, leg: &str) -> Hit {
        let mut row = Map::new();
        row.insert("vector_id".to_string(), Value::from(vector_id));
        row.insert("leg".to_string(), Value::from(leg));
        Hit {
            row_id: vector_id as u64,
            score,
            row,
        }
    }

    /// Builds a hit without any id column.
    fn anonymous_hit(score: f64) -> Hit {
        Hit {
            row_id: 0,
            score,
            row: Map::new(),
        }
    }

    #[test]
    fn duplicates_keep_min_distance_for_vector_order() {
        let legs = vec![
            vec![hit(1, 0.5, "day1"), hit(2, 0.7, "day1")],
            vec![hit(1, 0.1, "day2")],
            vec![hit(1, 0.9, "day3")],
        ];
        let outcome = merge_hits(legs, "vector_id", ScoreOrder::LowerIsBetter, 10);
        assert_eq!(outcome.duplicates_dropped, 2);
        assert_eq!(outcome.hits.len(), 2);
        assert_eq!(outcome.hits[0].row.get("leg"), Some(&Value::from("day2")));
        assert!((outcome.hits[0].score - 0.1).abs() < 1e-12);
        assert_eq!(outcome.hits[1].row.get("vector_id"), Some(&Value::from(2)));
    }

    #[test]
    fn duplicates_keep_max_score_for_text_order() {
        let legs = vec![
            vec![hit(7, 1.0, "day1")],
            vec![hit(7, 3.0, "day2"), hit(8, 2.0, "day2")],
        ];
        let outcome = merge_hits(legs, "vector_id", ScoreOrder::HigherIsBetter, 10);
        assert_eq!(outcome.duplicates_dropped, 1);
        assert_eq!(outcome.hits[0].row.get("leg"), Some(&Value::from("day2")));
        assert!((outcome.hits[0].score - 3.0).abs() < 1e-12);
        assert_eq!(outcome.hits[1].row.get("vector_id"), Some(&Value::from(8)));
    }

    #[test]
    fn score_ties_keep_the_first_encountered_hit() {
        let legs = vec![vec![hit(1, 0.5, "day1")], vec![hit(1, 0.5, "day2")]];
        let outcome = merge_hits(legs, "vector_id", ScoreOrder::LowerIsBetter, 10);
        assert_eq!(outcome.hits.len(), 1);
        assert_eq!(outcome.duplicates_dropped, 1);
        assert_eq!(outcome.hits[0].row.get("leg"), Some(&Value::from("day1")));
    }

    #[test]
    fn empty_legs_are_harmless_and_all_empty_yields_nothing() {
        let outcome = merge_hits(
            vec![vec![], vec![hit(1, 0.2, "day2")], vec![]],
            "vector_id",
            ScoreOrder::LowerIsBetter,
            5,
        );
        assert_eq!(outcome.hits.len(), 1);
        assert_eq!(outcome.duplicates_dropped, 0);
        let outcome = merge_hits(vec![vec![], vec![]], "vector_id", ScoreOrder::HigherIsBetter, 5);
        assert!(outcome.hits.is_empty());
        assert_eq!(outcome.duplicates_dropped, 0);
    }

    #[test]
    fn rows_without_the_id_column_never_dedup() {
        let legs = vec![vec![anonymous_hit(0.3)], vec![anonymous_hit(0.3)]];
        let outcome = merge_hits(legs, "vector_id", ScoreOrder::LowerIsBetter, 10);
        assert_eq!(outcome.hits.len(), 2);
        assert_eq!(outcome.duplicates_dropped, 0);
    }

    #[test]
    fn merged_ranking_is_truncated_to_k() {
        let legs = vec![
            vec![hit(1, 0.9, "a"), hit(2, 0.1, "a")],
            vec![hit(3, 0.5, "b"), hit(4, 0.2, "b")],
        ];
        let outcome = merge_hits(legs, "vector_id", ScoreOrder::LowerIsBetter, 2);
        assert_eq!(outcome.hits.len(), 2);
        assert_eq!(outcome.hits[0].row.get("vector_id"), Some(&Value::from(2)));
        assert_eq!(outcome.hits[1].row.get("vector_id"), Some(&Value::from(4)));
    }
}
