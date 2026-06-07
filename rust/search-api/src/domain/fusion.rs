//! Hybrid result fusion: the declarative fusion configuration and the RRF implementation.

use std::collections::HashMap;

use serde_json::{Map, Value, json};

use crate::domain::query::{FusedHit, Hit};

/// Default rank-smoothing constant for reciprocal-rank fusion.
pub const DEFAULT_RRF_K: f64 = 60.0;

/// Default vector-leg weight for weighted fusion. The text leg takes the complement.
pub const DEFAULT_WEIGHTED_VECTOR_WEIGHT: f64 = 0.7;

/// Declarative fusion configuration carried by hybrid requests. New strategies slot in as
/// variants with their own `fuse` arm.
#[derive(Debug, Clone, PartialEq)]
pub enum FusionSpec {
    /// Reciprocal-rank fusion with the given rank-smoothing constant.
    Rrf {
        /// Rank-smoothing constant. Must be positive.
        rrf_k: f64,
    },
    /// Weighted score fusion of min-max normalized legs.
    Weighted {
        /// Vector-leg weight in `[0, 1]`. The text leg takes `1 - vector_weight`.
        vector_weight: f64,
    },
}

impl Default for FusionSpec {
    fn default() -> Self {
        Self::Rrf { rrf_k: DEFAULT_RRF_K }
    }
}

impl FusionSpec {
    /// Merges `legs` (each ordered best-first) into at most `k` fused hits ordered best-first.
    ///
    /// Fusion is a pure function of the input legs so it stays unit-testable without any engine
    /// or server. Exactly two legs are expected as a hard constraint: the first leg is the vector
    /// leg (a distance, lower is better) and the second is the text leg (BM25, higher is better).
    /// Panics in debug builds if `legs.len() > 2` for `FusionSpec::Weighted`, since the weighting
    /// and normalization direction are derived from leg position.
    pub fn fuse(&self, legs: Vec<Vec<Hit>>, k: usize) -> Vec<FusedHit> {
        match self {
            Self::Rrf { rrf_k } => rrf_fuse(*rrf_k, legs, k),
            Self::Weighted { vector_weight } => weighted_fuse(*vector_weight, legs, k),
        }
    }

    /// Renders the strategy as the stable JSON recorded under the `recall.fusion` span attribute.
    ///
    /// The shapes the offline recall job parses are `{"rrf":{"k":60.0}}` and
    /// `{"weighted":{"vector_weight":0.7}}`.
    pub fn to_recall_json(&self) -> Value {
        match self {
            Self::Rrf { rrf_k } => json!({ "rrf": { "k": rrf_k } }),
            Self::Weighted { vector_weight } => json!({ "weighted": { "vector_weight": vector_weight } }),
        }
    }
}

/// Reciprocal-rank fusion.
///
/// The fused score of a row is the sum over the legs containing it of `1 / (rrf_k + rank)` with
/// 1-based ranks. Row JSON objects are merged across legs, first leg wins on key conflicts.
fn rrf_fuse(rrf_k: f64, legs: Vec<Vec<Hit>>, k: usize) -> Vec<FusedHit> {
    let mut fused: HashMap<u64, FusedHit> = HashMap::new();
    let mut order: Vec<u64> = Vec::new();
    for leg in legs {
        for (rank, hit) in leg.into_iter().enumerate() {
            let contribution = 1.0 / (rrf_k + (rank as f64) + 1.0);
            let entry = fused.entry(hit.row_id).or_insert_with(|| {
                order.push(hit.row_id);
                FusedHit {
                    row_id: hit.row_id,
                    score: 0.0,
                    row: Map::new(),
                }
            });
            entry.score += contribution;
            for (key, value) in hit.row {
                entry.row.entry(key).or_insert(value);
            }
        }
    }
    let mut ranked: Vec<FusedHit> = order.into_iter().filter_map(|row_id| fused.remove(&row_id)).collect();
    ranked.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    ranked.truncate(k);
    ranked
}

/// Weighted score fusion of min-max normalized legs.
///
/// Each leg is normalized independently into `[0, 1]`. The first leg is the vector leg, whose score
/// is a distance (lower is better), so it is inverted: the smallest distance maps to 1.0. The
/// second leg is the text leg, whose BM25 score is larger-is-better, so the largest maps to 1.0. A
/// leg whose scores are all equal (including a single-hit leg) maps every hit to 1.0. The fused
/// score of a row is `vector_weight * vector_norm + (1 - vector_weight) * text_norm`, treating a
/// missing leg contribution as 0. Row JSON objects are merged across legs, first leg wins on key
/// conflicts.
fn weighted_fuse(vector_weight: f64, legs: Vec<Vec<Hit>>, k: usize) -> Vec<FusedHit> {
    debug_assert!(
        legs.len() <= 2,
        "weighted_fuse expects at most 2 legs; got {}",
        legs.len()
    );
    let text_weight = 1.0 - vector_weight;
    let mut fused: HashMap<u64, FusedHit> = HashMap::new();
    let mut order: Vec<u64> = Vec::new();
    for (leg_index, leg) in legs.into_iter().enumerate() {
        let lower_is_better = leg_index == 0;
        let weight = if lower_is_better { vector_weight } else { text_weight };
        let normalized = min_max_normalize(&leg, lower_is_better);
        for (hit, norm) in leg.into_iter().zip(normalized) {
            let row_id = hit.row_id;
            let entry = fused.entry(row_id).or_insert_with(|| {
                order.push(row_id);
                FusedHit {
                    row_id,
                    score: 0.0,
                    row: Map::new(),
                }
            });
            entry.score += weight * norm;
            for (key, value) in hit.row {
                entry.row.entry(key).or_insert(value);
            }
        }
    }
    let mut ranked: Vec<FusedHit> = order.into_iter().filter_map(|row_id| fused.remove(&row_id)).collect();
    ranked.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    ranked.truncate(k);
    ranked
}

/// Min-max normalizes one leg's scores into `[0, 1]`, one entry per hit in input order.
///
/// When `lower_is_better` the smallest score maps to 1.0 (vector distances); otherwise the largest
/// maps to 1.0 (BM25). A degenerate leg whose scores are all equal maps every hit to 1.0 so a
/// single-hit or all-tied leg still contributes its full weight.
fn min_max_normalize(leg: &[Hit], lower_is_better: bool) -> Vec<f64> {
    if leg.is_empty() {
        return Vec::new();
    }
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for hit in leg {
        min = min.min(hit.score);
        max = max.max(hit.score);
    }
    let span = max - min;
    leg.iter()
        .map(|hit| {
            if span <= 0.0 {
                1.0
            } else if lower_is_better {
                (max - hit.score) / span
            } else {
                (hit.score - min) / span
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, Value};

    use super::*;

    /// Builds a hit with the given row id and a single `id` column.
    fn hit(row_id: u64) -> Hit {
        let mut row = Map::new();
        row.insert("id".to_string(), Value::from(row_id));
        Hit {
            row_id,
            score: 0.0,
            row,
        }
    }

    #[test]
    fn rrf_scores_match_the_formula() {
        let fusion = FusionSpec::Rrf { rrf_k: 60.0 };
        let fused = fusion.fuse(vec![vec![hit(1), hit(2)], vec![hit(2), hit(3)]], 10);
        assert_eq!(fused.len(), 3);
        assert_eq!(fused[0].row_id, 2);
        let expected_top = 1.0 / 62.0 + 1.0 / 61.0;
        assert!((fused[0].score - expected_top).abs() < 1e-12);
        let expected_single = 1.0 / 61.0;
        assert!((fused[1].score - expected_single).abs() < 1e-12);
        assert!((fused[2].score - 1.0 / 62.0).abs() < 1e-12);
    }

    #[test]
    fn rrf_respects_custom_constant_and_truncates_to_k() {
        let fusion = FusionSpec::Rrf { rrf_k: 1.0 };
        let fused = fusion.fuse(vec![vec![hit(7), hit(8)], vec![hit(7)]], 1);
        assert_eq!(fused.len(), 1);
        assert_eq!(fused[0].row_id, 7);
        assert!((fused[0].score - 1.0).abs() < 1e-12);
    }

    #[test]
    fn rrf_merges_row_columns_across_legs() {
        let mut left_row = Map::new();
        left_row.insert("a".to_string(), Value::from(1));
        let mut right_row = Map::new();
        right_row.insert("b".to_string(), Value::from(2));
        let legs = vec![
            vec![Hit {
                row_id: 5,
                score: 0.0,
                row: left_row,
            }],
            vec![Hit {
                row_id: 5,
                score: 0.0,
                row: right_row,
            }],
        ];
        let fused = FusionSpec::Rrf { rrf_k: 60.0 }.fuse(legs, 10);
        assert_eq!(fused.len(), 1);
        assert_eq!(fused[0].row.get("a"), Some(&Value::from(1)));
        assert_eq!(fused[0].row.get("b"), Some(&Value::from(2)));
    }

    #[test]
    fn default_spec_builds_rrf_with_sixty() {
        let spec = FusionSpec::default();
        assert_eq!(spec, FusionSpec::Rrf { rrf_k: DEFAULT_RRF_K });
        let fused = spec.fuse(vec![vec![hit(1)]], 5);
        assert!((fused[0].score - 1.0 / 61.0).abs() < 1e-12);
    }

    /// Builds a hit with an explicit score and a single `id` column.
    fn scored(row_id: u64, score: f64) -> Hit {
        let mut row = Map::new();
        row.insert("id".to_string(), Value::from(row_id));
        Hit { row_id, score, row }
    }

    #[test]
    fn weighted_normalizes_each_leg_and_combines_with_the_weight() {
        let vector_leg = vec![scored(1, 0.0), scored(2, 1.0)];
        let text_leg = vec![scored(2, 10.0), scored(3, 0.0)];
        let fused = FusionSpec::Weighted { vector_weight: 0.6 }.fuse(vec![vector_leg, text_leg], 10);
        let score = |id: u64| fused.iter().find(|hit| hit.row_id == id).map(|hit| hit.score).unwrap();
        assert!(
            (score(1) - 0.6).abs() < 1e-12,
            "best vector hit gets full vector weight"
        );
        assert!(
            (score(2) - 0.4).abs() < 1e-12,
            "worst vector + best text gets full text weight"
        );
        assert!((score(3) - 0.0).abs() < 1e-12, "worst in both legs scores zero");
        assert_eq!(fused[0].row_id, 1, "ordering is by fused score, best first");
    }

    #[test]
    fn weighted_single_hit_leg_maps_to_full_weight() {
        let fused = FusionSpec::Weighted { vector_weight: 0.7 }.fuse(vec![vec![scored(5, 4.2)]], 10);
        assert_eq!(fused.len(), 1);
        assert!(
            (fused[0].score - 0.7).abs() < 1e-12,
            "a single-hit vector leg normalizes to 1.0"
        );
    }

    #[test]
    fn weighted_all_ties_in_a_leg_map_to_full_weight() {
        let vector_leg = vec![scored(1, 2.0), scored(2, 2.0)];
        let fused = FusionSpec::Weighted { vector_weight: 1.0 }.fuse(vec![vector_leg], 10);
        for hit in &fused {
            assert!((hit.score - 1.0).abs() < 1e-12, "tied distances all normalize to 1.0");
        }
    }

    #[test]
    fn weight_extreme_zero_ignores_the_vector_leg() {
        let vector_leg = vec![scored(1, 0.0), scored(2, 1.0)];
        let text_leg = vec![scored(2, 10.0), scored(3, 0.0)];
        let fused = FusionSpec::Weighted { vector_weight: 0.0 }.fuse(vec![vector_leg, text_leg], 10);
        let score = |id: u64| fused.iter().find(|hit| hit.row_id == id).map(|hit| hit.score).unwrap();
        assert!(
            (score(1) - 0.0).abs() < 1e-12,
            "vector-only hit contributes nothing at weight 0"
        );
        assert!(
            (score(2) - 1.0).abs() < 1e-12,
            "best text hit takes the full text weight"
        );
        assert_eq!(fused[0].row_id, 2);
    }

    #[test]
    fn weight_extreme_one_ignores_the_text_leg() {
        let vector_leg = vec![scored(1, 0.0), scored(2, 1.0)];
        let text_leg = vec![scored(2, 10.0), scored(3, 0.0)];
        let fused = FusionSpec::Weighted { vector_weight: 1.0 }.fuse(vec![vector_leg, text_leg], 10);
        let score = |id: u64| fused.iter().find(|hit| hit.row_id == id).map(|hit| hit.score).unwrap();
        assert!(
            (score(1) - 1.0).abs() < 1e-12,
            "best vector hit takes the full vector weight"
        );
        assert!(
            (score(3) - 0.0).abs() < 1e-12,
            "text-only hit contributes nothing at weight 1"
        );
    }

    #[test]
    fn weighted_recall_json_shape_is_stable() {
        assert_eq!(
            FusionSpec::Weighted { vector_weight: 0.7 }.to_recall_json().to_string(),
            r#"{"weighted":{"vector_weight":0.7}}"#
        );
        assert_eq!(
            FusionSpec::Rrf { rrf_k: 60.0 }.to_recall_json().to_string(),
            r#"{"rrf":{"k":60.0}}"#
        );
    }
}
