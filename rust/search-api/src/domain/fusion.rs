//! Hybrid result fusion: the declarative fusion configuration and the RRF implementation.

use std::collections::HashMap;

use serde_json::Map;

use crate::domain::query::{FusedHit, Hit};

/// Default rank-smoothing constant for reciprocal-rank fusion.
pub const DEFAULT_RRF_K: f64 = 60.0;

/// Declarative fusion configuration carried by hybrid requests. New strategies slot in as
/// variants with their own `fuse` arm.
#[derive(Debug, Clone, PartialEq)]
pub enum FusionSpec {
    /// Reciprocal-rank fusion with the given rank-smoothing constant.
    Rrf {
        /// Rank-smoothing constant. Must be positive.
        rrf_k: f64,
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
    /// or server.
    pub fn fuse(&self, legs: Vec<Vec<Hit>>, k: usize) -> Vec<FusedHit> {
        match self {
            Self::Rrf { rrf_k } => rrf_fuse(*rrf_k, legs, k),
        }
    }
}

/// Reciprocal-rank fusion.
///
/// The fused score of a row is the sum over the legs containing it of `1 / (rrf_k + rank)` with
/// 1-based ranks. Row JSON objects are merged across legs, first leg wins on key conflicts.
fn rrf_fuse(rrf_k: f64, legs: Vec<Vec<Hit>>, k: usize) -> Vec<FusedHit> {
    let mut fused: HashMap<u64, FusedHit> = HashMap::new();
    for leg in legs {
        for (rank, hit) in leg.into_iter().enumerate() {
            let contribution = 1.0 / (rrf_k + (rank as f64) + 1.0);
            let entry = fused.entry(hit.row_id).or_insert_with(|| FusedHit {
                row_id: hit.row_id,
                score: 0.0,
                row: Map::new(),
            });
            entry.score += contribution;
            for (key, value) in hit.row {
                entry.row.entry(key).or_insert(value);
            }
        }
    }
    let mut ranked: Vec<FusedHit> = fused.into_values().collect();
    ranked.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    ranked.truncate(k);
    ranked
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
}
