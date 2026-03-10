//! Hybrid ranking: blend normalised vector and BM25 scores.
//!
//! Both scoring signals use different scales and orientations:
//!
//! * **Vector score** — a distance/dissimilarity; lower is better.
//! * **BM25 score** — a relevance score; higher is better.
//!
//! `hybrid_rank` normalises both to `[0, 1]` with **0 = best** and then
//! computes a weighted combination:
//!
//! ```text
//! hybrid = alpha × v_norm + (1 − alpha) × (1 − bm25_norm)
//! ```
//!
//! * `alpha = 1.0` → pure vector ordering.
//! * `alpha = 0.0` → pure lexical (BM25) ordering.
//! * `alpha = 0.5` (default) → equal blend.

use std::collections::HashMap;

use shardlake_core::types::{SearchResult, VectorId};

/// Blend vector search results with BM25 scores and return top-k.
///
/// # Arguments
///
/// * `vector_results` — candidates from ANN search (id + vector_score, lower = better).
/// * `bm25_scores` — BM25 score for each id (higher = better); ids absent from this map
///   receive a score of `0.0`.
/// * `k` — number of results to return.
/// * `alpha` — vector weight (`0.0`–`1.0`); `1.0` = pure vector, `0.0` = pure lexical.
pub fn hybrid_rank(
    vector_results: &[SearchResult],
    bm25_scores: &HashMap<VectorId, f32>,
    k: usize,
    alpha: f32,
) -> Vec<SearchResult> {
    if vector_results.is_empty() {
        return Vec::new();
    }
    let alpha = alpha.clamp(0.0, 1.0);

    // ---- normalise vector scores to [0, 1] (0 = best) ----
    let v_min = vector_results
        .iter()
        .map(|r| r.score)
        .fold(f32::INFINITY, f32::min);
    let v_max = vector_results
        .iter()
        .map(|r| r.score)
        .fold(f32::NEG_INFINITY, f32::max);
    let v_range = (v_max - v_min).max(f32::EPSILON);

    // ---- normalise BM25 scores to [0, 1] (1 = best) ----
    let bm25_values: Vec<f32> = vector_results
        .iter()
        .map(|r| *bm25_scores.get(&r.id).unwrap_or(&0.0))
        .collect();
    let bm25_min = bm25_values.iter().copied().fold(f32::INFINITY, f32::min);
    let bm25_max = bm25_values
        .iter()
        .copied()
        .fold(f32::NEG_INFINITY, f32::max);
    let bm25_range = (bm25_max - bm25_min).max(f32::EPSILON);

    let mut combined: Vec<SearchResult> = vector_results
        .iter()
        .zip(bm25_values.iter())
        .map(|(r, &bm25)| {
            let v_norm = (r.score - v_min) / v_range; // 0 = best
            let bm25_norm = (bm25 - bm25_min) / bm25_range; // 0 = worst, 1 = best
            let hybrid = alpha * v_norm + (1.0 - alpha) * (1.0 - bm25_norm);
            SearchResult {
                id: r.id,
                score: hybrid,
                metadata: r.metadata.clone(),
            }
        })
        .collect();

    combined.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    combined.dedup_by_key(|r| r.id);
    combined.truncate(k);
    combined
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use shardlake_core::types::{SearchResult, VectorId};

    fn sr(id: u64, score: f32) -> SearchResult {
        SearchResult {
            id: VectorId(id),
            score,
            metadata: None,
        }
    }

    #[test]
    fn test_pure_vector_alpha_one() {
        let vec_results = vec![sr(1, 0.1), sr(2, 0.5), sr(3, 0.3)];
        let bm25: HashMap<VectorId, f32> = [(VectorId(2), 10.0), (VectorId(3), 5.0)]
            .into_iter()
            .collect();
        let out = hybrid_rank(&vec_results, &bm25, 3, 1.0);
        // With alpha=1.0, ordering should follow vector scores: 1(0.1), 3(0.3), 2(0.5)
        assert_eq!(out[0].id, VectorId(1));
        assert_eq!(out[1].id, VectorId(3));
        assert_eq!(out[2].id, VectorId(2));
    }

    #[test]
    fn test_pure_lexical_alpha_zero() {
        let vec_results = vec![sr(1, 0.1), sr(2, 0.5), sr(3, 0.3)];
        let bm25: HashMap<VectorId, f32> = [
            (VectorId(1), 1.0),
            (VectorId(2), 10.0), // BM25 best
            (VectorId(3), 5.0),
        ]
        .into_iter()
        .collect();
        let out = hybrid_rank(&vec_results, &bm25, 3, 0.0);
        // With alpha=0.0, ordering follows BM25 desc: 2(10), 3(5), 1(1)
        assert_eq!(out[0].id, VectorId(2));
        assert_eq!(out[1].id, VectorId(3));
        assert_eq!(out[2].id, VectorId(1));
    }

    #[test]
    fn test_k_truncation() {
        let vec_results = vec![sr(1, 0.1), sr(2, 0.2), sr(3, 0.3)];
        let bm25 = HashMap::new();
        let out = hybrid_rank(&vec_results, &bm25, 2, 0.5);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn test_empty_input() {
        let out = hybrid_rank(&[], &HashMap::new(), 5, 0.5);
        assert!(out.is_empty());
    }

    #[test]
    fn test_missing_bm25_scores_treated_as_zero() {
        let vec_results = vec![sr(1, 0.1), sr(2, 0.2)];
        let bm25: HashMap<VectorId, f32> = [(VectorId(1), 5.0)].into_iter().collect();
        // Should not panic even though id 2 is absent from bm25 map
        let out = hybrid_rank(&vec_results, &bm25, 2, 0.5);
        assert_eq!(out.len(), 2);
    }
}
