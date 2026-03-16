//! Weighted hybrid ranking: combining vector-score and BM25-score signals.
//!
//! # Overview
//!
//! [`rank_hybrid`] accepts two candidate lists — one from vector (ANN/exact)
//! retrieval and one from BM25 lexical retrieval — and merges them into a
//! single, deterministically-ordered top-*k* result list using an explicit
//! [`HybridRankingPolicy`].
//!
//! # Normalization
//!
//! Both score lists follow the **lower-is-better** convention used throughout
//! Shardlake (vector distance scores and negated BM25 scores alike).  Before
//! combining them, each list is independently normalized to the \[0, 1\] range
//! using min–max normalization:
//!
//! ```text
//! norm(score) = (score − min_score) / (max_score − min_score)
//! ```
//!
//! When all scores in a list are identical (including when the list contains
//! exactly one entry) the normalized score is `0.0` for every entry — i.e. all
//! candidates are considered equally good on that signal.
//!
//! # Weighted combination
//!
//! The hybrid score for each candidate is:
//!
//! ```text
//! hybrid_score = vector_weight × vector_norm + bm25_weight × bm25_norm
//! ```
//!
//! where `vector_norm` and `bm25_norm` are the normalized scores for that
//! candidate.  A lower `hybrid_score` still means a better result.
//!
//! # Missing-signal behavior
//!
//! A candidate that appears in only one of the two lists has no score for the
//! other signal.  The missing normalized score is treated as `1.0` (the worst
//! possible normalized value), giving the other signal's score full influence
//! for that candidate.  This means a candidate with a strong score on its only
//! available signal can still outrank a candidate with mediocre scores on both;
//! however, a candidate that scores poorly on its only available signal will
//! rank behind well-scoring candidates that appear in both lists.
//!
//! # Tie-breaking
//!
//! When two candidates produce an identical `hybrid_score` the one with the
//! **lower [`VectorId`]** wins, giving a fully deterministic order that is
//! stable across runs regardless of shard enumeration order or hash map
//! iteration order.
//!
//! # Examples
//!
//! ```
//! use shardlake_core::types::{SearchResult, VectorId};
//! use shardlake_index::ranking::{HybridRankingPolicy, rank_hybrid};
//!
//! // Doc 2 appears in both lists and has the best score on each signal,
//! // so it should rank first after blending.
//! let vector_results = vec![
//!     SearchResult { id: VectorId(1), score: 0.5, metadata: None },
//!     SearchResult { id: VectorId(2), score: 0.1, metadata: None }, // best vector
//! ];
//! let bm25_results = vec![
//!     SearchResult { id: VectorId(2), score: -2.0, metadata: None }, // best BM25
//!     SearchResult { id: VectorId(3), score: -1.0, metadata: None },
//! ];
//!
//! let policy = HybridRankingPolicy { vector_weight: 0.7, bm25_weight: 0.3 };
//! let ranked = rank_hybrid(vector_results, bm25_results, &policy, 3);
//! assert_eq!(ranked.len(), 3);
//! // Doc 2 appears in both lists with the best score on each; it ranks first.
//! assert_eq!(ranked[0].id, VectorId(2));
//! ```

use std::collections::HashMap;

use shardlake_core::types::{SearchResult, VectorId};

// ── HybridRankingPolicy ───────────────────────────────────────────────────────

/// Weighting policy for blending vector-distance and BM25 lexical scores.
///
/// Both weights must be **non-negative** and at least one must be **positive**.
/// The weights do not need to sum to 1; they are used as-is in the linear
/// combination of the two normalized score signals.
///
/// # Validation
///
/// Call [`HybridRankingPolicy::validate`] before using a policy obtained from
/// untrusted input.  It returns an error when either weight is negative or
/// when both are zero.
///
/// # Examples
///
/// ```
/// use shardlake_index::ranking::HybridRankingPolicy;
///
/// // Equal blend.
/// let equal = HybridRankingPolicy { vector_weight: 0.5, bm25_weight: 0.5 };
/// assert!(equal.validate().is_ok());
///
/// // Pure vector.
/// let pure_vec = HybridRankingPolicy { vector_weight: 1.0, bm25_weight: 0.0 };
/// assert!(pure_vec.validate().is_ok());
///
/// // Invalid: both weights are zero.
/// let zero = HybridRankingPolicy { vector_weight: 0.0, bm25_weight: 0.0 };
/// assert!(zero.validate().is_err());
/// ```
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HybridRankingPolicy {
    /// Weight applied to the normalized vector-distance score.
    pub vector_weight: f32,
    /// Weight applied to the normalized BM25 lexical score.
    pub bm25_weight: f32,
}

impl HybridRankingPolicy {
    /// Validate the policy.
    ///
    /// Returns an error string when:
    /// - `vector_weight` is negative,
    /// - `bm25_weight` is negative, or
    /// - both weights are zero (or NaN).
    ///
    /// # Errors
    ///
    /// Returns `Err(String)` describing the violated constraint.
    pub fn validate(&self) -> Result<(), String> {
        if self.vector_weight < 0.0 {
            return Err("vector_weight must be ≥ 0".into());
        }
        if self.bm25_weight < 0.0 {
            return Err("bm25_weight must be ≥ 0".into());
        }
        if self.vector_weight == 0.0 && self.bm25_weight == 0.0 {
            return Err("at least one of vector_weight or bm25_weight must be > 0".into());
        }
        Ok(())
    }
}

impl Default for HybridRankingPolicy {
    /// Equal weighting of both signals.
    fn default() -> Self {
        Self {
            vector_weight: 0.5,
            bm25_weight: 0.5,
        }
    }
}

// ── rank_hybrid ───────────────────────────────────────────────────────────────

/// Merge and rank `vector_results` and `bm25_results` using `policy`.
///
/// Returns the globally-ranked top-`k` results following the rules described
/// in the [module documentation](self).  Returns an empty [`Vec`] when `k` is
/// 0 or both input lists are empty.
///
/// The output `score` field on each returned [`SearchResult`] contains the raw
/// **hybrid score** (the weighted sum of the two normalized scores, in the
/// \[0, 1\] range).  A lower score means a better result.
///
/// Metadata is preserved: if a candidate appears in both lists, the metadata
/// from `vector_results` takes precedence; if it appears in only one list its
/// metadata is used as-is.
///
/// # Panics
///
/// Does not panic.  NaN scores in either input list are treated as the worst
/// possible score (equivalent to `1.0` after normalization).
pub fn rank_hybrid(
    vector_results: Vec<SearchResult>,
    bm25_results: Vec<SearchResult>,
    policy: &HybridRankingPolicy,
    k: usize,
) -> Vec<SearchResult> {
    if k == 0 {
        return Vec::new();
    }

    // Normalize each list independently (lower-is-better, min-max).
    let vector_norm = normalize_scores(&vector_results);
    let bm25_norm = normalize_scores(&bm25_results);

    // Build lookup maps: VectorId → (normalized_score, metadata).
    let vector_map: HashMap<VectorId, (f32, Option<serde_json::Value>)> = vector_results
        .into_iter()
        .zip(vector_norm)
        .map(|(r, norm)| (r.id, (norm, r.metadata)))
        .collect();

    let bm25_map: HashMap<VectorId, f32> = bm25_results
        .into_iter()
        .zip(bm25_norm)
        .map(|(r, norm)| (r.id, norm))
        .collect();

    // Collect the union of all candidate IDs.
    let mut all_ids: Vec<VectorId> = vector_map.keys().copied().collect();
    for id in bm25_map.keys() {
        if !vector_map.contains_key(id) {
            all_ids.push(*id);
        }
    }

    if all_ids.is_empty() {
        return Vec::new();
    }

    // Compute hybrid scores.
    let mut candidates: Vec<SearchResult> = all_ids
        .into_iter()
        .map(|id| {
            let (v_norm, metadata) = vector_map
                .get(&id)
                .map(|(n, m)| (*n, m.clone()))
                .unwrap_or((1.0_f32, None));
            let b_norm = bm25_map.get(&id).copied().unwrap_or(1.0_f32);

            // If both weights are concentrated on one signal, only that
            // signal's score participates (missing signal contributes 0 × 1.0
            // when the weight for that signal is 0.0, which is correct).
            let hybrid_score = policy.vector_weight * v_norm + policy.bm25_weight * b_norm;

            SearchResult {
                id,
                score: hybrid_score,
                metadata,
            }
        })
        .collect();

    // Sort: primary = hybrid_score ascending, secondary = VectorId ascending.
    candidates.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.id.cmp(&b.id))
    });

    candidates.truncate(k);
    candidates
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Min–max normalize a slice of [`SearchResult`] scores to the \[0, 1\] range.
///
/// Returns a `Vec<f32>` of the same length as `results`, each value in \[0, 1\].
/// When `results` is empty or all scores are identical (or NaN), every
/// normalized value is `0.0`.
fn normalize_scores(results: &[SearchResult]) -> Vec<f32> {
    if results.is_empty() {
        return Vec::new();
    }

    // Fold to find min and max, treating NaN as worst (positive infinity).
    let (min_score, max_score) =
        results
            .iter()
            .fold((f32::INFINITY, f32::NEG_INFINITY), |acc, r| {
                let s = if r.score.is_nan() {
                    f32::INFINITY
                } else {
                    r.score
                };
                (acc.0.min(s), acc.1.max(s))
            });

    let range = max_score - min_score;

    results
        .iter()
        .map(|r| {
            let s = if r.score.is_nan() {
                f32::INFINITY
            } else {
                r.score
            };
            if range == 0.0 || range.is_nan() {
                0.0_f32
            } else {
                (s - min_score) / range
            }
        })
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use shardlake_core::types::VectorId;

    fn sr(id: u64, score: f32) -> SearchResult {
        SearchResult {
            id: VectorId(id),
            score,
            metadata: None,
        }
    }

    // ── HybridRankingPolicy::validate ─────────────────────────────────────────

    #[test]
    fn policy_default_is_valid() {
        assert!(HybridRankingPolicy::default().validate().is_ok());
    }

    #[test]
    fn policy_rejects_negative_vector_weight() {
        let p = HybridRankingPolicy {
            vector_weight: -0.1,
            bm25_weight: 0.5,
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn policy_rejects_negative_bm25_weight() {
        let p = HybridRankingPolicy {
            vector_weight: 0.5,
            bm25_weight: -0.1,
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn policy_rejects_both_zero_weights() {
        let p = HybridRankingPolicy {
            vector_weight: 0.0,
            bm25_weight: 0.0,
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn policy_accepts_pure_vector() {
        let p = HybridRankingPolicy {
            vector_weight: 1.0,
            bm25_weight: 0.0,
        };
        assert!(p.validate().is_ok());
    }

    #[test]
    fn policy_accepts_pure_bm25() {
        let p = HybridRankingPolicy {
            vector_weight: 0.0,
            bm25_weight: 1.0,
        };
        assert!(p.validate().is_ok());
    }

    // ── normalize_scores ─────────────────────────────────────────────────────

    #[test]
    fn normalize_empty_list() {
        let norms = normalize_scores(&[]);
        assert!(norms.is_empty());
    }

    #[test]
    fn normalize_single_element() {
        let norms = normalize_scores(&[sr(1, 0.42)]);
        assert_eq!(norms, vec![0.0]);
    }

    #[test]
    fn normalize_all_equal_scores() {
        let results = vec![sr(1, 2.0), sr(2, 2.0), sr(3, 2.0)];
        let norms = normalize_scores(&results);
        assert_eq!(norms, vec![0.0, 0.0, 0.0]);
    }

    #[test]
    fn normalize_distinct_scores() {
        // Scores: 0.0 (best), 0.5, 1.0 (worst) → normalized: 0.0, 0.5, 1.0
        let results = vec![sr(1, 0.0), sr(2, 0.5), sr(3, 1.0)];
        let norms = normalize_scores(&results);
        assert!((norms[0] - 0.0).abs() < 1e-6);
        assert!((norms[1] - 0.5).abs() < 1e-6);
        assert!((norms[2] - 1.0).abs() < 1e-6);
    }

    #[test]
    fn normalize_negative_scores_bm25_convention() {
        // BM25 returns negated scores: -3.0 (best), -2.0, -0.5 (worst)
        let results = vec![sr(1, -3.0), sr(2, -2.0), sr(3, -0.5)];
        let norms = normalize_scores(&results);
        assert!((norms[0] - 0.0).abs() < 1e-6); // best → 0
        assert!((norms[2] - 1.0).abs() < 1e-6); // worst → 1
    }

    // ── rank_hybrid ───────────────────────────────────────────────────────────

    #[test]
    fn rank_hybrid_k_zero_returns_empty() {
        let results = rank_hybrid(
            vec![sr(1, 0.1)],
            vec![sr(1, -1.0)],
            &HybridRankingPolicy::default(),
            0,
        );
        assert!(results.is_empty());
    }

    #[test]
    fn rank_hybrid_both_empty_returns_empty() {
        let results = rank_hybrid(vec![], vec![], &HybridRankingPolicy::default(), 5);
        assert!(results.is_empty());
    }

    #[test]
    fn rank_hybrid_pure_vector_ignores_bm25() {
        // With bm25_weight = 0.0 the BM25 list should have no effect on ranking.
        let policy = HybridRankingPolicy {
            vector_weight: 1.0,
            bm25_weight: 0.0,
        };
        // Doc 1 has better (lower) vector score than doc 2.
        let vector = vec![sr(1, 0.1), sr(2, 0.9)];
        // BM25 disagrees: doc 2 is ranked above doc 1 in BM25.
        let bm25 = vec![sr(2, -5.0), sr(1, -0.1)];
        let ranked = rank_hybrid(vector, bm25, &policy, 2);
        assert_eq!(ranked[0].id, VectorId(1));
        assert_eq!(ranked[1].id, VectorId(2));
    }

    #[test]
    fn rank_hybrid_pure_bm25_ignores_vector() {
        let policy = HybridRankingPolicy {
            vector_weight: 0.0,
            bm25_weight: 1.0,
        };
        // Doc 3 has the best BM25 score (most negative = highest relevance).
        let bm25 = vec![sr(1, -1.0), sr(2, -2.0), sr(3, -5.0)];
        let vector = vec![sr(1, 0.05), sr(2, 0.3), sr(3, 0.99)];
        let ranked = rank_hybrid(vector, bm25, &policy, 3);
        assert_eq!(ranked[0].id, VectorId(3));
    }

    #[test]
    fn rank_hybrid_blends_both_signals() {
        // Doc 1: great vector score, poor BM25.
        // Doc 2: great BM25 score, poor vector.
        // Doc 3: mediocre on both.
        // With equal weights doc 2's BM25 advantage should lift it.
        let policy = HybridRankingPolicy {
            vector_weight: 0.5,
            bm25_weight: 0.5,
        };
        // vector scores: doc 1 = best (0.0), doc 2 = worst (1.0), doc 3 = mid (0.5)
        let vector = vec![sr(1, 0.0), sr(2, 1.0), sr(3, 0.5)];
        // bm25 scores (negated): doc 1 = worst (-0.1), doc 2 = best (-10.0), doc 3 = mid (-5.0)
        let bm25 = vec![sr(1, -0.1), sr(2, -10.0), sr(3, -5.0)];

        // After normalization:
        //   vector: doc1 = 0.0, doc3 = 0.5, doc2 = 1.0
        //   bm25:   doc2 = 0.0, doc3 = 0.508..., doc1 = 1.0
        //
        // Hybrid (equal weights × 0.5):
        //   doc1 = 0.5*(0.0) + 0.5*(1.0) = 0.5
        //   doc2 = 0.5*(1.0) + 0.5*(0.0) = 0.5  → tie, lower id (1) < (2) but doc1 id=1 < doc2 id=2
        //   doc3 = 0.5*(0.5) + 0.5*(0.508) ≈ 0.504
        //
        // Expected order: doc1 (0.5, id=1) < doc2 (0.5, id=2) < doc3 (~0.504)
        // Actually doc1 and doc2 both have 0.5 → tie broken by id: doc1 (id=1) wins.
        let ranked = rank_hybrid(vector, bm25, &policy, 3);
        assert_eq!(ranked.len(), 3);
        // Both doc1 and doc2 have hybrid score 0.5; tie breaks by id → doc1 < doc2
        assert_eq!(ranked[0].id, VectorId(1));
        assert_eq!(ranked[1].id, VectorId(2));
        assert_eq!(ranked[2].id, VectorId(3));
    }

    #[test]
    fn rank_hybrid_missing_vector_score_penalized() {
        // Doc 3 is only in BM25 results (no vector score).
        // All BM25 scores are identical so bm25_norm = 0.0 for every doc.
        // Doc 3's missing vector score is treated as 1.0 (worst).
        //
        // Hybrid scores (vector_weight=0.7, bm25_weight=0.3):
        //   doc1: 0.7 * v_norm(doc1) + 0.3 * 0.0
        //   doc2: 0.7 * v_norm(doc2) + 0.3 * 0.0
        //   doc3: 0.7 * 1.0          + 0.3 * 0.0  = 0.7  (missing vector → penalized)
        //
        // With vector scores 0.1 (doc1, best) and 0.2 (doc2, worst):
        //   doc1_vnorm = 0.0  → hybrid = 0.0
        //   doc2_vnorm = 1.0  → hybrid = 0.7
        //   doc3          → hybrid = 0.7  (tie with doc2; id=2 < id=3 → doc2 ranks ahead)
        //
        // Expected order: doc1, doc2, doc3
        let policy = HybridRankingPolicy {
            vector_weight: 0.7,
            bm25_weight: 0.3,
        };
        let vector = vec![sr(1, 0.1), sr(2, 0.2)];
        // All BM25 scores identical so bm25_norm is 0.0 for all.
        let bm25 = vec![sr(1, -3.0), sr(2, -3.0), sr(3, -3.0)];
        let ranked = rank_hybrid(vector, bm25, &policy, 3);
        let ids: Vec<u64> = ranked.iter().map(|r| r.id.0).collect();
        assert!(ids.contains(&3));
        assert_eq!(
            ids[2], 3,
            "doc 3 (missing vector score) should rank last: got {ids:?}"
        );
    }

    #[test]
    fn rank_hybrid_missing_bm25_score_penalized() {
        // Doc 3 is only in vector results (no BM25 score).
        // All vector scores are identical so vector_norm = 0.0 for every doc.
        // Doc 3's missing BM25 score is treated as 1.0 (worst).
        //
        // Hybrid scores (vector_weight=0.3, bm25_weight=0.7):
        //   doc1: 0.3 * 0.0 + 0.7 * b_norm(doc1)
        //   doc2: 0.3 * 0.0 + 0.7 * b_norm(doc2)
        //   doc3: 0.3 * 0.0 + 0.7 * 1.0 = 0.7  (missing BM25 → penalized)
        //
        // With BM25 scores -3.0 (doc1, best) and -1.0 (doc2, worst):
        //   doc1_bnorm = 0.0  → hybrid = 0.0
        //   doc2_bnorm = 1.0  → hybrid = 0.7
        //   doc3          → hybrid = 0.7  (tie with doc2; id=2 < id=3 → doc2 ranks ahead)
        //
        // Expected order: doc1, doc2, doc3
        let policy = HybridRankingPolicy {
            vector_weight: 0.3,
            bm25_weight: 0.7,
        };
        // All vector scores identical so vector_norm is 0.0 for all.
        let vector = vec![sr(1, 0.5), sr(2, 0.5), sr(3, 0.5)];
        let bm25 = vec![sr(1, -3.0), sr(2, -1.0)];
        let ranked = rank_hybrid(vector, bm25, &policy, 3);
        let ids: Vec<u64> = ranked.iter().map(|r| r.id.0).collect();
        assert_eq!(
            ids[2], 3,
            "doc 3 (missing bm25 score) should rank last: got {ids:?}"
        );
    }

    #[test]
    fn rank_hybrid_stable_tie_breaking_by_id() {
        // All candidates end up with identical hybrid scores; ordering must be
        // deterministic and follow VectorId ascending.
        let policy = HybridRankingPolicy {
            vector_weight: 1.0,
            bm25_weight: 0.0,
        };
        // All vector scores identical → all normalized to 0.0 → all hybrid = 0.0.
        let vector = vec![sr(5, 0.5), sr(2, 0.5), sr(9, 0.5), sr(1, 0.5)];
        let ranked = rank_hybrid(vector, vec![], &policy, 4);
        let ids: Vec<u64> = ranked.iter().map(|r| r.id.0).collect();
        assert_eq!(ids, vec![1, 2, 5, 9]);
    }

    #[test]
    fn rank_hybrid_truncates_to_k() {
        let policy = HybridRankingPolicy::default();
        let vector = vec![sr(1, 0.1), sr(2, 0.2), sr(3, 0.3), sr(4, 0.4), sr(5, 0.5)];
        let ranked = rank_hybrid(vector, vec![], &policy, 3);
        assert_eq!(ranked.len(), 3);
    }

    #[test]
    fn rank_hybrid_k_larger_than_candidates() {
        let policy = HybridRankingPolicy::default();
        let vector = vec![sr(1, 0.1), sr(2, 0.9)];
        let ranked = rank_hybrid(vector, vec![], &policy, 100);
        assert_eq!(ranked.len(), 2);
    }

    #[test]
    fn rank_hybrid_metadata_from_vector_takes_precedence() {
        let policy = HybridRankingPolicy::default();
        let vector = vec![SearchResult {
            id: VectorId(1),
            score: 0.1,
            metadata: Some(serde_json::json!({"source": "vector"})),
        }];
        let bm25 = vec![SearchResult {
            id: VectorId(1),
            score: -2.0,
            metadata: Some(serde_json::json!({"source": "bm25"})),
        }];
        let ranked = rank_hybrid(vector, bm25, &policy, 1);
        assert_eq!(
            ranked[0].metadata,
            Some(serde_json::json!({"source": "vector"}))
        );
    }

    #[test]
    fn rank_hybrid_scores_are_in_zero_one_range() {
        let policy = HybridRankingPolicy::default();
        let vector = vec![sr(1, 0.1), sr(2, 0.5), sr(3, 0.9)];
        let bm25 = vec![sr(2, -3.0), sr(3, -1.5), sr(4, -0.5)];
        let ranked = rank_hybrid(vector, bm25, &policy, 10);
        for r in &ranked {
            assert!(
                r.score >= 0.0 && r.score <= 1.0,
                "hybrid score out of [0,1]: {} for id {}",
                r.score,
                r.id
            );
        }
    }
}
