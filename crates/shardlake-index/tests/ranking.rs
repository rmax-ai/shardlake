//! Integration tests for weighted hybrid ranking.
//!
//! These tests exercise [`rank_hybrid`] end-to-end using realistic
//! BM25-style negated scores alongside vector distance scores.

use shardlake_core::types::{SearchResult, VectorId};
use shardlake_index::ranking::{rank_hybrid, HybridRankingPolicy};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn sr(id: u64, score: f32) -> SearchResult {
    SearchResult {
        id: VectorId(id),
        score,
        metadata: None,
    }
}

// ── Pure-vector ranking ───────────────────────────────────────────────────────

#[test]
fn pure_vector_ranking_ordered_by_vector_score() {
    let policy = HybridRankingPolicy {
        vector_weight: 1.0,
        bm25_weight: 0.0,
    };
    // Cosine distances — lower is better.
    let vector_results = vec![sr(10, 0.05), sr(20, 0.15), sr(30, 0.30), sr(40, 0.60)];

    let ranked = rank_hybrid(vector_results, vec![], &policy, 4);
    let ids: Vec<u64> = ranked.iter().map(|r| r.id.0).collect();
    assert_eq!(ids, vec![10, 20, 30, 40]);
}

#[test]
fn pure_vector_ranking_truncates_to_k() {
    let policy = HybridRankingPolicy {
        vector_weight: 1.0,
        bm25_weight: 0.0,
    };
    let vector_results = vec![sr(1, 0.1), sr(2, 0.2), sr(3, 0.3)];
    let ranked = rank_hybrid(vector_results, vec![], &policy, 2);
    assert_eq!(ranked.len(), 2);
    assert_eq!(ranked[0].id, VectorId(1));
    assert_eq!(ranked[1].id, VectorId(2));
}

// ── Pure-lexical ranking ──────────────────────────────────────────────────────

#[test]
fn pure_bm25_ranking_ordered_by_bm25_score() {
    let policy = HybridRankingPolicy {
        vector_weight: 0.0,
        bm25_weight: 1.0,
    };
    // BM25 returns negated scores: -4.0 (best relevance), -2.5, -1.0 (worst).
    let bm25_results = vec![sr(100, -1.0), sr(200, -4.0), sr(300, -2.5)];

    let ranked = rank_hybrid(vec![], bm25_results, &policy, 3);
    let ids: Vec<u64> = ranked.iter().map(|r| r.id.0).collect();
    assert_eq!(ids, vec![200, 300, 100]);
}

#[test]
fn pure_bm25_ranking_empty_vector_list() {
    let policy = HybridRankingPolicy {
        vector_weight: 0.0,
        bm25_weight: 1.0,
    };
    let bm25_results = vec![sr(1, -2.0), sr(2, -1.0)];
    let ranked = rank_hybrid(vec![], bm25_results, &policy, 5);
    assert_eq!(ranked.len(), 2);
    assert_eq!(ranked[0].id, VectorId(1)); // -2.0 < -1.0 → doc 1 is better
}

// ── Blended ranking ───────────────────────────────────────────────────────────

#[test]
fn blended_ranking_candidate_in_both_lists_ranks_first() {
    // Doc 99 appears in both lists with good scores on each.
    // All other docs appear in only one list and should rank below.
    let policy = HybridRankingPolicy {
        vector_weight: 0.5,
        bm25_weight: 0.5,
    };
    let vector_results = vec![
        sr(99, 0.0), // best vector
        sr(10, 0.3),
        sr(11, 0.6),
    ];
    let bm25_results = vec![
        sr(99, -5.0), // best BM25
        sr(20, -3.0),
        sr(21, -1.5),
    ];
    let ranked = rank_hybrid(vector_results, bm25_results, &policy, 5);
    // Doc 99 should rank first: normalized scores both 0.0 → hybrid = 0.0
    assert_eq!(ranked[0].id, VectorId(99));
}

#[test]
fn blended_ranking_deterministic_across_repeated_calls() {
    let policy = HybridRankingPolicy {
        vector_weight: 0.4,
        bm25_weight: 0.6,
    };
    let vector = vec![sr(1, 0.1), sr(2, 0.5), sr(3, 0.9)];
    let bm25 = vec![sr(1, -3.0), sr(2, -1.0), sr(4, -4.0)];

    let run1 = rank_hybrid(vector.clone(), bm25.clone(), &policy, 4);
    let run2 = rank_hybrid(vector, bm25, &policy, 4);

    let ids1: Vec<u64> = run1.iter().map(|r| r.id.0).collect();
    let ids2: Vec<u64> = run2.iter().map(|r| r.id.0).collect();
    assert_eq!(ids1, ids2, "ranking must be deterministic");
}

#[test]
fn blended_ranking_non_unit_weights_produce_valid_output() {
    // Weights don't need to sum to 1.
    let policy = HybridRankingPolicy {
        vector_weight: 3.0,
        bm25_weight: 7.0,
    };
    assert!(policy.validate().is_ok());

    let vector = vec![sr(1, 0.2), sr(2, 0.8)];
    let bm25 = vec![sr(1, -4.0), sr(2, -1.0)];
    let ranked = rank_hybrid(vector, bm25, &policy, 2);
    // With heavy BM25 weight doc 1 (better BM25) should rank first.
    assert_eq!(ranked.len(), 2);
    assert_eq!(ranked[0].id, VectorId(1));
}

// ── Stable ordering ───────────────────────────────────────────────────────────

#[test]
fn ranking_stable_on_identical_hybrid_scores_tie_breaks_by_id_asc() {
    let policy = HybridRankingPolicy {
        vector_weight: 1.0,
        bm25_weight: 0.0,
    };
    // All vector scores identical → all normalized to 0.0 → all hybrid = 0.
    let vector = vec![sr(7, 1.0), sr(3, 1.0), sr(5, 1.0), sr(1, 1.0)];
    let ranked = rank_hybrid(vector, vec![], &policy, 4);
    let ids: Vec<u64> = ranked.iter().map(|r| r.id.0).collect();
    assert_eq!(
        ids,
        vec![1, 3, 5, 7],
        "ties must be broken by VectorId ascending"
    );
}

// ── Missing-signal behavior ───────────────────────────────────────────────────

#[test]
fn candidates_in_both_lists_rank_above_vector_only() {
    // Doc 3 appears only in BM25 (no vector score) and has the WORST BM25 score,
    // so it is penalized on both signals and should rank last.
    //
    // vector: doc1=0.0 (best), doc2=0.1 (worst) — doc3 missing → 1.0
    // bm25:   doc1=-3.0 (best), doc2=-2.0, doc3=-1.0 (worst) — normalized last
    // policy: vector_weight=0.9, bm25_weight=0.1
    //
    // Hybrid scores:
    //   doc1 = 0.9*0.0 + 0.1*0.0 = 0.000
    //   doc2 = 0.9*1.0 + 0.1*0.5 = 0.950
    //   doc3 = 0.9*1.0 + 0.1*1.0 = 1.000  ← last
    let policy = HybridRankingPolicy {
        vector_weight: 0.9,
        bm25_weight: 0.1,
    };
    let vector = vec![sr(1, 0.0), sr(2, 0.1)];
    let bm25 = vec![sr(1, -3.0), sr(2, -2.0), sr(3, -1.0)];

    let ranked = rank_hybrid(vector, bm25, &policy, 3);
    let ids: Vec<u64> = ranked.iter().map(|r| r.id.0).collect();
    assert_eq!(
        ids[2], 3,
        "BM25-only doc with worst BM25 score must rank last: got {ids:?}"
    );
}

#[test]
fn candidates_in_both_lists_rank_above_bm25_only() {
    // Doc 3 appears only in vector (no BM25 score) and has the WORST vector score,
    // so it is penalized on both signals and should rank last.
    //
    // bm25:   doc1=-3.0 (best), doc2=-2.0 (worst) — doc3 missing → 1.0
    // vector: doc1=0.0 (best), doc2=0.1, doc3=0.5 (worst) — normalized last
    // policy: vector_weight=0.1, bm25_weight=0.9
    //
    // Hybrid scores:
    //   doc1 = 0.1*0.0 + 0.9*0.0 = 0.000
    //   doc2 = 0.1*0.2 + 0.9*1.0 = 0.920
    //   doc3 = 0.1*1.0 + 0.9*1.0 = 1.000  ← last
    let policy = HybridRankingPolicy {
        vector_weight: 0.1,
        bm25_weight: 0.9,
    };
    let vector = vec![sr(1, 0.0), sr(2, 0.1), sr(3, 0.5)];
    let bm25 = vec![sr(1, -3.0), sr(2, -2.0)];

    let ranked = rank_hybrid(vector, bm25, &policy, 3);
    let ids: Vec<u64> = ranked.iter().map(|r| r.id.0).collect();
    assert_eq!(
        ids[2], 3,
        "vector-only doc with worst vector score must rank last: got {ids:?}"
    );
}

// ── Edge cases ────────────────────────────────────────────────────────────────

#[test]
fn both_empty_returns_empty() {
    let policy = HybridRankingPolicy::default();
    let ranked = rank_hybrid(vec![], vec![], &policy, 10);
    assert!(ranked.is_empty());
}

#[test]
fn k_zero_returns_empty() {
    let policy = HybridRankingPolicy::default();
    let ranked = rank_hybrid(vec![sr(1, 0.1)], vec![sr(1, -2.0)], &policy, 0);
    assert!(ranked.is_empty());
}

#[test]
fn k_larger_than_union_returns_all() {
    let policy = HybridRankingPolicy::default();
    let vector = vec![sr(1, 0.1), sr(2, 0.2)];
    let bm25 = vec![sr(3, -1.5)];
    let ranked = rank_hybrid(vector, bm25, &policy, 100);
    assert_eq!(ranked.len(), 3);
}

#[test]
fn single_candidate_in_each_list_no_overlap() {
    let policy = HybridRankingPolicy::default();
    let vector = vec![sr(1, 0.1)];
    let bm25 = vec![sr(2, -2.0)];
    let ranked = rank_hybrid(vector, bm25, &policy, 2);
    assert_eq!(ranked.len(), 2);
}

#[test]
fn hybrid_scores_always_non_negative() {
    let policy = HybridRankingPolicy {
        vector_weight: 0.6,
        bm25_weight: 0.4,
    };
    let vector = vec![sr(1, -1.0), sr(2, 0.0), sr(3, 1.0)];
    let bm25 = vec![sr(1, -5.0), sr(2, -2.5), sr(3, 0.0)];
    let ranked = rank_hybrid(vector, bm25, &policy, 3);
    for r in &ranked {
        assert!(r.score >= 0.0, "hybrid score must be ≥ 0, got {}", r.score);
    }
}
