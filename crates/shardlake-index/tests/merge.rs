//! Focused tests for the global top-*k* merge stage.
//!
//! Each test validates one specific aspect of [`GlobalMerge`] behaviour so
//! that regressions are easy to pinpoint.

use shardlake_core::types::{SearchResult, VectorId};
use shardlake_index::{merge::merge_global_top_k, GlobalMerge, MergeStage};

// ── helpers ──────────────────────────────────────────────────────────────────

fn result(id: u64, score: f32) -> SearchResult {
    SearchResult {
        id: VectorId(id),
        score,
        metadata: None,
    }
}

fn result_with_meta(id: u64, score: f32, tag: &str) -> SearchResult {
    SearchResult {
        id: VectorId(id),
        score,
        metadata: Some(serde_json::json!({ "tag": tag })),
    }
}

// ── top-k truncation ─────────────────────────────────────────────────────────

#[test]
fn merge_truncates_to_k() {
    let candidates = vec![
        result(1, 0.1),
        result(2, 0.2),
        result(3, 0.3),
        result(4, 0.4),
    ];
    let merged = merge_global_top_k(candidates, 2);
    assert_eq!(merged.len(), 2);
    assert_eq!(merged[0].id, VectorId(1));
    assert_eq!(merged[1].id, VectorId(2));
}

#[test]
fn merge_returns_all_when_fewer_than_k() {
    let candidates = vec![result(1, 0.5), result(2, 0.1)];
    let merged = merge_global_top_k(candidates, 10);
    assert_eq!(merged.len(), 2);
}

#[test]
fn merge_k_zero_returns_empty() {
    let candidates = vec![result(1, 0.1), result(2, 0.2)];
    let merged = merge_global_top_k(candidates, 0);
    assert!(merged.is_empty());
}

// ── empty input ───────────────────────────────────────────────────────────────

#[test]
fn merge_empty_input_returns_empty() {
    let merged = merge_global_top_k(vec![], 5);
    assert!(merged.is_empty());
}

#[test]
fn merge_empty_shard_among_non_empty() {
    // Simulate two shards where one returns no candidates.
    let shard_a: Vec<SearchResult> = vec![];
    let shard_b = vec![result(10, 0.3), result(20, 0.1)];

    let all: Vec<SearchResult> = shard_a.into_iter().chain(shard_b).collect();
    let merged = merge_global_top_k(all, 5);
    assert_eq!(merged.len(), 2);
    assert_eq!(merged[0].id, VectorId(20));
    assert_eq!(merged[1].id, VectorId(10));
}

// ── duplicate candidates ──────────────────────────────────────────────────────

#[test]
fn merge_deduplicates_keeps_best_score() {
    // VectorId(1) appears from two shards with different scores.
    let candidates = vec![
        result(1, 0.40), // worse
        result(2, 0.20),
        result(1, 0.15), // best score for id=1
        result(3, 0.30),
    ];
    let merged = merge_global_top_k(candidates, 10);

    // Exactly three unique ids.
    assert_eq!(merged.len(), 3);

    // id=1 must appear only once and with the best score 0.15.
    let entry = merged.iter().find(|r| r.id == VectorId(1)).unwrap();
    assert_eq!(entry.score, 0.15_f32, "expected score 0.15 for id=1");
}

#[test]
fn merge_deduplicates_retains_metadata_of_best_entry() {
    // When a vector appears in multiple shards the metadata from the
    // best-scoring entry must be preserved.
    let candidates = vec![
        result_with_meta(7, 0.80, "worse"),
        result_with_meta(7, 0.25, "best"),
    ];
    let merged = merge_global_top_k(candidates, 5);
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].id, VectorId(7));
    assert_eq!(
        merged[0].metadata,
        Some(serde_json::json!({ "tag": "best" }))
    );
}

#[test]
fn merge_all_same_id_returns_single_best() {
    let candidates = vec![result(42, 0.9), result(42, 0.3), result(42, 0.6)];
    let merged = merge_global_top_k(candidates, 5);
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].id, VectorId(42));
    assert_eq!(merged[0].score, 0.3_f32);
}

// ── stable ordering / tie-breaking ───────────────────────────────────────────

#[test]
fn merge_tie_broken_by_vector_id_ascending() {
    // Three vectors with identical scores – lower VectorId must come first.
    let candidates = vec![result(30, 0.5), result(10, 0.5), result(20, 0.5)];
    let merged = merge_global_top_k(candidates, 3);
    assert_eq!(merged[0].id, VectorId(10));
    assert_eq!(merged[1].id, VectorId(20));
    assert_eq!(merged[2].id, VectorId(30));
}

#[test]
fn merge_order_is_score_ascending_then_id_ascending() {
    // Mix of unique scores and a tie.
    let candidates = vec![
        result(5, 0.2),
        result(3, 0.1),
        result(8, 0.2), // tie with id=5
        result(1, 0.3),
    ];
    let merged = merge_global_top_k(candidates, 4);
    assert_eq!(merged.len(), 4);
    assert_eq!(merged[0].id, VectorId(3)); // 0.1
    assert_eq!(merged[1].id, VectorId(5)); // 0.2, id=5 < 8
    assert_eq!(merged[2].id, VectorId(8)); // 0.2, id=8
    assert_eq!(merged[3].id, VectorId(1)); // 0.3
}

#[test]
fn merge_is_stable_regardless_of_input_order() {
    // Different permutations of the same input must produce the same output.
    let base = vec![result(2, 0.5), result(1, 0.5), result(3, 0.5)];

    let mut perm1 = base.clone();
    perm1.sort_by_key(|r| r.id.0);
    let mut perm2 = base.clone();
    perm2.sort_by_key(|r| std::cmp::Reverse(r.id.0));

    let out1 = merge_global_top_k(perm1, 3);
    let out2 = merge_global_top_k(perm2, 3);

    let ids1: Vec<u64> = out1.iter().map(|r| r.id.0).collect();
    let ids2: Vec<u64> = out2.iter().map(|r| r.id.0).collect();
    assert_eq!(
        ids1, ids2,
        "output must be identical for all input permutations"
    );
}

// ── multi-shard simulation ────────────────────────────────────────────────────

#[test]
fn merge_global_top_k_across_multiple_shards() {
    // Simulate candidates from 3 shards where some ids overlap.
    let shard_a = vec![result(1, 0.10), result(2, 0.20), result(3, 0.50)];
    let shard_b = vec![result(2, 0.18), result(4, 0.25), result(5, 0.60)]; // id=2 better here
    let shard_c = vec![result(3, 0.55), result(6, 0.15)];

    let all: Vec<SearchResult> = shard_a.into_iter().chain(shard_b).chain(shard_c).collect();

    let merged = merge_global_top_k(all, 4);

    assert_eq!(merged.len(), 4);
    // Best order: id=1(0.10), id=6(0.15), id=2(0.18), id=4(0.25)
    assert_eq!(merged[0].id, VectorId(1));
    assert_eq!(merged[1].id, VectorId(6));
    assert_eq!(merged[2].id, VectorId(2));
    // id=2 must have score 0.18 (from shard_b), not 0.20.
    assert_eq!(merged[2].score, 0.18_f32);
    assert_eq!(merged[3].id, VectorId(4));
}

// ── trait object surface ──────────────────────────────────────────────────────

#[test]
fn global_merge_trait_object_works() {
    let merge: Box<dyn MergeStage> = Box::new(GlobalMerge);
    let candidates = vec![result(3, 0.9), result(1, 0.1), result(2, 0.5)];
    let merged = merge.merge(candidates, 2);
    assert_eq!(merged.len(), 2);
    assert_eq!(merged[0].id, VectorId(1));
    assert_eq!(merged[1].id, VectorId(2));
}
