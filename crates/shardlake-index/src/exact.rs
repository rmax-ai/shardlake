//! Exact nearest-neighbour search (ground-truth baseline).

use std::collections::HashSet;

use shardlake_core::types::{DistanceMetric, SearchResult, VectorId, VectorRecord};

/// Zero-sized marker type for constructing exact search results.
///
/// All methods are free functions; this struct exists for namespacing only.
pub struct ExactSearcher;

/// Compute distance between two vectors according to `metric`.
pub fn distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::Euclidean => a
            .iter()
            .zip(b)
            .map(|(x, y)| (x - y) * (x - y))
            .sum::<f32>()
            .sqrt(),
        DistanceMetric::Cosine => {
            let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
            let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
            let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
            if na == 0.0 || nb == 0.0 {
                1.0
            } else {
                1.0 - dot / (na * nb)
            }
        }
        DistanceMetric::InnerProduct => {
            let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
            -dot // negate so that lower = better
        }
    }
}

/// Exact brute-force top-k search over a flat list of records.
pub fn exact_search(
    query: &[f32],
    records: &[VectorRecord],
    metric: DistanceMetric,
    k: usize,
) -> Vec<SearchResult> {
    let mut scored: Vec<SearchResult> = records
        .iter()
        .map(|rec| SearchResult {
            id: rec.id,
            score: distance(query, &rec.data, metric),
            metadata: rec.metadata.clone(),
        })
        .collect();
    scored.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    scored.truncate(k);
    scored
}

/// Merge multiple ranked lists and return top-k.
pub fn merge_top_k(mut results: Vec<SearchResult>, k: usize) -> Vec<SearchResult> {
    results.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut seen = HashSet::new();
    results.retain(|result| seen.insert(result.id));
    results.truncate(k);
    results
}

/// Recall@k: fraction of ground-truth top-k ids that appear in `retrieved`.
pub fn recall_at_k(ground_truth: &[VectorId], retrieved: &[VectorId]) -> f64 {
    if ground_truth.is_empty() {
        return 1.0;
    }
    let k = ground_truth.len();
    let hits = retrieved
        .iter()
        .filter(|id| ground_truth.contains(id))
        .count();
    hits as f64 / k as f64
}

/// Precision@k: fraction of retrieved ids that are in the ground-truth top-k.
///
/// Returns `0.0` when `retrieved` is empty because an ANN search that produces no
/// candidates should not appear as a perfect-quality result.
pub fn precision_at_k(ground_truth: &[VectorId], retrieved: &[VectorId]) -> f64 {
    if retrieved.is_empty() {
        return 0.0;
    }
    let hits = retrieved
        .iter()
        .filter(|id| ground_truth.contains(id))
        .count();
    hits as f64 / retrieved.len() as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardlake_core::types::VectorRecord;

    fn make_records() -> Vec<VectorRecord> {
        vec![
            VectorRecord {
                id: VectorId(1),
                data: vec![1.0, 0.0],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(2),
                data: vec![0.0, 1.0],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(3),
                data: vec![1.0, 1.0],
                metadata: None,
            },
        ]
    }

    #[test]
    fn test_distance_euclidean() {
        let a = [3.0f32, 4.0];
        let b = [0.0f32, 0.0];
        let dist = distance(&a, &b, DistanceMetric::Euclidean);
        assert!((dist - 5.0).abs() < 1e-6, "expected 5.0, got {dist}");
    }

    #[test]
    fn test_distance_cosine_identical_direction() {
        // Parallel vectors have cosine distance 0.
        let a = [1.0f32, 0.0];
        let b = [2.0f32, 0.0];
        let dist = distance(&a, &b, DistanceMetric::Cosine);
        assert!(dist.abs() < 1e-6, "expected 0.0, got {dist}");
    }

    #[test]
    fn test_distance_cosine_orthogonal() {
        // Orthogonal vectors have cosine similarity 0, so distance = 1.
        let a = [1.0f32, 0.0];
        let b = [0.0f32, 1.0];
        let dist = distance(&a, &b, DistanceMetric::Cosine);
        assert!((dist - 1.0).abs() < 1e-6, "expected 1.0, got {dist}");
    }

    #[test]
    fn test_distance_cosine_opposite_direction() {
        // Antiparallel vectors have cosine similarity -1, so distance = 2.
        let a = [1.0f32, 0.0];
        let b = [-1.0f32, 0.0];
        let dist = distance(&a, &b, DistanceMetric::Cosine);
        assert!((dist - 2.0).abs() < 1e-6, "expected 2.0, got {dist}");
    }

    #[test]
    fn test_distance_cosine_zero_vector_returns_one() {
        // When either vector is the zero vector the distance is 1.0 by convention.
        let a = [0.0f32, 0.0];
        let b = [1.0f32, 0.0];
        let dist = distance(&a, &b, DistanceMetric::Cosine);
        assert!((dist - 1.0).abs() < 1e-6, "expected 1.0, got {dist}");
    }

    #[test]
    fn test_distance_inner_product_negated_dot() {
        // InnerProduct returns -dot(a, b) so that lower = better.
        let a = [1.0f32, 2.0, 3.0];
        let b = [4.0f32, 5.0, 6.0];
        let expected = -(1.0 * 4.0 + 2.0 * 5.0 + 3.0 * 6.0); // -32
        let dist = distance(&a, &b, DistanceMetric::InnerProduct);
        assert!(
            (dist - expected).abs() < 1e-5,
            "expected {expected}, got {dist}"
        );
    }

    #[test]
    fn test_exact_search_euclidean() {
        let records = make_records();
        let query = [1.0f32, 0.1];
        let results = exact_search(&query, &records, DistanceMetric::Euclidean, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, VectorId(1));
    }

    #[test]
    fn test_exact_search_cosine() {
        // query points mostly along x-axis: id=1 ([1,0]) should be the closest by
        // cosine similarity, and id=2 ([0,1]) should be the farthest.
        let records = make_records();
        let query = [1.0f32, 0.0];
        let results = exact_search(&query, &records, DistanceMetric::Cosine, 3);
        assert_eq!(results.len(), 3);
        assert_eq!(
            results[0].id,
            VectorId(1),
            "id=1 should be nearest by cosine"
        );
        // id=2 is orthogonal (distance=1.0); id=3 is at 45° (distance < 1.0)
        assert_eq!(
            results[2].id,
            VectorId(2),
            "id=2 (orthogonal) should be farthest by cosine"
        );
    }

    #[test]
    fn test_exact_search_inner_product() {
        // For inner product (negated dot), the vector with the largest dot product
        // with the query wins (has the smallest/most-negative score).
        // query = [1, 1], records: id=1 [1,0] dot=1, id=2 [0,1] dot=1, id=3 [1,1] dot=2
        let records = make_records();
        let query = [1.0f32, 1.0];
        let results = exact_search(&query, &records, DistanceMetric::InnerProduct, 3);
        assert_eq!(results.len(), 3);
        assert_eq!(
            results[0].id,
            VectorId(3),
            "id=3 ([1,1]) has the largest dot product with [1,1]"
        );
    }

    #[test]
    fn test_exact_search_cosine_vs_euclidean_differ() {
        // Construct records where cosine and euclidean rankings differ:
        //   id=1: [10, 0]  — far from query [1, 0] in L2, but identical direction (cosine dist=0)
        //   id=2: [1, 0]   — nearest in L2 as well, identical direction (cosine dist=0)
        //   id=3: [0.1, 1.0] — mixed angle: closer in L2 than id=1, but nonzero cosine distance
        let records = vec![
            VectorRecord {
                id: VectorId(1),
                data: vec![10.0, 0.0],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(2),
                data: vec![1.0, 0.0],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(3),
                data: vec![0.1, 1.0],
                metadata: None,
            },
        ];
        let query = [1.0f32, 0.0];

        let euclidean = exact_search(&query, &records, DistanceMetric::Euclidean, 3);
        let cosine = exact_search(&query, &records, DistanceMetric::Cosine, 3);

        // Euclidean: id=2 (dist 0) < id=1 (dist 9) < id=3 (dist ~1.0)
        assert_eq!(euclidean[0].id, VectorId(2));
        // Cosine: id=1 and id=2 are tied (same direction); id=3 has nonzero angle
        // Both id=1 and id=2 have cosine distance 0 from [1,0]; id=3 is farther.
        let cosine_top2_ids: std::collections::HashSet<_> =
            cosine[..2].iter().map(|r| r.id).collect();
        assert!(
            cosine_top2_ids.contains(&VectorId(1)),
            "id=1 should be in top-2 by cosine"
        );
        assert!(
            cosine_top2_ids.contains(&VectorId(2)),
            "id=2 should be in top-2 by cosine"
        );
        assert_eq!(cosine[2].id, VectorId(3), "id=3 should be last by cosine");
    }

    #[test]
    fn test_recall_at_k() {
        let gt = vec![VectorId(1), VectorId(2), VectorId(3)];
        let ret = vec![VectorId(1), VectorId(2), VectorId(5)];
        let r = recall_at_k(&gt, &ret);
        assert!((r - 2.0 / 3.0).abs() < 1e-6);
    }

    #[test]
    fn test_precision_at_k_partial() {
        let gt = vec![VectorId(1), VectorId(2), VectorId(3)];
        let ret = vec![VectorId(1), VectorId(4), VectorId(5)];
        let p = precision_at_k(&gt, &ret);
        assert!((p - 1.0 / 3.0).abs() < 1e-6);
    }

    #[test]
    fn test_precision_at_k_perfect() {
        let gt = vec![VectorId(1), VectorId(2)];
        let ret = vec![VectorId(2), VectorId(1)];
        assert_eq!(precision_at_k(&gt, &ret), 1.0);
    }

    #[test]
    fn test_precision_at_k_empty_retrieved() {
        let gt = vec![VectorId(1)];
        assert_eq!(precision_at_k(&gt, &[]), 0.0);
    }

    #[test]
    fn test_merge_top_k_deduplicates_ids_by_best_score() {
        let merged = merge_top_k(
            vec![
                SearchResult {
                    id: VectorId(1),
                    score: 0.30,
                    metadata: Some(serde_json::json!({ "rank": "worse" })),
                },
                SearchResult {
                    id: VectorId(2),
                    score: 0.10,
                    metadata: None,
                },
                SearchResult {
                    id: VectorId(1),
                    score: 0.20,
                    metadata: Some(serde_json::json!({ "rank": "best" })),
                },
                SearchResult {
                    id: VectorId(3),
                    score: 0.15,
                    metadata: None,
                },
            ],
            3,
        );

        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].id, VectorId(2));
        assert_eq!(merged[1].id, VectorId(3));
        assert_eq!(merged[2].id, VectorId(1));
        assert_eq!(merged[2].score, 0.20);
        assert_eq!(
            merged[2].metadata,
            Some(serde_json::json!({ "rank": "best" }))
        );
    }
}
