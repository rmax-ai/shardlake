//! Exact nearest-neighbour search (ground-truth baseline).

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
    results.dedup_by_key(|r| r.id);
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
    fn test_exact_search_euclidean() {
        let records = make_records();
        let query = [1.0f32, 0.1];
        let results = exact_search(&query, &records, DistanceMetric::Euclidean, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, VectorId(1));
    }

    #[test]
    fn test_recall_at_k() {
        let gt = vec![VectorId(1), VectorId(2), VectorId(3)];
        let ret = vec![VectorId(1), VectorId(2), VectorId(5)];
        let r = recall_at_k(&gt, &ret);
        assert!((r - 2.0 / 3.0).abs() < 1e-6);
    }
}
