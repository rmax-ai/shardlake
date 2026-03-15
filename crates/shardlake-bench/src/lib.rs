//! Benchmark harness: recall@k vs exact baseline, latency, artifact size.

pub mod generate;

use std::{sync::Arc, time::Instant};

use serde::{Deserialize, Serialize};
use tracing::info;

use shardlake_core::{
    config::FanOutPolicy,
    types::{DistanceMetric, VectorId, VectorRecord},
};
use shardlake_index::{
    exact::{exact_search, recall_at_k},
    IndexSearcher,
};
use shardlake_storage::{paths, ObjectStore};

/// Summary statistics for one benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub num_queries: usize,
    pub k: usize,
    pub nprobe: usize,
    pub recall_at_k: f64,
    pub mean_latency_us: f64,
    pub p99_latency_us: f64,
    pub artifact_size_bytes: u64,
}

/// Run benchmark comparing approximate search against exact baseline.
///
/// `queries`: query vectors (with ids, which are ignored for querying).
/// `corpus`: full corpus to use for exact ground-truth.
pub fn run_benchmark(
    searcher: &IndexSearcher,
    store: &Arc<dyn ObjectStore>,
    queries: &[VectorRecord],
    corpus: &[VectorRecord],
    k: usize,
    policy: &FanOutPolicy,
    metric: DistanceMetric,
) -> BenchmarkReport {
    let mut latencies_us: Vec<f64> = Vec::with_capacity(queries.len());
    let mut recalls: Vec<f64> = Vec::with_capacity(queries.len());

    for query in queries {
        let gt = exact_search(&query.data, corpus, metric, k);
        let gt_ids: Vec<VectorId> = gt.iter().map(|r| r.id).collect();

        let t0 = Instant::now();
        let approx = searcher.search(&query.data, k, policy).unwrap_or_default();
        let elapsed_us = t0.elapsed().as_micros() as f64;

        let approx_ids: Vec<VectorId> = approx.iter().map(|r| r.id).collect();
        let r = recall_at_k(&gt_ids, &approx_ids);

        latencies_us.push(elapsed_us);
        recalls.push(r);
    }

    let mean_recall = recalls.iter().sum::<f64>() / recalls.len() as f64;
    let mean_latency = latencies_us.iter().sum::<f64>() / latencies_us.len() as f64;
    latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p99_idx = ((latencies_us.len() as f64 * 0.99) as usize).min(latencies_us.len() - 1);
    let p99_latency = latencies_us.get(p99_idx).copied().unwrap_or(0.0);

    let keys = store.list(paths::indexes_prefix()).unwrap_or_default();
    let artifact_size_bytes: u64 = keys
        .iter()
        .filter_map(|k| store.get(k).ok())
        .map(|b| b.len() as u64)
        .sum();

    let report = BenchmarkReport {
        num_queries: queries.len(),
        k,
        nprobe: policy.candidate_centroids as usize,
        recall_at_k: mean_recall,
        mean_latency_us: mean_latency,
        p99_latency_us: p99_latency,
        artifact_size_bytes,
    };

    info!(
        recall_at_k = report.recall_at_k,
        mean_latency_us = report.mean_latency_us,
        p99_latency_us = report.p99_latency_us,
        artifact_size_bytes = report.artifact_size_bytes,
        "Benchmark complete"
    );

    report
}

#[cfg(test)]
mod tests {
    use shardlake_core::types::VectorId;
    use shardlake_index::exact::recall_at_k;

    #[test]
    fn test_recall_perfect() {
        let gt = vec![VectorId(1), VectorId(2), VectorId(3)];
        let ret = gt.clone();
        assert_eq!(recall_at_k(&gt, &ret), 1.0);
    }

    #[test]
    fn test_recall_zero() {
        let gt = vec![VectorId(1), VectorId(2)];
        let ret = vec![VectorId(3), VectorId(4)];
        assert_eq!(recall_at_k(&gt, &ret), 0.0);
    }
}
