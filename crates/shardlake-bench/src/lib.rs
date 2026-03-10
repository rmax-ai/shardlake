//! Benchmark harness: recall@k vs exact baseline, latency, artifact size.

use std::{sync::Arc, time::Instant};

use serde::{Deserialize, Serialize};
use tracing::info;

use shardlake_core::types::{DistanceMetric, QueryMode, VectorId, VectorRecord};
use shardlake_index::{
    bm25::extract_text,
    exact::{exact_search, recall_at_k},
    IndexSearcher,
};
use shardlake_storage::ObjectStore;

/// Summary statistics for one benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub num_queries: usize,
    pub k: usize,
    pub nprobe: usize,
    pub mode: QueryMode,
    pub recall_at_k: f64,
    pub mean_latency_us: f64,
    pub p99_latency_us: f64,
    pub artifact_size_bytes: u64,
}

/// Configuration for a benchmark run.
pub struct BenchmarkConfig {
    pub k: usize,
    pub nprobe: usize,
    pub metric: DistanceMetric,
    pub mode: QueryMode,
    /// Hybrid blending weight. Only used when `mode = Hybrid`.
    pub alpha: f32,
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
    nprobe: usize,
    metric: DistanceMetric,
) -> BenchmarkReport {
    let cfg = BenchmarkConfig {
        k,
        nprobe,
        metric,
        mode: QueryMode::Vector,
        alpha: 0.5,
    };
    run_benchmark_mode(searcher, store, queries, corpus, &cfg)
}

/// Run benchmark for a specific query mode with hybrid alpha weight.
pub fn run_benchmark_mode(
    searcher: &IndexSearcher,
    store: &Arc<dyn ObjectStore>,
    queries: &[VectorRecord],
    corpus: &[VectorRecord],
    cfg: &BenchmarkConfig,
) -> BenchmarkReport {
    let mut latencies_us: Vec<f64> = Vec::with_capacity(queries.len());
    let mut recalls: Vec<f64> = Vec::with_capacity(queries.len());

    for query in queries {
        let gt = exact_search(&query.data, corpus, cfg.metric, cfg.k);
        let gt_ids: Vec<VectorId> = gt.iter().map(|r| r.id).collect();

        let query_text = extract_text(&query.metadata);

        let t0 = Instant::now();
        let approx = match cfg.mode {
            QueryMode::Vector => searcher
                .search(&query.data, cfg.k, cfg.nprobe)
                .unwrap_or_default(),
            QueryMode::Lexical => searcher
                .search_lexical(&query_text, cfg.k)
                .unwrap_or_default(),
            QueryMode::Hybrid => searcher
                .search_hybrid(&query.data, &query_text, cfg.k, cfg.nprobe, cfg.alpha)
                .unwrap_or_default(),
        };
        let elapsed_us = t0.elapsed().as_micros() as f64;

        let approx_ids: Vec<VectorId> = approx.iter().map(|r| r.id).collect();
        let r = recall_at_k(&gt_ids, &approx_ids);

        latencies_us.push(elapsed_us);
        recalls.push(r);
    }

    let mean_recall = recalls.iter().sum::<f64>() / recalls.len().max(1) as f64;
    let mean_latency = latencies_us.iter().sum::<f64>() / latencies_us.len().max(1) as f64;
    latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p99_idx =
        ((latencies_us.len() as f64 * 0.99) as usize).min(latencies_us.len().saturating_sub(1));
    let p99_latency = latencies_us.get(p99_idx).copied().unwrap_or(0.0);

    let keys = store.list("indexes/").unwrap_or_default();
    let artifact_size_bytes: u64 = keys
        .iter()
        .filter_map(|k| store.get(k).ok())
        .map(|b| b.len() as u64)
        .sum();

    let report = BenchmarkReport {
        num_queries: queries.len(),
        k: cfg.k,
        nprobe: cfg.nprobe,
        mode: cfg.mode,
        recall_at_k: mean_recall,
        mean_latency_us: mean_latency,
        p99_latency_us: p99_latency,
        artifact_size_bytes,
    };

    info!(
        mode = %cfg.mode,
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
