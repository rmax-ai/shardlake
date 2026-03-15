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
    exact::{exact_search, precision_at_k, recall_at_k},
    IndexSearcher, Result as IndexResult,
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

/// Evaluation report produced by [`run_eval_ann`].
///
/// Extends the basic benchmark report with precision@k, making it suitable for
/// regression tracking and comparison across different `nprobe` settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalAnnReport {
    /// Number of query vectors evaluated.
    pub num_queries: usize,
    /// Number of nearest neighbours retrieved per query.
    pub k: usize,
    /// Number of shards probed per query.
    pub nprobe: usize,
    /// Mean recall@k across all queries (fraction of true top-k found).
    pub recall_at_k: f64,
    /// Mean precision@k across all queries (fraction of retrieved results that are true top-k).
    pub precision_at_k: f64,
    /// Mean per-query ANN search latency in microseconds.
    pub mean_latency_us: f64,
    /// 99th-percentile per-query ANN search latency in microseconds.
    pub p99_latency_us: f64,
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
    let p99_latency = nearest_rank_percentile(&mut latencies_us, 0.99);

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

/// Evaluate ANN quality with recall@k, precision@k, and latency metrics.
///
/// Unlike [`run_benchmark`] this function does not measure artifact size and
/// instead focuses on per-query quality and latency, returning an
/// [`EvalAnnReport`] that is suitable for regression tracking.
///
/// # Arguments
///
/// * `searcher` – A loaded [`IndexSearcher`] ready to serve queries.
/// * `queries` – Query vectors (ids are ignored for search; used only for ground-truth lookup).
/// * `corpus` – Full corpus used to compute the exact ground-truth top-k per query.
/// * `k` – Number of nearest neighbours to retrieve.
/// * `policy` – Query-time fan-out policy for centroid, shard, and per-shard limits.
/// * `metric` – Distance metric used to compute the ground-truth.
pub fn run_eval_ann(
    searcher: &IndexSearcher,
    queries: &[VectorRecord],
    corpus: &[VectorRecord],
    k: usize,
    policy: &FanOutPolicy,
    metric: DistanceMetric,
) -> IndexResult<EvalAnnReport> {
    if queries.is_empty() {
        return Err(shardlake_index::IndexError::Other(
            "eval-ann requires at least one query vector".to_string(),
        ));
    }

    let nprobe = policy.candidate_centroids as usize;

    let mut latencies_us: Vec<f64> = Vec::with_capacity(queries.len());
    let mut recalls: Vec<f64> = Vec::with_capacity(queries.len());
    let mut precisions: Vec<f64> = Vec::with_capacity(queries.len());

    for query in queries {
        let gt = exact_search(&query.data, corpus, metric, k);
        let gt_ids: Vec<VectorId> = gt.iter().map(|r| r.id).collect();

        let t0 = Instant::now();
        let approx = searcher.search(&query.data, k, policy)?;
        let elapsed_us = t0.elapsed().as_micros() as f64;

        let approx_ids: Vec<VectorId> = approx.iter().map(|r| r.id).collect();
        recalls.push(recall_at_k(&gt_ids, &approx_ids));
        precisions.push(precision_at_k(&gt_ids, &approx_ids));
        latencies_us.push(elapsed_us);
    }

    let mean_recall = recalls.iter().sum::<f64>() / recalls.len() as f64;
    let mean_precision = precisions.iter().sum::<f64>() / precisions.len() as f64;
    let mean_latency = latencies_us.iter().sum::<f64>() / latencies_us.len() as f64;
    let p99_latency = nearest_rank_percentile(&mut latencies_us, 0.99);

    let report = EvalAnnReport {
        num_queries: queries.len(),
        k,
        nprobe,
        recall_at_k: mean_recall,
        precision_at_k: mean_precision,
        mean_latency_us: mean_latency,
        p99_latency_us: p99_latency,
    };

    info!(
        recall_at_k = report.recall_at_k,
        precision_at_k = report.precision_at_k,
        mean_latency_us = report.mean_latency_us,
        p99_latency_us = report.p99_latency_us,
        "ANN evaluation complete"
    );

    Ok(report)
}

fn nearest_rank_percentile(values: &mut [f64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let rank = ((values.len() as f64) * percentile).ceil() as usize;
    let index = rank.saturating_sub(1).min(values.len() - 1);
    values[index]
}

#[cfg(test)]
mod tests {
    use super::nearest_rank_percentile;
    use shardlake_core::types::VectorId;
    use shardlake_index::exact::{precision_at_k, recall_at_k};

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

    #[test]
    fn test_precision_perfect() {
        let gt = vec![VectorId(1), VectorId(2)];
        let ret = vec![VectorId(1), VectorId(2)];
        assert_eq!(precision_at_k(&gt, &ret), 1.0);
    }

    #[test]
    fn test_precision_partial() {
        let gt = vec![VectorId(1), VectorId(2), VectorId(3)];
        let ret = vec![VectorId(1), VectorId(4), VectorId(5)];
        let p = precision_at_k(&gt, &ret);
        assert!((p - 1.0 / 3.0).abs() < 1e-6);
    }

    #[test]
    fn test_nearest_rank_percentile_uses_explicit_rank() {
        let mut values: Vec<f64> = (1..=101).map(f64::from).collect();
        assert_eq!(nearest_rank_percentile(&mut values, 0.99), 100.0);
    }

    #[test]
    fn test_nearest_rank_percentile_handles_single_value() {
        let mut values = vec![42.0];
        assert_eq!(nearest_rank_percentile(&mut values, 0.99), 42.0);
    }
}
