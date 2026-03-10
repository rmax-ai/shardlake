//! Benchmark harness: recall@k vs exact baseline, latency, throughput, and cost estimation.
//!
//! # Workload kinds
//!
//! Three query workloads are supported via [`WorkloadKind`]:
//!
//! - [`WorkloadKind::Cold`] – the shard cache is cleared before every query, simulating a
//!   cold-start scenario (e.g. first query after a restart).
//! - [`WorkloadKind::Warm`] – every shard touched by the first query pass is kept in RAM, so
//!   subsequent queries benefit from the in-memory cache.
//! - [`WorkloadKind::Mixed`] – queries alternate: even-indexed queries are cold (cache cleared
//!   beforehand), odd-indexed queries are warm.

pub mod generator;

use std::{sync::Arc, time::Instant};

use serde::{Deserialize, Serialize};
use tracing::info;

use shardlake_core::types::{DistanceMetric, VectorId, VectorRecord};
use shardlake_index::{
    exact::{exact_search, recall_at_k},
    IndexSearcher,
};
use shardlake_storage::ObjectStore;

/// Selects the query-cache behaviour used during a benchmark run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadKind {
    /// Clear shard cache before every query (cold start).
    Cold,
    /// Pre-warm cache, then measure cached queries.
    #[default]
    Warm,
    /// Alternate cold and warm queries (50% cold, 50% warm).
    Mixed,
}

impl std::fmt::Display for WorkloadKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkloadKind::Cold => write!(f, "cold"),
            WorkloadKind::Warm => write!(f, "warm"),
            WorkloadKind::Mixed => write!(f, "mixed"),
        }
    }
}

/// Summary statistics for one benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    /// Query workload type used in this run.
    pub workload: WorkloadKind,
    /// Number of queries executed.
    pub num_queries: usize,
    /// Number of nearest neighbours retrieved per query.
    pub k: usize,
    /// Number of shards probed per query.
    pub nprobe: usize,
    /// Mean Recall@k across all queries.
    pub recall_at_k: f64,
    /// Mean per-query latency in microseconds.
    pub mean_latency_us: f64,
    /// 99th-percentile per-query latency in microseconds.
    pub p99_latency_us: f64,
    /// Throughput in queries per second (derived from mean latency).
    pub throughput_qps: f64,
    /// Total size of all index artifact files in bytes (disk footprint).
    pub artifact_size_bytes: u64,
    /// Size of the raw vector JSONL stored in the dataset, in bytes.
    pub raw_vector_size_bytes: u64,
    /// Ratio of raw vector bytes to index artifact bytes.
    ///
    /// A value > 1 means the index is smaller than the raw data (compression).
    /// Zero when `artifact_size_bytes` is zero.
    pub compression_ratio: f64,
    /// Estimated peak heap for loaded shard data, in bytes.
    ///
    /// Computed as the sum of shard artifact sizes, which approximates the
    /// in-memory footprint of a fully warm cache.
    pub memory_bytes: u64,
}

/// Run benchmark comparing approximate search against exact baseline.
///
/// # Arguments
///
/// - `searcher`: the [`IndexSearcher`] under test.
/// - `store`: object store used to measure artifact sizes.
/// - `queries`: query vectors (ids are used only for grouping metadata).
/// - `corpus`: full corpus for computing exact ground-truth neighbours.
/// - `k`: number of nearest neighbours to retrieve.
/// - `nprobe`: shards to probe per query.
/// - `metric`: distance metric used for exact-search ground truth.
/// - `workload`: cache behaviour: [`WorkloadKind::Cold`], [`WorkloadKind::Warm`], or
///   [`WorkloadKind::Mixed`].
#[allow(clippy::too_many_arguments)]
pub fn run_benchmark(
    searcher: &IndexSearcher,
    store: &Arc<dyn ObjectStore>,
    queries: &[VectorRecord],
    corpus: &[VectorRecord],
    k: usize,
    nprobe: usize,
    metric: DistanceMetric,
    workload: WorkloadKind,
) -> BenchmarkReport {
    let mut latencies_us: Vec<f64> = Vec::with_capacity(queries.len());
    let mut recalls: Vec<f64> = Vec::with_capacity(queries.len());

    // Warm-up pass: run all queries once so the shard cache is populated.
    // For cold and mixed workloads we still do this so the first measured
    // query is not penalised by OS-level file I/O warm-up.
    if workload == WorkloadKind::Warm || workload == WorkloadKind::Mixed {
        for query in queries {
            let _ = searcher.search(&query.data, k, nprobe);
        }
    }

    for (idx, query) in queries.iter().enumerate() {
        let gt = exact_search(&query.data, corpus, metric, k);
        let gt_ids: Vec<VectorId> = gt.iter().map(|r| r.id).collect();

        // For cold workloads clear the cache before every query.
        // For mixed workloads clear the cache before even-indexed queries.
        let is_cold = match workload {
            WorkloadKind::Cold => true,
            WorkloadKind::Mixed => idx % 2 == 0,
            WorkloadKind::Warm => false,
        };
        if is_cold {
            searcher.clear_cache();
        }

        let t0 = Instant::now();
        let approx = searcher.search(&query.data, k, nprobe).unwrap_or_default();
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
    let throughput_qps = if mean_latency > 0.0 {
        1_000_000.0 / mean_latency
    } else {
        0.0
    };

    // --- Cost estimation ---

    // Disk footprint: sum of all index artifact files.
    let index_keys = store.list("indexes/").unwrap_or_default();
    let artifact_size_bytes: u64 = index_keys
        .iter()
        .filter_map(|key| store.get(key).ok())
        .map(|b| b.len() as u64)
        .sum();

    // Memory estimate: same as artifact size (shard binaries loaded into RAM).
    let memory_bytes = artifact_size_bytes;

    // Raw vector data: sum all files under datasets/.
    let dataset_keys = store.list("datasets/").unwrap_or_default();
    let raw_vector_size_bytes: u64 = dataset_keys
        .iter()
        .filter_map(|key| store.get(key).ok())
        .map(|b| b.len() as u64)
        .sum();

    let compression_ratio = if artifact_size_bytes > 0 {
        raw_vector_size_bytes as f64 / artifact_size_bytes as f64
    } else {
        0.0
    };

    let report = BenchmarkReport {
        workload,
        num_queries: queries.len(),
        k,
        nprobe,
        recall_at_k: mean_recall,
        mean_latency_us: mean_latency,
        p99_latency_us: p99_latency,
        throughput_qps,
        artifact_size_bytes,
        raw_vector_size_bytes,
        compression_ratio,
        memory_bytes,
    };

    info!(
        workload = %workload,
        recall_at_k = report.recall_at_k,
        mean_latency_us = report.mean_latency_us,
        p99_latency_us = report.p99_latency_us,
        throughput_qps = report.throughput_qps,
        artifact_size_bytes = report.artifact_size_bytes,
        memory_bytes = report.memory_bytes,
        compression_ratio = report.compression_ratio,
        "Benchmark complete"
    );

    report
}

#[cfg(test)]
mod tests {
    use shardlake_core::types::VectorId;
    use shardlake_index::exact::recall_at_k;

    use super::WorkloadKind;

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
    fn test_workload_kind_display() {
        assert_eq!(WorkloadKind::Cold.to_string(), "cold");
        assert_eq!(WorkloadKind::Warm.to_string(), "warm");
        assert_eq!(WorkloadKind::Mixed.to_string(), "mixed");
    }

    #[test]
    fn test_workload_kind_default() {
        assert_eq!(WorkloadKind::default(), WorkloadKind::Warm);
    }
}
