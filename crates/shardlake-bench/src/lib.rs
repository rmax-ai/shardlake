//! Benchmark harness: recall@k vs exact baseline, latency, artifact size.
//! Also provides partition evaluation utilities and cost-estimation metrics.

pub mod generate;

use std::{collections::HashMap, sync::Arc, time::Instant};

use serde::{Deserialize, Serialize};
use tracing::info;

use shardlake_core::{
    config::FanOutPolicy,
    types::{DistanceMetric, VectorId, VectorRecord},
};
use shardlake_index::{
    exact::{exact_search, precision_at_k, recall_at_k},
    kmeans::top_n_centroids,
    IndexSearcher, Result as IndexResult,
};
use shardlake_manifest::Manifest;
use shardlake_storage::{paths, ObjectStore};

/// Errors that can arise while evaluating partition quality.
#[derive(Debug, thiserror::Error)]
pub enum PartitioningError {
    /// Approximate search failed for one of the evaluated queries.
    #[error("approximate search failed for query {query_id}: {source}")]
    ApproximateSearch {
        /// The record id of the query being evaluated.
        query_id: u64,
        /// The underlying search failure.
        #[source]
        source: shardlake_index::IndexError,
    },
}

/// Cost-estimation metrics for an index configuration.
///
/// These metrics allow comparing the resource cost of different index
/// configurations alongside performance and quality metrics.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use shardlake_bench::compute_cost_metrics;
///
/// let metrics = compute_cost_metrics(&store, &manifest);
/// println!("disk: {} bytes, compression ratio: {:.2}x",
///     metrics.disk_footprint_bytes, metrics.compression_ratio);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostMetrics {
    /// Estimated in-memory footprint of the indexed vector data in bytes.
    ///
    /// For uncompressed indexes: `total_vectors × dims × 4` (f32 vectors).
    /// For PQ-compressed indexes: `total_vectors × M` (1 byte per code) plus
    /// the codebook itself (`M × K × sub_dims × 4` bytes of f32 centroids).
    pub memory_usage_bytes: u64,
    /// Total size of all index artifact files on disk in bytes.
    pub disk_footprint_bytes: u64,
    /// Vector compression ratio: raw vector bytes divided by the size of the
    /// PQ-encoded vector representation, i.e. `(dims × 4) / M` where `M` is
    /// the number of PQ sub-spaces.
    ///
    /// Returns `1.0` for uncompressed indexes (no compression applied).
    pub compression_ratio: f64,
}

/// Compute cost-estimation metrics for the index described by `manifest`.
///
/// The `store` is used to measure the total on-disk footprint of all index
/// artifacts.  This function is independent of the full benchmark pipeline
/// and can be reused by any command that needs resource-cost information.
///
/// # Arguments
///
/// * `store` – Object store used to list and measure artifact sizes.
/// * `manifest` – Manifest of the index being measured.
pub fn compute_cost_metrics(store: &Arc<dyn ObjectStore>, manifest: &Manifest) -> CostMetrics {
    let disk_footprint_bytes: u64 = store
        .list(paths::indexes_prefix())
        .unwrap_or_default()
        .iter()
        .filter_map(|k| store.get(k).ok())
        .map(|b| b.len() as u64)
        .sum();

    let total_vectors = manifest.total_vector_count;
    let dims = manifest.dims as u64;
    // f32 is 4 bytes per component.
    let raw_vector_bytes = total_vectors * dims * 4;

    let (memory_usage_bytes, compression_ratio) =
        if manifest.compression.enabled && manifest.compression.codec == "pq8" {
            let m = manifest.compression.pq_num_subspaces as u64;
            let k = manifest.compression.pq_codebook_size as u64;
            // Guard against a zero-subspace value (malformed manifest).
            if m == 0 {
                (raw_vector_bytes, 1.0)
            } else {
                let sub_dims = dims / m;
                // Encoded vectors: one byte per sub-space per vector.
                let encoded_bytes = total_vectors * m;
                // Codebook: M centroids-tables each with K × sub_dims f32 values.
                let codebook_bytes = m * k * sub_dims * 4;
                let memory = encoded_bytes + codebook_bytes;
                let ratio = (dims as f64 * 4.0) / m as f64;
                (memory, ratio)
            }
        } else {
            (raw_vector_bytes, 1.0)
        };

    CostMetrics {
        memory_usage_bytes,
        disk_footprint_bytes,
        compression_ratio,
    }
}

/// Summary statistics for one benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    /// Number of query vectors evaluated.
    pub num_queries: usize,
    /// Number of nearest neighbours retrieved per query.
    pub k: usize,
    /// Number of shards probed per query.
    pub nprobe: usize,
    /// Mean recall@k across all queries (fraction of true top-k found).
    pub recall_at_k: f64,
    /// Mean per-query ANN search latency in microseconds.
    pub mean_latency_us: f64,
    /// 99th-percentile per-query ANN search latency in microseconds.
    pub p99_latency_us: f64,
    /// Query throughput: queries executed per second (wall-clock time over all queries).
    pub throughput_qps: f64,
    /// Cost-estimation metrics: memory usage, disk footprint, and compression ratio.
    pub cost_metrics: CostMetrics,
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
    let cost_metrics = compute_cost_metrics(store, searcher.manifest());

    if queries.is_empty() {
        return BenchmarkReport {
            num_queries: 0,
            k,
            nprobe: policy.candidate_centroids as usize,
            recall_at_k: 0.0,
            mean_latency_us: 0.0,
            p99_latency_us: 0.0,
            throughput_qps: 0.0,
            cost_metrics,
        };
    }

    let mut latencies_us: Vec<f64> = Vec::with_capacity(queries.len());
    let mut recalls: Vec<f64> = Vec::with_capacity(queries.len());

    let wall_start = Instant::now();
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
    let wall_elapsed_secs = wall_start.elapsed().as_secs_f64();

    let mean_recall = recalls.iter().sum::<f64>() / recalls.len() as f64;
    let mean_latency = latencies_us.iter().sum::<f64>() / latencies_us.len() as f64;
    let p99_latency = nearest_rank_percentile(&mut latencies_us, 0.99);
    let throughput_qps = queries.len() as f64 / wall_elapsed_secs.max(f64::EPSILON);

    let report = BenchmarkReport {
        num_queries: queries.len(),
        k,
        nprobe: policy.candidate_centroids as usize,
        recall_at_k: mean_recall,
        mean_latency_us: mean_latency,
        p99_latency_us: p99_latency,
        throughput_qps,
        cost_metrics,
    };

    info!(
        recall_at_k = report.recall_at_k,
        mean_latency_us = report.mean_latency_us,
        p99_latency_us = report.p99_latency_us,
        throughput_qps = report.throughput_qps,
        disk_footprint_bytes = report.cost_metrics.disk_footprint_bytes,
        memory_usage_bytes = report.cost_metrics.memory_usage_bytes,
        compression_ratio = report.cost_metrics.compression_ratio,
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

// ── Partition evaluation ───────────────────────────────────────────────────────

/// Per-shard hotness entry: how often a shard was probed relative to query count.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardHotnessEntry {
    /// 0-based shard identifier (matches [`shardlake_core::types::ShardId`]).
    pub shard_id: u32,
    /// Fraction of evaluated queries that probed this shard (in `[0, 1]`).
    pub probe_fraction: f64,
}

/// Full partition quality report produced by [`evaluate_partitioning`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitioningReport {
    /// Index version this report was generated for.
    pub index_version: String,
    /// Total number of vectors in the index.
    pub total_vectors: u64,
    /// Number of non-empty shards.
    pub num_shards: usize,
    /// The *k* used for recall@k computation.
    pub k: usize,
    /// Number of shards probed per query.
    pub nprobe: usize,
    /// Number of query vectors used for routing / recall evaluation.
    pub num_queries: usize,
    // ── Shard size distribution ────────────────────────────────────────────
    /// Vector count of the smallest shard.
    pub min_shard_size: u64,
    /// Vector count of the largest shard.
    pub max_shard_size: u64,
    /// Mean vector count across all shards.
    pub mean_shard_size: f64,
    /// Population standard deviation of shard vector counts.
    pub std_dev_shard_size: f64,
    /// Imbalance ratio: `max_shard_size / mean_shard_size`.
    ///
    /// A perfectly balanced partition has imbalance 1.0; values above 1.0
    /// indicate progressively more skewed distributions.
    pub imbalance_ratio: f64,
    /// Per-shard vector counts: `(shard_id, vector_count)` pairs in shard order.
    pub per_shard_vector_counts: Vec<(u32, u64)>,
    // ── Routing accuracy ──────────────────────────────────────────────────
    /// Fraction of queries where the exact top-1 neighbour's assigned shard
    /// was among the `nprobe` probed shards.
    ///
    /// `None` when the manifest lacks centroid metadata (manifest_version < 2)
    /// or when no query vectors were evaluated.
    pub routing_accuracy: Option<f64>,
    // ── Recall impact ─────────────────────────────────────────────────────
    /// Recall@k achieved with the current partition and `nprobe` setting.
    ///
    /// `None` when no query vectors were evaluated.
    pub recall_at_k: Option<f64>,
    // ── Shard hotness ─────────────────────────────────────────────────────
    /// Per-shard probe fractions across all evaluated queries.
    ///
    /// Empty when no query vectors were evaluated or centroid metadata is
    /// absent (manifest_version < 2).
    pub shard_hotness: Vec<ShardHotnessEntry>,
}

/// Evaluate partition quality against a built index.
///
/// Computes:
/// - **Shard size distribution** directly from `manifest.shards`.
/// - **Routing accuracy**: fraction of queries where the exact top-1 neighbour's
///   assigned shard (nearest centroid) is in the `nprobe`-probed set.
/// - **Recall impact**: recall@k achieved with the current `nprobe` setting.
/// - **Shard hotness**: per-shard fraction of queries that probe each shard.
///
/// `queries` is the sample of vectors used for routing / recall measurements.
/// `corpus` is the full set of vectors used as the exact-search ground truth.
/// Routing accuracy and hotness are skipped (set to `None` / empty) when the
/// manifest lacks centroid metadata (manifest_version < 2).
pub fn evaluate_partitioning(
    searcher: &IndexSearcher,
    queries: &[VectorRecord],
    corpus: &[VectorRecord],
    k: usize,
    nprobe: usize,
    metric: DistanceMetric,
) -> Result<PartitioningReport, PartitioningError> {
    let manifest = searcher.manifest();

    // ── 1. Shard size distribution ─────────────────────────────────────────
    let per_shard_vector_counts: Vec<(u32, u64)> = manifest
        .shards
        .iter()
        .map(|s| (s.shard_id.0, s.vector_count))
        .collect();

    let num_shards = manifest.shards.len();
    let total_vectors = manifest.total_vector_count;

    let min_shard_size = per_shard_vector_counts
        .iter()
        .map(|&(_, c)| c)
        .min()
        .unwrap_or(0);
    let max_shard_size = per_shard_vector_counts
        .iter()
        .map(|&(_, c)| c)
        .max()
        .unwrap_or(0);
    let mean_shard_size = if num_shards > 0 {
        total_vectors as f64 / num_shards as f64
    } else {
        0.0
    };
    let variance = if num_shards > 0 {
        per_shard_vector_counts
            .iter()
            .map(|&(_, c)| (c as f64 - mean_shard_size).powi(2))
            .sum::<f64>()
            / num_shards as f64
    } else {
        0.0
    };
    let std_dev_shard_size = variance.sqrt();
    let imbalance_ratio = if mean_shard_size > 0.0 {
        max_shard_size as f64 / mean_shard_size
    } else {
        0.0
    };

    // ── 2. Routing accuracy, recall@k, and shard hotness ──────────────────
    let centroids: Vec<Vec<f32>> = manifest.shards.iter().map(|s| s.centroid.clone()).collect();
    let has_centroids = !centroids.is_empty() && centroids.iter().all(|c| !c.is_empty());

    if queries.is_empty() {
        return Ok(PartitioningReport {
            index_version: manifest.index_version.0.clone(),
            total_vectors,
            num_shards,
            k,
            nprobe,
            num_queries: 0,
            min_shard_size,
            max_shard_size,
            mean_shard_size,
            std_dev_shard_size,
            imbalance_ratio,
            per_shard_vector_counts,
            routing_accuracy: None,
            recall_at_k: None,
            shard_hotness: Vec::new(),
        });
    }

    // Fast lookup: VectorId → vector data for GT neighbour assignment.
    let corpus_map: HashMap<VectorId, &[f32]> =
        corpus.iter().map(|r| (r.id, r.data.as_slice())).collect();

    let mut routing_correct = 0usize;
    let mut recalls: Vec<f64> = Vec::with_capacity(queries.len());
    let mut probe_counts = vec![0usize; num_shards];
    let fan_out_policy = FanOutPolicy {
        candidate_centroids: nprobe as u32,
        candidate_shards: 0,
        max_vectors_per_shard: 0,
    };

    for query in queries {
        // Exact top-k ground truth for recall@k.
        let gt = exact_search(&query.data, corpus, metric, k);
        let gt_ids: Vec<VectorId> = gt.iter().map(|r| r.id).collect();

        // Approximate search for recall impact. This works for both modern and
        // legacy manifests because `IndexSearcher` falls back to loading shard
        // centroids from artifact bytes when the manifest does not embed them.
        let approx = searcher
            .search(&query.data, k, &fan_out_policy)
            .map_err(|source| PartitioningError::ApproximateSearch {
                query_id: query.id.0,
                source,
            })?;
        let approx_ids: Vec<VectorId> = approx.iter().map(|r| r.id).collect();
        recalls.push(recall_at_k(&gt_ids, &approx_ids));

        if has_centroids {
            // Which shards are probed for this query?
            let probe_indices =
                top_n_centroids(&query.data, &centroids, nprobe.min(centroids.len()));
            for &idx in &probe_indices {
                if idx < num_shards {
                    probe_counts[idx] += 1;
                }
            }

            // Routing accuracy: is the GT top-1 neighbour's assigned shard probed?
            if let Some(&gt_top1_id) = gt_ids.first() {
                if let Some(neighbor_data) = corpus_map.get(&gt_top1_id) {
                    let neighbor_shard = top_n_centroids(neighbor_data, &centroids, 1);
                    if let Some(&ns_idx) = neighbor_shard.first() {
                        if probe_indices.contains(&ns_idx) {
                            routing_correct += 1;
                        }
                    }
                }
            }
        }
    }

    let n = queries.len();
    debug_assert!(n > 0, "queries must be non-empty at this point");
    let recall_at_k_val = recalls.iter().sum::<f64>() / n as f64;
    let routing_accuracy = has_centroids.then_some(routing_correct as f64 / n as f64);

    let shard_hotness = if has_centroids {
        per_shard_vector_counts
            .iter()
            .enumerate()
            .map(|(i, &(shard_id, _))| ShardHotnessEntry {
                shard_id,
                probe_fraction: probe_counts[i] as f64 / n as f64,
            })
            .collect()
    } else {
        Vec::new()
    };

    info!(
        routing_accuracy = ?routing_accuracy,
        recall_at_k = recall_at_k_val,
        imbalance_ratio,
        "Partition evaluation complete"
    );

    Ok(PartitioningReport {
        index_version: manifest.index_version.0.clone(),
        total_vectors,
        num_shards,
        k,
        nprobe,
        num_queries: n,
        min_shard_size,
        max_shard_size,
        mean_shard_size,
        std_dev_shard_size,
        imbalance_ratio,
        per_shard_vector_counts,
        routing_accuracy,
        recall_at_k: Some(recall_at_k_val),
        shard_hotness,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::nearest_rank_percentile;
    use super::*;
    use shardlake_core::{
        config::SystemConfig,
        types::{
            DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
        },
    };
    use shardlake_index::{
        exact::{precision_at_k, recall_at_k},
        BuildParams, IndexBuilder, IndexSearcher,
    };
    use shardlake_storage::{LocalObjectStore, ObjectStore};
    use tempfile::tempdir;

    fn make_records(n: usize, dims: usize) -> Vec<VectorRecord> {
        (0..n)
            .map(|i| {
                let base = i * dims;
                VectorRecord {
                    id: VectorId(i as u64),
                    data: (0..dims).map(|d| (base + d) as f32).collect(),
                    metadata: None,
                }
            })
            .collect()
    }

    fn build_test_index(
        store: &LocalObjectStore,
        records: Vec<VectorRecord>,
        num_shards: u32,
        storage_root: std::path::PathBuf,
    ) -> shardlake_manifest::Manifest {
        let dims = records[0].data.len();
        let config = SystemConfig {
            storage_root,
            num_shards,
            kmeans_iters: 5,
            nprobe: 2,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            candidate_shards: 0,
            max_vectors_per_shard: 0,
            kmeans_sample_size: None,
            pq_enabled: false,
            pq_num_subspaces: SystemConfig::default_pq_num_subspaces(),
            pq_codebook_size: SystemConfig::default_pq_codebook_size(),
            ..SystemConfig::default()
        };
        IndexBuilder::new(store, &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-test".into()),
                embedding_version: EmbeddingVersion("emb-test".into()),
                index_version: IndexVersion("idx-test".into()),
                metric: DistanceMetric::Euclidean,
                dims,
                vectors_key: shardlake_storage::paths::dataset_vectors_key("ds-test"),
                metadata_key: shardlake_storage::paths::dataset_metadata_key("ds-test"),
                pq_params: None,
            })
            .unwrap()
    }

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

    #[test]
    fn run_benchmark_reports_correct_field_counts() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(20, 4);
        let manifest =
            build_test_index(store.as_ref(), records.clone(), 2, tmp.path().to_path_buf());

        let queries: Vec<VectorRecord> = records[..5].to_vec();
        let policy = shardlake_core::config::FanOutPolicy {
            candidate_centroids: 1,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let store_arc: Arc<dyn ObjectStore> = store;
        let searcher = IndexSearcher::new(Arc::clone(&store_arc), manifest);
        let report = run_benchmark(
            &searcher,
            &store_arc,
            &queries,
            &records,
            3,
            &policy,
            DistanceMetric::Euclidean,
        );

        assert_eq!(report.num_queries, 5);
        assert_eq!(report.k, 3);
        assert_eq!(report.nprobe, 1);
        assert!((0.0..=1.0).contains(&report.recall_at_k));
        assert!(report.mean_latency_us >= 0.0);
        assert!(report.mean_latency_us.is_finite());
        assert!(report.p99_latency_us.is_finite());
        assert!(report.p99_latency_us >= 0.0);
        assert!(report.throughput_qps > 0.0);
        assert!(report.cost_metrics.disk_footprint_bytes > 0);
    }

    #[test]
    fn run_benchmark_empty_queries_return_zero_metrics() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(10, 4);
        let manifest =
            build_test_index(store.as_ref(), records.clone(), 1, tmp.path().to_path_buf());

        let policy = shardlake_core::config::FanOutPolicy {
            candidate_centroids: 1,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let store_arc: Arc<dyn ObjectStore> = store;
        let searcher = IndexSearcher::new(Arc::clone(&store_arc), manifest);
        let report = run_benchmark(
            &searcher,
            &store_arc,
            &[],
            &records,
            3,
            &policy,
            DistanceMetric::Euclidean,
        );

        assert_eq!(report.num_queries, 0);
        assert_eq!(report.k, 3);
        assert_eq!(report.nprobe, 1);
        assert_eq!(report.recall_at_k, 0.0);
        assert_eq!(report.mean_latency_us, 0.0);
        assert_eq!(report.p99_latency_us, 0.0);
        assert_eq!(report.throughput_qps, 0.0);
        assert!(report.cost_metrics.disk_footprint_bytes > 0);
    }

    #[test]
    fn run_benchmark_recall_is_perfect_with_full_probe() {
        // With nprobe large enough to cover all shards, recall should be 1.0.
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(20, 4);
        let manifest =
            build_test_index(store.as_ref(), records.clone(), 2, tmp.path().to_path_buf());

        let queries: Vec<VectorRecord> = records[..5].to_vec();
        // candidate_centroids=2 covers both shards
        let policy = shardlake_core::config::FanOutPolicy {
            candidate_centroids: 2,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let store_arc: Arc<dyn ObjectStore> = store;
        let searcher = IndexSearcher::new(Arc::clone(&store_arc), manifest);
        let report = run_benchmark(
            &searcher,
            &store_arc,
            &queries,
            &records,
            3,
            &policy,
            DistanceMetric::Euclidean,
        );

        assert!((report.recall_at_k - 1.0_f64).abs() < 1e-9);
    }

    #[test]
    fn run_benchmark_throughput_is_positive() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(10, 4);
        let manifest =
            build_test_index(store.as_ref(), records.clone(), 1, tmp.path().to_path_buf());

        let policy = shardlake_core::config::FanOutPolicy {
            candidate_centroids: 1,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let store_arc: Arc<dyn ObjectStore> = store;
        let searcher = IndexSearcher::new(Arc::clone(&store_arc), manifest);
        let report = run_benchmark(
            &searcher,
            &store_arc,
            &records,
            &records,
            3,
            &policy,
            DistanceMetric::Euclidean,
        );

        assert!(report.throughput_qps > 0.0, "throughput must be positive");
        // throughput (qps) should be in a plausible range: wall-clock time includes
        // both exact-search overhead and approximate search, so throughput will be
        // somewhat lower than 1e6 / mean_latency_us but should be at least 1 qps.
        assert!(report.throughput_qps >= 1.0);
        assert!(report.throughput_qps < 1e12);
    }

    #[test]
    fn evaluate_partitioning_reports_shard_size_distribution() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(20, 4);
        let manifest =
            build_test_index(store.as_ref(), records.clone(), 2, tmp.path().to_path_buf());

        let searcher = IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest);
        let report =
            evaluate_partitioning(&searcher, &[], &records, 5, 1, DistanceMetric::Euclidean)
                .unwrap();

        assert_eq!(report.num_shards, report.per_shard_vector_counts.len());
        assert_eq!(report.total_vectors, 20);
        let total: u64 = report.per_shard_vector_counts.iter().map(|&(_, c)| c).sum();
        assert_eq!(total, 20);
        assert!(report.imbalance_ratio >= 1.0);
        // No queries → routing/recall are None.
        assert!(report.routing_accuracy.is_none());
        assert!(report.recall_at_k.is_none());
        assert!(report.shard_hotness.is_empty());
    }

    #[test]
    fn evaluate_partitioning_reports_routing_and_recall_with_queries() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(40, 4);
        let manifest =
            build_test_index(store.as_ref(), records.clone(), 2, tmp.path().to_path_buf());

        let queries: Vec<VectorRecord> = records[..10].to_vec();
        let searcher = IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest);
        let report = evaluate_partitioning(
            &searcher,
            &queries,
            &records,
            5,
            1,
            DistanceMetric::Euclidean,
        )
        .unwrap();

        assert_eq!(report.num_queries, 10);
        assert!(report.routing_accuracy.is_some());
        let ra = report.routing_accuracy.unwrap();
        assert!((0.0..=1.0).contains(&ra));

        assert!(report.recall_at_k.is_some());
        let r = report.recall_at_k.unwrap();
        assert!((0.0..=1.0).contains(&r));

        assert_eq!(report.shard_hotness.len(), report.num_shards);
        let hotness_sum: f64 = report.shard_hotness.iter().map(|e| e.probe_fraction).sum();
        // nprobe=1, so each query probes exactly 1 shard → sum = 1.0
        assert!((hotness_sum - 1.0_f64).abs() < 1e-9);
    }

    #[test]
    fn evaluate_partitioning_keeps_recall_for_legacy_manifests() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(40, 4);
        let mut manifest =
            build_test_index(store.as_ref(), records.clone(), 2, tmp.path().to_path_buf());
        manifest.manifest_version = 1;
        for shard in &mut manifest.shards {
            shard.centroid.clear();
        }

        let queries: Vec<VectorRecord> = records[..10].to_vec();
        let searcher = IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest);
        let report = evaluate_partitioning(
            &searcher,
            &queries,
            &records,
            5,
            1,
            DistanceMetric::Euclidean,
        )
        .unwrap();

        assert_eq!(report.num_queries, 10);
        assert!(report.routing_accuracy.is_none());
        assert!(report.shard_hotness.is_empty());
        assert!(report.recall_at_k.is_some());
        let recall = report.recall_at_k.unwrap();
        assert!((0.0..=1.0).contains(&recall));
    }

    #[test]
    fn evaluate_partitioning_std_dev_zero_for_equal_shards() {
        // When all shards have exactly the same size, std_dev == 0 and imbalance == 1.
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        // 2 records, 1 shard → single shard with 2 vectors.
        let records = make_records(2, 2);
        let manifest =
            build_test_index(store.as_ref(), records.clone(), 1, tmp.path().to_path_buf());

        let searcher = IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest);
        let report =
            evaluate_partitioning(&searcher, &[], &records, 1, 1, DistanceMetric::Euclidean)
                .unwrap();

        assert_eq!(report.num_shards, 1);
        assert!((report.std_dev_shard_size - 0.0_f64).abs() < 1e-9);
        assert!((report.imbalance_ratio - 1.0_f64).abs() < 1e-9);
    }

    #[test]
    fn evaluate_partitioning_propagates_search_errors() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(8, 4);
        let manifest =
            build_test_index(store.as_ref(), records.clone(), 2, tmp.path().to_path_buf());

        let bad_query = VectorRecord {
            id: VectorId(999),
            data: vec![0.0, 1.0],
            metadata: None,
        };
        let searcher = IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest);
        let err = evaluate_partitioning(
            &searcher,
            &[bad_query],
            &records,
            3,
            1,
            DistanceMetric::Euclidean,
        )
        .unwrap_err();

        assert!(matches!(
            err,
            PartitioningError::ApproximateSearch { query_id: 999, .. }
        ));
    }

    // ── compute_cost_metrics tests ────────────────────────────────────────────

    #[test]
    fn cost_metrics_uncompressed_index_has_ratio_one() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        // 10 records with 4 dimensions.
        let records = make_records(10, 4);
        let manifest = build_test_index(store.as_ref(), records, 1, tmp.path().to_path_buf());

        let store_arc: Arc<dyn ObjectStore> = store;
        let metrics = compute_cost_metrics(&store_arc, &manifest);

        // Uncompressed index → ratio must be exactly 1.0.
        assert_eq!(metrics.compression_ratio, 1.0);
        // Disk footprint must be positive (artifacts were written).
        assert!(metrics.disk_footprint_bytes > 0);
        // Memory usage = total_vectors × dims × 4 bytes.
        let expected_memory: u64 = manifest.total_vector_count * u64::from(manifest.dims) * 4;
        assert_eq!(metrics.memory_usage_bytes, expected_memory);
    }

    #[test]
    fn cost_metrics_pq_index_has_compression_ratio_gt_one() {
        use shardlake_index::pq::PqParams;

        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        // 50 records with 8 dimensions; 2 sub-spaces so ratio = (8*4)/2 = 16.
        let dims = 8usize;
        let records = make_records(50, dims);
        let num_subspaces = 2u32;
        let codebook_size = 4u32; // keep small for test speed

        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 1,
            kmeans_iters: 5,
            nprobe: 1,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            candidate_shards: 0,
            max_vectors_per_shard: 0,
            kmeans_sample_size: None,
            pq_enabled: true,
            pq_num_subspaces: num_subspaces,
            pq_codebook_size: codebook_size,
            ..SystemConfig::default()
        };
        let manifest = IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-pq".into()),
                embedding_version: EmbeddingVersion("emb-pq".into()),
                index_version: IndexVersion("idx-pq".into()),
                metric: DistanceMetric::Euclidean,
                dims,
                vectors_key: shardlake_storage::paths::dataset_vectors_key("ds-pq"),
                metadata_key: shardlake_storage::paths::dataset_metadata_key("ds-pq"),
                pq_params: Some(PqParams {
                    num_subspaces: num_subspaces as usize,
                    codebook_size: codebook_size as usize,
                }),
            })
            .unwrap();

        let store_arc: Arc<dyn ObjectStore> = store;
        let metrics = compute_cost_metrics(&store_arc, &manifest);

        // compression_ratio = (dims × 4) / num_subspaces = (8 × 4) / 2 = 16.
        let expected_ratio = (dims as f64 * 4.0) / num_subspaces as f64;
        assert!(
            (metrics.compression_ratio - expected_ratio).abs() < 1e-9,
            "expected ratio {expected_ratio}, got {}",
            metrics.compression_ratio
        );
        assert!(metrics.compression_ratio > 1.0);
        assert!(metrics.disk_footprint_bytes > 0);

        // PQ memory = encoded_bytes + codebook_bytes.
        let total_vectors = manifest.total_vector_count;
        let m = u64::from(num_subspaces);
        let k = u64::from(codebook_size);
        let sub_dims = dims as u64 / m;
        let expected_memory = total_vectors * m + m * k * sub_dims * 4;
        assert_eq!(metrics.memory_usage_bytes, expected_memory);
    }

    #[test]
    fn cost_metrics_disk_footprint_matches_sum_of_artifacts() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(10, 4);
        let manifest = build_test_index(store.as_ref(), records, 1, tmp.path().to_path_buf());

        let store_arc: Arc<dyn ObjectStore> = store;
        let metrics = compute_cost_metrics(&store_arc, &manifest);

        // Manually sum artifact sizes the same way compute_cost_metrics does.
        let manual_sum: u64 = store_arc
            .list(shardlake_storage::paths::indexes_prefix())
            .unwrap_or_default()
            .iter()
            .filter_map(|k| store_arc.get(k).ok())
            .map(|b| b.len() as u64)
            .sum();

        assert_eq!(metrics.disk_footprint_bytes, manual_sum);
    }

    #[test]
    fn cost_metrics_zero_vector_index_has_zero_memory() {
        // Build a manifest with zero vectors to verify the calculation does not panic.
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(4, 4);
        let mut manifest = build_test_index(store.as_ref(), records, 1, tmp.path().to_path_buf());
        // Artificially set vector count to 0.
        manifest.total_vector_count = 0;

        let store_arc: Arc<dyn ObjectStore> = store;
        let metrics = compute_cost_metrics(&store_arc, &manifest);

        assert_eq!(metrics.memory_usage_bytes, 0);
        assert_eq!(metrics.compression_ratio, 1.0);
    }
}
