//! Benchmark harness: recall@k vs exact baseline, latency, and cost metrics.
//! Also provides partition evaluation utilities.

pub mod generate;

use std::{
    collections::{BTreeSet, HashMap},
    fmt,
    sync::Arc,
    time::Instant,
};

use serde::{Deserialize, Serialize};
use tracing::info;

use shardlake_core::{
    config::FanOutPolicy,
    types::{DistanceMetric, VectorId, VectorRecord},
};
use shardlake_index::{
    bm25::Bm25Index,
    exact::{exact_search, precision_at_k, recall_at_k},
    kmeans::top_n_centroids,
    ranking::{rank_hybrid, HybridRankingPolicy},
    IndexSearcher, Result as IndexResult, PQ8_CODEC,
};
use shardlake_manifest::Manifest;
use shardlake_storage::ObjectStore;

/// Convert an optional metadata [`serde_json::Value`] to a plain text string
/// suitable for BM25 querying.
///
/// - `None` → empty string (BM25 search returns no results for an empty query).
/// - `String` values → returned as-is.
/// - All other JSON values → formatted with `serde_json::Value::to_string()`.
///
/// This matches the text extraction convention used when building BM25 indexes
/// from corpus metadata, ensuring that query text and index text are tokenised
/// consistently.
pub fn metadata_to_text(meta: &Option<serde_json::Value>) -> String {
    match meta {
        None => String::new(),
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
    }
}

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
    let disk_footprint_bytes: u64 = index_artifact_keys(manifest)
        .into_iter()
        .filter_map(|key| match artifact_size_bytes(store.as_ref(), &key) {
            Ok(size) => Some(size),
            Err(err) => {
                tracing::warn!(
                    artifact_key = %key,
                    error = %err,
                    "failed to read index artifact while computing disk footprint"
                );
                None
            }
        })
        .sum();

    let total_vectors = manifest.total_vector_count;
    let dims = manifest.dims as u64;
    // f32 is 4 bytes per component.
    let raw_vector_bytes = saturating_product(&[total_vectors, dims, 4], "raw_vector_bytes");

    let (memory_usage_bytes, compression_ratio) =
        if manifest.compression.enabled && manifest.compression.codec == PQ8_CODEC {
            let m = manifest.compression.pq_num_subspaces as u64;
            let k = manifest.compression.pq_codebook_size as u64;
            // Guard against a zero-subspace value (malformed manifest): fall back
            // to the uncompressed estimate and warn the caller.
            if m == 0 {
                tracing::warn!(
                    "manifest has pq8 compression enabled but pq_num_subspaces is 0; \
                     falling back to uncompressed memory estimate"
                );
                (raw_vector_bytes, 1.0)
            } else {
                let sub_dims = dims / m;
                // Encoded vectors: one byte per sub-space per vector.
                let encoded_bytes = saturating_product(&[total_vectors, m], "encoded_bytes");
                // Codebook: M centroids-tables each with K × sub_dims f32 values.
                let codebook_bytes = saturating_product(&[m, k, sub_dims, 4], "codebook_bytes");
                let memory = saturating_add(encoded_bytes, codebook_bytes, "memory_usage_bytes");
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

fn artifact_size_bytes(store: &dyn ObjectStore, key: &str) -> shardlake_storage::Result<u64> {
    if let Some(path) = store.local_path_for(key)? {
        match std::fs::metadata(&path) {
            Ok(metadata) => return Ok(metadata.len()),
            Err(err) => {
                tracing::warn!(
                    artifact_key = %key,
                    path = %path.display(),
                    error = %err,
                    "failed to stat local artifact path while computing disk footprint; falling back to object read"
                );
            }
        }
    }

    store.get(key).map(|bytes| bytes.len() as u64)
}

fn saturating_product(parts: &[u64], metric_name: &str) -> u64 {
    parts
        .iter()
        .copied()
        .try_fold(1_u64, |acc, value| acc.checked_mul(value))
        .unwrap_or_else(|| {
            tracing::warn!(
                metric = metric_name,
                factors = ?parts,
                "overflow while computing benchmark cost metrics; saturating to u64::MAX"
            );
            u64::MAX
        })
}

fn saturating_add(left: u64, right: u64, metric_name: &str) -> u64 {
    left.checked_add(right).unwrap_or_else(|| {
        tracing::warn!(
            metric = metric_name,
            left,
            right,
            "overflow while computing benchmark cost metrics; saturating to u64::MAX"
        );
        u64::MAX
    })
}

fn index_artifact_keys(manifest: &Manifest) -> BTreeSet<String> {
    let mut keys = BTreeSet::from([Manifest::storage_key(&manifest.index_version)]);
    keys.extend(
        manifest
            .shards
            .iter()
            .map(|shard| shard.artifact_key.clone()),
    );

    if let Some(codebook_key) = &manifest.compression.codebook_key {
        keys.insert(codebook_key.clone());
    }
    if let Some(coarse_quantizer_key) = &manifest.coarse_quantizer_key {
        keys.insert(coarse_quantizer_key.clone());
    }
    if let Some(lexical) = &manifest.lexical {
        keys.insert(lexical.artifact_key.clone());
    }

    keys
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

/// One row in an ANN family comparison report.
///
/// Captures the alias and ANN family name together with the evaluation metrics
/// for that family, allowing callers to compare results across families in a
/// single [`CompareAnnReport`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnFamilyReport {
    /// Alias used to load the index (e.g. `"latest"`, `"hnsw-exp"`).
    pub alias: String,
    /// Canonical ANN algorithm family name recorded in the manifest
    /// (e.g. `"ivf_flat"`, `"ivf_pq"`, `"hnsw"`, `"diskann"`).
    pub ann_family: String,
    /// Evaluation metrics for this family.
    #[serde(flatten)]
    pub eval: EvalAnnReport,
}

/// Comparison report across multiple ANN families.
///
/// Produced by running [`run_eval_ann`] once per alias and collecting the
/// results into a single document.  Each entry corresponds to one index alias
/// and includes the ANN family name extracted from the manifest, making it
/// straightforward to compare quality and latency across families.
///
/// # Example (JSON)
///
/// ```json
/// {
///   "entries": [
///     { "alias": "ivf-idx",  "ann_family": "ivf_flat", "num_queries": 100, ... },
///     { "alias": "hnsw-idx", "ann_family": "hnsw",     "num_queries": 100, ... }
///   ]
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompareAnnReport {
    /// One entry per evaluated alias / ANN family, in the order they were
    /// supplied to the CLI.
    pub entries: Vec<AnnFamilyReport>,
}

/// Workload simulation mode for benchmark runs.
///
/// Controls whether the shard cache is pre-warmed, cleared, or left to warm
/// organically so that benchmark results reflect cache-cold, cache-warm, or
/// realistic mixed serving behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadMode {
    /// Cold workload: the shard cache is empty at the start of every query.
    ///
    /// Simulates a freshly started process with no pre-warmed state.  A new
    /// [`IndexSearcher`] is created for each query so that no shard data is
    /// carried across requests.
    Cold,
    /// Warm workload: the shard cache is fully pre-loaded before the timed run.
    ///
    /// Simulates a long-running process where all accessed shards are already
    /// resident in memory.  An un-timed warm-up pass is executed first;
    /// latencies measured thereafter reflect pure in-memory query costs.
    Warm,
    /// Mixed workload: no explicit pre-warming or cache clearing.
    ///
    /// The cache transitions from cold to warm as queries progress, combining
    /// cold-start behaviour in the early queries with warm-cache behaviour in
    /// later queries.  This mirrors a typical serving process that starts cold
    /// and gradually warms up.
    Mixed,
}

impl fmt::Display for WorkloadMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkloadMode::Cold => write!(f, "cold"),
            WorkloadMode::Warm => write!(f, "warm"),
            WorkloadMode::Mixed => write!(f, "mixed"),
        }
    }
}

/// Summary statistics for a workload-aware benchmark run.
///
/// Extends [`BenchmarkReport`] with the workload mode and observed cache hit
/// rate, making the scenario under test explicit in every output line.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadReport {
    /// The workload simulation mode used for this run.
    pub workload: WorkloadMode,
    /// Observed cache hit rate during the measured pass (0.0 – 1.0).
    ///
    /// For [`WorkloadMode::Cold`] the cache starts empty before every query,
    /// but legacy manifests may still observe intra-query hits when routing
    /// and scoring touch the same shard. For [`WorkloadMode::Warm`] it will be
    /// close to `1.0` after the warm-up pass. For [`WorkloadMode::Mixed`] it
    /// reflects the aggregate proportion of shard accesses that were served
    /// from cache.
    pub cache_hit_rate: f64,
    /// Core benchmark statistics (flattened into this struct for JSON output).
    #[serde(flatten)]
    pub benchmark: BenchmarkReport,
}

/// Run a workload-aware benchmark that simulates cold, warm, or mixed cache
/// behaviour.
///
/// # Arguments
///
/// * `store`    – Object store holding the index artifacts.
/// * `manifest` – Loaded manifest for the index under test.
/// * `queries`  – Query vectors used for recall and latency measurement.
/// * `corpus`   – Full corpus used to compute exact ground-truth top-k.
/// * `k`        – Number of nearest neighbours to retrieve per query.
/// * `policy`   – Fan-out policy (centroid/shard/vector limits).
/// * `metric`   – Distance metric for ground-truth computation.
/// * `workload` – Workload simulation mode (cold / warm / mixed).
///
/// # Returns
///
/// Returns a zeroed report when `queries` is empty.
#[allow(clippy::too_many_arguments)]
pub fn run_workload_benchmark(
    store: &Arc<dyn ObjectStore>,
    manifest: &Manifest,
    queries: &[VectorRecord],
    corpus: &[VectorRecord],
    k: usize,
    policy: &FanOutPolicy,
    metric: DistanceMetric,
    workload: WorkloadMode,
) -> WorkloadReport {
    let cost_metrics = compute_cost_metrics(store, manifest);

    if queries.is_empty() {
        return WorkloadReport {
            workload,
            cache_hit_rate: 0.0,
            benchmark: BenchmarkReport {
                num_queries: 0,
                k,
                nprobe: policy.candidate_centroids as usize,
                recall_at_k: 0.0,
                mean_latency_us: 0.0,
                p99_latency_us: 0.0,
                throughput_qps: 0.0,
                cost_metrics,
            },
        };
    }

    let (latencies_us, recalls, cache_hit_rate, wall_elapsed_secs) = match workload {
        WorkloadMode::Cold => {
            // Create a fresh IndexSearcher for every query to guarantee an
            // empty shard cache at the start of each request.
            let mut latencies_us: Vec<f64> = Vec::with_capacity(queries.len());
            let mut recalls: Vec<f64> = Vec::with_capacity(queries.len());
            let mut total_hits = 0_u64;
            let mut total_misses = 0_u64;

            let wall_start = Instant::now();
            for query in queries {
                let searcher = IndexSearcher::new(Arc::clone(store), manifest.clone());
                let gt = exact_search(&query.data, corpus, metric, k);
                let gt_ids: Vec<VectorId> = gt.iter().map(|r| r.id).collect();

                let t0 = Instant::now();
                let approx = searcher.search(&query.data, k, policy).unwrap_or_default();
                latencies_us.push(t0.elapsed().as_micros() as f64);

                let approx_ids: Vec<VectorId> = approx.iter().map(|r| r.id).collect();
                recalls.push(recall_at_k(&gt_ids, &approx_ids));
                let (hits, misses) = searcher.cache_access_counts();
                total_hits = total_hits.saturating_add(hits);
                total_misses = total_misses.saturating_add(misses);
            }
            let wall_elapsed_secs = wall_start.elapsed().as_secs_f64();

            (
                latencies_us,
                recalls,
                hit_rate(total_hits, total_misses),
                wall_elapsed_secs,
            )
        }

        WorkloadMode::Warm => {
            // Create a single searcher and run an un-timed warm-up pass so
            // that all accessed shards are resident before measurement begins.
            let searcher = IndexSearcher::new(Arc::clone(store), manifest.clone());

            for query in queries {
                let _ = searcher.search(&query.data, k, policy);
            }

            let (hits_before, misses_before) = searcher.cache_access_counts();

            // Measured pass – shards should now be in the LRU cache.
            let mut latencies_us: Vec<f64> = Vec::with_capacity(queries.len());
            let mut recalls: Vec<f64> = Vec::with_capacity(queries.len());

            let wall_start = Instant::now();
            for query in queries {
                let gt = exact_search(&query.data, corpus, metric, k);
                let gt_ids: Vec<VectorId> = gt.iter().map(|r| r.id).collect();

                let t0 = Instant::now();
                let approx = searcher.search(&query.data, k, policy).unwrap_or_default();
                latencies_us.push(t0.elapsed().as_micros() as f64);

                let approx_ids: Vec<VectorId> = approx.iter().map(|r| r.id).collect();
                recalls.push(recall_at_k(&gt_ids, &approx_ids));
            }
            let wall_elapsed_secs = wall_start.elapsed().as_secs_f64();
            let (hits_after, misses_after) = searcher.cache_access_counts();
            let delta_hits = hits_after.saturating_sub(hits_before);
            let delta_misses = misses_after.saturating_sub(misses_before);
            let cache_hit_rate = hit_rate(delta_hits, delta_misses);

            (latencies_us, recalls, cache_hit_rate, wall_elapsed_secs)
        }

        WorkloadMode::Mixed => {
            // Create a single searcher without any pre-warming.  The cache
            // transitions organically from cold to warm as the run progresses.
            let searcher = IndexSearcher::new(Arc::clone(store), manifest.clone());

            let mut latencies_us: Vec<f64> = Vec::with_capacity(queries.len());
            let mut recalls: Vec<f64> = Vec::with_capacity(queries.len());

            let wall_start = Instant::now();
            for query in queries {
                let gt = exact_search(&query.data, corpus, metric, k);
                let gt_ids: Vec<VectorId> = gt.iter().map(|r| r.id).collect();

                let t0 = Instant::now();
                let approx = searcher.search(&query.data, k, policy).unwrap_or_default();
                latencies_us.push(t0.elapsed().as_micros() as f64);

                let approx_ids: Vec<VectorId> = approx.iter().map(|r| r.id).collect();
                recalls.push(recall_at_k(&gt_ids, &approx_ids));
            }
            let wall_elapsed_secs = wall_start.elapsed().as_secs_f64();
            let (hits, misses) = searcher.cache_access_counts();
            let cache_hit_rate = hit_rate(hits, misses);

            (latencies_us, recalls, cache_hit_rate, wall_elapsed_secs)
        }
    };

    let mut latencies_us = latencies_us;
    let mean_recall = recalls.iter().sum::<f64>() / recalls.len() as f64;
    let mean_latency = latencies_us.iter().sum::<f64>() / latencies_us.len() as f64;
    let p99_latency = nearest_rank_percentile(&mut latencies_us, 0.99);
    let throughput_qps = queries.len() as f64 / wall_elapsed_secs.max(f64::EPSILON);

    let report = WorkloadReport {
        workload,
        cache_hit_rate,
        benchmark: BenchmarkReport {
            num_queries: queries.len(),
            k,
            nprobe: policy.candidate_centroids as usize,
            recall_at_k: mean_recall,
            mean_latency_us: mean_latency,
            p99_latency_us: p99_latency,
            throughput_qps,
            cost_metrics,
        },
    };

    info!(
        workload = %workload,
        recall_at_k = report.benchmark.recall_at_k,
        mean_latency_us = report.benchmark.mean_latency_us,
        p99_latency_us = report.benchmark.p99_latency_us,
        throughput_qps = report.benchmark.throughput_qps,
        cache_hit_rate = report.cache_hit_rate,
        disk_footprint_bytes = report.benchmark.cost_metrics.disk_footprint_bytes,
        memory_usage_bytes = report.benchmark.cost_metrics.memory_usage_bytes,
        compression_ratio = report.benchmark.cost_metrics.compression_ratio,
        "Workload benchmark complete"
    );

    report
}

fn hit_rate(hits: u64, misses: u64) -> f64 {
    let total = hits + misses;
    if total == 0 {
        0.0
    } else {
        hits as f64 / total as f64
    }
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

// ── Hybrid recall evaluation ───────────────────────────────────────────────────

/// Quality and latency metrics for a single retrieval mode (vector-only, BM25-only, or hybrid).
///
/// All recall and precision values are in the range `[0, 1]`.  Latencies are in microseconds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrievalModeMetrics {
    /// Mean recall@k across all evaluated queries (fraction of true top-k found).
    pub recall_at_k: f64,
    /// Mean precision@k across all evaluated queries (fraction of retrieved results that are true top-k).
    pub precision_at_k: f64,
    /// Mean per-query search latency in microseconds.
    pub mean_latency_us: f64,
    /// 99th-percentile per-query search latency in microseconds.
    pub p99_latency_us: f64,
}

/// Evaluation report produced by [`run_eval_hybrid`].
///
/// Compares vector-only, BM25-only, and hybrid retrieval across the same query
/// set using recall@k and precision@k against an exact vector ground-truth, so
/// the improvement (or regression) from adding the lexical signal is immediately
/// visible.
///
/// # Example
///
/// ```rust,ignore
/// use shardlake_bench::run_eval_hybrid;
/// use shardlake_index::ranking::HybridRankingPolicy;
///
/// let policy = HybridRankingPolicy { vector_weight: 0.7, bm25_weight: 0.3 };
/// let report = run_eval_hybrid(
///     &searcher, &bm25, &queries, &query_texts, &corpus, 10, &fan_out_policy,
///     metric, &policy,
/// )?;
/// println!("hybrid recall@10: {:.4}", report.hybrid.recall_at_k);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalHybridReport {
    /// Number of query vectors evaluated.
    pub num_queries: usize,
    /// Number of nearest neighbours retrieved per query.
    pub k: usize,
    /// Number of shards probed per query.
    pub nprobe: usize,
    /// Weight applied to the normalized vector-distance score in hybrid ranking.
    pub vector_weight: f32,
    /// Weight applied to the normalized BM25 score in hybrid ranking.
    pub bm25_weight: f32,
    /// Metrics for vector-only retrieval (ANN search with no lexical signal).
    pub vector_only: RetrievalModeMetrics,
    /// Metrics for BM25-only retrieval (lexical search with no vector signal).
    pub bm25_only: RetrievalModeMetrics,
    /// Metrics for hybrid retrieval (combined vector + BM25 signal).
    pub hybrid: RetrievalModeMetrics,
}

/// Evaluate hybrid retrieval quality versus vector-only and BM25-only baselines.
///
/// For each query the function runs three search modes — vector-only ANN,
/// BM25-only lexical, and weighted hybrid — and measures recall@k and
/// precision@k against an exact brute-force ground truth derived from the full
/// `corpus`.  The three sets of metrics are returned together in an
/// [`EvalHybridReport`] that makes quality differences directly comparable.
///
/// # Arguments
///
/// * `searcher`      – A loaded [`IndexSearcher`] used for vector-only and hybrid ANN.
/// * `bm25`          – A loaded [`Bm25Index`] used for BM25-only and hybrid lexical search.
/// * `queries`       – Query vectors; ids and data are used for ANN and ground-truth.
/// * `query_texts`   – BM25 query strings, one per entry in `queries` (same order).
/// * `corpus`        – Full corpus used to compute exact ground-truth top-k per query.
/// * `k`             – Number of nearest neighbours to retrieve per query.
/// * `policy`        – Query-time fan-out policy (centroid, shard, and per-shard limits).
/// * `metric`        – Distance metric for exact ground-truth computation.
/// * `hybrid_policy` – Weighting policy for blending vector and BM25 scores.
///
/// # Errors
///
/// Returns an error when `queries` is empty or when the ANN search fails.
#[allow(clippy::too_many_arguments)]
pub fn run_eval_hybrid(
    searcher: &IndexSearcher,
    bm25: &Bm25Index,
    queries: &[VectorRecord],
    query_texts: &[String],
    corpus: &[VectorRecord],
    k: usize,
    policy: &FanOutPolicy,
    metric: DistanceMetric,
    hybrid_policy: &HybridRankingPolicy,
) -> IndexResult<EvalHybridReport> {
    if queries.is_empty() {
        return Err(shardlake_index::IndexError::Other(
            "eval-hybrid requires at least one query vector".to_string(),
        ));
    }
    if query_texts.len() != queries.len() {
        return Err(shardlake_index::IndexError::Other(format!(
            "eval-hybrid requires one query text per query vector (got {} query vectors and {} query texts)",
            queries.len(),
            query_texts.len()
        )));
    }

    let nprobe = policy.candidate_centroids as usize;

    let mut vec_recalls: Vec<f64> = Vec::with_capacity(queries.len());
    let mut vec_precisions: Vec<f64> = Vec::with_capacity(queries.len());
    let mut vec_latencies_us: Vec<f64> = Vec::with_capacity(queries.len());

    let mut bm25_recalls: Vec<f64> = Vec::with_capacity(queries.len());
    let mut bm25_precisions: Vec<f64> = Vec::with_capacity(queries.len());
    let mut bm25_latencies_us: Vec<f64> = Vec::with_capacity(queries.len());

    let mut hybrid_recalls: Vec<f64> = Vec::with_capacity(queries.len());
    let mut hybrid_precisions: Vec<f64> = Vec::with_capacity(queries.len());
    let mut hybrid_latencies_us: Vec<f64> = Vec::with_capacity(queries.len());

    for (query, text) in queries.iter().zip(query_texts.iter()) {
        // Exact ground truth for vector-based recall.
        let gt = exact_search(&query.data, corpus, metric, k);
        let gt_ids: Vec<VectorId> = gt.iter().map(|r| r.id).collect();

        // ── Vector-only ──────────────────────────────────────────────────
        let t0 = Instant::now();
        let vec_results = searcher.search(&query.data, k, policy)?;
        vec_latencies_us.push(t0.elapsed().as_micros() as f64);
        let vec_ids: Vec<VectorId> = vec_results.iter().map(|r| r.id).collect();
        vec_recalls.push(recall_at_k(&gt_ids, &vec_ids));
        vec_precisions.push(precision_at_k(&gt_ids, &vec_ids));

        // ── BM25-only ────────────────────────────────────────────────────
        let t0 = Instant::now();
        let bm25_results = bm25.search(text, k);
        bm25_latencies_us.push(t0.elapsed().as_micros() as f64);
        let bm25_ids: Vec<VectorId> = bm25_results.iter().map(|r| r.id).collect();
        bm25_recalls.push(recall_at_k(&gt_ids, &bm25_ids));
        bm25_precisions.push(precision_at_k(&gt_ids, &bm25_ids));

        // ── Hybrid ───────────────────────────────────────────────────────
        // Re-run vector search to get a fresh set of results for blending.
        // We request k results from each signal and let rank_hybrid merge them.
        let t0 = Instant::now();
        let vec_for_hybrid = searcher.search(&query.data, k, policy)?;
        let bm25_for_hybrid = bm25.search(text, k);
        let hybrid_results = rank_hybrid(vec_for_hybrid, bm25_for_hybrid, hybrid_policy, k);
        hybrid_latencies_us.push(t0.elapsed().as_micros() as f64);
        let hybrid_ids: Vec<VectorId> = hybrid_results.iter().map(|r| r.id).collect();
        hybrid_recalls.push(recall_at_k(&gt_ids, &hybrid_ids));
        hybrid_precisions.push(precision_at_k(&gt_ids, &hybrid_ids));
    }

    let mean_f64 = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;

    let vector_only = RetrievalModeMetrics {
        recall_at_k: mean_f64(&vec_recalls),
        precision_at_k: mean_f64(&vec_precisions),
        mean_latency_us: mean_f64(&vec_latencies_us),
        p99_latency_us: nearest_rank_percentile(&mut vec_latencies_us, 0.99),
    };
    let bm25_only = RetrievalModeMetrics {
        recall_at_k: mean_f64(&bm25_recalls),
        precision_at_k: mean_f64(&bm25_precisions),
        mean_latency_us: mean_f64(&bm25_latencies_us),
        p99_latency_us: nearest_rank_percentile(&mut bm25_latencies_us, 0.99),
    };
    let hybrid = RetrievalModeMetrics {
        recall_at_k: mean_f64(&hybrid_recalls),
        precision_at_k: mean_f64(&hybrid_precisions),
        mean_latency_us: mean_f64(&hybrid_latencies_us),
        p99_latency_us: nearest_rank_percentile(&mut hybrid_latencies_us, 0.99),
    };

    let report = EvalHybridReport {
        num_queries: queries.len(),
        k,
        nprobe,
        vector_weight: hybrid_policy.vector_weight,
        bm25_weight: hybrid_policy.bm25_weight,
        vector_only,
        bm25_only,
        hybrid,
    };

    info!(
        num_queries = report.num_queries,
        k = report.k,
        vector_only_recall = report.vector_only.recall_at_k,
        bm25_only_recall = report.bm25_only.recall_at_k,
        hybrid_recall = report.hybrid.recall_at_k,
        "Hybrid evaluation complete"
    );

    Ok(report)
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
    use std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

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

    struct CountingLocalPathStore {
        inner: LocalObjectStore,
        get_calls: AtomicUsize,
    }

    impl CountingLocalPathStore {
        fn new(root: &std::path::Path) -> Self {
            Self {
                inner: LocalObjectStore::new(root).unwrap(),
                get_calls: AtomicUsize::new(0),
            }
        }

        fn get_calls(&self) -> usize {
            self.get_calls.load(Ordering::SeqCst)
        }
    }

    impl ObjectStore for CountingLocalPathStore {
        fn put(&self, key: &str, data: Vec<u8>) -> shardlake_storage::Result<()> {
            self.inner.put(key, data)
        }

        fn get(&self, key: &str) -> shardlake_storage::Result<Vec<u8>> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get(key)
        }

        fn exists(&self, key: &str) -> shardlake_storage::Result<bool> {
            self.inner.exists(key)
        }

        fn list(&self, prefix: &str) -> shardlake_storage::Result<Vec<String>> {
            self.inner.list(prefix)
        }

        fn delete(&self, key: &str) -> shardlake_storage::Result<()> {
            self.inner.delete(key)
        }

        fn local_path_for(&self, key: &str) -> shardlake_storage::Result<Option<PathBuf>> {
            self.inner.local_path_for(key)
        }
    }

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
                ann_family: None,
                hnsw_config: None,
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
                ann_family: None,
                hnsw_config: None,
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

        // Manually sum only this manifest's artifacts.
        let manual_sum: u64 = store_arc
            .list(&format!("indexes/{}/", manifest.index_version.0))
            .unwrap()
            .iter()
            .filter_map(|key| store_arc.get(key).ok())
            .map(|b| b.len() as u64)
            .sum();

        assert_eq!(metrics.disk_footprint_bytes, manual_sum);
    }

    #[test]
    fn cost_metrics_disk_footprint_ignores_other_indexes_in_storage() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());

        let first_manifest = build_test_index(
            store.as_ref(),
            make_records(10, 4),
            1,
            tmp.path().to_path_buf(),
        );
        let _second_manifest = IndexBuilder::new(store.as_ref(), &SystemConfig::default())
            .build(BuildParams {
                records: make_records(10, 4),
                dataset_version: DatasetVersion("ds-other".into()),
                embedding_version: EmbeddingVersion("emb-other".into()),
                index_version: IndexVersion("idx-other".into()),
                metric: DistanceMetric::Euclidean,
                dims: 4,
                vectors_key: shardlake_storage::paths::dataset_vectors_key("ds-other"),
                metadata_key: shardlake_storage::paths::dataset_metadata_key("ds-other"),
                pq_params: None,
                ann_family: None,
                hnsw_config: None,
            })
            .unwrap();

        let store_arc: Arc<dyn ObjectStore> = store;
        let metrics = compute_cost_metrics(&store_arc, &first_manifest);
        let this_index_only: u64 = store_arc
            .list(&format!("indexes/{}/", first_manifest.index_version.0))
            .unwrap()
            .iter()
            .map(|key| store_arc.get(key).unwrap().len() as u64)
            .sum();
        let all_indexes: u64 = store_arc
            .list(shardlake_storage::paths::indexes_prefix())
            .unwrap()
            .iter()
            .map(|key| store_arc.get(key).unwrap().len() as u64)
            .sum();

        assert_eq!(metrics.disk_footprint_bytes, this_index_only);
        assert!(all_indexes > this_index_only);
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

    #[test]
    fn cost_metrics_use_local_paths_without_reading_artifacts() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(CountingLocalPathStore::new(tmp.path()));
        let records = make_records(10, 4);
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 1,
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
        let manifest = IndexBuilder::new(&store.inner, &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-local-path".into()),
                embedding_version: EmbeddingVersion("emb-local-path".into()),
                index_version: IndexVersion("idx-local-path".into()),
                metric: DistanceMetric::Euclidean,
                dims: 4,
                vectors_key: shardlake_storage::paths::dataset_vectors_key("ds-local-path"),
                metadata_key: shardlake_storage::paths::dataset_metadata_key("ds-local-path"),
                pq_params: None,
                ann_family: None,
                hnsw_config: None,
            })
            .unwrap();

        let store_arc: Arc<dyn ObjectStore> = store.clone();
        let metrics = compute_cost_metrics(&store_arc, &manifest);

        assert!(metrics.disk_footprint_bytes > 0);
        assert_eq!(store.get_calls(), 0);
    }

    #[test]
    fn cost_metrics_saturate_on_overflow() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(4, 4);
        let mut manifest = build_test_index(store.as_ref(), records, 1, tmp.path().to_path_buf());
        manifest.total_vector_count = u64::MAX;
        manifest.dims = u32::MAX;

        let store_arc: Arc<dyn ObjectStore> = store;
        let metrics = compute_cost_metrics(&store_arc, &manifest);

        assert_eq!(metrics.memory_usage_bytes, u64::MAX);
        assert_eq!(metrics.compression_ratio, 1.0);
    }

    // ── WorkloadMode / run_workload_benchmark ─────────────────────────────────

    #[test]
    fn workload_mode_display_labels_are_correct() {
        assert_eq!(WorkloadMode::Cold.to_string(), "cold");
        assert_eq!(WorkloadMode::Warm.to_string(), "warm");
        assert_eq!(WorkloadMode::Mixed.to_string(), "mixed");
    }

    #[test]
    fn workload_mode_serialises_to_snake_case() {
        assert_eq!(
            serde_json::to_string(&WorkloadMode::Cold).unwrap(),
            "\"cold\""
        );
        assert_eq!(
            serde_json::to_string(&WorkloadMode::Warm).unwrap(),
            "\"warm\""
        );
        assert_eq!(
            serde_json::to_string(&WorkloadMode::Mixed).unwrap(),
            "\"mixed\""
        );
    }

    #[test]
    fn run_workload_benchmark_empty_queries_returns_zero_metrics_for_all_modes() {
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

        for mode in [WorkloadMode::Cold, WorkloadMode::Warm, WorkloadMode::Mixed] {
            let report = run_workload_benchmark(
                &store_arc,
                &manifest,
                &[],
                &records,
                3,
                &policy,
                DistanceMetric::Euclidean,
                mode,
            );
            assert_eq!(
                report.workload, mode,
                "workload field must match requested mode"
            );
            assert_eq!(report.benchmark.num_queries, 0);
            assert_eq!(report.benchmark.recall_at_k, 0.0);
            assert_eq!(report.benchmark.mean_latency_us, 0.0);
            assert_eq!(report.benchmark.throughput_qps, 0.0);
        }
    }

    #[test]
    fn run_workload_benchmark_cold_mode_starts_with_a_fresh_cache_per_query() {
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

        let report = run_workload_benchmark(
            &store_arc,
            &manifest,
            &queries,
            &records,
            3,
            &policy,
            DistanceMetric::Euclidean,
            WorkloadMode::Cold,
        );

        assert_eq!(report.workload, WorkloadMode::Cold);
        assert_eq!(report.benchmark.num_queries, 5);
        assert!((0.0..=1.0).contains(&report.cache_hit_rate));
        assert!((0.0..=1.0).contains(&report.benchmark.recall_at_k));
        assert!(report.benchmark.throughput_qps > 0.0);
    }

    #[test]
    fn run_workload_benchmark_cold_mode_counts_intra_query_hits_for_legacy_manifests() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(20, 4);
        let mut manifest =
            build_test_index(store.as_ref(), records.clone(), 2, tmp.path().to_path_buf());
        manifest.manifest_version = 1;
        for shard in &mut manifest.shards {
            shard.centroid.clear();
        }

        let queries: Vec<VectorRecord> = records[..5].to_vec();
        let policy = shardlake_core::config::FanOutPolicy {
            candidate_centroids: 1,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let store_arc: Arc<dyn ObjectStore> = store;

        let report = run_workload_benchmark(
            &store_arc,
            &manifest,
            &queries,
            &records,
            3,
            &policy,
            DistanceMetric::Euclidean,
            WorkloadMode::Cold,
        );

        assert_eq!(report.workload, WorkloadMode::Cold);
        assert!(
            report.cache_hit_rate > 0.0,
            "legacy manifests should count intra-query cache hits in cold mode"
        );
        assert!(report.cache_hit_rate < 1.0);
    }

    #[test]
    fn run_workload_benchmark_warm_mode_yields_high_cache_hit_rate() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(20, 4);
        let manifest =
            build_test_index(store.as_ref(), records.clone(), 2, tmp.path().to_path_buf());

        let queries: Vec<VectorRecord> = records[..5].to_vec();
        // nprobe covers both shards so the warm-up will load everything.
        let policy = shardlake_core::config::FanOutPolicy {
            candidate_centroids: 2,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let store_arc: Arc<dyn ObjectStore> = store;

        let report = run_workload_benchmark(
            &store_arc,
            &manifest,
            &queries,
            &records,
            3,
            &policy,
            DistanceMetric::Euclidean,
            WorkloadMode::Warm,
        );

        assert_eq!(report.workload, WorkloadMode::Warm);
        // After a full warm-up pass all shard loads in the measured pass should
        // be cache hits, so the measured-pass hit rate should be effectively 1.0.
        assert!(
            report.cache_hit_rate > 0.99,
            "warm workload should report measured-pass hits only, got {}",
            report.cache_hit_rate
        );
        assert_eq!(report.benchmark.num_queries, 5);
        assert!(report.benchmark.throughput_qps > 0.0);
    }

    #[test]
    fn run_workload_benchmark_mixed_mode_runs_without_errors() {
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

        let report = run_workload_benchmark(
            &store_arc,
            &manifest,
            &queries,
            &records,
            3,
            &policy,
            DistanceMetric::Euclidean,
            WorkloadMode::Mixed,
        );

        assert_eq!(report.workload, WorkloadMode::Mixed);
        assert_eq!(report.benchmark.num_queries, 5);
        assert!((0.0..=1.0).contains(&report.benchmark.recall_at_k));
        assert!((0.0..=1.0).contains(&report.cache_hit_rate));
        assert!(report.benchmark.throughput_qps > 0.0);
    }

    #[test]
    fn run_workload_benchmark_report_field_workload_matches_requested_mode() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(20, 4);
        let manifest =
            build_test_index(store.as_ref(), records.clone(), 2, tmp.path().to_path_buf());

        let queries: Vec<VectorRecord> = records[..3].to_vec();
        let policy = shardlake_core::config::FanOutPolicy {
            candidate_centroids: 1,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let store_arc: Arc<dyn ObjectStore> = store;

        for mode in [WorkloadMode::Cold, WorkloadMode::Warm, WorkloadMode::Mixed] {
            let report = run_workload_benchmark(
                &store_arc,
                &manifest,
                &queries,
                &records,
                3,
                &policy,
                DistanceMetric::Euclidean,
                mode,
            );
            assert_eq!(
                report.workload, mode,
                "WorkloadReport.workload must equal the requested mode"
            );
        }
    }

    // ── CompareAnnReport / AnnFamilyReport ────────────────────────────────────

    #[test]
    fn ann_family_report_flattens_eval_fields_in_json() {
        let eval = EvalAnnReport {
            num_queries: 10,
            k: 5,
            nprobe: 2,
            recall_at_k: 0.9,
            precision_at_k: 0.85,
            mean_latency_us: 50.0,
            p99_latency_us: 200.0,
        };
        let entry = AnnFamilyReport {
            alias: "hnsw-idx".into(),
            ann_family: "hnsw".into(),
            eval,
        };
        let json = serde_json::to_value(&entry).unwrap();
        // Flattened: eval fields appear at the top level.
        assert_eq!(json["alias"], "hnsw-idx");
        assert_eq!(json["ann_family"], "hnsw");
        assert_eq!(json["num_queries"], 10);
        assert_eq!(json["recall_at_k"], 0.9);
        assert_eq!(json["precision_at_k"], 0.85);
    }

    #[test]
    fn compare_ann_report_serialises_all_entries() {
        let make_entry = |alias: &str, family: &str, recall: f64| AnnFamilyReport {
            alias: alias.into(),
            ann_family: family.into(),
            eval: EvalAnnReport {
                num_queries: 5,
                k: 3,
                nprobe: 1,
                recall_at_k: recall,
                precision_at_k: recall,
                mean_latency_us: 10.0,
                p99_latency_us: 30.0,
            },
        };

        let report = CompareAnnReport {
            entries: vec![
                make_entry("ivf-idx", "ivf_flat", 0.8),
                make_entry("hnsw-idx", "hnsw", 0.95),
                make_entry("da-idx", "diskann", 0.88),
            ],
        };

        let json = serde_json::to_value(&report).unwrap();
        let entries = json["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0]["ann_family"], "ivf_flat");
        assert_eq!(entries[1]["ann_family"], "hnsw");
        assert_eq!(entries[2]["ann_family"], "diskann");
        assert_eq!(entries[1]["recall_at_k"], 0.95);
    }

    #[test]
    fn compare_ann_report_empty_entries_round_trips() {
        let report = CompareAnnReport { entries: vec![] };
        let json = serde_json::to_string(&report).unwrap();
        let decoded: CompareAnnReport = serde_json::from_str(&json).unwrap();
        assert!(decoded.entries.is_empty());
    }

    // ── run_eval_hybrid tests ─────────────────────────────────────────────────

    /// Build a [`Bm25Index`] from a slice of [`VectorRecord`] by serialising
    /// each record's metadata (or using an empty string when absent).
    fn build_bm25_from_records(records: &[VectorRecord]) -> Bm25Index {
        use shardlake_index::bm25::BM25Params;
        let docs: Vec<(shardlake_core::types::VectorId, String)> = records
            .iter()
            .map(|r| {
                let text = metadata_to_text(&r.metadata);
                (r.id, text)
            })
            .collect();
        let doc_refs: Vec<(shardlake_core::types::VectorId, &str)> =
            docs.iter().map(|(id, text)| (*id, text.as_str())).collect();
        Bm25Index::build(&doc_refs, BM25Params::default())
    }

    #[test]
    fn run_eval_hybrid_returns_error_for_empty_queries() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(10, 4);
        let manifest =
            build_test_index(store.as_ref(), records.clone(), 1, tmp.path().to_path_buf());
        let searcher = IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest);
        let bm25 = build_bm25_from_records(&records);
        let policy = shardlake_core::config::FanOutPolicy {
            candidate_centroids: 1,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let hybrid_policy = HybridRankingPolicy {
            vector_weight: 0.7,
            bm25_weight: 0.3,
        };

        let result = run_eval_hybrid(
            &searcher,
            &bm25,
            &[],
            &[],
            &records,
            3,
            &policy,
            DistanceMetric::Euclidean,
            &hybrid_policy,
        );
        assert!(result.is_err(), "empty queries must return an error");
    }

    #[test]
    fn run_eval_hybrid_reports_correct_field_counts() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(20, 4);
        let manifest =
            build_test_index(store.as_ref(), records.clone(), 2, tmp.path().to_path_buf());
        let searcher = IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest);

        let bm25 = build_bm25_from_records(&records);
        let queries: Vec<VectorRecord> = records[..5].to_vec();
        let query_texts: Vec<String> = queries
            .iter()
            .map(|r| metadata_to_text(&r.metadata))
            .collect();

        let policy = shardlake_core::config::FanOutPolicy {
            candidate_centroids: 1,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let hybrid_policy = HybridRankingPolicy {
            vector_weight: 0.7,
            bm25_weight: 0.3,
        };

        let report = run_eval_hybrid(
            &searcher,
            &bm25,
            &queries,
            &query_texts,
            &records,
            3,
            &policy,
            DistanceMetric::Euclidean,
            &hybrid_policy,
        )
        .unwrap();

        assert_eq!(report.num_queries, 5);
        assert_eq!(report.k, 3);
        assert_eq!(report.nprobe, 1);
        assert!((report.vector_weight - 0.7).abs() < 1e-6);
        assert!((report.bm25_weight - 0.3).abs() < 1e-6);

        for metrics in [&report.vector_only, &report.bm25_only, &report.hybrid] {
            assert!((0.0..=1.0).contains(&metrics.recall_at_k));
            assert!((0.0..=1.0).contains(&metrics.precision_at_k));
            assert!(metrics.mean_latency_us >= 0.0);
            assert!(metrics.p99_latency_us >= 0.0);
        }
    }

    #[test]
    fn run_eval_hybrid_returns_error_for_mismatched_query_text_count() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(10, 4);
        let manifest =
            build_test_index(store.as_ref(), records.clone(), 2, tmp.path().to_path_buf());
        let searcher = IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest);
        let bm25 = build_bm25_from_records(&records);
        let queries: Vec<VectorRecord> = records[..3].to_vec();
        let query_texts = vec![metadata_to_text(&queries[0].metadata)];

        let policy = shardlake_core::config::FanOutPolicy {
            candidate_centroids: 1,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let hybrid_policy = HybridRankingPolicy {
            vector_weight: 0.7,
            bm25_weight: 0.3,
        };

        let result = run_eval_hybrid(
            &searcher,
            &bm25,
            &queries,
            &query_texts,
            &records,
            3,
            &policy,
            DistanceMetric::Euclidean,
            &hybrid_policy,
        );

        assert!(
            result.is_err(),
            "mismatched query text count must return an error"
        );
    }

    #[test]
    fn run_eval_hybrid_vector_recall_perfect_with_full_probe() {
        // With nprobe covering all shards, vector-only recall should be 1.0.
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let records = make_records(20, 4);
        let manifest =
            build_test_index(store.as_ref(), records.clone(), 2, tmp.path().to_path_buf());
        let searcher = IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest);

        let bm25 = build_bm25_from_records(&records);
        let queries: Vec<VectorRecord> = records[..5].to_vec();
        let query_texts: Vec<String> = queries
            .iter()
            .map(|r| metadata_to_text(&r.metadata))
            .collect();

        // candidate_centroids=2 covers both shards.
        let policy = shardlake_core::config::FanOutPolicy {
            candidate_centroids: 2,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let hybrid_policy = HybridRankingPolicy {
            vector_weight: 1.0,
            bm25_weight: 0.0,
        };

        let report = run_eval_hybrid(
            &searcher,
            &bm25,
            &queries,
            &query_texts,
            &records,
            3,
            &policy,
            DistanceMetric::Euclidean,
            &hybrid_policy,
        )
        .unwrap();

        assert!(
            (report.vector_only.recall_at_k - 1.0).abs() < 1e-9,
            "expected perfect vector recall, got {}",
            report.vector_only.recall_at_k
        );
    }
}
