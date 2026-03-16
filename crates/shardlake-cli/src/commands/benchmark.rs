//! `shardlake benchmark` – recall@k, throughput, and latency report.

use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Result;
use clap::{Parser, ValueEnum};
use tracing::info;

use shardlake_core::{config::FanOutPolicy, types::VectorRecord};
use shardlake_index::IndexSearcher;
use shardlake_manifest::Manifest;
use shardlake_storage::{LocalObjectStore, ObjectStore};

/// Output format for the benchmark report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable text table (default).
    Text,
    /// Machine-readable JSON object, suitable for regression tracking.
    Json,
}

#[derive(Parser, Debug)]
pub struct BenchmarkArgs {
    /// Alias to benchmark (default: "latest").
    #[arg(long, default_value = "latest")]
    pub alias: String,
    /// Number of top results to retrieve.
    #[arg(long, default_value_t = 10)]
    pub k: usize,
    /// Number of nearest centroids to select per query (candidate_centroids).
    #[arg(long, default_value_t = 2)]
    pub nprobe: u32,
    /// Maximum number of shards to probe after centroid-to-shard deduplication.
    /// `0` means no cap.
    #[arg(long, default_value_t = 0)]
    pub candidate_shards: u32,
    /// Maximum number of vectors to evaluate per probed shard.
    /// `0` means no limit.
    #[arg(long, default_value_t = 0)]
    pub max_vectors_per_shard: u32,
    /// Maximum number of query vectors to use (0 = up to 100).
    #[arg(long, default_value_t = 0)]
    pub max_queries: usize,
    /// Output format: `text` (default) or `json`.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub output: OutputFormat,
}

pub async fn run(storage: PathBuf, args: BenchmarkArgs) -> Result<()> {
    let policy = FanOutPolicy {
        candidate_centroids: args.nprobe,
        candidate_shards: args.candidate_shards,
        max_vectors_per_shard: args.max_vectors_per_shard,
    };
    policy.validate().map_err(|e| anyhow::anyhow!("{}", e))?;

    let store = Arc::new(LocalObjectStore::new(&storage)?);
    let manifest = Manifest::load_alias(&*store, &args.alias)?;
    let metric = manifest.distance_metric;
    info!(index_version = %manifest.index_version, "Loaded manifest for benchmark");

    let vecs_bytes = store.get(&manifest.vectors_key)?;
    let reader = BufReader::new(vecs_bytes.as_slice());
    let mut corpus: Vec<VectorRecord> = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        corpus.push(serde_json::from_str(&line)?);
    }

    let limit = if args.max_queries == 0 {
        corpus.len().min(100)
    } else {
        args.max_queries.min(corpus.len())
    };
    let queries: Vec<VectorRecord> = corpus[..limit].to_vec();
    info!(
        n_queries = queries.len(),
        k = args.k,
        candidate_centroids = policy.candidate_centroids,
        candidate_shards = policy.candidate_shards,
        max_vectors_per_shard = policy.max_vectors_per_shard,
        "Running benchmark"
    );

    let searcher = IndexSearcher::new(
        Arc::clone(&store) as Arc<dyn shardlake_storage::ObjectStore>,
        manifest,
    );
    let store_arc: Arc<dyn shardlake_storage::ObjectStore> = store;
    let report = shardlake_bench::run_benchmark(
        &searcher, &store_arc, &queries, &corpus, args.k, &policy, metric,
    );

    match args.output {
        OutputFormat::Text => {
            println!("=== Benchmark Report ===");
            println!("  Queries:           {}", report.num_queries);
            println!("  k:                 {}", report.k);
            println!("  nprobe:            {}", report.nprobe);
            println!(
                "  Recall@{k}:         {:.4}",
                report.recall_at_k,
                k = report.k
            );
            println!("  Mean latency:      {:.1} µs", report.mean_latency_us);
            println!("  P99  latency:      {:.1} µs", report.p99_latency_us);
            println!("  Throughput:        {:.1} qps", report.throughput_qps);
            println!("  Artifact size:     {} bytes", report.artifact_size_bytes);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }

    Ok(())
}
