//! `shardlake benchmark` – recall@k, latency, throughput, and cost report.

use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Result;
use clap::Parser;
use tracing::info;

use shardlake_bench::WorkloadKind;
use shardlake_core::types::VectorRecord;
use shardlake_index::IndexSearcher;
use shardlake_manifest::Manifest;
use shardlake_storage::{LocalObjectStore, ObjectStore};

#[derive(Parser, Debug)]
pub struct BenchmarkArgs {
    /// Alias to benchmark (default: "latest").
    #[arg(long, default_value = "latest")]
    pub alias: String,
    /// Number of top results to retrieve.
    #[arg(long, default_value_t = 10)]
    pub k: usize,
    /// Number of shards to probe.
    #[arg(long, default_value_t = 2)]
    pub nprobe: usize,
    /// Maximum number of query vectors to use (0 = up to 100).
    #[arg(long, default_value_t = 0)]
    pub max_queries: usize,
    /// Query workload: cold (no cache), warm (pre-warmed cache), or mixed (alternating).
    #[arg(long, default_value = "warm")]
    pub workload: WorkloadKind,
}

pub async fn run(storage: PathBuf, args: BenchmarkArgs) -> Result<()> {
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
        nprobe = args.nprobe,
        workload = %args.workload,
        "Running benchmark"
    );

    let searcher = IndexSearcher::new(
        Arc::clone(&store) as Arc<dyn shardlake_storage::ObjectStore>,
        manifest,
    );
    let store_arc: Arc<dyn shardlake_storage::ObjectStore> = store;
    let report = shardlake_bench::run_benchmark(
        &searcher,
        &store_arc,
        &queries,
        &corpus,
        args.k,
        args.nprobe,
        metric,
        args.workload,
    );

    println!("=== Benchmark Report ===");
    println!("  Workload:          {}", report.workload);
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
    println!("  Throughput:        {:.1} QPS", report.throughput_qps);
    println!();
    println!("=== Cost Estimates ===");
    println!("  Index size:        {} bytes", report.artifact_size_bytes);
    println!(
        "  Raw vectors size:  {} bytes",
        report.raw_vector_size_bytes
    );
    println!("  Memory (est.):     {} bytes", report.memory_bytes);
    println!("  Compression ratio: {:.3}x", report.compression_ratio);

    Ok(())
}
