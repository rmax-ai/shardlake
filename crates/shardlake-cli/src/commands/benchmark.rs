//! `shardlake benchmark` – recall@k and latency report.

use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Result;
use clap::Parser;
use tracing::info;

use shardlake_core::types::{QueryMode, VectorRecord};
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
    /// Query mode to benchmark: `vector`, `lexical`, or `hybrid`.
    /// When `hybrid` or `lexical`, the server must have been started
    /// with `--enable-bm25` (or the benchmark corpus must have text metadata).
    #[arg(long, default_value = "vector")]
    pub mode: QueryMode,
    /// Hybrid blending weight (0.0–1.0). 1.0 = pure vector, 0.0 = pure
    /// lexical. Only used when `--mode hybrid`.
    #[arg(long, default_value_t = 0.5)]
    pub alpha: f32,
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
        mode = %args.mode,
        "Running benchmark"
    );

    // Build searcher with BM25 when needed for lexical/hybrid modes.
    let store_arc: Arc<dyn shardlake_storage::ObjectStore> = store;
    let searcher = match args.mode {
        QueryMode::Vector => IndexSearcher::new(Arc::clone(&store_arc), manifest),
        QueryMode::Lexical | QueryMode::Hybrid => {
            IndexSearcher::with_corpus(Arc::clone(&store_arc), manifest, &corpus)
        }
    };

    let report = shardlake_bench::run_benchmark_mode(
        &searcher,
        &store_arc,
        &queries,
        &corpus,
        &shardlake_bench::BenchmarkConfig {
            k: args.k,
            nprobe: args.nprobe,
            metric,
            mode: args.mode,
            alpha: args.alpha,
        },
    );

    println!("=== Benchmark Report ===");
    println!("  Queries:           {}", report.num_queries);
    println!("  k:                 {}", report.k);
    println!("  nprobe:            {}", report.nprobe);
    println!("  Mode:              {}", report.mode);
    if matches!(args.mode, QueryMode::Hybrid) {
        println!("  Alpha:             {:.2}", args.alpha);
    }
    println!(
        "  Recall@{k}:         {:.4}",
        report.recall_at_k,
        k = report.k
    );
    println!("  Mean latency:      {:.1} µs", report.mean_latency_us);
    println!("  P99  latency:      {:.1} µs", report.p99_latency_us);
    println!("  Artifact size:     {} bytes", report.artifact_size_bytes);

    Ok(())
}
