//! `shardlake eval-ann` – ANN quality evaluation with recall@k, precision@k, and latency.

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

/// Output format for the evaluation report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable text table (default).
    Text,
    /// Machine-readable JSON object, suitable for regression tracking.
    Json,
}

#[derive(Parser, Debug)]
pub struct EvalAnnArgs {
    /// Alias to evaluate (default: "latest").
    #[arg(long, default_value = "latest")]
    pub alias: String,
    /// Number of top results to retrieve per query.
    #[arg(long, default_value_t = 10)]
    pub k: usize,
    /// Number of shards to probe per query.
    #[arg(long, default_value_t = 2)]
    pub nprobe: usize,
    /// Maximum number of query vectors to evaluate (0 = min(corpus size, 100)).
    #[arg(long, default_value_t = 0)]
    pub max_queries: usize,
    /// Output format: `text` (default) or `json`.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub output: OutputFormat,
}

pub async fn run(storage: PathBuf, args: EvalAnnArgs) -> Result<()> {
    anyhow::ensure!(args.k >= 1, "--k must be at least 1");
    anyhow::ensure!(args.nprobe >= 1, "--nprobe must be at least 1");

    let policy = FanOutPolicy {
        candidate_centroids: args.nprobe as u32,
        candidate_shards: 0,
        max_vectors_per_shard: 0,
    };
    policy.validate().map_err(|err| anyhow::anyhow!(err))?;

    let store = Arc::new(LocalObjectStore::new(&storage)?);
    let manifest = Manifest::load_alias(&*store, &args.alias)?;
    let metric = manifest.distance_metric;
    info!(index_version = %manifest.index_version, "Loaded manifest for eval-ann");

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
        "Running ANN evaluation"
    );

    let searcher = IndexSearcher::new(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest);

    let report =
        shardlake_bench::run_eval_ann(&searcher, &queries, &corpus, args.k, &policy, metric)?;

    match args.output {
        OutputFormat::Text => {
            println!("=== ANN Evaluation Report ===");
            println!("  Queries:           {}", report.num_queries);
            println!("  k:                 {}", report.k);
            println!("  nprobe:            {}", report.nprobe);
            println!(
                "  Recall@{k}:         {:.4}",
                report.recall_at_k,
                k = report.k
            );
            println!(
                "  Precision@{k}:      {:.4}",
                report.precision_at_k,
                k = report.k
            );
            println!("  Mean latency:      {:.1} µs", report.mean_latency_us);
            println!("  P99  latency:      {:.1} µs", report.p99_latency_us);
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }

    Ok(())
}
