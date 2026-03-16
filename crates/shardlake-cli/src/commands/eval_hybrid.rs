//! `shardlake eval-hybrid` – hybrid retrieval quality evaluation.
//!
//! Compares vector-only, BM25-only, and hybrid retrieval modes against the
//! same exact ground truth, making recall and precision differences between
//! the three strategies directly visible.

use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{bail, Result};
use clap::{Parser, ValueEnum};
use tracing::info;

use shardlake_core::{config::FanOutPolicy, types::VectorRecord};
use shardlake_index::{ranking::HybridRankingPolicy, Bm25Index, IndexSearcher};
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

/// Arguments for the `eval-hybrid` subcommand.
#[derive(Parser, Debug)]
pub struct EvalHybridArgs {
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
    /// Weight applied to the normalized vector-distance score in hybrid ranking.
    ///
    /// Must be non-negative; at least one of `--vector-weight` and
    /// `--bm25-weight` must be positive.
    #[arg(long, default_value_t = 0.7)]
    pub vector_weight: f32,
    /// Weight applied to the normalized BM25 lexical score in hybrid ranking.
    ///
    /// Must be non-negative; at least one of `--vector-weight` and
    /// `--bm25-weight` must be positive.
    #[arg(long, default_value_t = 0.3)]
    pub bm25_weight: f32,
    /// Output format: `text` (default) or `json`.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub output: OutputFormat,
}

pub async fn run(storage: PathBuf, args: EvalHybridArgs) -> Result<()> {
    anyhow::ensure!(args.k >= 1, "--k must be at least 1");
    anyhow::ensure!(args.nprobe >= 1, "--nprobe must be at least 1");
    anyhow::ensure!(
        args.vector_weight >= 0.0,
        "--vector-weight must be non-negative"
    );
    anyhow::ensure!(
        args.bm25_weight >= 0.0,
        "--bm25-weight must be non-negative"
    );
    anyhow::ensure!(
        args.vector_weight > 0.0 || args.bm25_weight > 0.0,
        "at least one of --vector-weight and --bm25-weight must be positive"
    );

    let hybrid_policy = HybridRankingPolicy {
        vector_weight: args.vector_weight,
        bm25_weight: args.bm25_weight,
    };
    hybrid_policy
        .validate()
        .map_err(|err| anyhow::anyhow!(err))?;

    let policy = FanOutPolicy {
        candidate_centroids: args.nprobe as u32,
        candidate_shards: 0,
        max_vectors_per_shard: 0,
    };
    policy.validate().map_err(|err| anyhow::anyhow!(err))?;

    let store = Arc::new(LocalObjectStore::new(&storage)?);
    let manifest = Manifest::load_alias(&*store, &args.alias)?;

    let lexical_cfg = match &manifest.lexical {
        Some(cfg) => cfg.clone(),
        None => bail!(
            "alias '{}' resolves to an index manifest without a lexical (BM25) artifact. \
             `eval-hybrid` requires `manifest.lexical` to be populated, and the current \
             `build-index` CLI does not create that artifact yet.",
            args.alias
        ),
    };

    let metric = manifest.distance_metric;
    info!(
        index_version = %manifest.index_version,
        bm25_artifact = %lexical_cfg.artifact_key,
        "Loaded manifest for eval-hybrid"
    );

    // Load the BM25 index.
    let bm25 = Bm25Index::load(&*store, &lexical_cfg.artifact_key)?;

    // Load the corpus from the vectors artifact.
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

    // Extract BM25 query text from each query record's metadata.
    let query_texts: Vec<String> = queries
        .iter()
        .map(|r| shardlake_bench::metadata_to_text(&r.metadata))
        .collect();

    info!(
        n_queries = queries.len(),
        k = args.k,
        nprobe = args.nprobe,
        vector_weight = args.vector_weight,
        bm25_weight = args.bm25_weight,
        "Running hybrid evaluation"
    );

    let searcher = IndexSearcher::new(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest);

    let report = shardlake_bench::run_eval_hybrid(
        &searcher,
        &bm25,
        &queries,
        &query_texts,
        &corpus,
        args.k,
        &policy,
        metric,
        &hybrid_policy,
    )?;

    match args.output {
        OutputFormat::Text => {
            println!("=== Hybrid Retrieval Evaluation Report ===");
            println!("  Queries:           {}", report.num_queries);
            println!("  k:                 {}", report.k);
            println!("  nprobe:            {}", report.nprobe);
            println!("  vector_weight:     {:.2}", report.vector_weight);
            println!("  bm25_weight:       {:.2}", report.bm25_weight);
            println!();
            println!(
                "  Mode           Recall@{k:<3} Precision@{k:<3} Mean latency  P99 latency",
                k = report.k
            );
            println!("  {:-<73}", "");
            let print_mode = |name: &str, m: &shardlake_bench::RetrievalModeMetrics| {
                println!(
                    "  {:<12}   {:.4}      {:.4}        {:>10.1} µs  {:>10.1} µs",
                    name, m.recall_at_k, m.precision_at_k, m.mean_latency_us, m.p99_latency_us,
                );
            };
            print_mode("vector-only", &report.vector_only);
            print_mode("bm25-only", &report.bm25_only);
            print_mode("hybrid", &report.hybrid);

            // Print delta summary.
            let vec_r = report.vector_only.recall_at_k;
            let bm25_r = report.bm25_only.recall_at_k;
            let hybrid_r = report.hybrid.recall_at_k;
            println!();
            println!(
                "  Recall delta (hybrid vs vector-only): {:+.4}",
                hybrid_r - vec_r
            );
            println!(
                "  Recall delta (hybrid vs bm25-only):   {:+.4}",
                hybrid_r - bm25_r
            );
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }

    Ok(())
}
