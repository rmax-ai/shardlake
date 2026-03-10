//! `shardlake eval-ann` – evaluate ANN quality: recall@k, precision@k, and latency.
//!
//! Compares approximate search results against exact brute-force ground truth.
//! When the index was built with PQ (`--pq-m > 0`), the IVF-PQ search path is used;
//! otherwise the standard IVF + exact shard search is used as the approximate method.

use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use anyhow::Result;
use clap::Parser;
use tracing::info;

use shardlake_core::types::{VectorId, VectorRecord};
use shardlake_index::{
    exact::{exact_search, precision_at_k, recall_at_k},
    ivf_pq::IvfPqIndex,
    IndexSearcher,
};
use shardlake_manifest::Manifest;
use shardlake_storage::{LocalObjectStore, ObjectStore};

#[derive(Parser, Debug)]
pub struct EvalAnnArgs {
    /// Alias to evaluate.
    #[arg(long, default_value = "latest")]
    pub alias: String,
    /// Number of nearest neighbours to retrieve.
    #[arg(long, default_value_t = 10)]
    pub k: usize,
    /// Number of clusters / shards to probe.
    #[arg(long, default_value_t = 2)]
    pub nprobe: usize,
    /// Maximum number of query vectors to use (0 = min(corpus size, 100)).
    #[arg(long, default_value_t = 0)]
    pub max_queries: usize,
    /// Enable exact reranking of IVF-PQ candidates (only relevant when a PQ index exists).
    #[arg(long, default_value_t = false)]
    pub rerank: bool,
}

pub async fn run(storage: PathBuf, args: EvalAnnArgs) -> Result<()> {
    let store = Arc::new(LocalObjectStore::new(&storage)?);
    let manifest = Manifest::load_alias(&*store, &args.alias)?;
    let metric = manifest.distance_metric;
    info!(index_version = %manifest.index_version, "Loaded manifest for eval-ann");

    // Load the full corpus (used both as queries and for exact ground truth).
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
    let queries = &corpus[..limit];

    info!(
        n_queries = queries.len(),
        k = args.k,
        nprobe = args.nprobe,
        "Starting eval-ann"
    );

    // Determine which search path to use and load the appropriate index.
    let (index_type, pq_idx) = if let Some(pq_key) = manifest.pq_artifact_key.as_deref() {
        let pq_bytes = store.get(pq_key)?;
        let pq_idx = IvfPqIndex::from_bytes(&pq_bytes)?;
        let desc = format!(
            "IVF+PQ (m={}, ksub={}, rerank={})",
            pq_idx.pq.m, pq_idx.pq.k_sub, args.rerank
        );
        info!(%desc, "Using IVF-PQ search path");
        (desc, Some(pq_idx))
    } else {
        info!("Using IVF+Exact search path (no PQ artifact found)");
        ("IVF+Exact".to_string(), None)
    };

    let mut latencies_us: Vec<f64> = Vec::with_capacity(queries.len());
    let mut recalls: Vec<f64> = Vec::with_capacity(queries.len());
    let mut precisions: Vec<f64> = Vec::with_capacity(queries.len());

    if let Some(ref ivf_pq) = pq_idx {
        // IVF-PQ search path.
        for query in queries {
            let gt = exact_search(&query.data, &corpus, metric, args.k);
            let gt_ids: Vec<VectorId> = gt.iter().map(|r| r.id).collect();

            let t0 = Instant::now();
            let approx = ivf_pq.search(&query.data, args.k, args.nprobe, args.rerank);
            let elapsed_us = t0.elapsed().as_micros() as f64;

            let approx_ids: Vec<VectorId> = approx.iter().map(|r| r.id).collect();
            recalls.push(recall_at_k(&gt_ids, &approx_ids));
            precisions.push(precision_at_k(&gt_ids, &approx_ids));
            latencies_us.push(elapsed_us);
        }
    } else {
        // IVF+Exact (standard shard) search path.
        let searcher = IndexSearcher::new(
            Arc::clone(&store) as Arc<dyn shardlake_storage::ObjectStore>,
            manifest,
        );
        for query in queries {
            let gt = exact_search(&query.data, &corpus, metric, args.k);
            let gt_ids: Vec<VectorId> = gt.iter().map(|r| r.id).collect();

            let t0 = Instant::now();
            let approx = searcher.search(&query.data, args.k, args.nprobe)?;
            let elapsed_us = t0.elapsed().as_micros() as f64;

            let approx_ids: Vec<VectorId> = approx.iter().map(|r| r.id).collect();
            recalls.push(recall_at_k(&gt_ids, &approx_ids));
            precisions.push(precision_at_k(&gt_ids, &approx_ids));
            latencies_us.push(elapsed_us);
        }
    }

    let mean_recall = recalls.iter().sum::<f64>() / recalls.len() as f64;
    let mean_precision = precisions.iter().sum::<f64>() / precisions.len() as f64;
    let mean_latency = latencies_us.iter().sum::<f64>() / latencies_us.len() as f64;
    latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p99_idx = ((latencies_us.len() as f64 * 0.99) as usize).min(latencies_us.len() - 1);
    let p99_latency = latencies_us.get(p99_idx).copied().unwrap_or(0.0);

    println!("=== ANN Evaluation Report ===");
    println!("  Index type:        {index_type}");
    println!("  Queries:           {}", queries.len());
    println!("  k:                 {}", args.k);
    println!("  nprobe:            {}", args.nprobe);
    println!("  --- Quality ---");
    println!("  Recall@{k}:         {mean_recall:.4}", k = args.k);
    println!("  Precision@{k}:      {mean_precision:.4}", k = args.k);
    println!("  --- Latency ---");
    println!("  Mean:              {mean_latency:.1} µs");
    println!("  P99:               {p99_latency:.1} µs");

    Ok(())
}
