//! `shardlake evaluate-partitioning` – partition quality evaluation harness.
//!
//! Reports:
//! - **Shard size distribution** – min, max, mean, and standard deviation of
//!   per-shard vector counts.
//! - **Routing accuracy** – the fraction of corpus vectors whose nearest
//!   centroid matches the shard they were actually assigned to.
//! - **Recall impact** – Recall\@k swept across `nprobe = 1 … num_shards`,
//!   showing how probe depth trades off against search quality.
//! - **Shard hotness** – per-shard hit counts when routing query vectors with
//!   the configured `nprobe`.

use std::{
    collections::HashMap,
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Result;
use clap::Parser;
use tracing::info;

use shardlake_core::types::{VectorId, VectorRecord};
use shardlake_index::{
    exact::{exact_search, recall_at_k},
    kmeans::top_n_centroids,
    IndexSearcher,
};
use shardlake_manifest::Manifest;
use shardlake_storage::{LocalObjectStore, ObjectStore};

#[derive(Parser, Debug)]
pub struct EvaluatePartitioningArgs {
    /// Alias to evaluate (default: "latest").
    #[arg(long, default_value = "latest")]
    pub alias: String,
    /// Number of nearest neighbours for recall-impact evaluation.
    #[arg(long, default_value_t = 10)]
    pub k: usize,
    /// Number of shards to probe for routing-accuracy and hotness analysis.
    #[arg(long, default_value_t = 2)]
    pub nprobe: usize,
    /// Maximum query vectors to use (0 = up to 100).
    #[arg(long, default_value_t = 0)]
    pub max_queries: usize,
}

pub async fn run(storage: PathBuf, args: EvaluatePartitioningArgs) -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(&storage)?);
    let manifest = Manifest::load_alias(&*store, &args.alias)?;
    let metric = manifest.distance_metric;
    info!(
        index_version = %manifest.index_version,
        shards = manifest.shards.len(),
        "Loaded manifest for partition evaluation"
    );

    // ── Load corpus ──────────────────────────────────────────────────────────
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

    let query_limit = if args.max_queries == 0 {
        corpus.len().min(100)
    } else {
        args.max_queries.min(corpus.len())
    };
    let queries = &corpus[..query_limit];
    info!(
        corpus = corpus.len(),
        queries = queries.len(),
        "Corpus and query set ready"
    );

    // ── 1. Shard size distribution ───────────────────────────────────────────
    let counts: Vec<u64> = manifest.shards.iter().map(|s| s.vector_count).collect();
    let n_shards = counts.len();
    let min_count = counts.iter().copied().min().unwrap_or(0);
    let max_count = counts.iter().copied().max().unwrap_or(0);
    let mean_count = counts.iter().sum::<u64>() as f64 / n_shards as f64;
    let stddev_count = {
        let variance = counts
            .iter()
            .map(|&c| {
                let d = c as f64 - mean_count;
                d * d
            })
            .sum::<f64>()
            / n_shards as f64;
        variance.sqrt()
    };

    // ── 2. Routing accuracy ───────────────────────────────────────────────────
    // Build a map: VectorId → ShardId by loading each shard file.
    let searcher = IndexSearcher::new(Arc::clone(&store), manifest.clone());

    let mut vector_to_shard: HashMap<VectorId, u32> = HashMap::new();
    // Collect all centroids and their shard mapping for routing queries.
    let mut all_centroids: Vec<Vec<f32>> = Vec::new();
    let mut centroid_to_shard: Vec<u32> = Vec::new();

    for shard_def in &manifest.shards {
        let shard = searcher.load_shard(shard_def.shard_id)?;
        for rec in &shard.records {
            vector_to_shard.insert(rec.id, shard_def.shard_id.0);
        }
        for centroid in &shard.centroids {
            all_centroids.push(centroid.clone());
            centroid_to_shard.push(shard_def.shard_id.0);
        }
    }

    // For each corpus vector, predict the shard via nearest-centroid routing
    // (nprobe = 1) and check if it matches the actual assignment.
    let mut routing_hits: usize = 0;
    for rec in &corpus {
        if all_centroids.is_empty() {
            break;
        }
        let nearest = top_n_centroids(&rec.data, &all_centroids, 1);
        // If routing returns no result or an out-of-bounds centroid index, skip
        // this vector rather than counting it as a hit or miss, to avoid
        // silently skewing the accuracy metric.
        let predicted_shard = match nearest.first().and_then(|&ci| centroid_to_shard.get(ci)) {
            Some(&sid) => sid,
            None => {
                tracing::warn!(id = %rec.id, "routing returned no valid centroid; skipping");
                continue;
            }
        };
        let actual_shard = match vector_to_shard.get(&rec.id) {
            Some(&sid) => sid,
            None => {
                tracing::warn!(id = %rec.id, "vector not found in any shard; skipping");
                continue;
            }
        };
        if predicted_shard == actual_shard {
            routing_hits += 1;
        }
    }
    let routing_accuracy = if corpus.is_empty() {
        1.0f64
    } else {
        routing_hits as f64 / corpus.len() as f64
    };

    // ── 3. Recall impact sweep ────────────────────────────────────────────────
    // For each nprobe value from 1 to num_shards, compute mean Recall@k.
    let max_nprobe = n_shards;
    let mut recall_by_nprobe: Vec<(usize, f64)> = Vec::with_capacity(max_nprobe);
    for np in 1..=max_nprobe {
        let mut recalls: Vec<f64> = Vec::with_capacity(queries.len());
        for query in queries {
            let gt = exact_search(&query.data, &corpus, metric, args.k);
            let gt_ids: Vec<VectorId> = gt.iter().map(|r| r.id).collect();
            let approx = searcher.search(&query.data, args.k, np).unwrap_or_default();
            let approx_ids: Vec<VectorId> = approx.iter().map(|r| r.id).collect();
            recalls.push(recall_at_k(&gt_ids, &approx_ids));
        }
        let mean_recall = if recalls.is_empty() {
            0.0
        } else {
            recalls.iter().sum::<f64>() / recalls.len() as f64
        };
        recall_by_nprobe.push((np, mean_recall));
    }

    // ── 4. Shard hotness ─────────────────────────────────────────────────────
    // Count how many times each shard is selected when routing queries with
    // the configured nprobe.
    let mut hotness: HashMap<u32, usize> = HashMap::new();
    for query in queries {
        if all_centroids.is_empty() {
            break;
        }
        let probe_n = args.nprobe.min(all_centroids.len());
        let probe_indices = top_n_centroids(&query.data, &all_centroids, probe_n);
        let mut probe_shards: Vec<u32> = probe_indices
            .iter()
            .filter_map(|&ci| centroid_to_shard.get(ci))
            .copied()
            .collect();
        probe_shards.sort_unstable();
        probe_shards.dedup();
        for sid in probe_shards {
            *hotness.entry(sid).or_insert(0) += 1;
        }
    }
    let mut hotness_sorted: Vec<(u32, usize)> = hotness.into_iter().collect();
    hotness_sorted.sort_by_key(|&(sid, _)| sid);

    // ── Print report ─────────────────────────────────────────────────────────
    println!("=== Partition Evaluation Report ===");
    println!("  Index version:     {}", manifest.index_version);
    println!("  Shards:            {n_shards}");
    println!("  Total vectors:     {}", manifest.total_vector_count);
    println!();

    println!("── Shard Size Distribution ──────────────────────────");
    println!("  Min vectors/shard: {min_count}");
    println!("  Max vectors/shard: {max_count}");
    println!("  Mean:              {mean_count:.1}");
    println!("  Std dev:           {stddev_count:.1}");
    println!();

    println!("── Routing Accuracy (nprobe=1) ──────────────────────");
    println!(
        "  Correctly routed:  {routing_hits} / {} ({:.2}%)",
        corpus.len(),
        routing_accuracy * 100.0
    );
    println!();

    println!(
        "── Recall Impact (k={}) ─────────────────────────────",
        args.k
    );
    println!("  {:<8} {:<10}", "nprobe", "Recall@k");
    for (np, recall) in &recall_by_nprobe {
        println!("  {np:<8} {recall:<10.4}");
    }
    println!();

    println!(
        "── Shard Hotness (nprobe={}) ────────────────────────",
        args.nprobe
    );
    println!("  {:<12} {:<10}", "shard_id", "hits");
    for (sid, hits) in &hotness_sorted {
        println!("  {sid:<12} {hits:<10}");
    }

    Ok(())
}
