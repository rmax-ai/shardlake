//! `shardlake evaluate-partitioning` – partition quality report.
//!
//! Loads a built index manifest and evaluates shard size distribution,
//! routing accuracy, recall impact, and shard hotness.  Results are printed
//! to stdout in a human-readable format.

use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{ensure, Result};
use clap::Parser;
use tracing::info;

use shardlake_core::types::{IndexVersion, VectorRecord};
use shardlake_index::IndexSearcher;
use shardlake_manifest::Manifest;
use shardlake_storage::{LocalObjectStore, ObjectStore};

/// Arguments for the `evaluate-partitioning` subcommand.
#[derive(Parser, Debug)]
pub struct EvaluatePartitioningArgs {
    /// Index version to evaluate.  Takes precedence over `--alias` when both
    /// are supplied.
    #[arg(long)]
    pub index_version: Option<String>,

    /// Alias to resolve when `--index-version` is not provided.
    #[arg(long, default_value = "latest")]
    pub alias: String,

    /// Number of nearest neighbours to retrieve (for recall@k).  Must be ≥ 1.
    #[arg(long, default_value_t = 10, value_parser = parse_positive_usize)]
    pub k: usize,

    /// Number of shards to probe per query.  Must be ≥ 1.
    #[arg(long, default_value_t = 2, value_parser = parse_positive_usize)]
    pub nprobe: usize,

    /// Maximum number of query vectors to use from the corpus
    /// (0 = min(corpus size, 100)).
    #[arg(long, default_value_t = 0)]
    pub max_queries: usize,
}

/// Entry-point called by `main`.
pub async fn run(storage: PathBuf, args: EvaluatePartitioningArgs) -> Result<()> {
    validate_query_args(args.k, args.nprobe)?;

    let store = Arc::new(LocalObjectStore::new(&storage)?);

    let manifest = if let Some(ref iv) = args.index_version {
        Manifest::load(&*store, &IndexVersion(iv.clone()))?
    } else {
        Manifest::load_alias(&*store, &args.alias)?
    };

    let metric = manifest.distance_metric;
    info!(
        index_version = %manifest.index_version,
        shards = manifest.shards.len(),
        "Loaded manifest for partition evaluation"
    );

    // Load corpus vectors from the path recorded in the manifest.
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
        "Running partition evaluation"
    );

    let searcher = IndexSearcher::new(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest);

    let report = shardlake_bench::evaluate_partitioning(
        &searcher,
        &queries,
        &corpus,
        args.k,
        args.nprobe,
        metric,
    )?;

    // ── Print human-readable report ────────────────────────────────────────
    println!("=== Partition Evaluation Report ===");
    println!("  Index version:       {}", report.index_version);
    println!("  Total vectors:       {}", report.total_vectors);
    println!("  Shards:              {}", report.num_shards);
    println!("  k:                   {}", report.k);
    println!("  nprobe:              {}", report.nprobe);
    println!("  Queries:             {}", report.num_queries);

    println!();
    println!("Shard Size Distribution:");
    for &(shard_id, count) in &report.per_shard_vector_counts {
        let pct = if report.total_vectors > 0 {
            count as f64 / report.total_vectors as f64 * 100.0
        } else {
            0.0
        };
        println!("  shard-{shard_id:04}:  {count:>8} vectors  ({pct:>5.1}%)");
    }
    println!(
        "  Min:         {}    Max: {}",
        report.min_shard_size, report.max_shard_size
    );
    println!(
        "  Mean:        {:.1}  Std dev: {:.1}",
        report.mean_shard_size, report.std_dev_shard_size
    );
    println!("  Imbalance:   {:.3}  (max / mean)", report.imbalance_ratio);

    if report.num_queries > 0 {
        println!();
        println!("Routing & Recall (nprobe={}):", report.nprobe);
        match report.routing_accuracy {
            Some(ra) => println!("  Routing accuracy:    {ra:.4}"),
            None => println!("  Routing accuracy:    n/a (manifest lacks centroid metadata)"),
        }
        match report.recall_at_k {
            Some(r) => println!("  Recall@{}:           {r:.4}", report.k),
            None => println!("  Recall@{}:           n/a", report.k),
        }

        if !report.shard_hotness.is_empty() {
            println!();
            println!(
                "Shard Hotness (fraction of queries that probe each shard, nprobe={}):",
                report.nprobe
            );
            for entry in &report.shard_hotness {
                println!(
                    "  shard-{:04}:  {:.4}",
                    entry.shard_id, entry.probe_fraction
                );
            }
        }
    }

    Ok(())
}

fn validate_query_args(k: usize, nprobe: usize) -> Result<()> {
    ensure!(k > 0, "--k must be greater than 0");
    ensure!(nprobe > 0, "--nprobe must be greater than 0");
    Ok(())
}

fn parse_positive_usize(raw: &str) -> std::result::Result<usize, String> {
    let value = raw
        .parse::<usize>()
        .map_err(|err| format!("invalid integer value `{raw}`: {err}"))?;
    if value == 0 {
        return Err("value must be greater than 0".into());
    }
    Ok(value)
}

// ── tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use shardlake_core::{
        config::SystemConfig,
        types::{
            DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
        },
    };
    use shardlake_index::{BuildParams, IndexBuilder};
    use shardlake_storage::{paths, LocalObjectStore, ObjectStore};
    use tempfile::tempdir;

    use super::*;

    // ── helpers ────────────────────────────────────────────────────────────────

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

    fn write_corpus(store: &LocalObjectStore, records: &[VectorRecord], dataset_ver: &str) {
        let vectors_key = paths::dataset_vectors_key(dataset_ver);
        let lines: Vec<String> = records
            .iter()
            .map(|r| serde_json::to_string(r).unwrap())
            .collect();
        store
            .put(&vectors_key, lines.join("\n").into_bytes())
            .unwrap();
    }

    fn build_index_with_corpus(
        store: &LocalObjectStore,
        records: Vec<VectorRecord>,
        num_shards: u32,
        storage_root: std::path::PathBuf,
    ) -> shardlake_manifest::Manifest {
        let dims = records[0].data.len();
        let dataset_ver = "ds-ev";
        write_corpus(store, &records, dataset_ver);
        let metadata_key = paths::dataset_metadata_key(dataset_ver);
        store.put(&metadata_key, b"{}".to_vec()).unwrap();

        let config = SystemConfig {
            storage_root,
            num_shards,
            kmeans_iters: 5,
            nprobe: 2,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            kmeans_sample_size: None,
        };
        IndexBuilder::new(store, &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion(dataset_ver.into()),
                embedding_version: EmbeddingVersion("emb-v1".into()),
                index_version: IndexVersion("idx-ev".into()),
                metric: DistanceMetric::Euclidean,
                dims,
                vectors_key: paths::dataset_vectors_key(dataset_ver),
                metadata_key,
            })
            .unwrap()
    }

    // ── unit tests ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn run_validates_k_zero() {
        let tmp = tempdir().unwrap();
        let err = run(
            tmp.path().join("storage"),
            EvaluatePartitioningArgs {
                index_version: None,
                alias: "latest".into(),
                k: 0,
                nprobe: 2,
                max_queries: 0,
            },
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("--k must be greater than 0"));
    }

    #[tokio::test]
    async fn run_validates_nprobe_zero() {
        let tmp = tempdir().unwrap();
        let err = run(
            tmp.path().join("storage"),
            EvaluatePartitioningArgs {
                index_version: None,
                alias: "latest".into(),
                k: 10,
                nprobe: 0,
                max_queries: 0,
            },
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("--nprobe must be greater than 0"));
    }

    #[tokio::test]
    async fn run_errors_on_missing_index() {
        let tmp = tempdir().unwrap();
        let err = run(
            tmp.path().join("storage"),
            EvaluatePartitioningArgs {
                index_version: Some("idx-missing".into()),
                alias: "latest".into(),
                k: 10,
                nprobe: 2,
                max_queries: 0,
            },
        )
        .await
        .unwrap_err();
        assert!(
            !err.to_string().is_empty(),
            "expected an error for missing index"
        );
    }

    #[tokio::test]
    async fn run_succeeds_against_built_index_by_version() {
        let tmp = tempdir().unwrap();
        let storage = tmp.path().join("storage");
        let store = LocalObjectStore::new(&storage).unwrap();
        let records = make_records(20, 4);
        let manifest = build_index_with_corpus(&store, records, 2, storage.clone());

        run(
            storage,
            EvaluatePartitioningArgs {
                index_version: Some(manifest.index_version.0.clone()),
                alias: "latest".into(),
                k: 5,
                nprobe: 1,
                max_queries: 5,
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn run_succeeds_via_alias() {
        let tmp = tempdir().unwrap();
        let storage = tmp.path().join("storage");
        let store = LocalObjectStore::new(&storage).unwrap();
        let records = make_records(20, 4);
        let manifest = build_index_with_corpus(&store, records, 2, storage.clone());
        manifest.publish_alias(&store).unwrap();

        run(
            storage,
            EvaluatePartitioningArgs {
                index_version: None,
                alias: manifest.alias.clone(),
                k: 5,
                nprobe: 1,
                max_queries: 5,
            },
        )
        .await
        .unwrap();
    }
}
