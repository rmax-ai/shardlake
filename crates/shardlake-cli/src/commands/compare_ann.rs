//! `shardlake compare-ann` – compare IVF-PQ, HNSW, DiskANN (and others) in one run.
//!
//! Accepts one or more index aliases built with different `--ann-family` values.
//! Each alias is evaluated with [`shardlake_bench::run_eval_ann`] and the results
//! are collected into a [`CompareAnnReport`] that is printed as a text table or
//! as machine-readable JSON for regression tracking.

use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, Result};
use clap::Parser;
use tracing::info;

use shardlake_bench::{
    precompute_ground_truth_ids, run_eval_ann_with_ground_truth, AnnFamilyReport, CompareAnnReport,
};
use shardlake_core::{
    config::FanOutPolicy,
    types::{DistanceMetric, VectorRecord},
};
use shardlake_index::IndexSearcher;
use shardlake_manifest::Manifest;
use shardlake_storage::{LocalObjectStore, ObjectStore};

/// Default cap on query vectors when `--max-queries` is 0.
const DEFAULT_MAX_QUERIES: usize = 100;

/// Output format for the comparison report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputFormat {
    /// Human-readable text table (default).
    Text,
    /// Machine-readable JSON object, suitable for regression tracking.
    Json,
}

#[derive(Parser, Debug)]
pub struct CompareAnnArgs {
    /// One or more index aliases to compare (e.g. `--alias latest --alias hnsw-exp`).
    ///
    /// Each alias must point to an index built with a potentially different
    /// `--ann-family` value.  At least two aliases should be provided to
    /// produce a meaningful comparison; a single alias is also accepted.
    #[arg(long = "alias", num_args = 1..)]
    pub aliases: Vec<String>,
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

pub async fn run(storage: PathBuf, args: CompareAnnArgs) -> Result<()> {
    anyhow::ensure!(args.k >= 1, "--k must be at least 1");
    anyhow::ensure!(args.nprobe >= 1, "--nprobe must be at least 1");
    anyhow::ensure!(!args.aliases.is_empty(), "at least one --alias is required");

    let policy = FanOutPolicy {
        candidate_centroids: args.nprobe as u32,
        candidate_shards: 0,
        max_vectors_per_shard: 0,
    };
    policy.validate().map_err(|err| anyhow::anyhow!(err))?;

    let store = Arc::new(LocalObjectStore::new(&storage)?);

    // Load the corpus once from the first alias – all compared indexes must
    // reference the same dataset, so we reuse a single corpus across all runs.
    let first_manifest = Manifest::load_alias(&*store, &args.aliases[0])
        .with_context(|| format!("loading manifest for alias '{}'", args.aliases[0]))?;
    let metric = first_manifest.distance_metric;

    let vecs_bytes = store
        .get(&first_manifest.vectors_key)
        .with_context(|| "loading corpus vectors")?;
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
        corpus.len().min(DEFAULT_MAX_QUERIES)
    } else {
        args.max_queries.min(corpus.len())
    };
    let queries: Vec<VectorRecord> = corpus[..limit].to_vec();
    let ground_truth_ids = precompute_ground_truth_ids(&queries, &corpus, metric, args.k);

    info!(
        n_queries = queries.len(),
        k = args.k,
        nprobe = args.nprobe,
        n_aliases = args.aliases.len(),
        "Starting ANN family comparison"
    );

    let mut entries: Vec<AnnFamilyReport> = Vec::with_capacity(args.aliases.len());

    for alias in &args.aliases {
        let manifest = Manifest::load_alias(&*store, alias)
            .with_context(|| format!("loading manifest for alias '{alias}'"))?;
        ensure_comparable_alias(
            &args.aliases[0],
            &first_manifest.vectors_key,
            first_manifest.distance_metric,
            alias,
            &manifest.vectors_key,
            manifest.distance_metric,
        )?;
        let ann_family = canonical_ann_family(
            &manifest.algorithm.algorithm,
            manifest.compression.enabled,
            &manifest.compression.codec,
        );

        info!(
            alias = %alias,
            ann_family = %ann_family,
            index_version = %manifest.index_version,
            "Evaluating alias"
        );

        let searcher =
            IndexSearcher::new(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest.clone());
        let eval =
            run_eval_ann_with_ground_truth(&searcher, &queries, &ground_truth_ids, args.k, &policy)
                .with_context(|| {
                    format!("running eval-ann for alias '{alias}' (family: {ann_family})")
                })?;

        entries.push(AnnFamilyReport {
            alias: alias.clone(),
            ann_family,
            eval,
        });
    }

    let report = CompareAnnReport { entries };

    match args.output {
        OutputFormat::Text => print_text(&report),
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }

    Ok(())
}

fn print_text(report: &CompareAnnReport) {
    if report.entries.is_empty() {
        println!("=== ANN Family Comparison ===");
        println!("  (no entries)");
        return;
    }

    // Use the first entry for shared parameters (k, nprobe, num_queries are
    // the same for every entry since the same queries slice is used each time).
    let first = &report.entries[0];

    println!("=== ANN Family Comparison ===");
    println!(
        "  Queries:  {}  k:  {}  nprobe:  {}",
        first.eval.num_queries, first.eval.k, first.eval.nprobe
    );
    println!();
    println!(
        "  {:<16} {:<20} {:>11} {:>13} {:>13} {:>13}",
        "Family", "Alias", "Recall@k", "Precision@k", "Mean Lat", "P99 Lat"
    );
    println!("  {}", "-".repeat(89));
    for entry in &report.entries {
        let mean_lat = format!("{:.1} µs", entry.eval.mean_latency_us);
        let p99_lat = format!("{:.1} µs", entry.eval.p99_latency_us);
        println!(
            "  {:<16} {:<20} {:>11.4} {:>13.4} {:>13} {:>13}",
            entry.ann_family,
            entry.alias,
            entry.eval.recall_at_k,
            entry.eval.precision_at_k,
            mean_lat,
            p99_lat,
        );
    }
}

fn ensure_comparable_alias(
    reference_alias: &str,
    reference_vectors_key: &str,
    reference_metric: DistanceMetric,
    alias: &str,
    vectors_key: &str,
    metric: DistanceMetric,
) -> Result<()> {
    anyhow::ensure!(
        vectors_key == reference_vectors_key,
        "alias '{alias}' points to vectors artifact '{vectors_key}', but alias '{reference_alias}' points to '{reference_vectors_key}'; `compare-ann` requires all aliases to reference the same dataset"
    );
    anyhow::ensure!(
        metric == reference_metric,
        "alias '{alias}' uses distance metric '{metric}', but alias '{reference_alias}' uses '{reference_metric}'; `compare-ann` requires all aliases to use the same distance metric"
    );
    Ok(())
}

fn canonical_ann_family(
    algorithm: &str,
    compression_enabled: bool,
    compression_codec: &str,
) -> String {
    match algorithm {
        "ivf-flat" | "kmeans-flat" if compression_enabled && compression_codec == "pq8" => {
            "ivf_pq".to_owned()
        }
        "ivf-flat" | "kmeans-flat" => "ivf_flat".to_owned(),
        other => other.replace('-', "_"),
    }
}

#[cfg(test)]
mod tests {
    use super::{canonical_ann_family, ensure_comparable_alias};
    use shardlake_core::types::DistanceMetric;

    #[test]
    fn comparable_aliases_pass_validation() {
        ensure_comparable_alias(
            "ivf",
            "datasets/ds-v1/vectors.jsonl",
            DistanceMetric::Cosine,
            "hnsw",
            "datasets/ds-v1/vectors.jsonl",
            DistanceMetric::Cosine,
        )
        .unwrap();
    }

    #[test]
    fn mismatched_dataset_is_rejected() {
        let err = ensure_comparable_alias(
            "ivf",
            "datasets/ds-v1/vectors.jsonl",
            DistanceMetric::Cosine,
            "diskann",
            "datasets/ds-v2/vectors.jsonl",
            DistanceMetric::Cosine,
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("requires all aliases to reference the same dataset"));
    }

    #[test]
    fn mismatched_metric_is_rejected() {
        let err = ensure_comparable_alias(
            "ivf",
            "datasets/ds-v1/vectors.jsonl",
            DistanceMetric::Cosine,
            "diskann",
            "datasets/ds-v1/vectors.jsonl",
            DistanceMetric::Euclidean,
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("requires all aliases to use the same distance metric"));
    }

    #[test]
    fn canonical_ann_family_distinguishes_ivf_flat_and_pq() {
        assert_eq!(canonical_ann_family("ivf-flat", false, "none"), "ivf_flat");
        assert_eq!(canonical_ann_family("ivf-flat", true, "pq8"), "ivf_pq");
        assert_eq!(
            canonical_ann_family("kmeans-flat", false, "none"),
            "ivf_flat"
        );
    }

    #[test]
    fn canonical_ann_family_normalizes_hyphenated_names() {
        assert_eq!(canonical_ann_family("diskann", false, "none"), "diskann");
        assert_eq!(
            canonical_ann_family("custom-family", false, "none"),
            "custom_family"
        );
    }
}
