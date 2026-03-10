//! `shardlake generate-dataset` – generate a synthetic JSONL vector dataset.
//!
//! Vectors are drawn from Gaussian distributions around randomly placed cluster
//! centroids. The output is a JSONL file suitable for use with `shardlake ingest`.
//!
//! All parameters that influence the random number generator accept a `--seed`
//! value so that datasets can be reproduced exactly.

use std::io::{BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::info;

use shardlake_bench::generator::{generate_dataset, DatasetConfig};

#[derive(Parser, Debug)]
pub struct GenerateArgs {
    /// Number of dimensions for each generated vector.
    #[arg(long, default_value_t = 128)]
    pub dims: usize,
    /// Total number of vectors to generate.
    #[arg(long, default_value_t = 10_000)]
    pub num_vectors: usize,
    /// Number of clusters in the dataset (controls cluster structure).
    #[arg(long, default_value_t = 10)]
    pub num_clusters: usize,
    /// Standard deviation of each vector component around its cluster centroid.
    #[arg(long, default_value_t = 0.1)]
    pub cluster_spread: f32,
    /// Random seed for reproducibility.
    #[arg(long, default_value_t = 42)]
    pub seed: u64,
    /// Output JSONL file path.
    #[arg(long, default_value = "generated.jsonl")]
    pub output: PathBuf,
}

pub async fn run(_storage: PathBuf, args: GenerateArgs) -> Result<()> {
    info!(
        dims = args.dims,
        num_vectors = args.num_vectors,
        num_clusters = args.num_clusters,
        cluster_spread = args.cluster_spread,
        seed = args.seed,
        output = %args.output.display(),
        "Generating synthetic dataset"
    );

    let config = DatasetConfig {
        dims: args.dims,
        num_vectors: args.num_vectors,
        num_clusters: args.num_clusters,
        seed: args.seed,
        cluster_spread: args.cluster_spread,
    };

    let records = generate_dataset(&config);

    let file = std::fs::File::create(&args.output)
        .with_context(|| format!("Cannot create output file {}", args.output.display()))?;
    let mut writer = BufWriter::new(file);

    for record in &records {
        // Write in the ingest-compatible JSONL format: {id, vector, metadata}.
        // (VectorRecord uses the field name `data` internally; the `ingest`
        // command expects the JSON key to be `vector`.)
        let mut obj = serde_json::json!({
            "id": record.id.0,
            "vector": record.data,
        });
        if let Some(meta) = &record.metadata {
            obj["metadata"] = meta.clone();
        }
        let line = serde_json::to_string(&obj)?;
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;

    info!(
        path = %args.output.display(),
        vectors = records.len(),
        "Dataset written"
    );
    println!(
        "Generated {} vectors (dims={}, clusters={}) → {}",
        records.len(),
        args.dims,
        args.num_clusters,
        args.output.display()
    );
    Ok(())
}
