//! `shardlake generate` – write a reproducible synthetic benchmark dataset to storage.
//!
//! Generates a clustered vector corpus with configurable dimension, cluster
//! structure, and dataset size from a deterministic seed.  Identical arguments
//! always produce identical stored artifacts.

use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use tracing::info;

use shardlake_bench::generate::{generate_dataset, GenerateConfig};
use shardlake_core::types::{DatasetVersion, EmbeddingVersion};
use shardlake_manifest::{DatasetManifest, IngestMetadata, DATASET_MANIFEST_VERSION};
use shardlake_storage::{paths, LocalObjectStore, ObjectStore};

#[derive(Parser, Debug)]
pub struct GenerateArgs {
    /// Total number of vectors to generate.
    #[arg(long, default_value_t = 1_000)]
    pub num_vectors: usize,
    /// Dimensionality of each generated vector.
    #[arg(long, default_value_t = 128)]
    pub dims: usize,
    /// Number of clusters controlling the synthetic corpus structure.
    #[arg(long, default_value_t = 10)]
    pub num_clusters: usize,
    /// RNG seed for deterministic generation; the same seed always produces
    /// the same dataset.
    #[arg(long, default_value_t = 0xdead_beef)]
    pub seed: u64,
    /// Half-range of uniform noise added around each cluster centroid per
    /// dimension.  Smaller values produce tighter clusters.
    #[arg(long, default_value_t = 0.1)]
    pub cluster_spread: f32,
    /// Dataset version tag (defaults to a timestamp).
    #[arg(long)]
    pub dataset_version: Option<String>,
    /// Embedding version tag (defaults to same as `--dataset-version`).
    #[arg(long)]
    pub embedding_version: Option<String>,
}

pub async fn run(storage: PathBuf, args: GenerateArgs) -> Result<()> {
    let dataset_ver = args
        .dataset_version
        .unwrap_or_else(|| format!("ds-{}", Utc::now().format("%Y%m%dT%H%M%S")));
    let embedding_ver = args
        .embedding_version
        .unwrap_or_else(|| dataset_ver.clone());

    info!(
        num_vectors = args.num_vectors,
        dims = args.dims,
        num_clusters = args.num_clusters,
        seed = args.seed,
        dataset_version = %dataset_ver,
        "Generating synthetic dataset"
    );

    let config = GenerateConfig {
        num_vectors: args.num_vectors,
        dims: args.dims,
        num_clusters: args.num_clusters,
        seed: args.seed,
        cluster_spread: args.cluster_spread,
    };
    let records = generate_dataset(&config);

    let store = LocalObjectStore::new(&storage).context("failed to open storage")?;

    let vectors_key = paths::dataset_vectors_key(&dataset_ver);
    let metadata_key = paths::dataset_metadata_key(&dataset_ver);

    let mut jsonl: Vec<u8> = Vec::new();
    for record in &records {
        let line = serde_json::to_string(record)
            .with_context(|| format!("failed to serialise record {}", record.id))?;
        jsonl.extend_from_slice(line.as_bytes());
        jsonl.push(b'\n');
    }
    store.put(&vectors_key, jsonl)?;

    // Generated vectors carry no per-record metadata; write an empty object.
    store.put(&metadata_key, b"{}".to_vec())?;

    let dims_u32 = u32::try_from(args.dims)
        .context("vector dimension exceeds supported maximum of u32::MAX")?;

    let manifest = DatasetManifest {
        manifest_version: DATASET_MANIFEST_VERSION,
        dataset_version: DatasetVersion(dataset_ver.clone()),
        embedding_version: EmbeddingVersion(embedding_ver.clone()),
        dims: dims_u32,
        vector_count: records.len() as u64,
        vectors_key: vectors_key.clone(),
        metadata_key: metadata_key.clone(),
        ingest_metadata: Some(IngestMetadata {
            ingested_at: Utc::now(),
            ingester_version: env!("CARGO_PKG_VERSION").to_string(),
        }),
    };
    manifest.save(&store)?;

    info!(vectors_key, metadata_key, "Generate complete");
    println!(
        "Generated {} vectors (dims={}, clusters={}, seed={}) → dataset_version={}",
        records.len(),
        args.dims,
        args.num_clusters,
        args.seed,
        dataset_ver,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn run_writes_expected_artifacts() {
        let tmp = tempdir().unwrap();
        run(
            tmp.path().join("storage"),
            GenerateArgs {
                num_vectors: 20,
                dims: 4,
                num_clusters: 2,
                seed: 1,
                cluster_spread: 0.1,
                dataset_version: Some("ds-gen-test".into()),
                embedding_version: None,
            },
        )
        .await
        .unwrap();

        let storage = tmp.path().join("storage");
        // vectors JSONL should exist and have 20 lines.
        let vectors_path = storage.join("datasets/ds-gen-test/vectors.jsonl");
        let content = std::fs::read_to_string(&vectors_path).unwrap();
        let line_count = content.lines().count();
        assert_eq!(line_count, 20, "expected 20 vector lines, got {line_count}");

        // metadata.json should be an empty JSON object.
        let meta_path = storage.join("datasets/ds-gen-test/metadata.json");
        let meta: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(meta_path).unwrap()).unwrap();
        assert_eq!(meta, serde_json::json!({}));

        // info.json (dataset manifest) should record correct counts.
        let info_path = storage.join("datasets/ds-gen-test/info.json");
        let info: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(info_path).unwrap()).unwrap();
        assert_eq!(info["vector_count"], 20);
        assert_eq!(info["dims"], 4);
    }

    #[tokio::test]
    async fn run_is_reproducible() {
        let tmp = tempdir().unwrap();
        let storage = tmp.path().join("storage");

        let make_args = |ver: &str| GenerateArgs {
            num_vectors: 30,
            dims: 4,
            num_clusters: 3,
            seed: 77,
            cluster_spread: 0.05,
            dataset_version: Some(ver.to_string()),
            embedding_version: None,
        };

        run(storage.clone(), make_args("run-a")).await.unwrap();
        run(storage.clone(), make_args("run-b")).await.unwrap();

        let read_vectors = |ver: &str| {
            let p = storage.join(format!("datasets/{ver}/vectors.jsonl"));
            std::fs::read_to_string(p).unwrap()
        };

        assert_eq!(
            read_vectors("run-a"),
            read_vectors("run-b"),
            "identical seeds must produce identical JSONL output"
        );
    }
}
