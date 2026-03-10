//! `shardlake build-index` – build shard-based index from ingested dataset.

use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
};

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use tracing::info;

use shardlake_core::{
    config::SystemConfig,
    types::{DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorRecord},
};
use shardlake_index::{BuildParams, IndexBuilder};
use shardlake_storage::{LocalObjectStore, ObjectStore};

#[derive(Parser, Debug)]
pub struct BuildIndexArgs {
    /// Dataset version to build index for.
    #[arg(long)]
    pub dataset_version: String,
    /// Human-readable dataset identifier (slug or UUID; defaults to dataset version).
    #[arg(long)]
    pub dataset_id: Option<String>,
    /// Name of the model that produced the embeddings (e.g. "text-embedding-ada-002").
    #[arg(long, default_value = "unknown")]
    pub embedding_model: String,
    /// Embedding version (defaults to dataset version).
    #[arg(long)]
    pub embedding_version: Option<String>,
    /// Index version tag (defaults to a timestamp).
    #[arg(long)]
    pub index_version: Option<String>,
    /// Distance metric.
    #[arg(long, default_value = "cosine")]
    pub metric: DistanceMetric,
    /// Number of shards (K-means k).
    #[arg(long, default_value_t = 4)]
    pub num_shards: u32,
    /// K-means iterations.
    #[arg(long, default_value_t = 20)]
    pub kmeans_iters: u32,
    /// Number of shards to probe at query time.
    #[arg(long, default_value_t = 2)]
    pub nprobe: u32,
}

pub async fn run(storage: PathBuf, args: BuildIndexArgs) -> Result<()> {
    let store = LocalObjectStore::new(&storage)?;
    let dataset_ver = DatasetVersion(args.dataset_version.clone());
    let dataset_id = args
        .dataset_id
        .unwrap_or_else(|| args.dataset_version.clone());
    let embedding_ver = EmbeddingVersion(
        args.embedding_version
            .unwrap_or_else(|| args.dataset_version.clone()),
    );
    let index_ver = IndexVersion(
        args.index_version
            .unwrap_or_else(|| format!("idx-{}", Utc::now().format("%Y%m%dT%H%M%S"))),
    );

    let config = SystemConfig {
        storage_root: storage,
        num_shards: args.num_shards,
        kmeans_iters: args.kmeans_iters,
        nprobe: args.nprobe,
    };

    let info_key = format!("datasets/{}/info.json", dataset_ver.0);
    let info_bytes = store.get(&info_key).with_context(|| {
        format!(
            "Dataset {} not found; run `shardlake ingest` first",
            dataset_ver.0
        )
    })?;
    let info: serde_json::Value = serde_json::from_slice(&info_bytes)?;
    let vectors_key = info["vectors_key"]
        .as_str()
        .context("info.json missing vectors_key")?
        .to_string();
    let metadata_key = info["metadata_key"]
        .as_str()
        .context("info.json missing metadata_key")?
        .to_string();
    let dims = info["dims"].as_u64().context("info.json missing dims")? as usize;

    info!(dataset_version = %dataset_ver.0, "Loading vectors");
    let vecs_bytes = store.get(&vectors_key)?;
    let reader = BufReader::new(vecs_bytes.as_slice());
    let mut records: Vec<VectorRecord> = Vec::new();
    for line in reader.lines() {
        let line: String = line?;
        if line.trim().is_empty() {
            continue;
        }
        let rec: VectorRecord = serde_json::from_str(&line)?;
        records.push(rec);
    }

    info!(records = records.len(), dims, "Loaded vectors");

    let builder = IndexBuilder::new(&store, &config);
    let manifest = builder.build(BuildParams {
        records,
        dataset_id,
        dataset_version: dataset_ver,
        embedding_model: args.embedding_model,
        embedding_version: embedding_ver,
        index_version: index_ver,
        metric: args.metric,
        dims,
        vectors_key,
        metadata_key,
    })?;

    println!(
        "Index built → index_version={} ({} shards, {} vectors)",
        manifest.index_version,
        manifest.shards.len(),
        manifest.total_vector_count,
    );
    Ok(())
}
