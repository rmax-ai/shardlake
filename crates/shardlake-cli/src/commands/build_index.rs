//! `shardlake build-index` – build shard-based index from ingested dataset.

use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
};

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use rand::SeedableRng;
use tracing::info;

use shardlake_core::{
    config::SystemConfig,
    types::{DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorRecord},
};
use shardlake_index::{BuildParams, IndexBuilder, IvfPqIndex};
use shardlake_storage::{LocalObjectStore, ObjectStore};

#[derive(Parser, Debug)]
pub struct BuildIndexArgs {
    /// Dataset version to build index for.
    #[arg(long)]
    pub dataset_version: String,
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
    /// Number of PQ sub-spaces.  Set to 0 to skip PQ index building.
    /// `dims` must be divisible by this value.
    #[arg(long, default_value_t = 0)]
    pub pq_m: usize,
    /// Codewords per PQ sub-space (1–256, only relevant when --pq-m > 0).
    #[arg(long, default_value_t = 256)]
    pub pq_ksub: usize,
}

pub async fn run(storage: PathBuf, args: BuildIndexArgs) -> Result<()> {
    let store = LocalObjectStore::new(&storage)?;
    let dataset_ver = DatasetVersion(args.dataset_version.clone());
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

    // Build the standard IVF shard index.
    let builder = IndexBuilder::new(&store, &config);
    let mut manifest = builder.build(BuildParams {
        records: records.clone(),
        dataset_version: dataset_ver,
        embedding_version: embedding_ver,
        index_version: index_ver.clone(),
        metric: args.metric,
        dims,
        vectors_key,
        metadata_key,
    })?;

    // Optionally build the IVF-PQ index alongside the shard index.
    if args.pq_m > 0 {
        info!(
            pq_m = args.pq_m,
            pq_ksub = args.pq_ksub,
            "Building IVF-PQ index"
        );
        // Use the same fixed seed as the IVF shard builder for reproducible artifacts.
        let mut rng = rand::rngs::StdRng::seed_from_u64(0xdead_beef);
        let ivf_pq = IvfPqIndex::build(
            &records,
            args.num_shards as usize,
            args.pq_m,
            args.pq_ksub,
            args.kmeans_iters,
            args.metric,
            &mut rng,
        )?;

        let ivfpq_key = format!("indexes/{}/ivfpq.bin", index_ver.0);
        let ivfpq_bytes = ivf_pq.to_bytes()?;
        store.put(&ivfpq_key, ivfpq_bytes)?;
        info!(key = %ivfpq_key, "IVF-PQ artifact written");

        // Update the manifest to record the PQ artifact key and re-save.
        manifest.pq_artifact_key = Some(ivfpq_key);
        manifest
            .save(&store)
            .map_err(|e| anyhow::anyhow!("failed to update manifest: {e}"))?;
        info!("Manifest updated with pq_artifact_key");
    }

    let pq_note = if manifest.pq_artifact_key.is_some() {
        format!(", PQ m={} ksub={}", args.pq_m, args.pq_ksub)
    } else {
        String::new()
    };

    println!(
        "Index built → index_version={} ({} shards, {} vectors{})",
        manifest.index_version,
        manifest.shards.len(),
        manifest.total_vector_count,
        pq_note,
    );
    Ok(())
}
