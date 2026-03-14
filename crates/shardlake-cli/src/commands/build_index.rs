//! `shardlake build-index` – build shard-based index from ingested dataset.

use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
};

use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use tracing::info;

use shardlake_core::{
    config::SystemConfig,
    types::{DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorRecord},
};
use shardlake_index::{BuildParams, IndexBuilder};
use shardlake_manifest::{DatasetManifest, ManifestError};
use shardlake_storage::{LocalObjectStore, ObjectStore, StorageError};

#[derive(Parser, Debug)]
pub struct BuildIndexArgs {
    /// Dataset version to build index for.
    #[arg(long)]
    pub dataset_version: String,
    /// Embedding version (defaults to the dataset manifest embedding version).
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
    /// RNG seed for K-means initialisation.
    ///
    /// Using the same seed with identical inputs produces the same shard
    /// layout and artifact fingerprints, enabling reproducible builds.
    #[arg(long, default_value_t = shardlake_core::config::DEFAULT_KMEANS_SEED)]
    pub kmeans_seed: u64,
    /// Maximum number of vectors to sample for K-means centroid training.
    ///
    /// When absent, all vectors are used.  When set, a reproducible random
    /// sample of up to this many vectors is drawn before running K-means.
    /// All vectors are still assigned to the nearest centroid after training.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    pub kmeans_sample_size: Option<u32>,
}

pub async fn run(storage: PathBuf, args: BuildIndexArgs) -> Result<()> {
    validate_num_shards(args.num_shards)?;
    validate_kmeans_sample_size(args.kmeans_sample_size)?;

    let store = LocalObjectStore::new(&storage)?;
    let dataset_ver = DatasetVersion(args.dataset_version.clone());
    let index_ver = IndexVersion(
        args.index_version
            .unwrap_or_else(|| format!("idx-{}", Utc::now().format("%Y%m%dT%H%M%S"))),
    );

    let config = SystemConfig {
        storage_root: storage,
        num_shards: args.num_shards,
        kmeans_iters: args.kmeans_iters,
        nprobe: args.nprobe,
        kmeans_seed: args.kmeans_seed,
        candidate_shards: 0,
        max_vectors_per_shard: 0,
        kmeans_sample_size: args.kmeans_sample_size,
    };

    let dm = match DatasetManifest::load(&store, &dataset_ver) {
        Ok(dm) => dm,
        Err(err @ ManifestError::Storage(StorageError::NotFound(_))) => {
            return Err(anyhow::Error::new(err).context(format!(
                "Dataset {} not found; run `shardlake ingest` first",
                dataset_ver.0
            )));
        }
        Err(err) => return Err(err.into()),
    };
    let embedding_ver = EmbeddingVersion(
        args.embedding_version
            .unwrap_or_else(|| dm.embedding_version.0.clone()),
    );
    let vectors_key = dm.vectors_key.clone();
    let metadata_key = dm.metadata_key.clone();
    let dims = dm.dims as usize;

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
        dataset_version: dataset_ver,
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

fn validate_num_shards(num_shards: u32) -> Result<()> {
    anyhow::ensure!(num_shards > 0, "--num-shards must be greater than 0");
    Ok(())
}

fn validate_kmeans_sample_size(kmeans_sample_size: Option<u32>) -> Result<()> {
    anyhow::ensure!(
        kmeans_sample_size.unwrap_or(1) > 0,
        "--kmeans-sample-size must be greater than 0"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use shardlake_manifest::{DatasetManifest, Manifest, DATASET_MANIFEST_VERSION};
    use shardlake_storage::{paths, LocalObjectStore, ObjectStore};
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn validate_num_shards_rejects_zero() {
        let err = validate_num_shards(0).unwrap_err();
        assert!(err
            .to_string()
            .contains("--num-shards must be greater than 0"));
    }

    #[test]
    fn validate_kmeans_sample_size_rejects_zero() {
        let err = validate_kmeans_sample_size(Some(0)).unwrap_err();
        assert!(err
            .to_string()
            .contains("--kmeans-sample-size must be greater than 0"));
    }

    #[tokio::test]
    async fn run_rejects_zero_num_shards_before_loading_dataset() {
        let tmp = tempdir().unwrap();
        let err = run(
            tmp.path().to_path_buf(),
            BuildIndexArgs {
                dataset_version: "missing-dataset".into(),
                embedding_version: None,
                index_version: None,
                metric: DistanceMetric::Cosine,
                num_shards: 0,
                kmeans_iters: 20,
                nprobe: 2,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
            },
        )
        .await
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("--num-shards must be greater than 0"));
    }

    #[tokio::test]
    async fn run_rejects_zero_kmeans_sample_size_before_loading_dataset() {
        let tmp = tempdir().unwrap();
        let err = run(
            tmp.path().to_path_buf(),
            BuildIndexArgs {
                dataset_version: "missing-dataset".into(),
                embedding_version: None,
                index_version: None,
                metric: DistanceMetric::Cosine,
                num_shards: 1,
                kmeans_iters: 20,
                nprobe: 2,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: Some(0),
            },
        )
        .await
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("--kmeans-sample-size must be greater than 0"));
    }

    #[tokio::test]
    async fn run_defaults_embedding_version_from_dataset_manifest() {
        let tmp = tempdir().unwrap();
        let storage = tmp.path().join("storage");
        let store = LocalObjectStore::new(&storage).unwrap();
        let dataset_version = DatasetVersion("ds-test".into());
        let embedding_version = EmbeddingVersion("emb-manifest".into());

        let vectors_key = paths::dataset_vectors_key(&dataset_version.0);
        store
            .put(
                &vectors_key,
                br#"{"id":1,"data":[1.0,0.0],"metadata":null}
{"id":2,"data":[0.0,1.0],"metadata":null}
"#
                .to_vec(),
            )
            .unwrap();

        let metadata_key = paths::dataset_metadata_key(&dataset_version.0);
        store.put(&metadata_key, br#"{}"#.to_vec()).unwrap();

        DatasetManifest {
            manifest_version: DATASET_MANIFEST_VERSION,
            dataset_version: dataset_version.clone(),
            embedding_version: embedding_version.clone(),
            dims: 2,
            vector_count: 2,
            vectors_key: vectors_key.clone(),
            metadata_key: metadata_key.clone(),
            ingest_metadata: None,
        }
        .save(&store)
        .unwrap();

        run(
            storage,
            BuildIndexArgs {
                dataset_version: dataset_version.0.clone(),
                embedding_version: None,
                index_version: Some("idx-test".into()),
                metric: DistanceMetric::Cosine,
                num_shards: 2,
                kmeans_iters: 2,
                nprobe: 1,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
            },
        )
        .await
        .unwrap();

        let manifest = Manifest::load(&store, &IndexVersion("idx-test".into())).unwrap();
        assert_eq!(manifest.embedding_version, embedding_version);
    }

    #[tokio::test]
    async fn run_preserves_manifest_parse_errors() {
        let tmp = tempdir().unwrap();
        let storage = tmp.path().join("storage");
        let store = LocalObjectStore::new(&storage).unwrap();
        store
            .put(
                &DatasetManifest::storage_key(&DatasetVersion("ds-test".into())),
                b"{".to_vec(),
            )
            .unwrap();

        let err = run(
            storage,
            BuildIndexArgs {
                dataset_version: "ds-test".into(),
                embedding_version: None,
                index_version: Some("idx-test".into()),
                metric: DistanceMetric::Cosine,
                num_shards: 1,
                kmeans_iters: 2,
                nprobe: 1,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
            },
        )
        .await
        .unwrap_err();

        assert!(!err
            .to_string()
            .contains("Dataset ds-test not found; run `shardlake ingest` first"));
        assert!(err.to_string().contains("parse error"));
    }
}
