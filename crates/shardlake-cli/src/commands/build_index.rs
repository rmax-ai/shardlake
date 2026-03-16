//! `shardlake build-index` – build shard-based index from ingested dataset.
//!
//! When `--parallel` is supplied the command drives a local parallel build:
//! it calls [`plan_workers`] to partition shards across `--num-workers`
//! workers, executes every worker concurrently in a Rayon thread pool, then
//! assembles the final manifest with [`merge_worker_outputs`].  This
//! distributes the CPU-bound shard-write work across cores without the
//! multi-process coordination overhead of the `build-index-worker` workflow.

use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use rayon::prelude::*;
use tracing::info;

use shardlake_core::{
    config::SystemConfig,
    types::{DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorRecord},
};
use shardlake_index::{
    merge_worker_outputs, plan_workers, BuildParams, IndexBuilder, MergeParams, WorkerBuilder,
    WorkerOutput, WorkerPlanParams,
};
use shardlake_manifest::{DatasetManifest, Manifest, ManifestError};
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
    /// Enable local parallel build.
    ///
    /// When set, the build is driven through the distributed worker pipeline
    /// (plan → parallel execute → merge) entirely in-process, distributing
    /// shard construction across `--num-workers` Rayon threads. It produces
    /// equivalent shard artifacts to a sequential `build-index` run with the
    /// same arguments, although build metadata like timestamps and duration can
    /// differ.
    ///
    /// Omit this flag (default) to use the classic single-threaded
    /// [`IndexBuilder`] path.
    #[arg(long, default_value_t = false)]
    pub parallel: bool,
    /// Number of parallel workers used when `--parallel` is set.
    ///
    /// Defaults to the number of logical CPUs available to Rayon.  Values
    /// larger than `--num-shards` are silently clamped to the actual number of
    /// non-empty shards.  Must be greater than 0 when specified.
    #[arg(long)]
    pub num_workers: Option<usize>,
}

pub async fn run(storage: PathBuf, args: BuildIndexArgs) -> Result<()> {
    validate_num_shards(args.num_shards)?;
    validate_kmeans_sample_size(args.kmeans_sample_size)?;
    if let Some(nw) = args.num_workers {
        anyhow::ensure!(nw > 0, "--num-workers must be greater than 0");
        anyhow::ensure!(args.parallel, "--num-workers requires --parallel");
    }

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
        ..SystemConfig::default()
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

    let manifest = if args.parallel {
        run_parallel_build(
            &store,
            &config,
            records,
            dataset_ver,
            embedding_ver,
            index_ver,
            args.metric,
            dims,
            vectors_key,
            metadata_key,
            args.num_workers,
        )?
    } else {
        let builder = IndexBuilder::new(&store, &config);
        builder.build(BuildParams {
            records,
            dataset_version: dataset_ver,
            embedding_version: embedding_ver,
            index_version: index_ver,
            metric: args.metric,
            dims,
            vectors_key,
            metadata_key,
            pq_params: None,
        })?
    };

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

/// Drive a local parallel build using the distributed worker pipeline.
///
/// This is equivalent to running `build-index-worker --mode plan`, then
/// running every worker concurrently in a Rayon thread pool, then merging the
/// outputs – all within the current process.
#[allow(clippy::too_many_arguments)]
fn run_parallel_build(
    store: &LocalObjectStore,
    config: &SystemConfig,
    records: Vec<VectorRecord>,
    dataset_ver: DatasetVersion,
    embedding_ver: EmbeddingVersion,
    index_ver: IndexVersion,
    metric: DistanceMetric,
    dims: usize,
    vectors_key: String,
    metadata_key: String,
    num_workers: Option<usize>,
) -> Result<Manifest> {
    let effective_workers = num_workers.unwrap_or_else(rayon::current_num_threads);
    anyhow::ensure!(
        effective_workers > 0,
        "--num-workers must be greater than 0"
    );
    let start = Instant::now();

    info!(
        effective_workers,
        "Parallel build: planning shard assignments"
    );

    let plan = plan_workers(
        store,
        config,
        &records,
        WorkerPlanParams {
            index_version: index_ver.clone(),
            dataset_version: dataset_ver,
            embedding_version: embedding_ver,
            metric,
            dims,
            vectors_key,
            metadata_key,
            num_workers: effective_workers,
        },
    )?;

    let plan_key = shardlake_storage::paths::worker_plan_key(&index_ver.0);
    let plan_bytes = serde_json::to_vec(&plan)?;
    store.put(&plan_key, plan_bytes)?;
    info!(key = %plan_key, "Parallel build: worker plan written");

    info!(
        workers = plan.num_workers,
        shards = plan.shard_centroids.len(),
        "Parallel build: executing {} worker(s) concurrently",
        plan.num_workers,
    );

    // Wrap records in an Arc so each Rayon task can borrow them cheaply.
    // The store reference is shared directly; LocalObjectStore is Send + Sync.
    let records_arc = Arc::new(records);

    // Execute all workers in parallel using Rayon.  Each worker is
    // independent: it builds its assigned shards and writes shard artifacts
    // to storage.  Results are collected in worker-ID order.
    let outputs: Vec<WorkerOutput> = (0..plan.num_workers)
        .into_par_iter()
        .map(|worker_id| {
            let assignment = plan.assignment(worker_id).ok_or_else(|| {
                anyhow::anyhow!(
                    "internal error: worker_id {} is out of range for plan with {} workers",
                    worker_id,
                    plan.num_workers,
                )
            })?;
            let builder = WorkerBuilder::new(store);
            builder
                .execute(&plan, assignment, &records_arc)
                .map_err(|e| anyhow::anyhow!("worker {} failed: {}", worker_id, e))
        })
        .collect::<Result<Vec<_>>>()?;

    let elapsed = start.elapsed().as_secs_f64();

    info!(
        elapsed_secs = elapsed,
        workers = plan.num_workers,
        "Parallel build: merging worker outputs"
    );

    let manifest = merge_worker_outputs(
        &plan,
        outputs,
        MergeParams {
            alias: "latest".to_owned(),
            built_at: Utc::now(),
            builder_version: env!("CARGO_PKG_VERSION").to_string(),
            build_duration_secs: elapsed,
        },
    )?;

    // Persist the final manifest.
    manifest
        .save(store)
        .map_err(|e| anyhow::anyhow!("failed to save manifest: {e}"))?;

    Ok(manifest)
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
                parallel: false,
                num_workers: None,
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
                parallel: false,
                num_workers: None,
            },
        )
        .await
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("--kmeans-sample-size must be greater than 0"));
    }

    #[tokio::test]
    async fn run_rejects_num_workers_without_parallel_before_loading_dataset() {
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
                kmeans_sample_size: None,
                parallel: false,
                num_workers: Some(1),
            },
        )
        .await
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("--num-workers requires --parallel"));
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
                parallel: false,
                num_workers: None,
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
                parallel: false,
                num_workers: None,
            },
        )
        .await
        .unwrap_err();

        assert!(!err
            .to_string()
            .contains("Dataset ds-test not found; run `shardlake ingest` first"));
        assert!(err.to_string().contains("parse error"));
    }

    /// Helper that writes a minimal dataset to `store` and returns the storage
    /// path so tests can pass it to [`run`].
    fn write_minimal_dataset(store: &LocalObjectStore, dataset_version: &str, dims: usize) {
        use shardlake_core::types::{DatasetVersion, EmbeddingVersion};

        let dv = DatasetVersion(dataset_version.into());
        let ev = EmbeddingVersion("emb-par".into());

        let vectors_key = paths::dataset_vectors_key(dataset_version);
        let metadata_key = paths::dataset_metadata_key(dataset_version);

        // Write four vectors in two clear clusters so K-means always produces
        // two non-empty shards regardless of the random seed.
        let mut lines = String::new();
        for id in 1u64..=4 {
            let data: Vec<f32> = if id <= 2 {
                vec![1.0_f32; dims]
            } else {
                vec![-1.0_f32; dims]
            };
            let rec = serde_json::json!({ "id": id, "data": data, "metadata": null });
            lines.push_str(&rec.to_string());
            lines.push('\n');
        }
        store.put(&vectors_key, lines.into_bytes()).unwrap();
        store.put(&metadata_key, b"{}".to_vec()).unwrap();

        DatasetManifest {
            manifest_version: DATASET_MANIFEST_VERSION,
            dataset_version: dv,
            embedding_version: ev,
            dims: dims as u32,
            vector_count: 4,
            vectors_key,
            metadata_key,
            ingest_metadata: None,
        }
        .save(store)
        .unwrap();
    }

    #[tokio::test]
    async fn parallel_build_produces_valid_manifest() {
        let tmp = tempdir().unwrap();
        let storage = tmp.path().join("storage");
        let store = LocalObjectStore::new(&storage).unwrap();
        write_minimal_dataset(&store, "ds-par", 4);

        run(
            storage.clone(),
            BuildIndexArgs {
                dataset_version: "ds-par".into(),
                embedding_version: None,
                index_version: Some("idx-par".into()),
                metric: DistanceMetric::Euclidean,
                num_shards: 2,
                kmeans_iters: 2,
                nprobe: 1,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
                parallel: true,
                num_workers: Some(2),
            },
        )
        .await
        .unwrap();

        let manifest = Manifest::load(&store, &IndexVersion("idx-par".into())).unwrap();
        let plan_bytes = store
            .get(&paths::worker_plan_key("idx-par"))
            .expect("parallel build should persist worker plan");
        let plan: shardlake_index::WorkerPlan = serde_json::from_slice(&plan_bytes).unwrap();

        assert_eq!(manifest.total_vector_count, 4);
        assert!(!manifest.shards.is_empty());
        assert_eq!(plan.index_version.0, "idx-par");
        assert_eq!(plan.num_workers, 2);
    }

    #[tokio::test]
    async fn parallel_build_with_single_worker_matches_sequential() {
        let tmp = tempdir().unwrap();
        let storage = tmp.path().join("storage");
        let store = LocalObjectStore::new(&storage).unwrap();
        write_minimal_dataset(&store, "ds-cmp", 4);

        // Sequential build.
        run(
            storage.clone(),
            BuildIndexArgs {
                dataset_version: "ds-cmp".into(),
                embedding_version: None,
                index_version: Some("idx-seq".into()),
                metric: DistanceMetric::Euclidean,
                num_shards: 2,
                kmeans_iters: 2,
                nprobe: 1,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
                parallel: false,
                num_workers: None,
            },
        )
        .await
        .unwrap();

        // Parallel build with one worker.
        run(
            storage.clone(),
            BuildIndexArgs {
                dataset_version: "ds-cmp".into(),
                embedding_version: None,
                index_version: Some("idx-par1".into()),
                metric: DistanceMetric::Euclidean,
                num_shards: 2,
                kmeans_iters: 2,
                nprobe: 1,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
                parallel: true,
                num_workers: Some(1),
            },
        )
        .await
        .unwrap();

        let seq = Manifest::load(&store, &IndexVersion("idx-seq".into())).unwrap();
        let par = Manifest::load(&store, &IndexVersion("idx-par1".into())).unwrap();

        assert_eq!(seq.total_vector_count, par.total_vector_count);
        assert_eq!(seq.shards.len(), par.shards.len());
    }
}
