//! `shardlake build-index-worker` – distributed index build worker mode.
//!
//! This command drives the three-phase distributed build workflow:
//!
//! * **`--mode plan`** – trains the IVF coarse quantizer, partitions shards
//!   across workers, and writes a [`WorkerPlan`] to storage.  Run this once
//!   before launching individual workers.
//!
//! * **`--mode execute`** – loads the plan for a given index version, reads
//!   the dataset vectors, builds the shards assigned to `--worker-id`, and
//!   writes intermediate shard artifacts and an output-metadata JSON file to
//!   storage.
//!
//! * **`--mode merge`** – loads the worker plan and all worker output
//!   descriptors for an index version, validates completeness, and assembles
//!   the final `manifest.json`.  Run this once after all workers have
//!   finished.

use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
};

use anyhow::Result;
use chrono::Utc;
use clap::{Parser, ValueEnum};
use tracing::info;

use shardlake_core::{
    config::SystemConfig,
    types::{DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorRecord},
};
use shardlake_index::{
    merge_worker_outputs, plan_workers, MergeParams, WorkerBuilder, WorkerOutput, WorkerPlan,
    WorkerPlanParams,
};
use shardlake_manifest::{DatasetManifest, ManifestError};
use shardlake_storage::{LocalObjectStore, ObjectStore, StorageError};

/// Operating mode for the worker command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum WorkerMode {
    /// Train the IVF quantizer, assign vectors to shards, and write the
    /// distributed worker plan to storage.
    Plan,
    /// Load a previously written plan and build the shards assigned to this
    /// worker, writing shard artifacts and output metadata to storage.
    Execute,
    /// Collect all worker output descriptors and assemble the final manifest.
    ///
    /// Reads every `workers/<id>/output.json` file for the given index version
    /// and combines them into a `manifest.json` that is written to storage.
    Merge,
}

#[derive(Parser, Debug)]
pub struct BuildIndexWorkerArgs {
    /// Operating mode: `plan` or `execute`.
    #[arg(long, value_enum)]
    pub mode: WorkerMode,

    /// Dataset version to build the index from (required for `plan` mode).
    #[arg(long)]
    pub dataset_version: Option<String>,

    /// Index version tag.
    ///
    /// In `plan` mode: the version tag to assign the new index (defaults to a
    /// timestamp).  In `execute` mode: the index version whose plan to load.
    #[arg(long)]
    pub index_version: Option<String>,

    /// Embedding version (defaults to the dataset manifest embedding version).
    ///
    /// Only used in `plan` mode.
    #[arg(long)]
    pub embedding_version: Option<String>,

    /// Distance metric.  Only used in `plan` mode.
    #[arg(long, default_value = "cosine")]
    pub metric: DistanceMetric,

    /// Number of shards (K-means k).  Only used in `plan` mode.
    #[arg(long, default_value_t = 4)]
    pub num_shards: u32,

    /// K-means iterations.  Only used in `plan` mode.
    #[arg(long, default_value_t = 20)]
    pub kmeans_iters: u32,

    /// RNG seed for K-means centroid initialisation.  Only used in `plan` mode.
    #[arg(long, default_value_t = shardlake_core::config::DEFAULT_KMEANS_SEED)]
    pub kmeans_seed: u64,

    /// Maximum vectors to sample for K-means centroid training.  Only used in
    /// `plan` mode.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    pub kmeans_sample_size: Option<u32>,

    /// Total number of workers.  Only used in `plan` mode.
    #[arg(long, default_value_t = 1)]
    pub num_workers: usize,

    /// Zero-based worker ID.  Required in `execute` mode.
    #[arg(long)]
    pub worker_id: Option<usize>,

    /// Alias to record in the generated manifest.  Only used in `merge` mode.
    #[arg(long, default_value = "latest")]
    pub alias: String,
}

pub async fn run(storage: PathBuf, args: BuildIndexWorkerArgs) -> Result<()> {
    match args.mode {
        WorkerMode::Plan => run_plan(storage, args).await,
        WorkerMode::Execute => run_execute(storage, args).await,
        WorkerMode::Merge => run_merge(storage, args).await,
    }
}

async fn run_plan(storage: PathBuf, args: BuildIndexWorkerArgs) -> Result<()> {
    let dataset_version_str = args
        .dataset_version
        .ok_or_else(|| anyhow::anyhow!("--dataset-version is required in plan mode"))?;

    anyhow::ensure!(args.num_shards > 0, "--num-shards must be greater than 0");
    anyhow::ensure!(args.num_workers > 0, "--num-workers must be greater than 0");
    if let Some(s) = args.kmeans_sample_size {
        anyhow::ensure!(s > 0, "--kmeans-sample-size must be greater than 0");
    }

    let store = LocalObjectStore::new(&storage)?;
    let dataset_ver = DatasetVersion(dataset_version_str);
    let index_ver = IndexVersion(
        args.index_version
            .unwrap_or_else(|| format!("idx-{}", Utc::now().format("%Y%m%dT%H%M%S"))),
    );

    let config = SystemConfig {
        storage_root: storage,
        num_shards: args.num_shards,
        kmeans_iters: args.kmeans_iters,
        nprobe: 2,
        kmeans_seed: args.kmeans_seed,
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

    info!(dataset_version = %dataset_ver.0, "Loading vectors for planning");
    let vecs_bytes = store.get(&vectors_key)?;
    let reader = BufReader::new(vecs_bytes.as_slice());
    let mut records: Vec<VectorRecord> = Vec::new();
    for line_result in reader.lines() {
        let line: String = line_result?;
        if line.trim().is_empty() {
            continue;
        }
        let rec: VectorRecord = serde_json::from_str(&line)?;
        records.push(rec);
    }
    info!(records = records.len(), dims, "Loaded vectors");

    let plan = plan_workers(
        &store,
        &config,
        &records,
        WorkerPlanParams {
            index_version: index_ver.clone(),
            dataset_version: dataset_ver,
            embedding_version: embedding_ver,
            metric: args.metric,
            dims,
            vectors_key,
            metadata_key,
            num_workers: args.num_workers,
        },
    )?;

    // Persist the plan so workers can load it by index version.
    let plan_key = shardlake_storage::paths::worker_plan_key(&index_ver.0);
    let plan_bytes = serde_json::to_vec(&plan)?;
    store.put(&plan_key, plan_bytes)?;
    info!(key = %plan_key, "Worker plan written");

    println!(
        "Worker plan written → index_version={} ({} shards across {} workers)",
        plan.index_version,
        plan.shard_centroids.len(),
        plan.num_workers,
    );
    for assignment in &plan.assignments {
        println!(
            "  worker {:04}: {} shard(s): {:?}",
            assignment.worker_id,
            assignment.shard_ids.len(),
            assignment.shard_ids.iter().map(|s| s.0).collect::<Vec<_>>(),
        );
    }
    Ok(())
}

async fn run_execute(storage: PathBuf, args: BuildIndexWorkerArgs) -> Result<()> {
    let index_version_str = args
        .index_version
        .ok_or_else(|| anyhow::anyhow!("--index-version is required in execute mode"))?;
    let worker_id = args
        .worker_id
        .ok_or_else(|| anyhow::anyhow!("--worker-id is required in execute mode"))?;

    let store = LocalObjectStore::new(&storage)?;
    let index_ver = IndexVersion(index_version_str);

    // Load the worker plan from storage.
    let plan_key = shardlake_storage::paths::worker_plan_key(&index_ver.0);
    let plan_bytes = match store.get(&plan_key) {
        Ok(b) => b,
        Err(StorageError::NotFound(_)) => {
            return Err(anyhow::anyhow!(
                "Worker plan for index version '{}' not found; run `build-index-worker --mode plan` first",
                index_ver.0
            ));
        }
        Err(err) => return Err(err.into()),
    };
    let plan: WorkerPlan = serde_json::from_slice(&plan_bytes)?;

    let assignment = plan.assignment(worker_id).ok_or_else(|| {
        anyhow::anyhow!(
            "Worker ID {} is out of range; plan has {} workers (0..{})",
            worker_id,
            plan.num_workers,
            plan.num_workers.saturating_sub(1),
        )
    })?;

    // Load the dataset vectors.
    info!(dataset_version = %plan.dataset_version.0, "Loading vectors");
    let vecs_bytes = store.get(&plan.vectors_key)?;
    let reader = BufReader::new(vecs_bytes.as_slice());
    let mut records: Vec<VectorRecord> = Vec::new();
    for line_result in reader.lines() {
        let line: String = line_result?;
        if line.trim().is_empty() {
            continue;
        }
        let rec: VectorRecord = serde_json::from_str(&line)?;
        records.push(rec);
    }
    info!(
        records = records.len(),
        dims = plan.dims,
        worker_id,
        "Loaded vectors"
    );

    let builder = WorkerBuilder::new(&store);
    let output = builder.execute(&plan, assignment, &records)?;

    println!(
        "Worker {} done → {} shard(s) built for index_version={}",
        worker_id,
        output.shards.len(),
        plan.index_version,
    );
    for shard_out in &output.shards {
        println!(
            "  {} → {} vectors  fingerprint={}",
            shard_out.shard_id, shard_out.vector_count, shard_out.fingerprint
        );
    }
    Ok(())
}

async fn run_merge(storage: PathBuf, args: BuildIndexWorkerArgs) -> Result<()> {
    let index_version_str = args
        .index_version
        .ok_or_else(|| anyhow::anyhow!("--index-version is required in merge mode"))?;

    let store = LocalObjectStore::new(&storage)?;
    let index_ver = IndexVersion(index_version_str);

    // Load the worker plan.
    let plan_key = shardlake_storage::paths::worker_plan_key(&index_ver.0);
    let plan_bytes = match store.get(&plan_key) {
        Ok(b) => b,
        Err(StorageError::NotFound(_)) => {
            return Err(anyhow::anyhow!(
                "Worker plan for index version '{}' not found; run `build-index-worker --mode plan` first",
                index_ver.0
            ));
        }
        Err(err) => return Err(err.into()),
    };
    let plan: WorkerPlan = serde_json::from_slice(&plan_bytes)?;

    // Load all worker outputs.
    let mut outputs: Vec<WorkerOutput> = Vec::with_capacity(plan.num_workers);
    for worker_id in 0..plan.num_workers {
        let output_key = shardlake_storage::paths::worker_output_key(&index_ver.0, worker_id);
        let output_bytes = match store.get(&output_key) {
            Ok(b) => b,
            Err(StorageError::NotFound(_)) => {
                return Err(anyhow::anyhow!(
                    "Output for worker {} not found at '{}'; ensure all workers have completed",
                    worker_id,
                    output_key
                ));
            }
            Err(err) => return Err(err.into()),
        };
        let output: WorkerOutput = serde_json::from_slice(&output_bytes)?;
        outputs.push(output);
    }

    info!(
        workers = plan.num_workers,
        index_version = %index_ver.0,
        "Merging worker outputs into final manifest"
    );

    let merge_params = MergeParams {
        alias: args.alias,
        built_at: Utc::now(),
        builder_version: env!("CARGO_PKG_VERSION").to_string(),
        num_kmeans_iters: args.kmeans_iters,
        // Use the system default nprobe; the distributed plan does not capture
        // the original nprobe value, so the merge step records the default.
        nprobe_default: SystemConfig::default().nprobe,
        // Wall-clock duration is not tracked across distributed workers; record
        // zero to indicate this field was not measured for this build mode.
        build_duration_secs: 0.0,
    };

    let manifest =
        merge_worker_outputs(&plan, outputs, merge_params).map_err(anyhow::Error::new)?;

    manifest
        .save(&store)
        .map_err(|e| anyhow::anyhow!("failed to save manifest: {e}"))?;

    let manifest_key = shardlake_manifest::Manifest::storage_key(&manifest.index_version);
    info!(key = %manifest_key, "Manifest written");

    println!(
        "Merge complete → index_version={}  {} shard(s)  {} vectors total",
        manifest.index_version,
        manifest.shards.len(),
        manifest.total_vector_count,
    );
    for shard in &manifest.shards {
        println!(
            "  {} → {} vectors  fingerprint={}",
            shard.shard_id, shard.vector_count, shard.fingerprint
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use shardlake_manifest::{DatasetManifest, DATASET_MANIFEST_VERSION};
    use shardlake_storage::{paths, LocalObjectStore, ObjectStore};
    use tempfile::tempdir;

    use super::*;

    /// Write a minimal DatasetManifest and a single-record vectors file.
    fn write_test_dataset(store: &LocalObjectStore, dataset_version: &str, dims: usize) {
        use shardlake_core::types::{VectorId, VectorRecord};

        // Write vectors.jsonl – two clearly separated clusters.
        let vectors_key = paths::dataset_vectors_key(dataset_version);
        let metadata_key = paths::dataset_metadata_key(dataset_version);

        let mut lines = Vec::new();
        for i in 0..50u64 {
            let rec = VectorRecord {
                id: VectorId(i),
                data: vec![0.0f32; dims],
                metadata: None,
            };
            lines.push(serde_json::to_string(&rec).unwrap());
        }
        for i in 50..100u64 {
            let rec = VectorRecord {
                id: VectorId(i),
                data: vec![100.0f32; dims],
                metadata: None,
            };
            lines.push(serde_json::to_string(&rec).unwrap());
        }
        store
            .put(&vectors_key, lines.join("\n").into_bytes())
            .unwrap();
        store.put(&metadata_key, b"{}".to_vec()).unwrap();

        let dm = DatasetManifest {
            manifest_version: DATASET_MANIFEST_VERSION,
            dataset_version: shardlake_core::types::DatasetVersion(dataset_version.into()),
            embedding_version: shardlake_core::types::EmbeddingVersion(dataset_version.into()),
            dims: dims as u32,
            vector_count: 100,
            vectors_key,
            metadata_key,
            ingest_metadata: None,
        };
        dm.save(store).unwrap();
    }

    #[tokio::test]
    async fn plan_mode_writes_worker_plan_to_storage() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        write_test_dataset(&store, "ds-wtest", 2);

        run(
            tmp.path().to_path_buf(),
            BuildIndexWorkerArgs {
                mode: WorkerMode::Plan,
                dataset_version: Some("ds-wtest".into()),
                index_version: Some("idx-wtest".into()),
                embedding_version: None,
                metric: DistanceMetric::Euclidean,
                num_shards: 2,
                kmeans_iters: 20,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
                num_workers: 2,
                worker_id: None,
                alias: "latest".into(),
            },
        )
        .await
        .unwrap();

        let plan_key = paths::worker_plan_key("idx-wtest");
        let plan_bytes = store.get(&plan_key).unwrap();
        let plan: WorkerPlan = serde_json::from_slice(&plan_bytes).unwrap();
        assert_eq!(plan.index_version.0, "idx-wtest");
        assert!(!plan.shard_centroids.is_empty());
    }

    #[tokio::test]
    async fn execute_mode_builds_shards() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        write_test_dataset(&store, "ds-wexec", 2);

        // Plan first.
        run(
            tmp.path().to_path_buf(),
            BuildIndexWorkerArgs {
                mode: WorkerMode::Plan,
                dataset_version: Some("ds-wexec".into()),
                index_version: Some("idx-wexec".into()),
                embedding_version: None,
                metric: DistanceMetric::Euclidean,
                num_shards: 2,
                kmeans_iters: 20,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
                num_workers: 2,
                worker_id: None,
                alias: "latest".into(),
            },
        )
        .await
        .unwrap();

        // Load the plan to know how many workers were allocated.
        let plan_key = paths::worker_plan_key("idx-wexec");
        let plan_bytes = store.get(&plan_key).unwrap();
        let plan: WorkerPlan = serde_json::from_slice(&plan_bytes).unwrap();

        // Execute every worker.
        for w in 0..plan.num_workers {
            run(
                tmp.path().to_path_buf(),
                BuildIndexWorkerArgs {
                    mode: WorkerMode::Execute,
                    dataset_version: None,
                    index_version: Some("idx-wexec".into()),
                    embedding_version: None,
                    metric: DistanceMetric::Euclidean,
                    num_shards: 2,
                    kmeans_iters: 20,
                    kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                    kmeans_sample_size: None,
                    num_workers: 2,
                    worker_id: Some(w),
                    alias: "latest".into(),
                },
            )
            .await
            .unwrap();

            // Check output metadata was written.
            let output_key = paths::worker_output_key("idx-wexec", w);
            let raw = store.get(&output_key).unwrap();
            let loaded: shardlake_index::WorkerOutput = serde_json::from_slice(&raw).unwrap();
            assert_eq!(loaded.worker_id, w);
        }
    }

    #[tokio::test]
    async fn plan_mode_rejects_missing_dataset() {
        let tmp = tempdir().unwrap();
        let err = run(
            tmp.path().to_path_buf(),
            BuildIndexWorkerArgs {
                mode: WorkerMode::Plan,
                dataset_version: Some("missing-ds".into()),
                index_version: Some("idx-missing".into()),
                embedding_version: None,
                metric: DistanceMetric::Cosine,
                num_shards: 2,
                kmeans_iters: 5,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
                num_workers: 1,
                worker_id: None,
                alias: "latest".into(),
            },
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string().contains("not found"),
            "expected not-found error, got: {err}"
        );
    }

    #[tokio::test]
    async fn execute_mode_rejects_missing_plan() {
        let tmp = tempdir().unwrap();
        let err = run(
            tmp.path().to_path_buf(),
            BuildIndexWorkerArgs {
                mode: WorkerMode::Execute,
                dataset_version: None,
                index_version: Some("idx-no-plan".into()),
                embedding_version: None,
                metric: DistanceMetric::Cosine,
                num_shards: 2,
                kmeans_iters: 5,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
                num_workers: 1,
                worker_id: Some(0),
                alias: "latest".into(),
            },
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string().contains("not found"),
            "expected not-found error, got: {err}"
        );
    }

    #[tokio::test]
    async fn execute_mode_rejects_out_of_range_worker_id() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        write_test_dataset(&store, "ds-oor", 2);

        run(
            tmp.path().to_path_buf(),
            BuildIndexWorkerArgs {
                mode: WorkerMode::Plan,
                dataset_version: Some("ds-oor".into()),
                index_version: Some("idx-oor".into()),
                embedding_version: None,
                metric: DistanceMetric::Euclidean,
                num_shards: 2,
                kmeans_iters: 20,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
                num_workers: 2,
                worker_id: None,
                alias: "latest".into(),
            },
        )
        .await
        .unwrap();

        let err = run(
            tmp.path().to_path_buf(),
            BuildIndexWorkerArgs {
                mode: WorkerMode::Execute,
                dataset_version: None,
                index_version: Some("idx-oor".into()),
                embedding_version: None,
                metric: DistanceMetric::Cosine,
                num_shards: 2,
                kmeans_iters: 5,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
                num_workers: 2,
                worker_id: Some(999),
                alias: "latest".into(),
            },
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string().contains("out of range"),
            "expected out-of-range error, got: {err}"
        );
    }

    /// Helper: plan + execute all workers for a given index version.
    async fn plan_and_execute(
        store_path: &std::path::Path,
        dataset_version: &str,
        index_version: &str,
    ) -> WorkerPlan {
        let store = LocalObjectStore::new(store_path).unwrap();

        run(
            store_path.to_path_buf(),
            BuildIndexWorkerArgs {
                mode: WorkerMode::Plan,
                dataset_version: Some(dataset_version.into()),
                index_version: Some(index_version.into()),
                embedding_version: None,
                metric: DistanceMetric::Euclidean,
                num_shards: 2,
                kmeans_iters: 20,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
                num_workers: 2,
                worker_id: None,
                alias: "latest".into(),
            },
        )
        .await
        .unwrap();

        let plan_bytes = store.get(&paths::worker_plan_key(index_version)).unwrap();
        let plan: WorkerPlan = serde_json::from_slice(&plan_bytes).unwrap();

        for w in 0..plan.num_workers {
            run(
                store_path.to_path_buf(),
                BuildIndexWorkerArgs {
                    mode: WorkerMode::Execute,
                    dataset_version: None,
                    index_version: Some(index_version.into()),
                    embedding_version: None,
                    metric: DistanceMetric::Euclidean,
                    num_shards: 2,
                    kmeans_iters: 20,
                    kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                    kmeans_sample_size: None,
                    num_workers: 2,
                    worker_id: Some(w),
                    alias: "latest".into(),
                },
            )
            .await
            .unwrap();
        }

        plan
    }

    #[tokio::test]
    async fn merge_mode_writes_manifest() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        write_test_dataset(&store, "ds-merge-cli", 2);

        plan_and_execute(tmp.path(), "ds-merge-cli", "idx-merge-cli").await;

        run(
            tmp.path().to_path_buf(),
            BuildIndexWorkerArgs {
                mode: WorkerMode::Merge,
                dataset_version: None,
                index_version: Some("idx-merge-cli".into()),
                embedding_version: None,
                metric: DistanceMetric::Cosine,
                num_shards: 2,
                kmeans_iters: 20,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
                num_workers: 2,
                worker_id: None,
                alias: "latest".into(),
            },
        )
        .await
        .unwrap();

        // Manifest must be loadable.
        let manifest_key = paths::index_manifest_key("idx-merge-cli");
        let raw = store.get(&manifest_key).unwrap();
        let manifest: shardlake_manifest::Manifest = serde_json::from_slice(&raw).unwrap();
        assert_eq!(manifest.index_version.0, "idx-merge-cli");
        assert!(!manifest.shards.is_empty());
    }

    #[tokio::test]
    async fn merge_mode_rejects_missing_plan() {
        let tmp = tempdir().unwrap();
        let err = run(
            tmp.path().to_path_buf(),
            BuildIndexWorkerArgs {
                mode: WorkerMode::Merge,
                dataset_version: None,
                index_version: Some("idx-no-plan-merge".into()),
                embedding_version: None,
                metric: DistanceMetric::Cosine,
                num_shards: 2,
                kmeans_iters: 5,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
                num_workers: 1,
                worker_id: None,
                alias: "latest".into(),
            },
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string().contains("not found"),
            "expected not-found error, got: {err}"
        );
    }

    #[tokio::test]
    async fn merge_mode_rejects_missing_index_version_arg() {
        let tmp = tempdir().unwrap();
        let err = run(
            tmp.path().to_path_buf(),
            BuildIndexWorkerArgs {
                mode: WorkerMode::Merge,
                dataset_version: None,
                index_version: None, // missing
                embedding_version: None,
                metric: DistanceMetric::Cosine,
                num_shards: 2,
                kmeans_iters: 5,
                kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
                kmeans_sample_size: None,
                num_workers: 1,
                worker_id: None,
                alias: "latest".into(),
            },
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string().contains("--index-version"),
            "expected missing-arg error, got: {err}"
        );
    }
}
