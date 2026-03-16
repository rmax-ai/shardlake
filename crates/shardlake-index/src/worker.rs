//! Distributed build worker contract.
//!
//! An index build can be distributed across multiple workers by first running
//! a **plan** step that trains the IVF coarse quantizer and partitions shard
//! assignments across workers, then running each worker independently to build
//! its assigned shards.
//!
//! # Phases
//!
//! 1. **Plan** – [`plan_workers`] trains the IVF quantizer, assigns vectors to
//!    shards, partitions the non-empty shards evenly across `num_workers`
//!    workers, and returns a [`WorkerPlan`].  The plan and the coarse-quantizer
//!    artifact are written to storage so that workers can locate their inputs.
//!
//! 2. **Execute** – Each worker calls [`WorkerBuilder::execute`] with the
//!    [`WorkerPlan`] and its [`WorkerAssignment`].  The worker uses the plan's
//!    shared centroids to assign each dataset vector to a shard globally, then
//!    builds only the shards it owns and returns one [`WorkerShardOutput`] per
//!    built shard.  A [`WorkerOutput`] containing all shard descriptors is also
//!    written to storage.
//!
//! 3. **Merge** – A coordinator collects all [`WorkerOutput`]
//!    descriptors and assembles the final [`shardlake_manifest::Manifest`]
//!    by calling [`merge_worker_outputs`].
//!
//! # Reproducibility
//!
//! Given the same dataset, the same [`shardlake_core::config::SystemConfig`],
//! and the same `num_workers`, [`plan_workers`] always produces the same shard
//! assignments and centroid layout.  Workers therefore always produce identical
//! artifact bytes and fingerprints for the same inputs.

use chrono::{DateTime, Utc};
use rand::{seq::SliceRandom, SeedableRng};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use shardlake_core::{
    config::SystemConfig,
    types::{
        AnnFamily, DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, ShardId,
        VectorRecord,
    },
};
use shardlake_manifest::{
    AlgorithmMetadata, BuildMetadata, CompressionConfig, Manifest, RoutingMetadata, ShardDef,
    ShardSummary,
};
use shardlake_storage::ObjectStore;

use crate::{ivf::IvfQuantizer, plugin::HnswConfig, shard::ShardIndex, IndexError, Result};

// ─── Plan types ─────────────────────────────────────────────────────────────

/// Parameters required to produce a [`WorkerPlan`] via [`plan_workers`].
pub struct WorkerPlanParams {
    /// Index version being built.
    pub index_version: IndexVersion,
    /// Source dataset version.
    pub dataset_version: DatasetVersion,
    /// Embedding version to record in the manifest.
    pub embedding_version: EmbeddingVersion,
    /// Distance metric.
    pub metric: DistanceMetric,
    /// Vector dimensionality.
    pub dims: usize,
    /// Storage key of the dataset vectors JSONL file.
    pub vectors_key: String,
    /// Storage key of the dataset metadata JSON file.
    pub metadata_key: String,
    /// Number of workers to distribute shards across.
    ///
    /// Clamped to the number of non-empty shards when it exceeds that value,
    /// so callers may safely pass values larger than the actual shard count.
    pub num_workers: usize,
    /// ANN algorithm family to record in the merged manifest.
    pub ann_family: Option<AnnFamily>,
    /// Optional HNSW parameters to persist when `ann_family` is HNSW.
    pub hnsw_config: Option<HnswConfig>,
}

/// The portion of work assigned to one build worker.
///
/// Workers use [`WorkerPlan::shard_centroids`] together with these shard IDs
/// to determine which dataset vectors belong to their shards, without
/// reloading the coarse-quantizer artifact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerAssignment {
    /// Zero-based worker index.
    pub worker_id: usize,
    /// Total number of workers in this plan.
    pub num_workers: usize,
    /// Shard IDs this worker is responsible for building (ascending order).
    pub shard_ids: Vec<ShardId>,
}

/// Full plan for distributing an index build across workers.
///
/// Produced by [`plan_workers`] and stored at
/// [`shardlake_storage::paths::worker_plan_key`] so that individual workers
/// can load their assignment by indexing into [`WorkerPlan::assignments`].
///
/// The `shard_centroids` field holds one centroid per non-empty shard (ordered
/// by ascending [`ShardId`]).  Workers use these centroids together with
/// [`IvfQuantizer::from_centroids`] to assign each dataset vector to its
/// nearest shard and then filter to only the shards they own.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPlan {
    /// Index version being built.
    pub index_version: IndexVersion,
    /// Source dataset version.
    pub dataset_version: DatasetVersion,
    /// Embedding version to record in the manifest.
    pub embedding_version: EmbeddingVersion,
    /// Distance metric.
    pub metric: DistanceMetric,
    /// Vector dimensionality.
    pub dims: usize,
    /// Storage key of the dataset vectors JSONL file.
    pub vectors_key: String,
    /// Storage key of the dataset metadata JSON file.
    pub metadata_key: String,
    /// Number of workers actually used (may be less than requested when there
    /// are fewer non-empty shards than requested workers).
    pub num_workers: usize,
    /// Number of K-means iterations used during planning.
    #[serde(default = "default_kmeans_iters")]
    pub kmeans_iters: u32,
    /// RNG seed used for K-means centroid initialisation.
    #[serde(default = "default_kmeans_seed")]
    pub kmeans_seed: u64,
    /// Effective bounded sample size used during centroid training, when
    /// sampling actually occurred.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kmeans_sample_size: Option<u32>,
    /// Default `nprobe` value to record in the final manifest.
    #[serde(default = "default_nprobe")]
    pub nprobe_default: u32,
    /// Storage key of the trained IVF coarse-quantizer artifact.
    pub coarse_quantizer_key: String,
    /// ANN algorithm family to record in the merged manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann_family: Option<AnnFamily>,
    /// Optional HNSW parameters to persist when `ann_family` is HNSW.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hnsw_config: Option<HnswConfig>,
    /// All non-empty shard centroids, one per shard, in `ShardId` order.
    ///
    /// Inline in the plan so workers can reconstruct the [`IvfQuantizer`] for
    /// global vector assignment without loading the coarse-quantizer artifact.
    pub shard_centroids: Vec<Vec<f32>>,
    /// Per-worker assignments, indexed by `worker_id`.
    pub assignments: Vec<WorkerAssignment>,
}

impl WorkerPlan {
    /// Return the assignment for `worker_id`, or `None` if out of range.
    #[must_use]
    pub fn assignment(&self, worker_id: usize) -> Option<&WorkerAssignment> {
        self.assignments.get(worker_id)
    }
}

fn default_kmeans_iters() -> u32 {
    20
}

fn default_kmeans_seed() -> u64 {
    shardlake_core::config::DEFAULT_KMEANS_SEED
}

fn default_nprobe() -> u32 {
    SystemConfig::default().nprobe
}

// ─── Output types ────────────────────────────────────────────────────────────

/// Metadata about a single shard artifact emitted by a worker.
///
/// The merge step collects these descriptors from every worker's
/// [`WorkerOutput`] to assemble the final [`shardlake_manifest::Manifest`]
/// without reading the shard artifact bytes again.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerShardOutput {
    /// Shard identifier.
    pub shard_id: ShardId,
    /// Storage key of the shard artifact written by this worker.
    pub artifact_key: String,
    /// Number of vectors stored in this shard.
    pub vector_count: u64,
    /// FNV-1a fingerprint of the artifact bytes (16-digit lowercase hex).
    pub fingerprint: String,
    /// Centroid of this shard's Voronoi cell.
    pub centroid: Vec<f32>,
    /// Worker that built this shard.
    pub worker_id: usize,
}

/// All shard outputs produced by one worker in a single execution.
///
/// Written to [`shardlake_storage::paths::worker_output_key`] by
/// [`WorkerBuilder::execute`] so that the merge step can discover all
/// intermediate artifacts without scanning storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerOutput {
    /// Worker that produced these outputs.
    pub worker_id: usize,
    /// Index version these outputs belong to.
    pub index_version: IndexVersion,
    /// One entry per shard built by this worker.
    pub shards: Vec<WorkerShardOutput>,
}

// ─── Merge ───────────────────────────────────────────────────────────────────

/// Parameters required to finalise a distributed build via
/// [`merge_worker_outputs`].
pub struct MergeParams {
    /// Alias name to record in the manifest (e.g. `"latest"`).
    pub alias: String,
    /// Timestamp to record as the build time in the manifest.
    pub built_at: DateTime<Utc>,
    /// Crate version string recorded in [`BuildMetadata::builder_version`].
    pub builder_version: String,
    /// Wall-clock duration of the full distributed build in seconds.
    pub build_duration_secs: f64,
}

/// Assemble the final [`Manifest`] from all worker outputs produced by a
/// distributed build.
///
/// The merge step is the third and final phase of a distributed build:
///
/// 1. **Plan** – [`plan_workers`] trains the IVF quantizer and partitions shard
///    assignments.
/// 2. **Execute** – Each worker calls [`WorkerBuilder::execute`] and writes
///    shard artifacts and an `output.json` descriptor.
/// 3. **Merge** – This function collects all [`WorkerOutput`] descriptors and
///    assembles the final [`Manifest`], which can then be saved to storage.
///
/// # Determinism
///
/// Shards are sorted by [`ShardId`] ascending before being written into the
/// manifest, so equivalent sets of worker outputs always produce an identical
/// manifest regardless of the order in which `outputs` are supplied.
///
/// # Errors
///
/// Returns [`IndexError::Other`] when any of the following validation checks
/// fail:
///
/// - `outputs` is empty.
/// - A [`WorkerOutput`] has a [`WorkerOutput::index_version`] that does not
///   match the plan's index version.
/// - Two outputs share the same `worker_id` (duplicate worker).
/// - A worker output's `worker_id` is out of range for the plan.
/// - A [`WorkerShardOutput`] names a different `worker_id` than its parent
///   [`WorkerOutput`].
/// - The set of shard IDs across all outputs does not exactly match the set
///   of shard IDs in the plan (missing or unexpected shard).
/// - A shard ID appears more than once across all outputs (duplicate shard).
pub fn merge_worker_outputs(
    plan: &WorkerPlan,
    outputs: Vec<WorkerOutput>,
    params: MergeParams,
) -> Result<Manifest> {
    if outputs.is_empty() {
        return Err(IndexError::Other(
            "merge requires at least one worker output".into(),
        ));
    }

    // ── Validate index version consistency ──────────────────────────────────
    for output in &outputs {
        if output.index_version != plan.index_version {
            return Err(IndexError::Other(format!(
                "worker {} output index_version '{}' does not match plan index_version '{}'",
                output.worker_id, output.index_version, plan.index_version
            )));
        }
    }

    // ── Check for duplicate worker IDs ──────────────────────────────────────
    let mut seen_workers: std::collections::HashSet<usize> =
        std::collections::HashSet::with_capacity(outputs.len());
    for output in &outputs {
        if !seen_workers.insert(output.worker_id) {
            return Err(IndexError::Other(format!(
                "duplicate worker output for worker_id {}",
                output.worker_id
            )));
        }
    }

    // ── Check worker IDs are within range ───────────────────────────────────
    for output in &outputs {
        if output.worker_id >= plan.num_workers {
            return Err(IndexError::Other(format!(
                "worker output worker_id {} is out of range for plan with {} workers",
                output.worker_id, plan.num_workers
            )));
        }
    }

    // ── Check for missing workers ────────────────────────────────────────────
    for expected_id in 0..plan.num_workers {
        if !seen_workers.contains(&expected_id) {
            return Err(IndexError::Other(format!(
                "missing output for worker_id {expected_id}"
            )));
        }
    }

    // ── Collect and validate all shard outputs ───────────────────────────────
    let total_shards: usize = plan.shard_centroids.len();

    // Flatten all shard outputs and check for duplicates.
    let mut all_shards: Vec<WorkerShardOutput> = Vec::with_capacity(total_shards);
    let mut seen_shard_ids: std::collections::HashSet<u32> =
        std::collections::HashSet::with_capacity(total_shards);

    for output in outputs {
        for shard_out in output.shards {
            if shard_out.worker_id != output.worker_id {
                return Err(IndexError::Other(format!(
                    "shard_id {} reports worker_id {} but parent output is worker_id {}",
                    shard_out.shard_id.0, shard_out.worker_id, output.worker_id
                )));
            }
            if !seen_shard_ids.insert(shard_out.shard_id.0) {
                return Err(IndexError::Other(format!(
                    "duplicate shard_id {} in worker outputs",
                    shard_out.shard_id.0
                )));
            }
            // Validate the shard id is within the range declared by the plan.
            if shard_out.shard_id.0 as usize >= total_shards {
                return Err(IndexError::Other(format!(
                    "shard_id {} is out of range; plan has {} shards",
                    shard_out.shard_id.0, total_shards
                )));
            }
            all_shards.push(shard_out);
        }
    }

    // ── Validate full shard coverage ─────────────────────────────────────────
    for expected_shard in 0..total_shards as u32 {
        if !seen_shard_ids.contains(&expected_shard) {
            return Err(IndexError::Other(format!(
                "shard_id {expected_shard} is present in the plan but missing from worker outputs"
            )));
        }
    }

    // ── Sort shards deterministically by shard_id ────────────────────────────
    all_shards.sort_by_key(|s| s.shard_id.0);

    // ── Assemble ShardDefs ────────────────────────────────────────────────────
    let shard_defs: Vec<ShardDef> = all_shards
        .iter()
        .map(|s| {
            let file_location = s.artifact_key.clone();
            ShardDef {
                shard_id: s.shard_id,
                artifact_key: s.artifact_key.clone(),
                vector_count: s.vector_count,
                fingerprint: s.fingerprint.clone(),
                centroid: s.centroid.clone(),
                routing: Some(RoutingMetadata {
                    centroid_id: format!("shard-{:04}", s.shard_id.0),
                    index_type: "flat".into(),
                    file_location,
                }),
            }
        })
        .collect();

    let total_vector_count: u64 = shard_defs.iter().map(|s| s.vector_count).sum();

    let shard_summary = if shard_defs.is_empty() {
        None
    } else {
        let min_count = shard_defs.iter().map(|s| s.vector_count).min().unwrap_or(0);
        let max_count = shard_defs.iter().map(|s| s.vector_count).max().unwrap_or(0);
        Some(ShardSummary {
            num_shards: shard_defs.len() as u32,
            min_shard_vector_count: min_count,
            max_shard_vector_count: max_count,
        })
    };

    let mut algo_params = std::collections::BTreeMap::new();
    algo_params.insert("num_clusters".into(), serde_json::json!(total_shards));
    algo_params.insert("num_shards".into(), serde_json::json!(total_shards));
    algo_params.insert("kmeans_iters".into(), serde_json::json!(plan.kmeans_iters));
    algo_params.insert("kmeans_seed".into(), serde_json::json!(plan.kmeans_seed));
    if let Some(sample_size) = plan.kmeans_sample_size {
        algo_params.insert("kmeans_sample_size".into(), serde_json::json!(sample_size));
    }

    let algorithm_name = match plan.ann_family {
        Some(AnnFamily::Hnsw) => "hnsw",
        Some(AnnFamily::DiskAnn) => "diskann",
        _ => "ivf-flat",
    };
    if matches!(plan.ann_family, Some(AnnFamily::Hnsw)) {
        let hnsw = plan.hnsw_config.clone().unwrap_or_default();
        hnsw.validate()
            .map_err(|e| IndexError::Other(e.to_string()))?;
        algo_params.insert("hnsw_m".into(), serde_json::json!(hnsw.m));
        algo_params.insert(
            "hnsw_ef_construction".into(),
            serde_json::json!(hnsw.ef_construction),
        );
        algo_params.insert("hnsw_ef_search".into(), serde_json::json!(hnsw.ef_search));
    }

    let manifest = Manifest {
        manifest_version: 4,
        dataset_version: plan.dataset_version.clone(),
        embedding_version: plan.embedding_version.clone(),
        index_version: plan.index_version.clone(),
        alias: params.alias,
        dims: plan.dims as u32,
        distance_metric: plan.metric,
        vectors_key: plan.vectors_key.clone(),
        metadata_key: plan.metadata_key.clone(),
        total_vector_count,
        shards: shard_defs,
        build_metadata: BuildMetadata {
            built_at: params.built_at,
            builder_version: params.builder_version,
            num_kmeans_iters: plan.kmeans_iters,
            nprobe_default: plan.nprobe_default,
            build_duration_secs: params.build_duration_secs,
        },
        algorithm: AlgorithmMetadata {
            algorithm: algorithm_name.into(),
            variant: None,
            params: algo_params,
        },
        shard_summary,
        compression: CompressionConfig::default(),
        recall_estimate: None,
        coarse_quantizer_key: Some(plan.coarse_quantizer_key.clone()),
        lexical: None,
    };

    Ok(manifest)
}

// ─── Planning ────────────────────────────────────────────────────────────────

/// Plan and distribute an index build across workers.
///
/// Steps:
///
/// 1. Train an IVF coarse quantizer from `records` using the settings in
///    `config` (same training path as [`crate::builder::IndexBuilder`]).
/// 2. Assign each record to its nearest centroid.
/// 3. Compact away empty clusters.
/// 4. Partition non-empty shard IDs round-robin across `params.num_workers`.
/// 5. Persist the trained quantizer to storage at
///    [`shardlake_storage::paths::index_coarse_quantizer_key`].
/// 6. Return the [`WorkerPlan`].
///
/// The caller is responsible for serialising the returned plan and storing it
/// at [`shardlake_storage::paths::worker_plan_key`] so that workers can load
/// it by index version.
///
/// # Errors
///
/// Returns [`IndexError::Other`] when:
/// - `records` is empty.
/// - `config.num_shards` is `0`.
/// - `params.num_workers` is `0`.
/// - `params.dims` is `0`.
/// - Any record's vector length does not match `params.dims`.
/// - `config.kmeans_sample_size` is `Some(0)`.
pub fn plan_workers(
    store: &dyn ObjectStore,
    config: &SystemConfig,
    records: &[VectorRecord],
    params: WorkerPlanParams,
) -> Result<WorkerPlan> {
    if records.is_empty() {
        return Err(IndexError::Other("no records to plan".into()));
    }
    if config.num_shards == 0 {
        return Err(IndexError::Other(
            "num_shards must be greater than 0".into(),
        ));
    }
    if params.num_workers == 0 {
        return Err(IndexError::Other(
            "num_workers must be greater than 0".into(),
        ));
    }
    if params.dims == 0 {
        return Err(IndexError::Other("dims must be greater than 0".into()));
    }
    for record in records {
        if record.data.len() != params.dims {
            return Err(IndexError::Other(format!(
                "record {} has dimension mismatch: expected {}, got {}",
                record.id,
                params.dims,
                record.data.len()
            )));
        }
    }

    let k = config.num_shards as usize;
    let iters = config.kmeans_iters;
    let mut rng = rand::rngs::StdRng::seed_from_u64(config.kmeans_seed);
    let vecs: Vec<Vec<f32>> = records.iter().map(|r| r.data.clone()).collect();

    // Optionally sample a subset for centroid training (same logic as IndexBuilder).
    let (sampled, effective_sample_size): (Option<Vec<Vec<f32>>>, Option<u32>) =
        match config.kmeans_sample_size {
            Some(0) => {
                return Err(IndexError::Other(
                    "kmeans_sample_size must be greater than 0".into(),
                ))
            }
            Some(max_samples) => {
                let sample_size = (max_samples as usize).min(vecs.len());
                if sample_size >= vecs.len() {
                    (None, None)
                } else {
                    let mut indices: Vec<usize> = (0..vecs.len()).collect();
                    let (shuffled, _) = indices.partial_shuffle(&mut rng, sample_size);
                    (
                        Some(shuffled.iter().map(|&i| vecs[i].clone()).collect()),
                        Some(sample_size as u32),
                    )
                }
            }
            None => (None, None),
        };
    let training_vecs: &[Vec<f32>] = sampled.as_deref().unwrap_or(&vecs);

    info!(
        n = records.len(),
        k, iters, "Worker plan: training IVF coarse quantizer"
    );
    let quantizer = IvfQuantizer::train(training_vecs, k, iters, &mut rng);

    // Assign each vector to its nearest cluster.
    let mut shard_buckets: Vec<Vec<usize>> = vec![Vec::new(); quantizer.num_clusters()];
    for (idx, rec) in records.iter().enumerate() {
        let cluster = quantizer.assign(&rec.data);
        shard_buckets[cluster].push(idx);
    }

    // Compact away empty clusters.
    let non_empty: Vec<usize> = shard_buckets
        .iter()
        .enumerate()
        .filter(|(_, bucket)| !bucket.is_empty())
        .map(|(cluster_idx, _)| cluster_idx)
        .collect();

    if non_empty.is_empty() {
        return Err(IndexError::Other(
            "IVF planning produced no non-empty shards".into(),
        ));
    }
    if non_empty.len() != quantizer.num_clusters() {
        warn!(
            requested_clusters = quantizer.num_clusters(),
            retained_clusters = non_empty.len(),
            "Worker plan: compacting empty IVF clusters"
        );
    }

    // Build the compacted centroid list (one per non-empty cluster).
    let shard_centroids: Vec<Vec<f32>> = non_empty
        .iter()
        .map(|&cluster_idx| quantizer.centroids()[cluster_idx].clone())
        .collect();

    let num_shards = shard_centroids.len();
    // Clamp num_workers to the actual number of shards.
    let num_workers = params.num_workers.min(num_shards);

    // Persist the compacted quantizer.
    let compacted_quantizer = IvfQuantizer::from_centroids(shard_centroids.clone());
    let cq_key = shardlake_storage::paths::index_coarse_quantizer_key(&params.index_version.0);
    let cq_bytes = compacted_quantizer.to_bytes()?;
    store.put(&cq_key, cq_bytes)?;
    info!(
        key = %cq_key,
        clusters = num_shards,
        "Worker plan: coarse quantizer written"
    );

    // Distribute shards round-robin across workers.
    let mut assignments: Vec<WorkerAssignment> = (0..num_workers)
        .map(|worker_id| WorkerAssignment {
            worker_id,
            num_workers,
            shard_ids: Vec::new(),
        })
        .collect();
    for shard_idx in 0..num_shards {
        let worker_id = shard_idx % num_workers;
        assignments[worker_id]
            .shard_ids
            .push(ShardId(shard_idx as u32));
    }

    info!(
        num_shards,
        num_workers, "Worker plan: shards partitioned across workers"
    );

    Ok(WorkerPlan {
        index_version: params.index_version,
        dataset_version: params.dataset_version,
        embedding_version: params.embedding_version,
        metric: params.metric,
        dims: params.dims,
        vectors_key: params.vectors_key,
        metadata_key: params.metadata_key,
        num_workers,
        kmeans_iters: config.kmeans_iters,
        kmeans_seed: config.kmeans_seed,
        kmeans_sample_size: effective_sample_size,
        nprobe_default: config.nprobe,
        coarse_quantizer_key: cq_key,
        ann_family: params.ann_family,
        hnsw_config: params.hnsw_config,
        shard_centroids,
        assignments,
    })
}

// ─── Worker execution ────────────────────────────────────────────────────────

/// Executes the shard build for one worker's assignment.
///
/// # Examples
///
/// ```ignore
/// use shardlake_index::worker::{WorkerBuilder, WorkerPlan};
///
/// // Load plan from storage (omitted for brevity).
/// let plan: WorkerPlan = /* ... */;
/// let assignment = plan.assignment(0).expect("worker 0 not in plan");
///
/// let builder = WorkerBuilder::new(&store);
/// let output = builder.execute(&plan, assignment, &records)?;
/// println!("Worker 0 built {} shards", output.shards.len());
/// ```
pub struct WorkerBuilder<'a> {
    store: &'a dyn ObjectStore,
}

impl<'a> WorkerBuilder<'a> {
    /// Create a new worker builder backed by `store`.
    #[must_use]
    pub fn new(store: &'a dyn ObjectStore) -> Self {
        Self { store }
    }

    /// Build the shards assigned to this worker and write intermediate outputs.
    ///
    /// Steps:
    ///
    /// 1. Reconstruct the IVF quantizer from `plan.shard_centroids`.
    /// 2. Assign each record in `records` to a shard globally.
    /// 3. Build shard artifacts for the shards in `assignment.shard_ids`.
    /// 4. Write each shard artifact to storage.
    /// 5. Collect and write a [`WorkerOutput`] to
    ///    [`shardlake_storage::paths::worker_output_key`].
    /// 6. Return the [`WorkerOutput`].
    ///
    /// # Errors
    ///
    /// Returns [`IndexError`] when storage writes fail or when shard
    /// serialisation fails.
    pub fn execute(
        &self,
        plan: &WorkerPlan,
        assignment: &WorkerAssignment,
        records: &[VectorRecord],
    ) -> Result<WorkerOutput> {
        Self::validate_plan(plan)?;
        Self::validate_assignment(plan, assignment)?;
        Self::validate_records(plan, records)?;

        // Rebuild the quantizer from the plan's inline centroids so the
        // worker can assign vectors to shards without loading the .cq file.
        let quantizer = IvfQuantizer::from_centroids(plan.shard_centroids.clone());

        // Build a fast lookup set of the shard IDs owned by this worker.
        let owned: std::collections::HashSet<u32> =
            assignment.shard_ids.iter().map(|s| s.0).collect();

        // Assign all records to their nearest shard and collect only those
        // that belong to shards owned by this worker.
        let num_owned = assignment.shard_ids.len();
        let shard_id_to_local: std::collections::HashMap<u32, usize> = assignment
            .shard_ids
            .iter()
            .enumerate()
            .map(|(local_idx, shard_id)| (shard_id.0, local_idx))
            .collect();

        let mut shard_records: Vec<Vec<&VectorRecord>> = vec![Vec::new(); num_owned];
        for rec in records {
            let global_shard = quantizer.assign(&rec.data) as u32;
            if owned.contains(&global_shard) {
                let local_idx = shard_id_to_local[&global_shard];
                shard_records[local_idx].push(rec);
            }
        }

        let mut shard_outputs: Vec<WorkerShardOutput> = Vec::with_capacity(num_owned);

        for (local_idx, shard_id) in assignment.shard_ids.iter().enumerate() {
            let recs = &shard_records[local_idx];
            let centroid = plan.shard_centroids[shard_id.0 as usize].clone();

            if recs.is_empty() {
                warn!(
                    shard = %shard_id,
                    worker_id = assignment.worker_id,
                    "Worker: shard is empty after vector assignment"
                );
            }

            let artifact_key =
                shardlake_storage::paths::index_shard_key(&plan.index_version.0, shard_id.0);

            let owned_recs: Vec<VectorRecord> = recs.iter().map(|r| (*r).clone()).collect();
            let count = owned_recs.len() as u64;

            let idx = ShardIndex {
                shard_id: *shard_id,
                dims: plan.dims,
                centroids: vec![centroid.clone()],
                records: owned_recs,
            };
            let bytes = idx.to_bytes()?;
            let fingerprint = crate::artifact_fingerprint(&bytes);
            self.store.put(&artifact_key, bytes)?;

            info!(
                shard = %shard_id,
                vectors = count,
                key = %artifact_key,
                worker_id = assignment.worker_id,
                "Worker: shard written"
            );

            shard_outputs.push(WorkerShardOutput {
                shard_id: *shard_id,
                artifact_key,
                vector_count: count,
                fingerprint,
                centroid,
                worker_id: assignment.worker_id,
            });
        }

        let worker_output = WorkerOutput {
            worker_id: assignment.worker_id,
            index_version: plan.index_version.clone(),
            shards: shard_outputs,
        };

        // Persist output metadata so the merge step can discover it.
        let output_key = shardlake_storage::paths::worker_output_key(
            &plan.index_version.0,
            assignment.worker_id,
        );
        let output_bytes = serde_json::to_vec(&worker_output)
            .map_err(|e| IndexError::Other(format!("failed to serialise worker output: {e}")))?;
        self.store.put(&output_key, output_bytes)?;
        info!(
            key = %output_key,
            shards = worker_output.shards.len(),
            worker_id = assignment.worker_id,
            "Worker: output metadata written"
        );

        Ok(worker_output)
    }

    fn validate_plan(plan: &WorkerPlan) -> Result<()> {
        if plan.dims == 0 {
            return Err(IndexError::Other("plan dims must be greater than 0".into()));
        }
        if plan.num_workers == 0 {
            return Err(IndexError::Other(
                "plan num_workers must be greater than 0".into(),
            ));
        }
        if plan.assignments.len() != plan.num_workers {
            return Err(IndexError::Other(format!(
                "plan assignments length mismatch: expected {}, got {}",
                plan.num_workers,
                plan.assignments.len()
            )));
        }
        if plan.shard_centroids.is_empty() {
            return Err(IndexError::Other(
                "plan must contain at least one shard centroid".into(),
            ));
        }
        for (idx, centroid) in plan.shard_centroids.iter().enumerate() {
            if centroid.len() != plan.dims {
                return Err(IndexError::Other(format!(
                    "shard centroid {} has dimension mismatch: expected {}, got {}",
                    idx,
                    plan.dims,
                    centroid.len()
                )));
            }
        }
        Ok(())
    }

    fn validate_assignment(plan: &WorkerPlan, assignment: &WorkerAssignment) -> Result<()> {
        if assignment.num_workers != plan.num_workers {
            return Err(IndexError::Other(format!(
                "worker assignment num_workers mismatch: expected {}, got {}",
                plan.num_workers, assignment.num_workers
            )));
        }
        if assignment.worker_id >= plan.num_workers {
            return Err(IndexError::Other(format!(
                "worker assignment {} is out of range for plan with {} workers",
                assignment.worker_id, plan.num_workers
            )));
        }
        let mut seen_shards = std::collections::HashSet::new();
        let mut previous = None;
        for shard_id in &assignment.shard_ids {
            let shard_idx = shard_id.0 as usize;
            if shard_idx >= plan.shard_centroids.len() {
                return Err(IndexError::Other(format!(
                    "worker assignment {} references invalid shard id {} (plan has {} shard centroids)",
                    assignment.worker_id,
                    shard_id.0,
                    plan.shard_centroids.len()
                )));
            }
            if !seen_shards.insert(shard_id.0) {
                return Err(IndexError::Other(format!(
                    "worker assignment {} contains duplicate shard id {}",
                    assignment.worker_id, shard_id.0
                )));
            }
            if let Some(previous_shard_id) = previous {
                if shard_id.0 <= previous_shard_id {
                    return Err(IndexError::Other(format!(
                        "worker assignment {} shard ids must be strictly ascending",
                        assignment.worker_id
                    )));
                }
            }
            previous = Some(shard_id.0);
        }

        match plan.assignments.get(assignment.worker_id) {
            Some(expected) if expected == assignment => {}
            Some(_) => {
                return Err(IndexError::Other(format!(
                    "worker assignment {} does not match plan entry",
                    assignment.worker_id
                )));
            }
            None => {
                return Err(IndexError::Other(format!(
                    "worker assignment {} missing from plan",
                    assignment.worker_id
                )));
            }
        }

        Ok(())
    }

    fn validate_records(plan: &WorkerPlan, records: &[VectorRecord]) -> Result<()> {
        for record in records {
            if record.data.len() != plan.dims {
                return Err(IndexError::Other(format!(
                    "record {} has dimension mismatch: expected {}, got {}",
                    record.id,
                    plan.dims,
                    record.data.len()
                )));
            }
        }
        Ok(())
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use shardlake_core::types::VectorId;
    use shardlake_storage::LocalObjectStore;

    fn record(id: u64, data: Vec<f32>) -> VectorRecord {
        VectorRecord {
            id: VectorId(id),
            data,
            metadata: None,
        }
    }

    /// Build a small dataset spread across two obvious clusters in 2-D.
    fn two_cluster_records() -> Vec<VectorRecord> {
        // 50 records per cluster (more than k=2) so k-means reliably finds both.
        let mut recs: Vec<VectorRecord> = (0..50).map(|i| record(i, vec![0.0f32, 0.0])).collect();
        recs.extend((50..100).map(|i| record(i, vec![100.0f32, 100.0])));
        recs
    }

    fn default_config(tmp: &std::path::Path, num_shards: u32) -> SystemConfig {
        SystemConfig {
            storage_root: tmp.to_path_buf(),
            num_shards,
            kmeans_iters: 20,
            nprobe: 1,
            // Seed that reliably separates two well-separated clusters.
            kmeans_seed: 0xdead_beef,
            kmeans_sample_size: None,
            ..SystemConfig::default()
        }
    }

    fn plan_params(index_version: &str) -> WorkerPlanParams {
        WorkerPlanParams {
            index_version: IndexVersion(index_version.into()),
            dataset_version: DatasetVersion("ds-worker-test".into()),
            embedding_version: EmbeddingVersion("emb-worker-test".into()),
            metric: DistanceMetric::Euclidean,
            dims: 2,
            vectors_key: shardlake_storage::paths::dataset_vectors_key("ds-worker-test"),
            metadata_key: shardlake_storage::paths::dataset_metadata_key("ds-worker-test"),
            num_workers: 2,
            ann_family: None,
            hnsw_config: None,
        }
    }

    // ── plan_workers ───────────────────────────────────────────────────────

    #[test]
    fn plan_workers_rejects_empty_records() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = default_config(tmp.path(), 2);
        let err = plan_workers(&store, &config, &[], plan_params("idx-err-empty")).unwrap_err();
        assert!(err.to_string().contains("no records to plan"));
    }

    #[test]
    fn plan_workers_rejects_zero_num_shards() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = default_config(tmp.path(), 0);
        let err = plan_workers(
            &store,
            &config,
            &two_cluster_records(),
            plan_params("idx-err-shards"),
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("num_shards must be greater than 0"));
    }

    #[test]
    fn plan_workers_rejects_zero_num_workers() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = default_config(tmp.path(), 2);
        let mut params = plan_params("idx-err-workers");
        params.num_workers = 0;
        let err = plan_workers(&store, &config, &two_cluster_records(), params).unwrap_err();
        assert!(err
            .to_string()
            .contains("num_workers must be greater than 0"));
    }

    #[test]
    fn plan_workers_rejects_dimension_mismatch() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = default_config(tmp.path(), 2);
        let records = vec![
            record(0, vec![1.0f32, 2.0]),
            record(1, vec![1.0f32, 2.0, 3.0]), // wrong dims
        ];
        let err = plan_workers(&store, &config, &records, plan_params("idx-err-dims")).unwrap_err();
        assert!(err.to_string().contains("dimension mismatch"));
    }

    #[test]
    fn plan_workers_produces_correct_structure() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = default_config(tmp.path(), 2);
        let records = two_cluster_records();

        let plan = plan_workers(&store, &config, &records, plan_params("idx-plan-ok")).unwrap();

        assert!(plan.num_workers >= 1, "must have at least one worker");
        assert_eq!(plan.assignments.len(), plan.num_workers);

        // Shard centroids count must match what the assignments reference.
        let max_shard_id = plan
            .assignments
            .iter()
            .flat_map(|a| a.shard_ids.iter().map(|s| s.0 as usize))
            .max()
            .unwrap_or(0);
        assert!(
            plan.shard_centroids.len() > max_shard_id,
            "shard_centroids must cover all shard ids"
        );

        // Each shard ID must appear exactly once across all assignments.
        let mut all_shard_ids: Vec<u32> = plan
            .assignments
            .iter()
            .flat_map(|a| a.shard_ids.iter().map(|s| s.0))
            .collect();
        all_shard_ids.sort_unstable();
        let expected: Vec<u32> = (0..plan.shard_centroids.len() as u32).collect();
        assert_eq!(
            all_shard_ids, expected,
            "every shard must be assigned exactly once"
        );
    }

    #[test]
    fn plan_workers_records_build_metadata() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 2,
            kmeans_iters: 7,
            nprobe: 5,
            kmeans_seed: 1234,
            kmeans_sample_size: Some(10),
            ..SystemConfig::default()
        };

        let plan = plan_workers(
            &store,
            &config,
            &two_cluster_records(),
            plan_params("idx-plan-meta"),
        )
        .unwrap();

        assert_eq!(plan.kmeans_iters, 7);
        assert_eq!(plan.kmeans_seed, 1234);
        assert_eq!(plan.kmeans_sample_size, Some(10));
        assert_eq!(plan.nprobe_default, 5);
    }

    #[test]
    fn plan_workers_writes_coarse_quantizer_to_storage() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = default_config(tmp.path(), 2);

        let plan = plan_workers(
            &store,
            &config,
            &two_cluster_records(),
            plan_params("idx-cq"),
        )
        .unwrap();

        // The coarse quantizer artifact must be readable.
        let cq_bytes = store.get(&plan.coarse_quantizer_key).unwrap();
        let recovered = IvfQuantizer::from_bytes(&cq_bytes).unwrap();
        assert_eq!(recovered.num_clusters(), plan.shard_centroids.len());
    }

    #[test]
    fn plan_workers_is_reproducible() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = default_config(tmp.path(), 2);
        let records = two_cluster_records();

        let plan_a = plan_workers(&store, &config, &records, plan_params("idx-repro-a")).unwrap();
        let plan_b = plan_workers(&store, &config, &records, plan_params("idx-repro-b")).unwrap();

        assert_eq!(plan_a.shard_centroids, plan_b.shard_centroids);
        assert_eq!(plan_a.num_workers, plan_b.num_workers);
        for (a, b) in plan_a.assignments.iter().zip(plan_b.assignments.iter()) {
            assert_eq!(a.shard_ids, b.shard_ids);
        }
    }

    #[test]
    fn plan_workers_clamps_num_workers_to_shard_count() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        // 2 non-empty shards but requesting 10 workers.
        let config = default_config(tmp.path(), 2);
        let mut params = plan_params("idx-clamp");
        params.num_workers = 10;

        let plan = plan_workers(&store, &config, &two_cluster_records(), params).unwrap();
        assert!(
            plan.num_workers <= 2,
            "num_workers should be clamped to the number of non-empty shards"
        );
    }

    // ── WorkerBuilder::execute ─────────────────────────────────────────────

    #[test]
    fn worker_execute_builds_shards_and_writes_output() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = default_config(tmp.path(), 2);
        let records = two_cluster_records();

        let plan = plan_workers(&store, &config, &records, plan_params("idx-exec")).unwrap();
        let assignment = plan.assignment(0).unwrap();

        let builder = WorkerBuilder::new(&store);
        let output = builder.execute(&plan, assignment, &records).unwrap();

        assert_eq!(output.worker_id, 0);
        assert!(!output.shards.is_empty());
        // All shard artifacts must be reachable in storage.
        for shard_out in &output.shards {
            let bytes = store.get(&shard_out.artifact_key).unwrap();
            assert!(!bytes.is_empty());
        }
    }

    #[test]
    fn worker_execute_writes_output_metadata_to_storage() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = default_config(tmp.path(), 2);
        let records = two_cluster_records();

        let plan = plan_workers(&store, &config, &records, plan_params("idx-meta")).unwrap();
        let assignment = plan.assignment(0).unwrap();
        let builder = WorkerBuilder::new(&store);
        let output = builder.execute(&plan, assignment, &records).unwrap();

        let expected_key = shardlake_storage::paths::worker_output_key("idx-meta", 0);
        let raw = store.get(&expected_key).unwrap();
        let loaded: WorkerOutput = serde_json::from_slice(&raw).unwrap();
        assert_eq!(loaded.worker_id, output.worker_id);
        assert_eq!(loaded.shards.len(), output.shards.len());
    }

    #[test]
    fn worker_execute_is_reproducible() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = default_config(tmp.path(), 2);
        let records = two_cluster_records();

        let plan_a =
            plan_workers(&store, &config, &records, plan_params("idx-repro-exec-a")).unwrap();
        let plan_b =
            plan_workers(&store, &config, &records, plan_params("idx-repro-exec-b")).unwrap();

        let builder = WorkerBuilder::new(&store);
        let out_a = builder
            .execute(&plan_a, plan_a.assignment(0).unwrap(), &records)
            .unwrap();
        let out_b = builder
            .execute(&plan_b, plan_b.assignment(0).unwrap(), &records)
            .unwrap();

        // Same dataset + same config → same fingerprints.
        for (sa, sb) in out_a.shards.iter().zip(out_b.shards.iter()) {
            assert_eq!(
                sa.fingerprint, sb.fingerprint,
                "shard fingerprints must match across identical builds"
            );
            assert_eq!(sa.vector_count, sb.vector_count);
        }
    }

    #[test]
    fn worker_execute_output_covers_all_assigned_shards() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        // Four clearly-separated clusters in 2-D (50 records each).
        let mut records: Vec<VectorRecord> =
            (0..50).map(|i| record(i, vec![0.0f32, 0.0])).collect();
        records.extend((50..100).map(|i| record(i, vec![100.0f32, 0.0])));
        records.extend((100..150).map(|i| record(i, vec![0.0f32, 100.0])));
        records.extend((150..200).map(|i| record(i, vec![100.0f32, 100.0])));

        let config = default_config(tmp.path(), 4);
        let mut params = plan_params("idx-cover");
        params.num_workers = 2;

        let plan = plan_workers(&store, &config, &records, params).unwrap();
        let builder = WorkerBuilder::new(&store);

        for w in 0..plan.num_workers {
            let assignment = plan.assignment(w).unwrap();
            let output = builder.execute(&plan, assignment, &records).unwrap();
            assert_eq!(
                output.shards.len(),
                assignment.shard_ids.len(),
                "worker {w} must produce one output per assigned shard"
            );
        }
    }

    #[test]
    fn worker_execute_rejects_malformed_plan_inputs() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = default_config(tmp.path(), 2);
        let records = two_cluster_records();

        let mut plan =
            plan_workers(&store, &config, &records, plan_params("idx-bad-plan")).unwrap();
        let assignment = plan.assignment(0).unwrap().clone();

        plan.dims = 0;
        let err = WorkerBuilder::new(&store)
            .execute(&plan, &assignment, &records)
            .unwrap_err();
        assert!(err.to_string().contains("plan dims must be greater than 0"));

        let mut plan =
            plan_workers(&store, &config, &records, plan_params("idx-bad-centroid")).unwrap();
        let assignment = plan.assignment(0).unwrap().clone();
        plan.shard_centroids[0].pop();
        let err = WorkerBuilder::new(&store)
            .execute(&plan, &assignment, &records)
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("shard centroid 0 has dimension mismatch"));
    }

    #[test]
    fn worker_execute_rejects_invalid_assignment_and_record_dims() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = default_config(tmp.path(), 2);
        let records = two_cluster_records();

        let plan =
            plan_workers(&store, &config, &records, plan_params("idx-bad-assignment")).unwrap();
        let builder = WorkerBuilder::new(&store);

        let mut bad_assignment = plan.assignment(0).unwrap().clone();
        bad_assignment
            .shard_ids
            .push(ShardId(plan.shard_centroids.len() as u32));
        let err = builder
            .execute(&plan, &bad_assignment, &records)
            .unwrap_err();
        assert!(err.to_string().contains("references invalid shard id"));

        let bad_records = vec![record(0, vec![1.0f32]), record(1, vec![2.0f32, 3.0])];
        let err = builder
            .execute(&plan, plan.assignment(0).unwrap(), &bad_records)
            .unwrap_err();
        assert!(err.to_string().contains("dimension mismatch"));
    }

    #[test]
    fn worker_plan_roundtrip_serialisation() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = default_config(tmp.path(), 2);

        let plan = plan_workers(
            &store,
            &config,
            &two_cluster_records(),
            plan_params("idx-serial"),
        )
        .unwrap();

        let json = serde_json::to_string(&plan).unwrap();
        let restored: WorkerPlan = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.num_workers, plan.num_workers);
        assert_eq!(restored.shard_centroids, plan.shard_centroids);
        assert_eq!(restored.assignments.len(), plan.assignments.len());
    }
}
