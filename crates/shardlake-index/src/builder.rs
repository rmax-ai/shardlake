//! Offline index builder: partitions vectors into shards using K-means.

use chrono::Utc;
use rand::SeedableRng;
use rayon::prelude::*;
use tracing::{info, warn};

use shardlake_core::{
    config::SystemConfig,
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, ShardId, VectorRecord,
    },
};
use shardlake_manifest::{BuildMetadata, Manifest, ShardDef};
use shardlake_storage::ObjectStore;

use crate::{
    kmeans::{kmeans, nearest_centroid},
    shard::ShardIndex,
    IndexError, Result,
};

/// Parameters for an index build operation.
pub struct BuildParams {
    pub records: Vec<VectorRecord>,
    pub dataset_version: DatasetVersion,
    pub embedding_version: EmbeddingVersion,
    pub index_version: IndexVersion,
    pub metric: DistanceMetric,
    pub dims: usize,
    pub vectors_key: String,
    pub metadata_key: String,
    /// When `true` shard serialisation and storage writes run in parallel via rayon.
    pub parallel: bool,
    /// Distributed mode: zero-based index of this worker (0..`num_workers`).
    ///
    /// When set together with [`Self::num_workers`] the input `records` are
    /// partitioned using modulo assignment: worker `w` owns the shards whose
    /// IDs satisfy `shard_id % num_workers == worker_id`.  All workers must
    /// receive the same full `records` list so that K-means produces
    /// consistent centroids across workers.
    pub worker_id: Option<u32>,
    /// Distributed mode: total number of workers sharing this build.
    pub num_workers: Option<u32>,
}

/// Builds a shard-based index from a flat list of vector records.
pub struct IndexBuilder<'a> {
    store: &'a dyn ObjectStore,
    config: &'a SystemConfig,
}

impl<'a> IndexBuilder<'a> {
    /// Create a new builder backed by `store` and configured with `config`.
    pub fn new(store: &'a dyn ObjectStore, config: &'a SystemConfig) -> Self {
        Self { store, config }
    }

    /// Build the index and return the resulting manifest.
    ///
    /// When [`BuildParams::parallel`] is `true` shard writes are issued
    /// concurrently using a rayon thread-pool, which can halve wall-clock
    /// time on machines with multiple cores.
    ///
    /// When [`BuildParams::worker_id`] / [`BuildParams::num_workers`] are set
    /// the builder operates in *distributed mode*: it writes only the shards
    /// that belong to this worker and stores the result as a partial manifest
    /// that can later be merged with [`crate::merger::merge_manifests`].
    pub fn build(&self, params: BuildParams) -> Result<Manifest> {
        let BuildParams {
            records,
            dataset_version,
            embedding_version,
            index_version,
            metric,
            dims,
            vectors_key,
            metadata_key,
            parallel,
            worker_id,
            num_workers,
        } = params;

        if records.is_empty() {
            return Err(IndexError::Other("no records to index".into()));
        }

        let n = records.len();
        let k = self.config.num_shards as usize;
        let iters = self.config.kmeans_iters;

        // Distributed-mode bookkeeping.
        let (effective_worker_id, effective_num_workers) = match (worker_id, num_workers) {
            (Some(wid), Some(nw)) if nw > 0 => {
                if wid >= nw {
                    return Err(IndexError::Other(format!(
                        "worker_id ({wid}) must be less than num_workers ({nw})"
                    )));
                }
                (wid, nw)
            }
            (None, None) => (0, 1),
            _ => {
                return Err(IndexError::Other(
                    "worker_id and num_workers must be provided together".into(),
                ))
            }
        };

        info!(
            n,
            k,
            iters,
            worker_id = effective_worker_id,
            num_workers = effective_num_workers,
            "Running K-means to compute shard centroids"
        );

        let mut rng = rand::rngs::StdRng::seed_from_u64(0xdead_beef);
        let vecs: Vec<Vec<f32>> = records.iter().map(|r| r.data.clone()).collect();
        let centroids = kmeans(&vecs, k, iters, &mut rng);

        info!("Assigning vectors to shards");
        let mut shard_records: Vec<Vec<VectorRecord>> = vec![Vec::new(); k];
        for rec in records {
            let shard = nearest_centroid(&rec.data, &centroids);
            shard_records[shard].push(rec);
        }

        for (i, sr) in shard_records.iter().enumerate() {
            if sr.is_empty() {
                warn!(shard = i, "shard is empty after assignment");
            }
        }

        // In distributed mode this worker only writes the shards it owns.
        let owned_shard_ids: Vec<usize> = (0..k)
            .filter(|&i| (i as u32) % effective_num_workers == effective_worker_id)
            .collect();

        // Build (shard_id, records, centroid) tuples for shards owned by this
        // worker that are non-empty.
        let work_items: Vec<(usize, Vec<VectorRecord>, Vec<f32>)> = owned_shard_ids
            .into_iter()
            .filter_map(|i| {
                let recs = std::mem::take(&mut shard_records[i]);
                if recs.is_empty() {
                    None
                } else {
                    Some((i, recs, centroids[i].clone()))
                }
            })
            .collect();

        // Serialise shards – optionally in parallel.
        let build_shard =
            |(i, shard_recs, centroid): (usize, Vec<VectorRecord>, Vec<f32>)| -> Result<ShardDef> {
                let shard_id = ShardId(i as u32);
                let count = shard_recs.len() as u64;
                let idx = ShardIndex {
                    shard_id,
                    dims,
                    centroids: vec![centroid],
                    records: shard_recs,
                };
                let bytes = idx.to_bytes()?;
                let sha = fingerprint_hex(&bytes);
                let shard_artifact_key =
                    format!("indexes/{}/shards/{}.sidx", index_version.0, shard_id);
                self.store.put(&shard_artifact_key, bytes)?;
                info!(shard = %shard_id, vectors = count, key = %shard_artifact_key, "Shard written");
                Ok(ShardDef {
                    shard_id,
                    artifact_key: shard_artifact_key,
                    vector_count: count,
                    sha256: sha,
                })
            };

        let mut shard_defs: Vec<ShardDef> = if parallel {
            work_items
                .into_par_iter()
                .map(build_shard)
                .collect::<Result<Vec<_>>>()?
        } else {
            work_items
                .into_iter()
                .map(build_shard)
                .collect::<Result<Vec<_>>>()?
        };

        // Keep shards in a stable order by shard_id.
        shard_defs.sort_by_key(|s| s.shard_id.0);

        let actual_total: u64 = shard_defs.iter().map(|s| s.vector_count).sum();

        let manifest = Manifest {
            manifest_version: 1,
            dataset_version,
            embedding_version,
            index_version,
            alias: "latest".into(),
            dims: dims as u32,
            distance_metric: metric,
            vectors_key,
            metadata_key,
            total_vector_count: actual_total,
            shards: shard_defs,
            build_metadata: BuildMetadata {
                built_at: Utc::now(),
                builder_version: env!("CARGO_PKG_VERSION").into(),
                num_kmeans_iters: iters,
                nprobe_default: self.config.nprobe,
            },
        };

        manifest.save(self.store).map_err(IndexError::Manifest)?;
        info!(index_version = %manifest.index_version, "Manifest written");
        Ok(manifest)
    }
}

/// FNV-1a-based artifact fingerprint.
///
/// This is intentionally a fast, non-cryptographic hash used to detect
/// accidental corruption and enable deduplication during prototyping.
/// Replace with SHA-256 (e.g. `sha2` crate) before using in production.
fn fingerprint_hex(bytes: &[u8]) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{h:016x}")
}
