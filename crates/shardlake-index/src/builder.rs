//! Offline index builder: partitions vectors into shards using K-means.

use chrono::Utc;
use rand::SeedableRng;
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
        } = params;

        if records.is_empty() {
            return Err(IndexError::Other("no records to index".into()));
        }

        let n = records.len();
        let k = self.config.num_shards as usize;
        let iters = self.config.kmeans_iters;

        info!(n, k, iters, "Running K-means to compute shard centroids");

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

        let mut shard_defs = Vec::new();
        let mut actual_total: u64 = 0;
        for (i, shard_recs) in shard_records.into_iter().enumerate() {
            if shard_recs.is_empty() {
                continue;
            }
            let shard_id = ShardId(i as u32);
            let count = shard_recs.len() as u64;
            let idx = ShardIndex {
                shard_id,
                dims,
                centroids: vec![centroids[i].clone()],
                records: shard_recs,
            };
            let bytes = idx.to_bytes()?;
            let sha = fingerprint_hex(&bytes);
            let shard_artifact_key =
                format!("indexes/{}/shards/{}.sidx", index_version.0, shard_id);
            self.store.put(&shard_artifact_key, bytes)?;
            info!(shard = %shard_id, vectors = count, key = %shard_artifact_key, "Shard written");
            actual_total += count;
            shard_defs.push(ShardDef {
                shard_id,
                artifact_key: shard_artifact_key,
                vector_count: count,
                sha256: sha,
            });
        }

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
            pq_artifact_key: None,
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
