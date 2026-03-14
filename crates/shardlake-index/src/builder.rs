//! Offline index builder: partitions vectors into shards using an IVF coarse
//! quantizer trained with K-means.

use chrono::Utc;
use rand::SeedableRng;
use tracing::{info, warn};

use shardlake_core::{
    config::SystemConfig,
    error::CoreError,
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, ShardId, VectorRecord,
    },
};
use shardlake_manifest::{
    AlgorithmMetadata, BuildMetadata, CompressionConfig, Manifest, ShardDef, ShardSummary,
};
use shardlake_storage::ObjectStore;

use crate::{ivf::IvfQuantizer, shard::ShardIndex, IndexError, Result};

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

        if self.config.num_shards == 0 {
            return Err(IndexError::Other(
                "num_shards must be greater than 0".into(),
            ));
        }

        for record in &records {
            if record.data.len() != dims {
                return Err(IndexError::Other(format!(
                    "record {} has dimension mismatch: expected {}, got {}",
                    record.id,
                    dims,
                    record.data.len()
                )));
            }
        }

        if dims == 0 {
            return Err(IndexError::Core(CoreError::DimensionMismatch {
                expected: 1,
                got: 0,
            }));
        }

        let build_start = std::time::Instant::now();

        let n = records.len();
        let k = self.config.num_shards as usize;
        let iters = self.config.kmeans_iters;

        info!(n, k, iters, "Training IVF coarse quantizer");

        let mut rng = rand::rngs::StdRng::seed_from_u64(self.config.kmeans_seed);
        let vecs: Vec<Vec<f32>> = records.iter().map(|r| r.data.clone()).collect();
        let quantizer = IvfQuantizer::train(&vecs, k, iters, &mut rng);

        info!("Assigning vectors to IVF posting-list shards");
        let mut shard_records: Vec<Vec<VectorRecord>> = vec![Vec::new(); quantizer.num_clusters()];
        for rec in records {
            let shard = quantizer.assign(&rec.data);
            shard_records[shard].push(rec);
        }

        for (i, sr) in shard_records.iter().enumerate() {
            if sr.is_empty() {
                warn!(shard = i, "shard is empty after IVF assignment");
            }
        }

        let mut non_empty_clusters: Vec<(usize, Vec<VectorRecord>)> = shard_records
            .into_iter()
            .enumerate()
            .filter(|(_, shard_recs)| !shard_recs.is_empty())
            .collect();
        if non_empty_clusters.is_empty() {
            return Err(IndexError::Other(
                "IVF build produced no non-empty posting-list shards".into(),
            ));
        }
        if non_empty_clusters.len() != quantizer.num_clusters() {
            warn!(
                requested_clusters = quantizer.num_clusters(),
                retained_clusters = non_empty_clusters.len(),
                "Compacting empty IVF clusters to preserve cluster-to-shard mapping"
            );
        }
        let quantizer = IvfQuantizer::from_centroids(
            non_empty_clusters
                .iter()
                .map(|(cluster_idx, _)| quantizer.centroids()[*cluster_idx].clone())
                .collect(),
        );

        let mut shard_defs = Vec::new();
        let mut actual_total: u64 = 0;
        for (i, (_, shard_recs)) in non_empty_clusters.drain(..).enumerate() {
            let shard_id = ShardId(i as u32);
            let count = shard_recs.len() as u64;
            let idx = ShardIndex {
                shard_id,
                dims,
                centroids: vec![quantizer.centroids()[i].clone()],
                records: shard_recs,
            };
            let bytes = idx.to_bytes()?;
            let sha = crate::artifact_fingerprint(&bytes);
            let shard_artifact_key =
                shardlake_storage::paths::index_shard_key(&index_version.0, shard_id.0);
            self.store.put(&shard_artifact_key, bytes)?;
            info!(shard = %shard_id, vectors = count, key = %shard_artifact_key, "Shard written");
            actual_total += count;
            shard_defs.push(ShardDef {
                shard_id,
                artifact_key: shard_artifact_key,
                vector_count: count,
                fingerprint: sha,
                centroid: quantizer.centroids()[i].clone(),
            });
        }

        // Persist the coarse quantizer as a separate artifact.
        let cq_key = shardlake_storage::paths::index_coarse_quantizer_key(&index_version.0);
        let cq_bytes = quantizer.to_bytes()?;
        self.store.put(&cq_key, cq_bytes)?;
        info!(key = %cq_key, clusters = quantizer.num_clusters(), "Coarse quantizer written");

        let build_duration_secs = build_start.elapsed().as_secs_f64();

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
        algo_params.insert(
            "num_clusters".into(),
            serde_json::json!(quantizer.num_clusters()),
        );
        // `num_shards` equals `num_clusters` for ivf-flat: each cluster maps to exactly one
        // posting-list shard.  It is kept for backward compatibility with readers that
        // expect this param from the former "kmeans-flat" builds.
        algo_params.insert(
            "num_shards".into(),
            serde_json::json!(quantizer.num_clusters()),
        );
        algo_params.insert("kmeans_iters".into(), serde_json::json!(iters));
        algo_params.insert(
            "kmeans_seed".into(),
            serde_json::json!(self.config.kmeans_seed),
        );

        let manifest = Manifest {
            manifest_version: 3,
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
                build_duration_secs,
            },
            algorithm: AlgorithmMetadata {
                algorithm: "ivf-flat".into(),
                variant: None,
                params: algo_params,
            },
            shard_summary,
            compression: CompressionConfig::default(),
            recall_estimate: None,
            coarse_quantizer_key: Some(cq_key),
        };

        manifest.save(self.store).map_err(IndexError::Manifest)?;
        info!(index_version = %manifest.index_version, "Manifest written");
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use shardlake_core::types::VectorId;
    use shardlake_storage::LocalObjectStore;

    fn record(id: u64, dims: usize) -> VectorRecord {
        VectorRecord {
            id: VectorId(id),
            data: (0..dims).map(|idx| idx as f32).collect(),
            metadata: None,
        }
    }

    fn build_params(records: Vec<VectorRecord>, dims: usize) -> BuildParams {
        BuildParams {
            records,
            dataset_version: DatasetVersion("ds-test".into()),
            embedding_version: EmbeddingVersion("emb-test".into()),
            index_version: IndexVersion("idx-test".into()),
            metric: DistanceMetric::Cosine,
            dims,
            vectors_key: shardlake_storage::paths::dataset_vectors_key("ds-test"),
            metadata_key: shardlake_storage::paths::dataset_metadata_key("ds-test"),
        }
    }

    #[test]
    fn build_rejects_zero_num_shards() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 0,
            kmeans_iters: 2,
            nprobe: 1,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
        };

        let err = IndexBuilder::new(&store, &config)
            .build(build_params(vec![record(1, 2)], 2))
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("num_shards must be greater than 0"));
    }

    #[test]
    fn build_rejects_record_dimension_mismatch() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 1,
            kmeans_iters: 2,
            nprobe: 1,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
        };

        let err = IndexBuilder::new(&store, &config)
            .build(build_params(vec![record(1, 2), record(2, 3)], 2))
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("record 2 has dimension mismatch: expected 2, got 3"));
    }
}
