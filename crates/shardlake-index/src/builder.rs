//! Offline index builder: partitions vectors into shards using an IVF coarse
//! quantizer trained with K-means.

use chrono::Utc;
use rand::{seq::SliceRandom, SeedableRng};
use tracing::{info, warn};

use shardlake_core::{
    config::SystemConfig,
    error::CoreError,
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, ShardId, VectorRecord,
    },
};
use shardlake_manifest::{
    AlgorithmMetadata, BuildMetadata, CompressionConfig, Manifest, RoutingMetadata, ShardDef,
    ShardSummary,
};
use shardlake_storage::ObjectStore;

use crate::{
    kmeans::{kmeans, nearest_centroid},
    pq::{PqCodebook, PqParams},
    shard::{PqShard, ShardIndex},
    IndexError, Result, PQ8_CODEC,
};
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
    /// Optional PQ parameters.  When `Some`, the builder trains a PQ codebook
    /// and encodes shard vectors as PQ codes.  When `None`, raw vectors are
    /// stored (the original behaviour).
    ///
    /// If `None` and `SystemConfig::pq_enabled` is `true`, PQ parameters are
    /// derived from the config.
    pub pq_params: Option<PqParams>,
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
            pq_params,
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

        // Resolve PQ params: explicit BuildParams override, then config flag.
        let resolved_pq: Option<PqParams> = pq_params.or({
            if self.config.pq_enabled {
                Some(PqParams {
                    num_subspaces: self.config.pq_num_subspaces as usize,
                    codebook_size: self.config.pq_codebook_size as usize,
                })
            } else {
                None
            }
        });

        let build_start = std::time::Instant::now();

        let n = records.len();
        let k = self.config.num_shards as usize;
        let iters = self.config.kmeans_iters;

        info!(n, k, iters, "Training IVF coarse quantizer");

        let mut rng = rand::rngs::StdRng::seed_from_u64(self.config.kmeans_seed);
        let vecs: Vec<Vec<f32>> = records.iter().map(|r| r.data.clone()).collect();

        // Optionally sample a subset for centroid training.  All vectors are
        // still assigned to the nearest centroid after training, so no data
        // is lost when sampling is enabled.
        let sampled: Option<Vec<Vec<f32>>> = match self.config.kmeans_sample_size {
            Some(0) => {
                return Err(IndexError::Other(
                    "kmeans_sample_size must be greater than 0".into(),
                ))
            }
            Some(max_samples) => {
                let sample_size = (max_samples as usize).min(vecs.len());
                if sample_size >= vecs.len() {
                    // Sample covers the full set – no need to allocate.
                    None
                } else {
                    let mut indices: Vec<usize> = (0..vecs.len()).collect();
                    let (shuffled, _) = indices.partial_shuffle(&mut rng, sample_size);
                    Some(shuffled.iter().map(|&i| vecs[i].clone()).collect())
                }
            }
            None => None,
        };
        let effective_sample_size = sampled.as_ref().map(std::vec::Vec::len);
        let training_vecs: &[Vec<f32>] = sampled.as_deref().unwrap_or(&vecs);

        if let Some(sample_size) = effective_sample_size {
            info!(
                sample_size,
                total = n,
                "Sampling vectors for centroid training"
            );
        }

        let quantizer = IvfQuantizer::train(training_vecs, k, iters, &mut rng);

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

        // Train PQ codebook if requested.
        let codebook: Option<PqCodebook> = if let Some(ref pq) = resolved_pq {
            info!(
                m = pq.num_subspaces,
                k = pq.codebook_size,
                "Training PQ codebook"
            );
            let cb = PqCodebook::train(&vecs, pq.clone(), self.config.kmeans_seed, iters)?;
            // Persist the codebook as a separate artifact.
            let cb_key = shardlake_storage::paths::index_pq_codebook_key(&index_version.0);
            let cb_bytes = cb.to_bytes();
            self.store.put(&cb_key, cb_bytes)?;
            info!(key = %cb_key, "PQ codebook written");
            Some(cb)
        } else {
            None
        };
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

            let bytes = if let Some(ref cb) = codebook {
                // PQ-encoded shard (format version 2).
                let entries: Vec<_> = shard_recs
                    .iter()
                    .map(|r| (r.id, cb.encode(&r.data)))
                    .collect();
                let pq_shard = PqShard {
                    shard_id,
                    dims,
                    pq_m: cb.params.num_subspaces,
                    pq_k: cb.params.codebook_size,
                    centroids: vec![centroids[i].clone()],
                    entries,
                };
                pq_shard.to_bytes()?
            } else {
                // Raw-vector shard (format version 1).
                let idx = ShardIndex {
                    shard_id,
                    dims,
                    centroids: vec![centroids[i].clone()],
                    records: shard_recs,
                };
                idx.to_bytes()?
            };

            let sha = crate::artifact_fingerprint(&bytes);
            self.store.put(&shard_artifact_key, bytes)?;
            info!(shard = %shard_id, vectors = count, key = %shard_artifact_key, "Shard written");
            actual_total += count;
            let file_location = shard_artifact_key.clone();
            shard_defs.push(ShardDef {
                shard_id,
                artifact_key: shard_artifact_key,
                vector_count: count,
                fingerprint: sha,
                centroid: quantizer.centroids()[i].clone(),
                routing: Some(RoutingMetadata {
                    centroid_id: format!("shard-{:04}", shard_id.0),
                    index_type: "flat".into(),
                    file_location,
                }),
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
        if let Some(sample_size) = effective_sample_size {
            algo_params.insert("kmeans_sample_size".into(), serde_json::json!(sample_size));
        }

        let compression = if let Some(ref cb) = codebook {
            let cb_key = shardlake_storage::paths::index_pq_codebook_key(&index_version.0);
            CompressionConfig {
                enabled: true,
                codec: PQ8_CODEC.into(),
                pq_num_subspaces: cb.params.num_subspaces as u32,
                pq_codebook_size: cb.params.codebook_size as u32,
                codebook_key: Some(cb_key),
            }
        } else {
            CompressionConfig::default()
        };

        let manifest = Manifest {
            manifest_version: 4,
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
            compression,
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
            pq_params: None,
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
            kmeans_sample_size: None,
            ..SystemConfig::default()
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
            kmeans_sample_size: None,
            ..SystemConfig::default()
        };

        let err = IndexBuilder::new(&store, &config)
            .build(build_params(vec![record(1, 2), record(2, 3)], 2))
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("record 2 has dimension mismatch: expected 2, got 3"));
    }

    /// Verify that when `kmeans_sample_size` is set, all vectors are still
    /// assigned (no records dropped), the manifest records the parameter, and
    /// the resulting shard artifact fingerprints are non-empty.
    #[test]
    fn build_with_sample_size_assigns_all_vectors() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let n = 50usize;
        let dims = 4usize;
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 2,
            kmeans_iters: 5,
            nprobe: 1,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            // Use a sample smaller than the full dataset.
            kmeans_sample_size: Some(10),
        };

        let records: Vec<VectorRecord> = (0..n)
            .map(|i| VectorRecord {
                id: VectorId(i as u64),
                data: (0..dims).map(|d| (i * dims + d) as f32).collect(),
                metadata: None,
            })
            .collect();

        let manifest = IndexBuilder::new(&store, &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-sample".into()),
                embedding_version: EmbeddingVersion("emb-sample".into()),
                index_version: IndexVersion("idx-sample".into()),
                metric: DistanceMetric::Euclidean,
                dims,
                vectors_key: shardlake_storage::paths::dataset_vectors_key("ds-sample"),
                metadata_key: shardlake_storage::paths::dataset_metadata_key("ds-sample"),
            })
            .unwrap();

        // All vectors must be accounted for.
        assert_eq!(
            manifest.total_vector_count, n as u64,
            "all vectors must be assigned even when training uses a sample"
        );
        let shard_sum: u64 = manifest.shards.iter().map(|s| s.vector_count).sum();
        assert_eq!(shard_sum, n as u64);

        // Shard fingerprints must be populated.
        assert!(manifest.shards.iter().all(|s| !s.fingerprint.is_empty()));

        // The sample size must be recorded in algorithm.params.
        let param = manifest
            .algorithm
            .params
            .get("kmeans_sample_size")
            .expect("kmeans_sample_size must be recorded in algorithm.params");
        assert_eq!(param.as_u64().unwrap(), 10);
    }

    #[test]
    fn build_rejects_zero_kmeans_sample_size() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 1,
            kmeans_iters: 2,
            nprobe: 1,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            kmeans_sample_size: Some(0),
        };

        let err = IndexBuilder::new(&store, &config)
            .build(build_params(vec![record(1, 2), record(2, 2)], 2))
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("kmeans_sample_size must be greater than 0"));
    }

    #[test]
    fn build_omits_sample_size_when_sampling_is_not_needed() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let dims = 4usize;
        let records: Vec<VectorRecord> = (0..8)
            .map(|i| VectorRecord {
                id: VectorId(i as u64),
                data: (0..dims).map(|d| (i * dims + d) as f32).collect(),
                metadata: None,
            })
            .collect();
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 2,
            kmeans_iters: 5,
            nprobe: 1,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            kmeans_sample_size: Some(99),
        };

        let manifest = IndexBuilder::new(&store, &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-full".into()),
                embedding_version: EmbeddingVersion("emb-full".into()),
                index_version: IndexVersion("idx-full".into()),
                metric: DistanceMetric::Euclidean,
                dims,
                vectors_key: shardlake_storage::paths::dataset_vectors_key("ds-full"),
                metadata_key: shardlake_storage::paths::dataset_metadata_key("ds-full"),
            })
            .unwrap();

        assert!(
            !manifest.algorithm.params.contains_key("kmeans_sample_size"),
            "kmeans_sample_size should be omitted when training uses the full dataset"
        );
    }

    /// Two builds with the same `kmeans_sample_size` and seed must produce
    /// identical centroids and shard fingerprints.
    #[test]
    fn build_with_sample_size_is_deterministic() {
        let n = 50usize;
        let dims = 4usize;
        let records: Vec<VectorRecord> = (0..n)
            .map(|i| VectorRecord {
                id: VectorId(i as u64),
                data: (0..dims).map(|d| (i * dims + d) as f32).collect(),
                metadata: None,
            })
            .collect();

        let build_once = |idx_ver: &str| {
            let tmp = tempdir().unwrap();
            let store = LocalObjectStore::new(tmp.path()).unwrap();
            let config = SystemConfig {
                storage_root: tmp.path().to_path_buf(),
                num_shards: 2,
                kmeans_iters: 5,
                nprobe: 1,
                kmeans_seed: SystemConfig::default_kmeans_seed(),
                kmeans_sample_size: Some(10),
            };
            IndexBuilder::new(&store, &config)
                .build(BuildParams {
                    records: records.clone(),
                    dataset_version: DatasetVersion("ds-det-sample".into()),
                    embedding_version: EmbeddingVersion("emb-det-sample".into()),
                    index_version: IndexVersion(idx_ver.into()),
                    metric: DistanceMetric::Euclidean,
                    dims,
                    vectors_key: shardlake_storage::paths::dataset_vectors_key("ds-det-sample"),
                    metadata_key: shardlake_storage::paths::dataset_metadata_key("ds-det-sample"),
                })
                .unwrap()
        };

        let m1 = build_once("idx-det-s1");
        let m2 = build_once("idx-det-s2");

        assert_eq!(m1.shards.len(), m2.shards.len());
        for (s1, s2) in m1.shards.iter().zip(m2.shards.iter()) {
            assert_eq!(
                s1.fingerprint, s2.fingerprint,
                "shard {} fingerprint must match across builds with same seed and sample size",
                s1.shard_id
            );
            assert_eq!(
                s1.centroid, s2.centroid,
                "shard {} centroid must match across builds with same seed and sample size",
                s1.shard_id
            );
        }
    }
}
