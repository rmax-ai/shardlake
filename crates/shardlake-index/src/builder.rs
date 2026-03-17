//! Offline index builder: partitions vectors into shards using an IVF coarse
//! quantizer trained with K-means.
//!
//! Shard artifact construction (encoding, serialisation, fingerprinting, and
//! storage writes) is executed concurrently across shards using Rayon's
//! work-stealing thread pool. The final [`Manifest`] is assembled from the
//! collected results in deterministic shard-ID order, so repeated builds with
//! identical inputs and configuration produce bit-identical shard artifacts and
//! stable shard definitions even though time-based build metadata still varies.

use chrono::Utc;
use rand::{seq::SliceRandom, SeedableRng};
use rayon::prelude::*;
use tracing::{info, warn};

use shardlake_core::{
    config::SystemConfig,
    error::CoreError,
    types::{
        AnnFamily, DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, SearchResult,
        ShardId, VectorId, VectorRecord,
    },
};
use shardlake_manifest::{
    AlgorithmMetadata, BuildMetadata, CompressionConfig, Manifest, RecallEstimate, RoutingMetadata,
    ShardDef, ShardSummary,
};
use shardlake_storage::ObjectStore;

use crate::{
    exact::{exact_search, merge_top_k, recall_at_k},
    ivf::IvfQuantizer,
    plugin::{AnnRegistry, HnswConfig},
    pq::{PqCodebook, PqParams},
    shard::{PqShard, ShardIndex},
    IndexError, Result, PQ8_CODEC,
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
    /// Optional PQ parameters.  When `Some`, the builder trains a PQ codebook
    /// and encodes shard vectors as PQ codes.  When `None`, raw vectors are
    /// stored (the original behaviour).
    ///
    /// If `None` and `SystemConfig::pq_enabled` is `true`, PQ parameters are
    /// derived from the config.
    pub pq_params: Option<PqParams>,
    /// ANN algorithm family to use for candidate search within each shard.
    ///
    /// When `None` (or `Some(AnnFamily::IvfFlat)`), the builder emits
    /// `algorithm: "ivf-flat"` in the manifest. When
    /// `Some(AnnFamily::DiskAnn)`, the builder records `algorithm: "diskann"`
    /// after validating that the selected distance metric is compatible with
    /// the DiskANN experiment backend. When `Some(AnnFamily::Hnsw)`, the
    /// builder records `algorithm: "hnsw"` in the manifest with the HNSW graph
    /// parameters so that `IndexSearcher` selects `HnswPlugin` at query time.
    ///
    /// Defaults to `None` (IVF-flat behaviour).
    pub ann_family: Option<AnnFamily>,
    /// Optional HNSW graph parameters.
    ///
    /// Only used when `ann_family == Some(AnnFamily::Hnsw)`.  When `None` and
    /// HNSW is selected, [`HnswConfig::default`] is used.
    pub hnsw_config: Option<HnswConfig>,
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
            ann_family,
            hnsw_config,
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
        // AnnRegistry centralises this selection so build callers do not need
        // to branch on pq_enabled directly.
        let resolved_pq = AnnRegistry::resolve_build_params(pq_params, self.config);

        if resolved_pq.is_some() && metric != DistanceMetric::Euclidean {
            return Err(IndexError::Other(
                "PQ indexes currently support only euclidean distance".into(),
            ));
        }

        let resolved_ann_family = ann_family.unwrap_or(AnnFamily::IvfFlat);
        let resolved_hnsw_config = match resolved_ann_family {
            AnnFamily::Hnsw => {
                let config = hnsw_config.unwrap_or_default();
                config.validate()?;
                Some(config)
            }
            AnnFamily::DiskAnn => {
                AnnRegistry::get_flat(AnnFamily::DiskAnn.as_str())?.validate(dims, metric)?;
                None
            }
            _ => None,
        };

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

        // Optionally sample query indices for build-time recall estimation.
        // This must happen before `records` is consumed into `shard_records`.
        let recall_query_indices: Vec<usize> = if let Some(sz) = self.config.recall_sample_size {
            if sz == 0 {
                return Err(IndexError::Other(
                    "recall_sample_size must be greater than 0".into(),
                ));
            }
            let sample_size = (sz as usize).min(vecs.len());
            if sample_size >= vecs.len() {
                (0..vecs.len()).collect()
            } else {
                let mut indices: Vec<usize> = (0..vecs.len()).collect();
                let (shuffled, _) = indices.partial_shuffle(&mut rng, sample_size);
                shuffled.to_vec()
            }
        } else {
            Vec::new()
        };

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
        let non_empty_clusters: Vec<(usize, Vec<VectorRecord>)> = shard_records
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

        // Build each shard concurrently.  Each task is independent: it encodes
        // vectors, serialises the artifact, computes the fingerprint, and
        // writes the bytes to the object store.  Results are collected in
        // shard-ID (index) order by rayon's `collect`, preserving determinism.
        let shard_build_results: Vec<Result<(ShardDef, u64)>> = non_empty_clusters
            .into_par_iter()
            .enumerate()
            .map(|(i, (_, shard_recs))| {
                let shard_id = ShardId(i as u32);
                let count = shard_recs.len() as u64;
                let shard_artifact_key =
                    shardlake_storage::paths::index_shard_key(&index_version.0, shard_id.0);

                let bytes = if let Some(ref cb) = codebook {
                    // PQ-encoded shard (format version 2).
                    let entries: Vec<_> = shard_recs
                        .iter()
                        .map(|r| cb.encode(&r.data).map(|codes| (r.id, codes)))
                        .collect::<Result<Vec<_>>>()?;
                    let pq_shard = PqShard {
                        shard_id,
                        dims,
                        pq_m: cb.params.num_subspaces,
                        pq_k: cb.params.codebook_size,
                        centroids: vec![quantizer.centroids()[i].clone()],
                        entries,
                    };
                    pq_shard.to_bytes()?
                } else {
                    // Raw-vector shard (format version 1).
                    let idx = ShardIndex {
                        shard_id,
                        dims,
                        centroids: vec![quantizer.centroids()[i].clone()],
                        records: shard_recs,
                    };
                    idx.to_bytes()?
                };

                let sha = crate::artifact_fingerprint(&bytes);
                self.store.put(&shard_artifact_key, bytes)?;
                info!(shard = %shard_id, vectors = count, key = %shard_artifact_key, "Shard written");
                let file_location = shard_artifact_key.clone();
                Ok((
                    ShardDef {
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
                    },
                    count,
                ))
            })
            .collect();

        // Propagate the first error, if any, then unzip the successful results.
        let mut shard_defs = Vec::with_capacity(shard_build_results.len());
        let mut actual_total: u64 = 0;
        for result in shard_build_results {
            let (def, count) = result?;
            actual_total += count;
            shard_defs.push(def);
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

        // Select the algorithm name and record backend-specific params when requested.
        let algo_name = match resolved_ann_family {
            AnnFamily::Hnsw => {
                let hnsw = resolved_hnsw_config
                    .as_ref()
                    .expect("resolved HNSW config must exist for HNSW family");
                algo_params.insert("hnsw_m".into(), serde_json::json!(hnsw.m));
                algo_params.insert(
                    "hnsw_ef_construction".into(),
                    serde_json::json!(hnsw.ef_construction),
                );
                algo_params.insert("hnsw_ef_search".into(), serde_json::json!(hnsw.ef_search));
                "hnsw"
            }
            AnnFamily::DiskAnn => "diskann",
            AnnFamily::IvfFlat | AnnFamily::IvfPq => "ivf-flat",
        };

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

        // Compute build-time recall estimate when the caller requested it.
        let recall_estimate = if recall_query_indices.is_empty() {
            None
        } else {
            let recall_k = (self.config.recall_k as usize).max(1);
            let nprobe = (self.config.nprobe as usize).max(1);
            match estimate_recall(
                &recall_query_indices,
                &vecs,
                &quantizer,
                &shard_defs,
                self.store,
                metric,
                recall_k,
                nprobe,
                codebook.as_ref(),
            ) {
                Ok(est) => {
                    info!(
                        recall_at_k = est.recall_at_k,
                        k = est.k,
                        sample_size = est.sample_size,
                        "Build-time recall estimate computed"
                    );
                    Some(est)
                }
                Err(e) => {
                    warn!(error = %e, "Build-time recall estimation failed; continuing without estimate");
                    None
                }
            }
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
                algorithm: algo_name.into(),
                variant: None,
                params: algo_params,
            },
            shard_summary,
            compression,
            recall_estimate,
            coarse_quantizer_key: Some(cq_key),
            lexical: None,
        };

        manifest.save(self.store).map_err(IndexError::Manifest)?;
        info!(index_version = %manifest.index_version, "Manifest written");
        Ok(manifest)
    }
}

/// Estimate recall@k for a freshly-built index by comparing approximate
/// nearest-neighbour results against a brute-force ground truth.
///
/// All shard artifacts are loaded back from `store` once to reconstruct a
/// single in-memory corpus for ground-truth computation. Slice ranges into that
/// corpus are then reused during the per-query approximate search so each shard
/// artifact is read from storage exactly once without storing duplicate shard
/// copies.
///
/// Returns a [`RecallEstimate`] populated with the mean recall@k over all
/// sampled queries, or an error if any artifact cannot be loaded or
/// deserialized.
#[allow(clippy::too_many_arguments)]
fn estimate_recall(
    query_indices: &[usize],
    query_vectors: &[Vec<f32>],
    quantizer: &IvfQuantizer,
    shard_defs: &[ShardDef],
    store: &dyn ObjectStore,
    metric: DistanceMetric,
    k: usize,
    nprobe: usize,
    codebook: Option<&PqCodebook>,
) -> Result<RecallEstimate> {
    if query_indices.is_empty() {
        return Err(IndexError::Other(
            "recall estimation requires at least one sample query".into(),
        ));
    }

    // Load all shard artifacts once, reconstructing a single flat corpus.
    // Per-shard ranges into that corpus are retained so approximate search can
    // reuse the same vectors without a second copy. For PQ shards the codes are
    // decoded back to approximate float vectors using the codebook.
    let total_vectors = shard_defs
        .iter()
        .map(|def| def.vector_count as usize)
        .sum::<usize>();
    let mut shard_ranges = Vec::with_capacity(shard_defs.len());
    let mut corpus = Vec::with_capacity(total_vectors);

    for def in shard_defs {
        let bytes = store.get(&def.artifact_key)?;
        let shard_vecs: Vec<VectorRecord> = if let Some(cb) = codebook {
            let pq = PqShard::from_bytes(&bytes)?;
            pq.entries
                .iter()
                .map(|(id, codes)| {
                    cb.reconstruct(codes).map(|data| VectorRecord {
                        id: *id,
                        data,
                        metadata: None,
                    })
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            ShardIndex::from_bytes(&bytes)?.records
        };
        let start = corpus.len();
        corpus.extend(shard_vecs);
        shard_ranges.push(start..corpus.len());
    }

    let effective_k = k.min(corpus.len());
    if effective_k == 0 {
        return Err(IndexError::Other(
            "corpus is empty; cannot estimate recall".into(),
        ));
    }

    let nprobe = nprobe.min(shard_ranges.len());

    let mut total_recall = 0.0f64;
    for &query_idx in query_indices {
        let query = query_vectors.get(query_idx).ok_or_else(|| {
            IndexError::Other(format!(
                "recall sample query index {query_idx} is out of bounds for {} vectors",
                query_vectors.len()
            ))
        })?;
        // Exact brute-force ground truth over the full corpus.
        let gt = exact_search(query, &corpus, metric, effective_k);
        let gt_ids: Vec<VectorId> = gt.iter().map(|r| r.id).collect();

        // Approximate search: probe the nprobe nearest shards.
        let probe_indices = quantizer.top_probes(query, nprobe);
        let mut candidates: Vec<SearchResult> = Vec::new();
        for probe_idx in probe_indices {
            if let Some(range) = shard_ranges.get(probe_idx) {
                let results = exact_search(query, &corpus[range.clone()], metric, effective_k);
                candidates.extend(results);
            }
        }
        let approx = merge_top_k(candidates, effective_k);
        let approx_ids: Vec<VectorId> = approx.iter().map(|r| r.id).collect();

        total_recall += recall_at_k(&gt_ids, &approx_ids);
    }

    let mean_recall = total_recall / query_indices.len() as f64;

    Ok(RecallEstimate {
        k: effective_k as u32,
        recall_at_k: mean_recall as f32,
        sample_size: query_indices.len() as u64,
    })
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
            ann_family: None,
            hnsw_config: None,
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

    #[test]
    fn build_rejects_pq_for_non_euclidean_metric() {
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
            .build(BuildParams {
                records: vec![record(1, 4), record(2, 4)],
                dataset_version: DatasetVersion("ds-pq-metric".into()),
                embedding_version: EmbeddingVersion("emb-pq-metric".into()),
                index_version: IndexVersion("idx-pq-metric".into()),
                metric: DistanceMetric::Cosine,
                dims: 4,
                vectors_key: shardlake_storage::paths::dataset_vectors_key("ds-pq-metric"),
                metadata_key: shardlake_storage::paths::dataset_metadata_key("ds-pq-metric"),
                pq_params: Some(PqParams {
                    num_subspaces: 2,
                    codebook_size: 4,
                }),
                ann_family: None,
                hnsw_config: None,
            })
            .unwrap_err();
        assert!(err.to_string().contains("only euclidean distance"));
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
            ..SystemConfig::default()
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
                pq_params: None,
                ann_family: None,
                hnsw_config: None,
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
            ..SystemConfig::default()
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
            ..SystemConfig::default()
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
                pq_params: None,
                ann_family: None,
                hnsw_config: None,
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
                ..SystemConfig::default()
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
                    pq_params: None,
                    ann_family: None,
                    hnsw_config: None,
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

    /// When `recall_sample_size` is set, the builder must populate
    /// `manifest.recall_estimate` with valid values.
    #[test]
    fn build_with_recall_sample_size_populates_recall_estimate() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let n = 30usize;
        let dims = 4usize;
        let records: Vec<VectorRecord> = (0..n)
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
            nprobe: 2,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            recall_sample_size: Some(5),
            recall_k: 3,
            ..SystemConfig::default()
        };

        let manifest = IndexBuilder::new(&store, &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-recall".into()),
                embedding_version: EmbeddingVersion("emb-recall".into()),
                index_version: IndexVersion("idx-recall".into()),
                metric: DistanceMetric::Euclidean,
                dims,
                vectors_key: shardlake_storage::paths::dataset_vectors_key("ds-recall"),
                metadata_key: shardlake_storage::paths::dataset_metadata_key("ds-recall"),
                pq_params: None,
                ann_family: None,
                hnsw_config: None,
            })
            .unwrap();

        let est = manifest
            .recall_estimate
            .as_ref()
            .expect("recall_estimate must be populated when recall_sample_size is set");
        assert_eq!(est.k, 3, "recall_estimate.k must match recall_k config");
        assert!(
            est.sample_size > 0,
            "recall_estimate.sample_size must be > 0"
        );
        assert!(
            (0.0..=1.0).contains(&est.recall_at_k),
            "recall_estimate.recall_at_k must be in [0, 1], got {}",
            est.recall_at_k
        );
        // The manifest must still pass validation.
        manifest
            .validate()
            .expect("manifest with recall_estimate must pass validation");
    }

    /// When `recall_sample_size` is `None`, `manifest.recall_estimate` must be `None`.
    #[test]
    fn build_without_recall_sample_size_omits_recall_estimate() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 2,
            kmeans_iters: 5,
            nprobe: 1,
            recall_sample_size: None,
            ..SystemConfig::default()
        };

        let manifest = IndexBuilder::new(&store, &config)
            .build(build_params(
                (0..10)
                    .map(|i| VectorRecord {
                        id: VectorId(i as u64),
                        data: vec![i as f32, (i + 1) as f32],
                        metadata: None,
                    })
                    .collect(),
                2,
            ))
            .unwrap();

        assert!(
            manifest.recall_estimate.is_none(),
            "recall_estimate must be None when recall_sample_size is not set"
        );
    }

    /// Two builds with the same `recall_sample_size` and seed must produce
    /// identical recall estimates.
    #[test]
    fn recall_estimate_is_deterministic() {
        let n = 30usize;
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
                nprobe: 2,
                kmeans_seed: SystemConfig::default_kmeans_seed(),
                recall_sample_size: Some(5),
                recall_k: 3,
                ..SystemConfig::default()
            };
            IndexBuilder::new(&store, &config)
                .build(BuildParams {
                    records: records.clone(),
                    dataset_version: DatasetVersion("ds-recall-det".into()),
                    embedding_version: EmbeddingVersion("emb-recall-det".into()),
                    index_version: IndexVersion(idx_ver.into()),
                    metric: DistanceMetric::Euclidean,
                    dims,
                    vectors_key: shardlake_storage::paths::dataset_vectors_key("ds-recall-det"),
                    metadata_key: shardlake_storage::paths::dataset_metadata_key("ds-recall-det"),
                    pq_params: None,
                    ann_family: None,
                    hnsw_config: None,
                })
                .unwrap()
        };

        let m1 = build_once("idx-recall-det-1");
        let m2 = build_once("idx-recall-det-2");

        let e1 = m1.recall_estimate.unwrap();
        let e2 = m2.recall_estimate.unwrap();

        assert_eq!(
            e1.k, e2.k,
            "k must be identical across deterministic builds"
        );
        assert_eq!(
            e1.sample_size, e2.sample_size,
            "sample_size must be identical across deterministic builds"
        );
        assert!(
            (e1.recall_at_k - e2.recall_at_k).abs() < 1e-6,
            "recall_at_k must be identical across deterministic builds: {} vs {}",
            e1.recall_at_k,
            e2.recall_at_k
        );
    }

    /// Build with PQ compression enabled and recall_sample_size set; the
    /// recall estimate must still be populated and valid.
    #[test]
    fn recall_estimate_works_with_pq_compression() {
        let tmp = tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let n = 20usize;
        let dims = 4usize;
        let records: Vec<VectorRecord> = (0..n)
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
            nprobe: 2,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            pq_enabled: true,
            pq_num_subspaces: 2,
            pq_codebook_size: 4,
            recall_sample_size: Some(5),
            recall_k: 3,
            ..SystemConfig::default()
        };

        let manifest = IndexBuilder::new(&store, &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-recall-pq".into()),
                embedding_version: EmbeddingVersion("emb-recall-pq".into()),
                index_version: IndexVersion("idx-recall-pq".into()),
                metric: DistanceMetric::Euclidean,
                dims,
                vectors_key: shardlake_storage::paths::dataset_vectors_key("ds-recall-pq"),
                metadata_key: shardlake_storage::paths::dataset_metadata_key("ds-recall-pq"),
                pq_params: None,
                ann_family: None,
                hnsw_config: None,
            })
            .unwrap();

        let est = manifest
            .recall_estimate
            .as_ref()
            .expect("recall_estimate must be populated for PQ build with recall_sample_size set");
        assert_eq!(est.k, 3);
        assert!(est.sample_size > 0);
        assert!((0.0..=1.0).contains(&est.recall_at_k));
        manifest
            .validate()
            .expect("PQ manifest with recall_estimate must pass validation");
    }
}
