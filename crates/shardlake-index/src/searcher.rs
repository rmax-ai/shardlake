//! Query-time shard searcher with lazy loading and in-memory cache.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use tracing::{debug, info};

use shardlake_core::{
    config::FanOutPolicy,
    error::CoreError,
    types::{DistanceMetric, SearchResult, ShardId},
};
use shardlake_manifest::Manifest;
use shardlake_storage::ObjectStore;

use crate::{
    exact::{exact_search, merge_top_k},
    kmeans::top_n_centroids,
    pq::PqCodebook,
    shard::{PqShard, ShardIndex},
    IndexError, Result, PQ8_CODEC,
};

/// Searcher that loads shard indexes lazily from `store`, caching them in RAM.
pub struct IndexSearcher {
    store: Arc<dyn ObjectStore>,
    manifest: Manifest,
    cache: Mutex<HashMap<ShardId, Arc<ShardIndex>>>,
    pq_shard_cache: Mutex<HashMap<ShardId, Arc<PqShard>>>,
    /// PQ codebook; loaded once on first PQ search, then cached.
    codebook: Mutex<Option<Arc<PqCodebook>>>,
    metadata_cache: Mutex<Option<Arc<HashMap<String, serde_json::Value>>>>,
}

impl IndexSearcher {
    /// Create a new searcher from a loaded manifest.
    pub fn new(store: Arc<dyn ObjectStore>, manifest: Manifest) -> Self {
        info!(
            index_version = %manifest.index_version,
            shards = manifest.shards.len(),
            "IndexSearcher created"
        );
        Self {
            store,
            manifest,
            cache: Mutex::new(HashMap::new()),
            pq_shard_cache: Mutex::new(HashMap::new()),
            codebook: Mutex::new(None),
            metadata_cache: Mutex::new(None),
        }
    }

    /// Return the underlying manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Perform approximate top-k search using the provided [`FanOutPolicy`].
    ///
    /// The policy controls:
    /// - how many IVF centroids are selected for routing
    ///   ([`FanOutPolicy::candidate_centroids`]),
    /// - the maximum number of shards probed after deduplication
    ///   ([`FanOutPolicy::candidate_shards`], `0` = no cap), and
    /// - the maximum number of vectors evaluated per shard
    ///   ([`FanOutPolicy::max_vectors_per_shard`], `0` = no limit).
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        policy: &FanOutPolicy,
    ) -> Result<Vec<SearchResult>> {
        let expected_dims = self.manifest.dims as usize;
        if query.len() != expected_dims {
            return Err(IndexError::Core(CoreError::DimensionMismatch {
                expected: expected_dims,
                got: query.len(),
            }));
        }

        let metric: DistanceMetric = self.manifest.distance_metric;
        let pq_enabled =
            self.manifest.compression.enabled && self.manifest.compression.codec == PQ8_CODEC;

        // Collect centroids for routing from the manifest when available (manifest v2+).
        // Shards built with an older builder (manifest v1) have an empty centroid vec; for
        // those shards we fall back to loading the shard body to extract the centroid.
        let mut all_centroids: Vec<Vec<f32>> = Vec::new();
        let mut centroid_to_shard: Vec<ShardId> = Vec::new();

        for shard_def in &self.manifest.shards {
            if !shard_def.centroid.is_empty() {
                // Fast path: centroid is embedded in the manifest -- no I/O needed.
                // Validate that the centroid dimensionality matches the index dimensionality.
                if shard_def.centroid.len() != expected_dims {
                    return Err(IndexError::Core(CoreError::DimensionMismatch {
                        expected: expected_dims,
                        got: shard_def.centroid.len(),
                    }));
                }
                all_centroids.push(shard_def.centroid.clone());
                centroid_to_shard.push(shard_def.shard_id);
            } else {
                // Slow path: legacy manifest without centroid metadata -- load the shard
                // body to read its centroids (preserves backward compatibility).
                if pq_enabled {
                    let shard = self.load_pq_shard(shard_def.shard_id)?;
                    for c in &shard.centroids {
                        all_centroids.push(c.clone());
                        centroid_to_shard.push(shard_def.shard_id);
                    }
                } else {
                    let shard = self.load_shard(shard_def.shard_id)?;
                    for c in &shard.centroids {
                        all_centroids.push(c.clone());
                        centroid_to_shard.push(shard_def.shard_id);
                    }
                }
            }
        }

        if all_centroids.is_empty() {
            return Ok(Vec::new());
        }

        // ===== ROUTING STEP =====
        // Select the top `candidate_centroids` nearest IVF centroids.
        let n_centroids = (policy.candidate_centroids as usize).min(all_centroids.len());
        let probe_indices = top_n_centroids(query, &all_centroids, n_centroids);

        // Map centroid indices to shard ids and deduplicate.
        let mut probe_shards: Vec<ShardId> = probe_indices
            .into_iter()
            .filter_map(|i| centroid_to_shard.get(i).copied())
            .collect();
        let mut seen = HashSet::new();
        probe_shards.retain(|shard_id| seen.insert(*shard_id));

        // Apply candidate_shards cap (0 = no cap).
        if policy.candidate_shards > 0 {
            probe_shards.truncate(policy.candidate_shards as usize);
        }

        debug!(
            n_shards = probe_shards.len(),
            candidate_centroids = policy.candidate_centroids,
            candidate_shards = policy.candidate_shards,
            max_vectors_per_shard = policy.max_vectors_per_shard,
            "Probing shards"
        );

        if pq_enabled {
            self.search_pq_shards(
                query,
                &probe_shards,
                k,
                metric,
                policy.max_vectors_per_shard,
            )
        } else {
            let mut all_results = Vec::new();
            for shard_id in probe_shards {
                let shard = self.load_shard(shard_id)?;
                let records = if policy.max_vectors_per_shard > 0 {
                    let limit = (policy.max_vectors_per_shard as usize).min(shard.records.len());
                    &shard.records[..limit]
                } else {
                    &shard.records
                };
                let results = exact_search(query, records, metric, k);
                all_results.extend(results);
            }
            Ok(merge_top_k(all_results, k))
        }
    }

    // ── PQ search path ────────────────────────────────────────────────────────

    /// Search probed PQ-encoded shards using Asymmetric Distance Computation.
    fn search_pq_shards(
        &self,
        query: &[f32],
        probe_shards: &[ShardId],
        k: usize,
        metric: DistanceMetric,
        max_vectors_per_shard: u32,
    ) -> Result<Vec<SearchResult>> {
        if metric != DistanceMetric::Euclidean {
            return Err(IndexError::Other(
                "PQ search currently supports only euclidean distance".into(),
            ));
        }

        let codebook = self.load_codebook()?;
        let table = codebook.compute_distance_table(query)?;
        let metadata_map = self.load_metadata_map()?;

        let mut all_results: Vec<SearchResult> = Vec::new();

        for &shard_id in probe_shards {
            let shard = self.load_pq_shard(shard_id)?;
            let max_entries = if max_vectors_per_shard > 0 {
                (max_vectors_per_shard as usize).min(shard.entries.len())
            } else {
                shard.entries.len()
            };
            let mut scored: Vec<SearchResult> = shard
                .entries
                .iter()
                .take(max_entries)
                .map(|(id, codes)| SearchResult {
                    id: *id,
                    score: codebook.adc_distance(codes, &table),
                    metadata: metadata_map.get(&id.to_string()).cloned(),
                })
                .collect();
            scored.sort_by(|a, b| {
                a.score
                    .partial_cmp(&b.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            scored.truncate(k);
            all_results.extend(scored);
        }

        Ok(merge_top_k(all_results, k))
    }

    /// Load (or return from cache) the PQ codebook for this index.
    fn load_codebook(&self) -> Result<Arc<PqCodebook>> {
        {
            let guard = self
                .codebook
                .lock()
                .map_err(|_| IndexError::Other("codebook lock poisoned".into()))?;
            if let Some(ref cb) = *guard {
                return Ok(Arc::clone(cb));
            }
        }

        let cb_key = self
            .manifest
            .compression
            .codebook_key
            .as_deref()
            .ok_or_else(|| {
                IndexError::Other("PQ index has no codebook_key in compression config".into())
            })?;

        let bytes = self.store.get(cb_key)?;
        let cb = Arc::new(PqCodebook::from_bytes(&bytes)?);
        if cb.dims != self.manifest.dims as usize {
            return Err(IndexError::Other(format!(
                "PQ codebook dims {} do not match manifest dims {}",
                cb.dims, self.manifest.dims
            )));
        }
        if cb.params.num_subspaces != self.manifest.compression.pq_num_subspaces as usize {
            return Err(IndexError::Other(format!(
                "PQ codebook subspaces {} do not match manifest pq_num_subspaces {}",
                cb.params.num_subspaces, self.manifest.compression.pq_num_subspaces
            )));
        }
        if cb.params.codebook_size != self.manifest.compression.pq_codebook_size as usize {
            return Err(IndexError::Other(format!(
                "PQ codebook size {} do not match manifest pq_codebook_size {}",
                cb.params.codebook_size, self.manifest.compression.pq_codebook_size
            )));
        }

        let mut guard = self
            .codebook
            .lock()
            .map_err(|_| IndexError::Other("codebook lock poisoned".into()))?;
        *guard = Some(Arc::clone(&cb));
        Ok(cb)
    }

    fn load_metadata_map(&self) -> Result<Arc<HashMap<String, serde_json::Value>>> {
        {
            let guard = self
                .metadata_cache
                .lock()
                .map_err(|_| IndexError::Other("metadata cache lock poisoned".into()))?;
            if let Some(ref metadata) = *guard {
                return Ok(Arc::clone(metadata));
            }
        }

        let bytes = self.store.get(&self.manifest.metadata_key)?;
        let metadata: HashMap<String, serde_json::Value> = serde_json::from_slice(&bytes)
            .map_err(|err| IndexError::Other(format!("invalid dataset metadata map: {err}")))?;
        let metadata = Arc::new(metadata);

        let mut guard = self
            .metadata_cache
            .lock()
            .map_err(|_| IndexError::Other("metadata cache lock poisoned".into()))?;
        *guard = Some(Arc::clone(&metadata));
        Ok(metadata)
    }

    /// Load a PQ-encoded shard from cache or store.
    fn load_pq_shard(&self, shard_id: ShardId) -> Result<Arc<PqShard>> {
        {
            let cache = self
                .pq_shard_cache
                .lock()
                .map_err(|_| IndexError::Other("PQ shard cache lock poisoned".into()))?;
            if let Some(s) = cache.get(&shard_id) {
                return Ok(Arc::clone(s));
            }
        }

        let shard_def = self
            .manifest
            .shards
            .iter()
            .find(|s| s.shard_id == shard_id)
            .ok_or_else(|| IndexError::Other(format!("shard {shard_id} not in manifest")))?;

        let bytes = self.store.get(&shard_def.artifact_key)?;
        let shard = Arc::new(PqShard::from_bytes(&bytes)?);
        if shard.dims != self.manifest.dims as usize {
            return Err(IndexError::Other(format!(
                "PQ shard {shard_id} dims {} do not match manifest dims {}",
                shard.dims, self.manifest.dims
            )));
        }
        if shard.pq_m != self.manifest.compression.pq_num_subspaces as usize {
            return Err(IndexError::Other(format!(
                "PQ shard {shard_id} subspaces {} do not match manifest pq_num_subspaces {}",
                shard.pq_m, self.manifest.compression.pq_num_subspaces
            )));
        }
        if shard.pq_k != self.manifest.compression.pq_codebook_size as usize {
            return Err(IndexError::Other(format!(
                "PQ shard {shard_id} codebook size {} do not match manifest pq_codebook_size {}",
                shard.pq_k, self.manifest.compression.pq_codebook_size
            )));
        }

        let mut cache = self
            .pq_shard_cache
            .lock()
            .map_err(|_| IndexError::Other("PQ shard cache lock poisoned".into()))?;
        cache.insert(shard_id, Arc::clone(&shard));
        Ok(shard)
    }

    // ── Raw shard path ────────────────────────────────────────────────────────

    /// Load a raw shard from cache or store.
    fn load_shard(&self, shard_id: ShardId) -> Result<Arc<ShardIndex>> {
        {
            let cache = self
                .cache
                .lock()
                .map_err(|_| IndexError::Other("search cache lock poisoned".into()))?;
            if let Some(idx) = cache.get(&shard_id) {
                return Ok(Arc::clone(idx));
            }
        }

        let shard_def = self
            .manifest
            .shards
            .iter()
            .find(|s| s.shard_id == shard_id)
            .ok_or_else(|| IndexError::Other(format!("shard {shard_id} not in manifest")))?;

        let bytes = self.store.get(&shard_def.artifact_key)?;
        let idx = Arc::new(ShardIndex::from_bytes(&bytes)?);

        let mut cache = self
            .cache
            .lock()
            .map_err(|_| IndexError::Other("search cache lock poisoned".into()))?;
        cache.insert(shard_id, Arc::clone(&idx));
        Ok(idx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::builder::{BuildParams, IndexBuilder};
    use shardlake_core::{
        config::{FanOutPolicy, SystemConfig},
        types::{DatasetVersion, EmbeddingVersion, IndexVersion, VectorId, VectorRecord},
    };
    use shardlake_storage::LocalObjectStore;

    fn build_test_searcher(tmp: &tempfile::TempDir) -> IndexSearcher {
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 2,
            kmeans_iters: 2,
            nprobe: 2,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            candidate_shards: 0,
            max_vectors_per_shard: 0,
            kmeans_sample_size: None,
            ..SystemConfig::default()
        };
        let records = vec![
            VectorRecord {
                id: VectorId(1),
                data: vec![1.0, 0.0],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(2),
                data: vec![0.0, 1.0],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(3),
                data: vec![1.0, 1.0],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(4),
                data: vec![0.5, 0.5],
                metadata: None,
            },
        ];
        let manifest = IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-test".into()),
                embedding_version: EmbeddingVersion("emb-test".into()),
                index_version: IndexVersion("idx-test".into()),
                metric: DistanceMetric::Cosine,
                dims: 2,
                vectors_key: "datasets/ds-test/vectors.jsonl".into(),
                metadata_key: "datasets/ds-test/metadata.json".into(),
                pq_params: None,
            })
            .unwrap();
        IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest)
    }

    #[test]
    fn search_rejects_query_dimension_mismatch() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 1,
            kmeans_iters: 2,
            nprobe: 1,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            candidate_shards: 0,
            max_vectors_per_shard: 0,
            kmeans_sample_size: None,
            ..SystemConfig::default()
        };
        let records = vec![VectorRecord {
            id: VectorId(1),
            data: vec![1.0, 2.0],
            metadata: None,
        }];

        let manifest = IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-test".into()),
                embedding_version: EmbeddingVersion("emb-test".into()),
                index_version: IndexVersion("idx-test".into()),
                metric: DistanceMetric::Cosine,
                dims: 2,
                vectors_key: "datasets/ds-test/vectors.jsonl".into(),
                metadata_key: "datasets/ds-test/metadata.json".into(),
                pq_params: None,
            })
            .unwrap();

        let searcher = IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest);
        let policy = FanOutPolicy {
            candidate_centroids: 1,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let err = searcher.search(&[1.0, 2.0, 3.0], 1, &policy).unwrap_err();
        assert_eq!(
            err.to_string(),
            "core error: dimension mismatch: expected 2, got 3"
        );
    }

    #[test]
    fn candidate_shards_cap_limits_probed_shards() {
        let tmp = tempdir().unwrap();
        let searcher = build_test_searcher(&tmp);

        // With candidate_shards=1, only one shard is probed; results are still returned.
        let policy = FanOutPolicy {
            candidate_centroids: 4,
            candidate_shards: 1,
            max_vectors_per_shard: 0,
        };
        let results = searcher.search(&[1.0, 0.0], 2, &policy).unwrap();
        assert!(!results.is_empty(), "expected at least one result");
    }

    #[test]
    fn max_vectors_per_shard_limits_candidates() {
        let tmp = tempdir().unwrap();
        let searcher = build_test_searcher(&tmp);

        // Limit to 1 vector per shard; we still get results (just fewer candidates).
        let policy = FanOutPolicy {
            candidate_centroids: 4,
            candidate_shards: 0,
            max_vectors_per_shard: 1,
        };
        let results = searcher.search(&[1.0, 0.0], 4, &policy).unwrap();
        // With 2 shards and 1 vector/shard, we can get at most 2 results.
        assert!(results.len() <= 2);
    }
}
