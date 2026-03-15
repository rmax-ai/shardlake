//! Query-time shard searcher with lazy loading and in-memory cache.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use tracing::{debug, info};

use shardlake_core::{
    error::CoreError,
    types::{DistanceMetric, SearchResult, ShardId, VectorId},
};
use shardlake_manifest::Manifest;
use shardlake_storage::ObjectStore;

use crate::{
    exact::{distance, exact_search, merge_top_k},
    kmeans::top_n_centroids,
    shard::ShardIndex,
    IndexError, Result,
};

/// Searcher that loads shard indexes lazily from `store`, caching them in RAM.
pub struct IndexSearcher {
    store: Arc<dyn ObjectStore>,
    manifest: Manifest,
    cache: Mutex<HashMap<ShardId, Arc<ShardIndex>>>,
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
        }
    }

    /// Return the underlying manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Perform approximate top-k search using nprobe shard probing.
    pub fn search(&self, query: &[f32], k: usize, nprobe: usize) -> Result<Vec<SearchResult>> {
        let expected_dims = self.manifest.dims as usize;
        if query.len() != expected_dims {
            return Err(IndexError::Core(CoreError::DimensionMismatch {
                expected: expected_dims,
                got: query.len(),
            }));
        }

        let metric: DistanceMetric = self.manifest.distance_metric;

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
                let shard = self.load_shard(shard_def.shard_id)?;
                for c in &shard.centroids {
                    all_centroids.push(c.clone());
                    centroid_to_shard.push(shard_def.shard_id);
                }
            }
        }

        if all_centroids.is_empty() {
            return Ok(Vec::new());
        }

        let probe_indices = top_n_centroids(query, &all_centroids, nprobe.min(all_centroids.len()));
        let mut probe_shards: Vec<ShardId> = probe_indices
            .into_iter()
            .filter_map(|i| centroid_to_shard.get(i).copied())
            .collect();
        probe_shards.sort();
        probe_shards.dedup();

        debug!(n_shards = probe_shards.len(), "Probing shards");

        let mut all_results = Vec::new();
        for shard_id in probe_shards {
            let shard = self.load_shard(shard_id)?;
            let results = exact_search(query, &shard.records, metric, k);
            all_results.extend(results);
        }

        Ok(merge_top_k(all_results, k))
    }

    /// Rerank ANN candidates using exact distance to `query`.
    ///
    /// Fetches the raw vectors for each candidate from the in-memory shard
    /// cache and recomputes exact distances, returning the candidates sorted
    /// by their true distances.  Call this after [`IndexSearcher::search`] when a more
    /// accurate final ranking is required.
    ///
    /// Candidates whose vectors are not found in the cache (e.g. they were
    /// returned by a different searcher instance) retain their original score.
    ///
    /// # Errors
    ///
    /// Returns an error if the shard cache lock is poisoned.
    pub fn rerank(
        &self,
        query: &[f32],
        mut candidates: Vec<SearchResult>,
    ) -> Result<Vec<SearchResult>> {
        let metric = self.manifest.distance_metric;

        // Collect candidate IDs so we only clone the vectors we need.
        let candidate_ids: HashSet<VectorId> = candidates.iter().map(|r| r.id).collect();

        // Build a lookup: VectorId -> raw f32 vector from the shard cache.
        // The lock is released before we iterate over candidates.
        let vector_lookup: HashMap<VectorId, Vec<f32>> = {
            let cache = self.cache.lock().map_err(|_| {
                IndexError::Other(
                    "shard cache lock poisoned during rerank: a previous thread panicked \
                         while holding the cache lock"
                        .into(),
                )
            })?;
            cache
                .values()
                .flat_map(|shard| shard.records.iter())
                .filter(|record| candidate_ids.contains(&record.id))
                .map(|record| (record.id, record.data.clone()))
                .collect()
        };

        // Re-score each candidate with the exact distance function.
        for result in &mut candidates {
            if let Some(raw_vec) = vector_lookup.get(&result.id) {
                result.score = distance(query, raw_vec, metric);
            }
        }

        // Sort ascending (lower score = better) by exact distance.
        candidates.sort_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(candidates)
    }

    /// Load a shard from cache or store.
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
        config::SystemConfig,
        types::{DatasetVersion, EmbeddingVersion, IndexVersion, VectorId, VectorRecord},
    };
    use shardlake_storage::LocalObjectStore;

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
            kmeans_sample_size: None,
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
            })
            .unwrap();

        let searcher = IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest);
        let err = searcher.search(&[1.0, 2.0, 3.0], 1, 1).unwrap_err();
        assert_eq!(
            err.to_string(),
            "core error: dimension mismatch: expected 2, got 3"
        );
    }
}
