//! Query-time shard searcher with lazy loading and in-memory cache.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tracing::{debug, info};

use shardlake_core::{
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

        let probe_indices = top_n_centroids(query, &all_centroids, nprobe.min(all_centroids.len()));
        let mut probe_shards: Vec<ShardId> = probe_indices
            .into_iter()
            .filter_map(|i| centroid_to_shard.get(i).copied())
            .collect();
        probe_shards.sort();
        probe_shards.dedup();

        debug!(n_shards = probe_shards.len(), "Probing shards");

        if pq_enabled {
            self.search_pq_shards(query, &probe_shards, k, metric)
        } else {
            let mut all_results = Vec::new();
            for shard_id in probe_shards {
                let shard = self.load_shard(shard_id)?;
                let results = exact_search(query, &shard.records, metric, k);
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
        _metric: DistanceMetric,
    ) -> Result<Vec<SearchResult>> {
        let codebook = self.load_codebook()?;
        let table = codebook.compute_distance_table(query);

        let mut all_results: Vec<SearchResult> = Vec::new();

        for &shard_id in probe_shards {
            let shard = self.load_pq_shard(shard_id)?;
            let mut scored: Vec<SearchResult> = shard
                .entries
                .iter()
                .map(|(id, codes)| SearchResult {
                    id: *id,
                    score: codebook.adc_distance(codes, &table),
                    metadata: None,
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
        let err = searcher.search(&[1.0, 2.0, 3.0], 1, 1).unwrap_err();
        assert_eq!(
            err.to_string(),
            "core error: dimension mismatch: expected 2, got 3"
        );
    }
}
