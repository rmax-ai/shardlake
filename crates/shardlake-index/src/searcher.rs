//! Query-time shard searcher with lazy loading and in-memory cache.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tracing::{debug, info};

use shardlake_core::types::{DistanceMetric, SearchResult, ShardId};
use shardlake_manifest::Manifest;
use shardlake_storage::ObjectStore;

use crate::{
    exact::{exact_search, merge_top_k},
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

    /// Clear the shard cache.
    ///
    /// All subsequent queries will reload shard data from the object store.
    /// This is useful for simulating cold-start query workloads in benchmarks.
    pub fn clear_cache(&self) {
        self.cache.lock().unwrap().clear();
    }

    /// Perform approximate top-k search using nprobe shard probing.
    pub fn search(&self, query: &[f32], k: usize, nprobe: usize) -> Result<Vec<SearchResult>> {
        let metric: DistanceMetric = self.manifest.distance_metric;

        // Collect all centroids and map centroid index → shard id.
        let mut all_centroids: Vec<Vec<f32>> = Vec::new();
        let mut centroid_to_shard: Vec<ShardId> = Vec::new();

        for shard_def in &self.manifest.shards {
            let shard = self.load_shard(shard_def.shard_id)?;
            for c in &shard.centroids {
                all_centroids.push(c.clone());
                centroid_to_shard.push(shard_def.shard_id);
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

    /// Load a shard from cache or store.
    fn load_shard(&self, shard_id: ShardId) -> Result<Arc<ShardIndex>> {
        {
            let cache = self.cache.lock().unwrap();
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

        let mut cache = self.cache.lock().unwrap();
        cache.insert(shard_id, Arc::clone(&idx));
        Ok(idx)
    }
}
