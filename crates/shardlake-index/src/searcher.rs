//! Query-time shard searcher backed by an LRU [`ShardCache`].

use std::sync::Arc;

use tracing::{debug, info};

use shardlake_core::types::{DistanceMetric, SearchResult, ShardId};
use shardlake_manifest::Manifest;
use shardlake_storage::StorageBackend;

use crate::{
    cache::{CacheConfig, CacheMetrics, ShardCache},
    exact::{exact_search, merge_top_k},
    kmeans::top_n_centroids,
    shard::ShardIndex,
    IndexError, Result,
};

/// Searcher that loads shard indexes lazily from a storage backend, caching
/// them in an LRU [`ShardCache`].
pub struct IndexSearcher {
    store: Arc<dyn StorageBackend>,
    manifest: Manifest,
    cache: ShardCache,
}

impl IndexSearcher {
    /// Create a new searcher from a loaded manifest using default cache settings.
    pub fn new(store: Arc<dyn StorageBackend>, manifest: Manifest) -> Self {
        Self::with_cache_config(store, manifest, CacheConfig::default())
    }

    /// Create a new searcher with explicit cache configuration.
    pub fn with_cache_config(
        store: Arc<dyn StorageBackend>,
        manifest: Manifest,
        cache_config: CacheConfig,
    ) -> Self {
        info!(
            index_version = %manifest.index_version,
            shards = manifest.shards.len(),
            cache_capacity = cache_config.capacity,
            "IndexSearcher created"
        );
        Self {
            store,
            manifest,
            cache: ShardCache::new(cache_config),
        }
    }

    /// Return the underlying manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Return a snapshot of the shard cache metrics.
    pub fn cache_metrics(&self) -> CacheMetrics {
        self.cache.metrics()
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

        // Record the query for the prefetch policy (threshold 0 = disabled).
        let shard_defs: Vec<(ShardId, &str)> = self
            .manifest
            .shards
            .iter()
            .map(|s| (s.shard_id, s.artifact_key.as_str()))
            .collect();
        self.cache.record_query_and_prefetch(
            &probe_shards,
            &shard_defs,
            self.store.as_ref(),
            u64::MAX, // disabled by default; callers may set their own policy
        );

        let mut all_results = Vec::new();
        for shard_id in probe_shards {
            let shard = self.load_shard(shard_id)?;
            let results = exact_search(query, &shard.records, metric, k);
            all_results.extend(results);
        }

        Ok(merge_top_k(all_results, k))
    }

    /// Load a shard from the cache, falling back to the store on a miss.
    fn load_shard(&self, shard_id: ShardId) -> Result<Arc<ShardIndex>> {
        let shard_def = self
            .manifest
            .shards
            .iter()
            .find(|s| s.shard_id == shard_id)
            .ok_or_else(|| IndexError::Other(format!("shard {shard_id} not in manifest")))?;

        self.cache
            .get_or_load(shard_id, &shard_def.artifact_key, self.store.as_ref())
    }
}
