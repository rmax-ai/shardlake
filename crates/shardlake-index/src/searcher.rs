//! Query-time shard searcher with lazy loading and in-memory cache.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use tracing::{debug, info, instrument};

use shardlake_core::types::{DistanceMetric, SearchResult, ShardId};
use shardlake_manifest::Manifest;
use shardlake_storage::ObjectStore;

use crate::{
    exact::{exact_search, merge_top_k},
    kmeans::top_n_centroids,
    query_plan::QueryPlan,
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
    ///
    /// Records the following metrics (via the globally-installed `metrics` recorder):
    /// - `query_latency_seconds` histogram — wall-clock time for the full search.
    #[instrument(skip(self, query), fields(k, nprobe))]
    pub fn search(&self, query: &[f32], k: usize, nprobe: usize) -> Result<Vec<SearchResult>> {
        let t0 = Instant::now();
        let plan = self.build_query_plan(query, k, nprobe)?;
        let elapsed = t0.elapsed().as_secs_f64();
        metrics::histogram!("query_latency_seconds").record(elapsed);
        Ok(plan.results)
    }

    /// Perform approximate top-k search and return a full [`QueryPlan`] for debugging.
    ///
    /// The plan includes the selected centroids, searched shards, all candidate vectors
    /// gathered before reranking, and the final top-k results.
    #[instrument(skip(self, query), fields(k, nprobe))]
    pub fn search_with_plan(&self, query: &[f32], k: usize, nprobe: usize) -> Result<QueryPlan> {
        self.build_query_plan(query, k, nprobe)
    }

    // ---------------------------------------------------------------------------
    // Internal helpers
    // ---------------------------------------------------------------------------

    /// Core search logic shared by [`search`] and [`search_with_plan`].
    fn build_query_plan(&self, query: &[f32], k: usize, nprobe: usize) -> Result<QueryPlan> {
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
            return Ok(QueryPlan {
                selected_centroids: Vec::new(),
                searched_shards: Vec::new(),
                candidate_vectors: Vec::new(),
                results: Vec::new(),
            });
        }

        let probe_indices = top_n_centroids(query, &all_centroids, nprobe.min(all_centroids.len()));

        let selected_centroids: Vec<Vec<f32>> = probe_indices
            .iter()
            .filter_map(|&i| all_centroids.get(i).cloned())
            .collect();

        let mut probe_shards: Vec<ShardId> = probe_indices
            .into_iter()
            .filter_map(|i| centroid_to_shard.get(i).copied())
            .collect();
        probe_shards.sort();
        probe_shards.dedup();

        debug!(n_shards = probe_shards.len(), "Probing shards");

        let searched_shards: Vec<u32> = probe_shards.iter().map(|s| s.0).collect();

        let mut candidate_vectors: Vec<SearchResult> = Vec::new();
        for shard_id in probe_shards {
            let shard = self.load_shard(shard_id)?;
            let results = exact_search(query, &shard.records, metric, k);
            candidate_vectors.extend(results);
        }

        // Rerank stage: merge all candidates to final top-k.
        let n_candidates = candidate_vectors.len();
        let results = {
            let _rerank_span =
                tracing::info_span!("rerank", candidates = n_candidates, k).entered();
            merge_top_k(candidate_vectors.clone(), k)
        };

        Ok(QueryPlan {
            selected_centroids,
            searched_shards,
            candidate_vectors,
            results,
        })
    }

    /// Load a shard from cache or store.
    ///
    /// Records the following metrics:
    /// - `shard_cache_hits_total` counter — incremented on a cache hit.
    /// - `shard_load_latency_seconds` histogram — observed only on a cache miss.
    #[instrument(skip(self), fields(shard_id = shard_id.0))]
    fn load_shard(&self, shard_id: ShardId) -> Result<Arc<ShardIndex>> {
        {
            let cache = self.cache.lock().unwrap();
            if let Some(idx) = cache.get(&shard_id) {
                metrics::counter!("shard_cache_hits_total").increment(1);
                return Ok(Arc::clone(idx));
            }
        }

        let shard_def = self
            .manifest
            .shards
            .iter()
            .find(|s| s.shard_id == shard_id)
            .ok_or_else(|| IndexError::Other(format!("shard {shard_id} not in manifest")))?;

        let t0 = Instant::now();
        let bytes = self.store.get(&shard_def.artifact_key)?;
        let idx = Arc::new(ShardIndex::from_bytes(&bytes)?);
        let elapsed = t0.elapsed().as_secs_f64();
        metrics::histogram!("shard_load_latency_seconds").record(elapsed);

        let mut cache = self.cache.lock().unwrap();
        cache.insert(shard_id, Arc::clone(&idx));
        Ok(idx)
    }
}
