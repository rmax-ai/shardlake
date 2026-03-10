//! Query-time shard searcher with lazy loading and in-memory cache.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tracing::{debug, info};

use shardlake_core::types::{DistanceMetric, SearchResult, ShardId, VectorId, VectorRecord};
use shardlake_manifest::Manifest;
use shardlake_storage::ObjectStore;

use crate::{
    bm25::BM25Index,
    exact::{exact_search, merge_top_k},
    hybrid::hybrid_rank,
    kmeans::top_n_centroids,
    shard::ShardIndex,
    IndexError, Result,
};

/// Searcher that loads shard indexes lazily from `store`, caching them in RAM.
pub struct IndexSearcher {
    store: Arc<dyn ObjectStore>,
    manifest: Manifest,
    cache: Mutex<HashMap<ShardId, Arc<ShardIndex>>>,
    /// Optional BM25 index for lexical and hybrid retrieval.
    bm25: Option<BM25Index>,
}

impl IndexSearcher {
    /// Create a new searcher from a loaded manifest (vector-only mode).
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
            bm25: None,
        }
    }

    /// Create a searcher that also builds a BM25 index from the provided
    /// corpus records (enables lexical and hybrid search).
    pub fn with_corpus(
        store: Arc<dyn ObjectStore>,
        manifest: Manifest,
        corpus: &[VectorRecord],
    ) -> Self {
        let bm25 = BM25Index::from_records(corpus);
        info!(
            index_version = %manifest.index_version,
            shards = manifest.shards.len(),
            bm25_docs = bm25.num_docs(),
            "IndexSearcher created with BM25 index"
        );
        Self {
            store,
            manifest,
            cache: Mutex::new(HashMap::new()),
            bm25: Some(bm25),
        }
    }

    /// Return the underlying manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Return `true` if a BM25 index has been loaded (lexical/hybrid available).
    pub fn has_bm25(&self) -> bool {
        self.bm25.is_some()
    }

    /// Perform approximate top-k **vector** search using nprobe shard probing.
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

    /// Perform pure **lexical** (BM25) top-k search.
    ///
    /// Returns `Err` if no BM25 index has been loaded (i.e. the searcher was
    /// created with [`IndexSearcher::new`] instead of
    /// [`IndexSearcher::with_corpus`]).
    pub fn search_lexical(&self, query_text: &str, k: usize) -> Result<Vec<SearchResult>> {
        let bm25 = self.bm25.as_ref().ok_or_else(|| {
            IndexError::Other(
                "BM25 index not available; restart the server with a corpus loaded".into(),
            )
        })?;
        Ok(bm25.search(query_text, k))
    }

    /// Perform **hybrid** retrieval: blend ANN vector scores with BM25 scores.
    ///
    /// * `alpha = 1.0` → pure vector ordering.
    /// * `alpha = 0.0` → pure lexical (BM25) ordering.
    /// * `alpha = 0.5` → equal blend (default).
    ///
    /// Returns `Err` if no BM25 index has been loaded.
    pub fn search_hybrid(
        &self,
        query: &[f32],
        query_text: &str,
        k: usize,
        nprobe: usize,
        alpha: f32,
    ) -> Result<Vec<SearchResult>> {
        let bm25 = self.bm25.as_ref().ok_or_else(|| {
            IndexError::Other(
                "BM25 index not available; restart the server with a corpus loaded".into(),
            )
        })?;

        // Run vector search to get ANN candidates.
        let vec_results = self.search(query, k, nprobe)?;

        if vec_results.is_empty() {
            return Ok(Vec::new());
        }

        // Score the ANN candidates with BM25.
        let all_bm25 = bm25.score_all(query_text);
        let bm25_map: HashMap<VectorId, f32> = all_bm25.into_iter().collect();

        Ok(hybrid_rank(&vec_results, &bm25_map, k, alpha))
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
