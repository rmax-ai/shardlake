//! Modular query pipeline with explicit, testable stages.
//!
//! The pipeline breaks query execution into six named stages:
//!
//! 1. **embed_query** – accept a pre-computed query vector (pass-through).
//! 2. **route_centroids** – select the `candidate_shards` nearest shard
//!    centroids and return their shard IDs.
//! 3. **load_shards** – retrieve [`ShardIndex`] instances from cache or
//!    object store.
//! 4. **search_shards** – run exact ANN candidate search on each shard.
//! 5. **merge** – combine shard-local results into a global top-k list.
//! 6. **rerank** – re-sort the merged candidates and trim to `top_k`
//!    (no-op when `rerank_limit` is not configured).
//!
//! Use [`QueryPipeline::run`] to execute all stages synchronously, or
//! [`QueryPipeline::run_parallel`] for concurrent shard searching backed
//! by [`tokio::task::spawn_blocking`].

use std::sync::Arc;

use tracing::debug;

use shardlake_core::{
    query::QueryConfig,
    types::{DistanceMetric, SearchResult, ShardId},
};

use crate::{
    exact::{exact_search, merge_top_k},
    kmeans::top_n_centroids,
    searcher::IndexSearcher,
    shard::ShardIndex,
    IndexError, Result,
};

/// Orchestrates the query pipeline stages against a loaded [`IndexSearcher`].
pub struct QueryPipeline<'a> {
    searcher: &'a IndexSearcher,
}

impl<'a> QueryPipeline<'a> {
    /// Create a new pipeline bound to `searcher`.
    pub fn new(searcher: &'a IndexSearcher) -> Self {
        Self { searcher }
    }

    // -----------------------------------------------------------------------
    // Stage 1 – embed query
    // -----------------------------------------------------------------------

    /// Return the query vector unchanged.
    ///
    /// This stage exists as an explicit hook for future embedding models.
    /// For raw-vector queries it is a zero-cost pass-through.
    pub fn embed_query<'q>(&self, raw: &'q [f32]) -> &'q [f32] {
        raw
    }

    // -----------------------------------------------------------------------
    // Stage 2 – centroid routing
    // -----------------------------------------------------------------------

    /// Find the `candidate_shards` shard IDs whose centroids are nearest to
    /// `query`.
    ///
    /// Returns an empty `Vec` when the index contains no centroids.
    ///
    /// # Errors
    /// Propagates any shard-load errors encountered while collecting centroids.
    pub fn route_centroids(&self, query: &[f32], candidate_shards: usize) -> Result<Vec<ShardId>> {
        let manifest = self.searcher.manifest();

        // Load all shards up-front so we can borrow their centroid slices
        // without cloning.
        let loaded: Vec<Arc<ShardIndex>> = manifest
            .shards
            .iter()
            .map(|s| self.searcher.load_shard(s.shard_id))
            .collect::<Result<_>>()?;

        let mut all_centroids: Vec<&Vec<f32>> = Vec::new();
        let mut centroid_to_shard: Vec<ShardId> = Vec::new();

        for (shard_def, shard) in manifest.shards.iter().zip(&loaded) {
            for c in &shard.centroids {
                all_centroids.push(c);
                centroid_to_shard.push(shard_def.shard_id);
            }
        }

        if all_centroids.is_empty() {
            return Ok(Vec::new());
        }

        let n = candidate_shards.min(all_centroids.len());
        let probe_indices = top_n_centroids(query, &all_centroids, n);
        let mut shard_ids: Vec<ShardId> = probe_indices
            .into_iter()
            .filter_map(|i| centroid_to_shard.get(i).copied())
            .collect();
        shard_ids.sort_unstable();
        shard_ids.dedup();

        debug!(n_shards = shard_ids.len(), "Routed to shards");
        Ok(shard_ids)
    }

    // -----------------------------------------------------------------------
    // Stage 3 – shard loading
    // -----------------------------------------------------------------------

    /// Load [`ShardIndex`] instances for each ID, using the searcher's cache.
    ///
    /// # Errors
    /// Returns the first storage error encountered.
    pub fn load_shards(&self, shard_ids: &[ShardId]) -> Result<Vec<Arc<ShardIndex>>> {
        shard_ids
            .iter()
            .map(|&id| self.searcher.load_shard(id))
            .collect()
    }

    // -----------------------------------------------------------------------
    // Stage 4 – ANN candidate search
    // -----------------------------------------------------------------------

    /// Run exact search on each shard and concatenate the local results.
    ///
    /// `candidates` is the per-shard top-k used during this stage; the
    /// global merge step will reduce this further.
    #[must_use]
    pub fn search_shards(
        query: &[f32],
        shards: &[Arc<ShardIndex>],
        metric: DistanceMetric,
        candidates: usize,
    ) -> Vec<SearchResult> {
        let mut all = Vec::new();
        for shard in shards {
            let results = exact_search(query, &shard.records, metric, candidates);
            all.extend(results);
        }
        all
    }

    // -----------------------------------------------------------------------
    // Stage 5 – merge (see `exact::merge_top_k`)
    // -----------------------------------------------------------------------
    //
    // Merge is implemented directly with `merge_top_k` in `run` and
    // `run_parallel` to avoid an unnecessary wrapper.  The public stage
    // boundary is documented in the module-level doc comment above.

    // -----------------------------------------------------------------------
    // Stage 6 – rerank
    // -----------------------------------------------------------------------

    /// Re-sort `results` by score (ascending) and truncate to `limit`.
    ///
    /// With raw-vector queries the scores are already comparable across shards,
    /// so this stage simply trims over-expanded candidate lists down to the
    /// desired `top_k`.  It provides a hook for future model-based reranking.
    #[must_use]
    pub fn rerank(mut results: Vec<SearchResult>, limit: usize) -> Vec<SearchResult> {
        results.sort_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(limit);
        results
    }

    // -----------------------------------------------------------------------
    // Full pipeline – synchronous
    // -----------------------------------------------------------------------

    /// Execute all pipeline stages synchronously and return the top-k results.
    ///
    /// # Errors
    /// Returns an [`IndexError`] if centroid routing or shard loading fails.
    pub fn run(&self, query: &[f32], config: &QueryConfig) -> Result<Vec<SearchResult>> {
        let metric = config
            .distance_metric
            .unwrap_or(self.searcher.manifest().distance_metric);

        // Stage 1
        let query = self.embed_query(query);
        // Stage 2
        let shard_ids = self.route_centroids(query, config.candidate_shards)?;
        if shard_ids.is_empty() {
            return Ok(Vec::new());
        }
        // Stage 3
        let shards = self.load_shards(&shard_ids)?;
        let candidates_k = config.effective_candidates();
        // Stage 4
        let candidates = Self::search_shards(query, &shards, metric, candidates_k);
        // Stage 5 – merge
        let merged = merge_top_k(candidates, candidates_k);
        // Stage 6 – optional rerank
        Ok(if config.rerank_limit.is_some() {
            Self::rerank(merged, config.top_k)
        } else {
            merged
        })
    }

    // -----------------------------------------------------------------------
    // Full pipeline – async / parallel shard search
    // -----------------------------------------------------------------------

    /// Execute the pipeline with **parallel** shard searching.
    ///
    /// Stages 1–3 run synchronously (centroid routing and shard cache lookups
    /// are fast and share the same `Mutex`-protected cache).  Stage 4 fans out
    /// each shard search to [`tokio::task::spawn_blocking`] so that multiple
    /// CPU-bound searches run concurrently.  Stages 5–6 collect and merge the
    /// results on the calling task.
    ///
    /// # Arguments
    /// * `searcher` – shared reference to the index; must be `Arc` so the
    ///   ownership can be retained across the async boundary.
    /// * `query` – the query vector as a shared slice.
    /// * `config` – query-time parameters.
    ///
    /// # Errors
    /// Returns an [`IndexError`] on routing, loading, or task-join failures.
    pub async fn run_parallel(
        searcher: Arc<IndexSearcher>,
        query: Arc<[f32]>,
        config: QueryConfig,
    ) -> Result<Vec<SearchResult>> {
        let metric = config
            .distance_metric
            .unwrap_or(searcher.manifest().distance_metric);

        // Stages 1-3: synchronous (fast cache / routing work)
        let shard_ids = {
            let pipeline = QueryPipeline::new(&searcher);
            pipeline.route_centroids(&query, config.candidate_shards)?
        };

        if shard_ids.is_empty() {
            return Ok(Vec::new());
        }

        let shards: Vec<Arc<ShardIndex>> = shard_ids
            .iter()
            .map(|&id| searcher.load_shard(id))
            .collect::<Result<_>>()?;

        let candidates_k = config.effective_candidates();

        // Stage 4: parallel shard search
        let mut handles = Vec::with_capacity(shards.len());
        for shard in shards {
            let q = Arc::clone(&query);
            handles.push(tokio::task::spawn_blocking(move || {
                exact_search(&q, &shard.records, metric, candidates_k)
            }));
        }

        let mut all_candidates = Vec::new();
        for handle in handles {
            let results = handle
                .await
                .map_err(|e: tokio::task::JoinError| IndexError::Other(e.to_string()))?;
            all_candidates.extend(results);
        }

        // Stage 5: merge
        let merged = merge_top_k(all_candidates, candidates_k);

        // Stage 6: optional rerank
        Ok(if config.rerank_limit.is_some() {
            Self::rerank(merged, config.top_k)
        } else {
            merged
        })
    }
}
