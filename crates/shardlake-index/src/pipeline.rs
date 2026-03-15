//! Modular query pipeline stages.
//!
//! Query execution is broken into discrete, independently-testable stages:
//!
//! 1. **Embed** – transform raw query input into a vector representation.
//! 2. **Route** – select which shards to probe using centroid distances.
//! 3. **LoadShard** – retrieve shard data from storage (with optional caching).
//! 4. **CandidateSearch** – search a single shard for nearest-neighbour candidates.
//! 5. **Merge** – combine and deduplicate candidates from all probed shards.
//! 6. **Rerank** – optionally reorder the merged results.
//!
//! Assemble a pipeline using [`QueryPipeline::builder`] and call
//! [`QueryPipeline::run`] to execute a search query through all stages.

use std::sync::Arc;

use shardlake_core::{
    error::CoreError,
    types::{DistanceMetric, SearchResult, ShardId},
};
use shardlake_manifest::Manifest;
use shardlake_storage::ObjectStore;

use crate::{
    cache::{ShardCache, DEFAULT_SHARD_CACHE_CAPACITY},
    exact::{exact_search, merge_top_k},
    kmeans::top_n_centroids,
    shard::ShardIndex,
    IndexError, Result,
};

// ───────────────────────── stage traits ─────────────────────────────────────

/// Transforms raw query input into an embedded vector representation.
///
/// For workflows that receive pre-embedded vectors the [`IdentityEmbedder`]
/// is a no-op passthrough.  A real implementation might call a model-serving
/// endpoint to generate the embedding on the fly.
pub trait EmbedStage: Send + Sync {
    /// Embed `query` and return the resulting vector.
    fn embed(&self, query: &[f32]) -> Result<Vec<f32>>;
}

/// Selects which shards to probe for a given query.
///
/// `centroids` and `centroid_to_shard` are paired 1-to-1: `centroids[i]` is the
/// centroid vector for `centroid_to_shard[i]`.  Implementors may apply any
/// routing strategy and must return a deduplicated list of shard IDs.
pub trait RouteStage: Send + Sync {
    /// Return the shard IDs that should be probed for `query`.
    fn route(
        &self,
        query: &[f32],
        centroids: &[Vec<f32>],
        centroid_to_shard: &[ShardId],
        nprobe: usize,
    ) -> Vec<ShardId>;
}

/// Loads a shard by its ID from underlying storage.
///
/// Implementations are free to cache loaded shards in memory; the
/// [`CachedShardLoader`] provided by this crate does so with a
/// `Mutex<HashMap>`.
pub trait LoadShardStage: Send + Sync {
    /// Return the in-memory representation of `shard_id`.
    fn load(&self, shard_id: ShardId) -> Result<Arc<ShardIndex>>;
}

/// Searches a single shard for nearest-neighbour candidates.
pub trait CandidateSearchStage: Send + Sync {
    /// Return up to `k` nearest neighbours from `shard` for `query`.
    fn search(
        &self,
        query: &[f32],
        shard: &ShardIndex,
        metric: DistanceMetric,
        k: usize,
    ) -> Vec<SearchResult>;
}

/// Combines candidate results from all probed shards into a single ranked list.
pub trait MergeStage: Send + Sync {
    /// Merge `results` collected across shards and return the top `k`.
    fn merge(&self, results: Vec<SearchResult>, k: usize) -> Vec<SearchResult>;
}

/// Optionally reorders the merged results, e.g. using a cross-encoder.
///
/// A no-op implementation is provided by [`NoopReranker`].
pub trait RerankStage: Send + Sync {
    /// Rerank `results` for `query` and return the top `k`.
    fn rerank(&self, query: &[f32], results: Vec<SearchResult>, k: usize) -> Vec<SearchResult>;
}

// ────────────────────── default implementations ──────────────────────────────

/// Identity embedder: returns the query vector unchanged.
///
/// Use this when vectors are already embedded before they reach the pipeline.
pub struct IdentityEmbedder;

impl EmbedStage for IdentityEmbedder {
    fn embed(&self, query: &[f32]) -> Result<Vec<f32>> {
        Ok(query.to_vec())
    }
}

/// Centroid-based router that probes the `nprobe` nearest shards.
///
/// Uses squared L2 distance to the shard centroids, mirroring the strategy
/// used by [`crate::kmeans::top_n_centroids`].
pub struct CentroidRouter;

impl RouteStage for CentroidRouter {
    fn route(
        &self,
        query: &[f32],
        centroids: &[Vec<f32>],
        centroid_to_shard: &[ShardId],
        nprobe: usize,
    ) -> Vec<ShardId> {
        let n = nprobe.min(centroids.len());
        let probe_indices = top_n_centroids(query, centroids, n);
        let mut shard_ids: Vec<ShardId> = probe_indices
            .into_iter()
            .filter_map(|i| centroid_to_shard.get(i).copied())
            .collect();
        shard_ids.sort();
        shard_ids.dedup();
        shard_ids
    }
}

/// Shard loader that caches loaded shards in a bounded LRU cache.
///
/// Constructed from an [`ObjectStore`] and a [`Manifest`]; uses the manifest
/// to resolve the artifact key for each shard ID before fetching bytes from
/// the store.  Evicts the least-recently-used shard when the cache is full.
pub struct CachedShardLoader {
    store: Arc<dyn ObjectStore>,
    manifest: Manifest,
    cache: ShardCache<ShardIndex>,
}

impl CachedShardLoader {
    /// Create a new loader using the [`DEFAULT_SHARD_CACHE_CAPACITY`].
    pub fn new(store: Arc<dyn ObjectStore>, manifest: Manifest) -> Self {
        Self::with_cache_capacity(store, manifest, DEFAULT_SHARD_CACHE_CAPACITY)
    }

    /// Create a new loader with the given LRU cache `capacity`.
    ///
    /// Use this constructor when you want to size the cache based on runtime
    /// configuration, for example from
    /// [`SystemConfig::shard_cache_capacity`](shardlake_core::config::SystemConfig::shard_cache_capacity).
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is `0`.
    pub fn with_cache_capacity(
        store: Arc<dyn ObjectStore>,
        manifest: Manifest,
        capacity: usize,
    ) -> Self {
        Self {
            store,
            manifest,
            cache: ShardCache::new(capacity),
        }
    }
}

impl LoadShardStage for CachedShardLoader {
    fn load(&self, shard_id: ShardId) -> Result<Arc<ShardIndex>> {
        self.cache.get_or_load(shard_id, || {
            let shard_def = self
                .manifest
                .shards
                .iter()
                .find(|s| s.shard_id == shard_id)
                .ok_or_else(|| IndexError::Other(format!("shard {shard_id} not in manifest")))?;
            let bytes = self.store.get(&shard_def.artifact_key)?;
            Ok(Arc::new(ShardIndex::from_bytes(&bytes)?))
        })
    }
}

/// Exact brute-force candidate search within a single shard.
///
/// Delegates to [`crate::exact::exact_search`].
pub struct ExactCandidateSearch;

impl CandidateSearchStage for ExactCandidateSearch {
    fn search(
        &self,
        query: &[f32],
        shard: &ShardIndex,
        metric: DistanceMetric,
        k: usize,
    ) -> Vec<SearchResult> {
        exact_search(query, &shard.records, metric, k)
    }
}

/// Top-k merge with deduplication by vector ID.
///
/// Delegates to [`crate::exact::merge_top_k`].
pub struct TopKMerge;

impl MergeStage for TopKMerge {
    fn merge(&self, results: Vec<SearchResult>, k: usize) -> Vec<SearchResult> {
        merge_top_k(results, k)
    }
}

/// No-op reranker: passes results through unchanged.
///
/// Use when no reranking model is available.  The input is already sorted by
/// ascending score from the merge stage so this is safe to return as-is.
pub struct NoopReranker;

impl RerankStage for NoopReranker {
    fn rerank(&self, _query: &[f32], results: Vec<SearchResult>, _k: usize) -> Vec<SearchResult> {
        results
    }
}

// ─────────────────────────── pipeline ────────────────────────────────────────

/// A composable, multi-stage query execution pipeline.
///
/// # Stage ordering
///
/// ```text
/// query ──► EmbedStage ──► RouteStage ──► LoadShardStage
///                                              │
///                                      CandidateSearchStage
///                                              │
///                                         MergeStage ──► RerankStage ──► results
/// ```
///
/// # Default pipeline
///
/// Call [`QueryPipeline::builder`] followed by [`QueryPipelineBuilder::build`]
/// without any overrides to get a pipeline equivalent to the search path used
/// by [`crate::IndexSearcher`]: identity embedding → centroid routing →
/// cached shard loading → exact search → top-k merge → no-op reranking.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use shardlake_index::pipeline::QueryPipeline;
/// use shardlake_manifest::Manifest;
/// use shardlake_storage::ObjectStore;
///
/// fn run_query(store: Arc<dyn ObjectStore>, manifest: Manifest) {
///     let pipeline = QueryPipeline::builder(store, manifest).build();
///     let results = pipeline.run(&[1.0, 0.0], 10, 2).unwrap();
///     println!("{} results", results.len());
/// }
/// ```
pub struct QueryPipeline {
    embedder: Box<dyn EmbedStage>,
    router: Box<dyn RouteStage>,
    loader: Box<dyn LoadShardStage>,
    candidate_search: Box<dyn CandidateSearchStage>,
    merge: Box<dyn MergeStage>,
    reranker: Box<dyn RerankStage>,
    manifest: Manifest,
}

impl QueryPipeline {
    /// Return a builder for constructing a [`QueryPipeline`].
    ///
    /// The builder is pre-populated with default concrete implementations for
    /// every stage.
    pub fn builder(store: Arc<dyn ObjectStore>, manifest: Manifest) -> QueryPipelineBuilder {
        QueryPipelineBuilder::new(store, manifest)
    }

    /// Execute a query through all pipeline stages.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::Core`] if the embedded query dimensionality does
    /// not match the index, or any storage/parse error that arises during shard
    /// loading.
    pub fn run(&self, query: &[f32], k: usize, nprobe: usize) -> Result<Vec<SearchResult>> {
        let expected_dims = self.manifest.dims as usize;
        let metric = self.manifest.distance_metric;

        // Stage 1: Embed.
        let embedded = self.embedder.embed(query)?;
        if embedded.len() != expected_dims {
            return Err(IndexError::Core(CoreError::DimensionMismatch {
                expected: expected_dims,
                got: embedded.len(),
            }));
        }

        // Build the centroid table from the manifest (mirrors IndexSearcher::search).
        // For v2+ manifests the centroid is embedded in each ShardDef; for legacy
        // v1 manifests we fall back to loading the shard body to read its centroid.
        let mut all_centroids: Vec<Vec<f32>> = Vec::new();
        let mut centroid_to_shard: Vec<ShardId> = Vec::new();
        for shard_def in &self.manifest.shards {
            if !shard_def.centroid.is_empty() {
                if shard_def.centroid.len() != expected_dims {
                    return Err(IndexError::Core(CoreError::DimensionMismatch {
                        expected: expected_dims,
                        got: shard_def.centroid.len(),
                    }));
                }
                all_centroids.push(shard_def.centroid.clone());
                centroid_to_shard.push(shard_def.shard_id);
            } else {
                let shard = self.loader.load(shard_def.shard_id)?;
                for c in &shard.centroids {
                    all_centroids.push(c.clone());
                    centroid_to_shard.push(shard_def.shard_id);
                }
            }
        }

        if all_centroids.is_empty() {
            return Ok(Vec::new());
        }

        // Stage 2: Route.
        let probe_shards = self
            .router
            .route(&embedded, &all_centroids, &centroid_to_shard, nprobe);

        // Stages 3 + 4: Load each probed shard and collect candidates.
        let mut all_results = Vec::new();
        for shard_id in probe_shards {
            let shard = self.loader.load(shard_id)?;
            let results = self.candidate_search.search(&embedded, &shard, metric, k);
            all_results.extend(results);
        }

        // Stage 5: Merge.
        let merged = self.merge.merge(all_results, k);

        // Stage 6: Rerank.
        let final_results = self.reranker.rerank(&embedded, merged, k);

        Ok(final_results)
    }
}

// ─────────────────────────── builder ─────────────────────────────────────────

/// Builder for [`QueryPipeline`].
///
/// Each `with_*` method replaces the corresponding stage with a custom
/// implementation.  Call [`build`](QueryPipelineBuilder::build) to assemble
/// the final pipeline.
pub struct QueryPipelineBuilder {
    store: Arc<dyn ObjectStore>,
    manifest: Manifest,
    embedder: Box<dyn EmbedStage>,
    router: Box<dyn RouteStage>,
    loader: Option<Box<dyn LoadShardStage>>,
    candidate_search: Box<dyn CandidateSearchStage>,
    merge: Box<dyn MergeStage>,
    reranker: Box<dyn RerankStage>,
    shard_cache_capacity: usize,
}

impl QueryPipelineBuilder {
    fn new(store: Arc<dyn ObjectStore>, manifest: Manifest) -> Self {
        Self {
            store,
            manifest,
            embedder: Box::new(IdentityEmbedder),
            router: Box::new(CentroidRouter),
            loader: None,
            candidate_search: Box::new(ExactCandidateSearch),
            merge: Box::new(TopKMerge),
            reranker: Box::new(NoopReranker),
            shard_cache_capacity: DEFAULT_SHARD_CACHE_CAPACITY,
        }
    }

    /// Override the embed stage.
    pub fn with_embedder(mut self, embedder: Box<dyn EmbedStage>) -> Self {
        self.embedder = embedder;
        self
    }

    /// Override the route stage.
    pub fn with_router(mut self, router: Box<dyn RouteStage>) -> Self {
        self.router = router;
        self
    }

    /// Override the shard-load stage.
    pub fn with_loader(mut self, loader: Box<dyn LoadShardStage>) -> Self {
        self.loader = Some(loader);
        self
    }

    /// Override the candidate-search stage.
    pub fn with_candidate_search(mut self, search: Box<dyn CandidateSearchStage>) -> Self {
        self.candidate_search = search;
        self
    }

    /// Override the merge stage.
    pub fn with_merge(mut self, merge: Box<dyn MergeStage>) -> Self {
        self.merge = merge;
        self
    }

    /// Override the rerank stage.
    pub fn with_reranker(mut self, reranker: Box<dyn RerankStage>) -> Self {
        self.reranker = reranker;
        self
    }

    /// Set the shard LRU cache capacity for the default [`CachedShardLoader`].
    ///
    /// Ignored when a custom loader is supplied via
    /// [`with_loader`](Self::with_loader).  Defaults to
    /// [`DEFAULT_SHARD_CACHE_CAPACITY`].
    pub fn with_shard_cache_capacity(mut self, capacity: usize) -> Self {
        self.shard_cache_capacity = capacity;
        self
    }

    /// Assemble the [`QueryPipeline`].
    ///
    /// If no custom loader was provided via [`with_loader`](Self::with_loader),
    /// a [`CachedShardLoader`] backed by the store passed to
    /// [`QueryPipeline::builder`] is used with the configured cache capacity.
    pub fn build(self) -> QueryPipeline {
        let loader: Box<dyn LoadShardStage> = self.loader.unwrap_or_else(|| {
            Box::new(CachedShardLoader::with_cache_capacity(
                Arc::clone(&self.store),
                self.manifest.clone(),
                self.shard_cache_capacity,
            ))
        });
        QueryPipeline {
            embedder: self.embedder,
            router: self.router,
            loader,
            candidate_search: self.candidate_search,
            merge: self.merge,
            reranker: self.reranker,
            manifest: self.manifest,
        }
    }
}

// ─────────────────────────── tests ───────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use shardlake_core::types::{VectorId, VectorRecord};

    // ── EmbedStage ────────────────────────────────────────────────────────────

    #[test]
    fn identity_embedder_returns_query_unchanged() {
        let embedder = IdentityEmbedder;
        let input = vec![1.0f32, 2.0, 3.0];
        let output = embedder.embed(&input).unwrap();
        assert_eq!(output, input);
    }

    // ── RouteStage ────────────────────────────────────────────────────────────

    #[test]
    fn centroid_router_selects_nearest_shards() {
        let router = CentroidRouter;
        let centroids = vec![
            vec![0.0f32, 0.0],   // shard 0
            vec![10.0f32, 10.0], // shard 1
            vec![5.0f32, 5.0],   // shard 2
        ];
        let centroid_to_shard = vec![ShardId(0), ShardId(1), ShardId(2)];

        // Query near (0,0): shards 0 and 2 should be the two nearest.
        let shards = router.route(&[0.5, 0.5], &centroids, &centroid_to_shard, 2);
        assert_eq!(shards.len(), 2);
        assert!(shards.contains(&ShardId(0)));
        assert!(shards.contains(&ShardId(2)));
    }

    #[test]
    fn centroid_router_deduplicates_shard_ids() {
        let router = CentroidRouter;
        // Two centroids that both map to the same shard.
        let centroids = vec![vec![0.0f32, 0.0], vec![0.1f32, 0.1]];
        let centroid_to_shard = vec![ShardId(0), ShardId(0)];
        let shards = router.route(&[0.0, 0.0], &centroids, &centroid_to_shard, 2);
        assert_eq!(shards, vec![ShardId(0)]);
    }

    #[test]
    fn centroid_router_clamps_nprobe_to_centroid_count() {
        let router = CentroidRouter;
        let centroids = vec![vec![1.0f32, 0.0]];
        let centroid_to_shard = vec![ShardId(5)];
        let shards = router.route(&[1.0, 0.0], &centroids, &centroid_to_shard, 100);
        assert_eq!(shards, vec![ShardId(5)]);
    }

    // ── CandidateSearchStage ──────────────────────────────────────────────────

    #[test]
    fn exact_candidate_search_returns_nearest() {
        let searcher = ExactCandidateSearch;
        let shard = ShardIndex {
            shard_id: ShardId(0),
            dims: 2,
            centroids: vec![vec![0.0, 0.0]],
            records: vec![
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
                    data: vec![5.0, 5.0],
                    metadata: None,
                },
            ],
        };
        let results = searcher.search(&[1.0, 0.1], &shard, DistanceMetric::Euclidean, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, VectorId(1));
    }

    // ── MergeStage ────────────────────────────────────────────────────────────

    #[test]
    fn topk_merge_deduplicates_and_returns_best_score() {
        use shardlake_core::types::VectorId;
        let merge = TopKMerge;
        let results = vec![
            SearchResult {
                id: VectorId(1),
                score: 0.5,
                metadata: None,
            },
            SearchResult {
                id: VectorId(2),
                score: 0.2,
                metadata: None,
            },
            SearchResult {
                id: VectorId(1),
                score: 0.3,
                metadata: None,
            }, // duplicate – keep 0.3
        ];
        let merged = merge.merge(results, 2);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].id, VectorId(2)); // 0.2 is better (lower)
        assert_eq!(merged[1].id, VectorId(1));
        assert!((merged[1].score - 0.3).abs() < f32::EPSILON);
    }

    // ── RerankStage ───────────────────────────────────────────────────────────

    #[test]
    fn noop_reranker_returns_results_unchanged() {
        let reranker = NoopReranker;
        let results = vec![
            SearchResult {
                id: VectorId(1),
                score: 0.1,
                metadata: None,
            },
            SearchResult {
                id: VectorId(2),
                score: 0.9,
                metadata: None,
            },
        ];
        let out = reranker.rerank(&[0.0f32], results.clone(), 2);
        assert_eq!(out.len(), results.len());
        assert_eq!(out[0].id, results[0].id);
        assert_eq!(out[1].id, results[1].id);
    }
}
