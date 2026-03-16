//! Modular query pipeline stages for ANN retrieval.
//!
//! Query execution is broken into independently-testable stages:
//!
//! 1. **Embed** - transform raw query input into an embedding.
//! 2. **Route** - select the IVF shards to probe using centroid distances.
//! 3. **Load shard** - retrieve shard data from storage, typically with caching.
//! 4. **Candidate search** - search each probed shard for approximate or exact candidates.
//! 5. **Merge** - combine per-shard candidates into a single ranked set.
//! 6. **Rerank** - optionally rescore merged candidates with exact distances.
//!
//! [`QueryPipeline`] keeps the modular stage surface introduced on `main` while
//! adding ANN-specific candidate and rerank stages such as [`PqCandidateStage`]
//! and [`ExactRerankStage`].

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Instant,
};

use tracing::debug;

use shardlake_core::{
    error::CoreError,
    types::{DistanceMetric, SearchResult, ShardId, VectorRecord},
};
use shardlake_manifest::Manifest;
use shardlake_storage::ObjectStore;

use crate::{
    exact::{distance, exact_search, merge_top_k},
    kmeans::top_n_centroids,
    merge::GlobalMerge,
    metrics::CacheMetrics,
    pq::PqCodebook,
    shard::ShardIndex,
    IndexError, Result,
};

/// Transforms raw query input into an embedded vector representation.
pub trait EmbedStage: Send + Sync {
    /// Embed `query` and return the resulting vector.
    fn embed(&self, query: &[f32]) -> Result<Vec<f32>>;
}

/// Selects which shards should be probed for a given query.
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

/// Loads shard contents from the underlying object store.
pub trait LoadShardStage: Send + Sync {
    /// Return the in-memory representation of `shard_id`.
    fn load(&self, shard_id: ShardId) -> Result<Arc<ShardIndex>>;
}

/// Searches a single loaded shard for nearest-neighbour candidates.
pub trait CandidateSearchStage: Send + Sync {
    /// Return up to `k` candidates from `shard` for `query`.
    fn search(
        &self,
        query: &[f32],
        shard: &ShardIndex,
        metric: DistanceMetric,
        k: usize,
    ) -> Result<Vec<SearchResult>>;
}

/// Combines candidate results from all probed shards into a single ranked list.
pub trait MergeStage: Send + Sync {
    /// Merge `results` collected across shards and return the top `k`.
    fn merge(&self, results: Vec<SearchResult>, k: usize) -> Vec<SearchResult>;
}

/// Optionally reorders merged candidates using the original probed records.
pub trait RerankStage: Send + Sync {
    /// Rerank `results` for `query` and return the top `k`.
    fn rerank(
        &self,
        query: &[f32],
        results: Vec<SearchResult>,
        probed_records: &[VectorRecord],
        metric: DistanceMetric,
        k: usize,
    ) -> Vec<SearchResult>;
}

/// Identity embedder: returns the query vector unchanged.
pub struct IdentityEmbedder;

impl EmbedStage for IdentityEmbedder {
    fn embed(&self, query: &[f32]) -> Result<Vec<f32>> {
        Ok(query.to_vec())
    }
}

/// Centroid router that probes the `nprobe` nearest IVF centroids.
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
            .filter_map(|index| centroid_to_shard.get(index).copied())
            .collect();
        shard_ids.sort();
        shard_ids.dedup();
        shard_ids
    }
}

/// Shard loader that caches loaded shards in an in-memory map.
///
/// Constructed from an [`ObjectStore`] and a [`Manifest`]; uses the manifest
/// to resolve the artifact key for each shard ID before fetching bytes from
/// the store.
///
/// Cache observability is available via [`CachedShardLoader::metrics`]: a
/// shared [`CacheMetrics`] instance that tracks hits, misses, load latency,
/// and retained bytes for every shard loaded through this loader.
pub struct CachedShardLoader {
    store: Arc<dyn ObjectStore>,
    manifest: Manifest,
    cache: Mutex<HashMap<ShardId, Arc<ShardIndex>>>,
    metrics: Arc<CacheMetrics>,
}

impl CachedShardLoader {
    /// Create a new loader backed by `store` and `manifest`.
    pub fn new(store: Arc<dyn ObjectStore>, manifest: Manifest) -> Self {
        Self {
            store,
            manifest,
            cache: Mutex::new(HashMap::new()),
            metrics: Arc::new(CacheMetrics::new()),
        }
    }

    /// Return a shared reference to the cache metrics for this loader.
    ///
    /// The returned [`Arc`] is the same instance used internally, so callers
    /// observe live counter updates without any extra synchronisation cost.
    pub fn metrics(&self) -> Arc<CacheMetrics> {
        Arc::clone(&self.metrics)
    }
}

impl LoadShardStage for CachedShardLoader {
    fn load(&self, shard_id: ShardId) -> Result<Arc<ShardIndex>> {
        {
            let cache = self.cache.lock().map_err(|_| {
                IndexError::Other(
                    "shard loader cache lock poisoned: a panic occurred while holding the lock"
                        .into(),
                )
            })?;
            if let Some(idx) = cache.get(&shard_id) {
                self.metrics.record_hit();
                return Ok(Arc::clone(idx));
            }
        }

        let shard_def = self
            .manifest
            .shards
            .iter()
            .find(|shard| shard.shard_id == shard_id)
            .ok_or_else(|| IndexError::Other(format!("shard {shard_id} not in manifest")))?;

        self.metrics.record_miss();

        let t0 = Instant::now();
        let bytes = self.store.get(&shard_def.artifact_key);
        let elapsed_ns = t0.elapsed().as_nanos() as u64;
        self.metrics.record_load_attempt(elapsed_ns);
        let bytes = bytes?;
        let idx = Arc::new(ShardIndex::from_bytes(&bytes)?);

        let mut cache = self
            .cache
            .lock()
            .map_err(|_| IndexError::Other("shard loader cache lock poisoned".into()))?;
        cache.insert(shard_id, Arc::clone(&idx));
        self.metrics.record_retained_bytes(bytes.len() as u64);
        Ok(idx)
    }
}

/// Exact brute-force candidate search within a single shard.
pub struct ExactCandidateSearch;

impl CandidateSearchStage for ExactCandidateSearch {
    fn search(
        &self,
        query: &[f32],
        shard: &ShardIndex,
        metric: DistanceMetric,
        k: usize,
    ) -> Result<Vec<SearchResult>> {
        Ok(exact_search(query, &shard.records, metric, k))
    }
}

/// Backward-compatible alias for the original exact candidate stage name.
pub use ExactCandidateSearch as ExactCandidateStage;

/// Product-quantized candidate search using asymmetric distance computation.
pub struct PqCandidateStage {
    codebook: Arc<PqCodebook>,
}

impl PqCandidateStage {
    /// Create a PQ-backed candidate search stage.
    pub fn new(codebook: Arc<PqCodebook>) -> Self {
        Self { codebook }
    }

    /// Return the underlying codebook.
    pub fn codebook(&self) -> &PqCodebook {
        &self.codebook
    }
}

impl CandidateSearchStage for PqCandidateStage {
    fn search(
        &self,
        query: &[f32],
        shard: &ShardIndex,
        metric: DistanceMetric,
        k: usize,
    ) -> Result<Vec<SearchResult>> {
        if metric != DistanceMetric::Euclidean {
            return Err(IndexError::Other(
                "PQ search currently supports only euclidean distance".into(),
            ));
        }

        let tables = self.codebook.compute_distance_table(query)?;
        let mut scored = Vec::with_capacity(shard.records.len());
        for record in &shard.records {
            let codes = self.codebook.encode(&record.data)?;
            scored.push(SearchResult {
                id: record.id,
                score: self.codebook.adc_distance(&codes, &tables),
                metadata: record.metadata.clone(),
            });
        }

        scored.sort_by(|left, right| {
            left.score
                .partial_cmp(&right.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(k);
        Ok(scored)
    }
}

/// Merge stage that keeps the best `k` scored candidates with deduplication.
///
/// Delegates to [`GlobalMerge`] so ordering is deterministic: results are
/// sorted by score ascending with vector ID as a tie-breaker.
pub struct TopKMerge;

impl MergeStage for TopKMerge {
    fn merge(&self, results: Vec<SearchResult>, k: usize) -> Vec<SearchResult> {
        GlobalMerge.merge(results, k)
    }
}

/// No-op reranker: passes results through unchanged.
pub struct NoopReranker;

impl RerankStage for NoopReranker {
    fn rerank(
        &self,
        _query: &[f32],
        results: Vec<SearchResult>,
        _probed_records: &[VectorRecord],
        _metric: DistanceMetric,
        k: usize,
    ) -> Vec<SearchResult> {
        results.into_iter().take(k).collect()
    }
}

/// Exact reranker that rescales approximate candidates with float distances.
pub struct ExactRerankStage;

impl RerankStage for ExactRerankStage {
    fn rerank(
        &self,
        query: &[f32],
        results: Vec<SearchResult>,
        probed_records: &[VectorRecord],
        metric: DistanceMetric,
        k: usize,
    ) -> Vec<SearchResult> {
        let lookup: HashMap<_, &VectorRecord> = probed_records
            .iter()
            .map(|record| (record.id, record))
            .collect();
        let rescored = results
            .into_iter()
            .filter_map(|result| {
                lookup.get(&result.id).map(|record| SearchResult {
                    id: result.id,
                    score: distance(query, &record.data, metric),
                    metadata: result.metadata,
                })
            })
            .collect();
        merge_top_k(rescored, k)
    }
}

/// A composable multi-stage ANN query execution pipeline.
pub struct QueryPipeline {
    embedder: Arc<dyn EmbedStage>,
    router: Arc<dyn RouteStage>,
    loader: Arc<dyn LoadShardStage>,
    candidate_search: Arc<dyn CandidateSearchStage>,
    merge: Arc<dyn MergeStage>,
    reranker: Option<Arc<dyn RerankStage>>,
    manifest: Manifest,
    rerank_oversample: usize,
}

impl QueryPipeline {
    /// Return a builder for constructing a [`QueryPipeline`].
    pub fn builder(store: Arc<dyn ObjectStore>, manifest: Manifest) -> QueryPipelineBuilder {
        QueryPipelineBuilder::new(store, manifest)
    }

    /// Return the manifest the pipeline was configured from.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Execute a query through all pipeline stages.
    pub fn run(&self, query: &[f32], k: usize, nprobe: usize) -> Result<Vec<SearchResult>> {
        let expected_dims = self.manifest.dims as usize;
        let metric = self.manifest.distance_metric;

        let embedded = self.embedder.embed(query)?;
        if embedded.len() != expected_dims {
            return Err(IndexError::Core(CoreError::DimensionMismatch {
                expected: expected_dims,
                got: embedded.len(),
            }));
        }

        let mut all_centroids = Vec::new();
        let mut centroid_to_shard = Vec::new();
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
                for centroid in &shard.centroids {
                    all_centroids.push(centroid.clone());
                    centroid_to_shard.push(shard_def.shard_id);
                }
            }
        }

        if all_centroids.is_empty() {
            return Ok(Vec::new());
        }

        let probe_shards = self
            .router
            .route(&embedded, &all_centroids, &centroid_to_shard, nprobe);
        debug!(n_shards = probe_shards.len(), "Probing shards");

        let candidates_per_shard = if self.reranker.is_some() {
            k.saturating_mul(self.rerank_oversample).max(k)
        } else {
            k
        };

        let mut all_results = Vec::new();
        let mut probed_shards = Vec::new();
        for shard_id in probe_shards {
            let shard = self.loader.load(shard_id)?;
            all_results.extend(self.candidate_search.search(
                &embedded,
                &shard,
                metric,
                candidates_per_shard,
            )?);
            if self.reranker.is_some() {
                probed_shards.push(shard);
            }
        }

        let merged = self.merge.merge(all_results, candidates_per_shard);
        if let Some(reranker) = &self.reranker {
            let candidate_ids: HashSet<_> = merged.iter().map(|result| result.id).collect();
            let mut probed_records = Vec::with_capacity(candidate_ids.len());
            for shard in &probed_shards {
                probed_records.extend(
                    shard
                        .records
                        .iter()
                        .filter(|record| candidate_ids.contains(&record.id))
                        .cloned(),
                );
            }
            Ok(reranker.rerank(&embedded, merged, &probed_records, metric, k))
        } else {
            Ok(merged)
        }
    }

    /// Backward-compatible alias for [`QueryPipeline::run`].
    pub fn search(&self, query: &[f32], k: usize, nprobe: usize) -> Result<Vec<SearchResult>> {
        self.run(query, k, nprobe)
    }
}

/// Builder for [`QueryPipeline`].
pub struct QueryPipelineBuilder {
    store: Arc<dyn ObjectStore>,
    manifest: Manifest,
    embedder: Arc<dyn EmbedStage>,
    router: Arc<dyn RouteStage>,
    loader: Option<Arc<dyn LoadShardStage>>,
    candidate_search: Arc<dyn CandidateSearchStage>,
    merge: Arc<dyn MergeStage>,
    reranker: Option<Arc<dyn RerankStage>>,
    rerank_oversample: usize,
}

impl QueryPipelineBuilder {
    fn new(store: Arc<dyn ObjectStore>, manifest: Manifest) -> Self {
        Self {
            store,
            manifest,
            embedder: Arc::new(IdentityEmbedder),
            router: Arc::new(CentroidRouter),
            loader: None,
            candidate_search: Arc::new(ExactCandidateSearch),
            merge: Arc::new(GlobalMerge),
            reranker: None,
            rerank_oversample: 1,
        }
    }

    /// Override the embed stage.
    #[must_use]
    pub fn with_embedder(mut self, embedder: Box<dyn EmbedStage>) -> Self {
        self.embedder = embedder.into();
        self
    }

    /// Override the route stage.
    #[must_use]
    pub fn with_router(mut self, router: Box<dyn RouteStage>) -> Self {
        self.router = router.into();
        self
    }

    /// Override the shard-load stage.
    #[must_use]
    pub fn with_loader(mut self, loader: Box<dyn LoadShardStage>) -> Self {
        self.loader = Some(loader.into());
        self
    }

    /// Override the candidate-search stage.
    #[must_use]
    pub fn with_candidate_search(mut self, search: Box<dyn CandidateSearchStage>) -> Self {
        self.candidate_search = search.into();
        self
    }

    /// Override the merge stage.
    #[must_use]
    pub fn with_merge(mut self, merge: Box<dyn MergeStage>) -> Self {
        self.merge = merge.into();
        self
    }

    /// Override the rerank stage.
    #[must_use]
    pub fn with_reranker(mut self, reranker: Box<dyn RerankStage>) -> Self {
        self.reranker = Some(reranker.into());
        self
    }

    /// Convenience alias for setting the candidate search stage with an [`Arc`].
    #[must_use]
    pub fn candidate_stage(mut self, stage: Arc<dyn CandidateSearchStage>) -> Self {
        self.candidate_search = stage;
        self
    }

    /// Convenience alias for setting the rerank stage with an [`Arc`].
    #[must_use]
    pub fn rerank_stage(mut self, stage: Arc<dyn RerankStage>) -> Self {
        self.reranker = Some(stage);
        self
    }

    /// Set the oversample factor used before reranking.
    #[must_use]
    pub fn rerank_oversample(mut self, oversample: usize) -> Self {
        self.rerank_oversample = oversample.max(1);
        self
    }

    /// Assemble the [`QueryPipeline`].
    pub fn build(self) -> QueryPipeline {
        let loader = self.loader.unwrap_or_else(|| {
            Arc::new(CachedShardLoader::new(
                Arc::clone(&self.store),
                self.manifest.clone(),
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
            rerank_oversample: self.rerank_oversample,
        }
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
        types::{DatasetVersion, EmbeddingVersion, IndexVersion, VectorId},
    };
    use shardlake_storage::LocalObjectStore;

    fn make_records(n: usize, dims: usize) -> Vec<VectorRecord> {
        (0..n)
            .map(|index| VectorRecord {
                id: VectorId(index as u64),
                data: (0..dims)
                    .map(|dimension| (index * dims + dimension) as f32 / 100.0)
                    .collect(),
                metadata: None,
            })
            .collect()
    }

    fn build_pipeline(records: Vec<VectorRecord>) -> QueryPipeline {
        let tmp = tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 2,
            kmeans_iters: 5,
            nprobe: 2,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            kmeans_sample_size: None,
            ..SystemConfig::default()
        };
        let manifest = IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-pipeline".into()),
                embedding_version: EmbeddingVersion("emb-pipeline".into()),
                index_version: IndexVersion("idx-pipeline".into()),
                metric: DistanceMetric::Euclidean,
                dims: 4,
                vectors_key: "datasets/ds-pipeline/vectors.jsonl".into(),
                metadata_key: "datasets/ds-pipeline/metadata.json".into(),
                pq_params: None,
            })
            .unwrap();
        std::mem::forget(tmp);
        QueryPipeline::builder(store, manifest).build()
    }

    #[test]
    fn identity_embedder_returns_query_unchanged() {
        let embedder = IdentityEmbedder;
        let query = vec![1.0, 2.0, 3.0];
        assert_eq!(embedder.embed(&query).unwrap(), query);
    }

    #[test]
    fn centroid_router_deduplicates_shards() {
        let router = CentroidRouter;
        let shards = router.route(
            &[0.0, 0.0],
            &[vec![0.0, 0.0], vec![0.1, 0.1]],
            &[ShardId(0), ShardId(0)],
            2,
        );
        assert_eq!(shards, vec![ShardId(0)]);
    }

    #[test]
    fn exact_candidate_search_returns_nearest() {
        let stage = ExactCandidateSearch;
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
            ],
        };
        let results = stage
            .search(&[1.0, 0.1], &shard, DistanceMetric::Euclidean, 1)
            .unwrap();
        assert_eq!(results[0].id, VectorId(1));
    }

    #[test]
    fn noop_reranker_returns_results_unchanged() {
        let reranker = NoopReranker;
        let results = vec![SearchResult {
            id: VectorId(1),
            score: 0.1,
            metadata: None,
        }];
        let reranked = reranker.rerank(&[0.0], results.clone(), &[], DistanceMetric::Euclidean, 1);
        assert_eq!(reranked.len(), results.len());
        assert_eq!(reranked[0].id, results[0].id);
        assert!((reranked[0].score - results[0].score).abs() < f32::EPSILON);
    }

    #[test]
    fn search_alias_matches_run() {
        let records = make_records(10, 4);
        let query = records[0].data.clone();
        let pipeline = build_pipeline(records);
        let from_run = pipeline.run(&query, 3, 2).unwrap();
        let from_search = pipeline.search(&query, 3, 2).unwrap();
        let run_ids: Vec<_> = from_run.iter().map(|result| result.id).collect();
        let search_ids: Vec<_> = from_search.iter().map(|result| result.id).collect();
        assert_eq!(run_ids, search_ids);
    }

    #[test]
    fn rerank_oversample_is_clamped_to_one() {
        let records = make_records(10, 4);
        let query = records[0].data.clone();
        let tmp = tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 1,
            kmeans_iters: 5,
            nprobe: 1,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            kmeans_sample_size: None,
            ..SystemConfig::default()
        };
        let manifest = IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-oversample".into()),
                embedding_version: EmbeddingVersion("emb-oversample".into()),
                index_version: IndexVersion("idx-oversample".into()),
                metric: DistanceMetric::Euclidean,
                dims: 4,
                vectors_key: "datasets/ds-oversample/vectors.jsonl".into(),
                metadata_key: "datasets/ds-oversample/metadata.json".into(),
                pq_params: None,
            })
            .unwrap();
        let pipeline = QueryPipeline::builder(store, manifest)
            .rerank_stage(Arc::new(NoopReranker))
            .rerank_oversample(0)
            .build();
        let results = pipeline.run(&query, 3, 1).unwrap();
        assert_eq!(results.len(), 3);
    }
}
