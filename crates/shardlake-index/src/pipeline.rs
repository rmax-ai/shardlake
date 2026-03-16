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
//! and [`ExactRerankStage`]. Stages 3 and 4 can execute concurrently across
//! probed shards using Rayon.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Instant,
};

use rayon::prelude::*;
use tracing::{debug, debug_span};

use shardlake_core::{
    config::{PrefetchPolicy, QueryConfig},
    error::CoreError,
    types::{DistanceMetric, SearchResult, ShardId, VectorRecord},
};
use shardlake_manifest::Manifest;
use shardlake_storage::{LocalObjectStore, ObjectStore};

use crate::{
    cache::{CacheAccess, ShardCache, DEFAULT_SHARD_CACHE_CAPACITY},
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

/// Shard loader that caches loaded shards in a bounded LRU cache.
///
/// Constructed from an [`ObjectStore`] and a [`Manifest`]; uses the manifest
/// to resolve the artifact key for each shard ID before fetching bytes from
/// the store.  Evicts the least-recently-used shard when the cache is full.
///
/// Optional shard warming can be enabled via [`CachedShardLoader::with_prefetch`].
/// When active, shards whose probe count reaches the configured threshold are
/// loaded proactively on the next cache-miss event.
///
/// Cache observability is available via [`CachedShardLoader::metrics`]: a
/// shared [`CacheMetrics`] instance that tracks hits, misses, load latency,
/// and retained bytes for every shard loaded through this loader.
pub struct CachedShardLoader {
    store: Arc<dyn ObjectStore>,
    manifest: Manifest,
    metrics: Arc<CacheMetrics>,
    cache: ShardCache<ShardIndex>,
    access_counts: Mutex<HashMap<ShardId, u64>>,
    policy: Option<PrefetchPolicy>,
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
        assert!(
            capacity > 0,
            "CachedShardLoader shard cache capacity must be at least 1"
        );
        Self {
            store,
            manifest,
            metrics: Arc::new(CacheMetrics::new()),
            cache: ShardCache::new(capacity),
            access_counts: Mutex::new(HashMap::new()),
            policy: None,
        }
    }

    /// Enable best-effort warming of hot shards after cache misses.
    #[must_use]
    pub fn with_prefetch(mut self, policy: PrefetchPolicy) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Return a shared reference to the cache metrics for this loader.
    ///
    /// The returned [`Arc`] is the same instance used internally, so callers
    /// observe live counter updates without any extra synchronisation cost.
    pub fn metrics(&self) -> Arc<CacheMetrics> {
        Arc::clone(&self.metrics)
    }

    fn record_access(&self, shard_id: ShardId) -> Result<()> {
        let mut counts = self
            .access_counts
            .lock()
            .map_err(|_| IndexError::Other("shard loader access-count lock poisoned".into()))?;
        *counts.entry(shard_id).or_insert(0) += 1;
        Ok(())
    }

    fn prefetch_candidates(&self) -> Result<Vec<(ShardId, String)>> {
        let Some(policy) = self.policy.as_ref().filter(|policy| policy.enabled) else {
            return Ok(Vec::new());
        };

        let threshold = u64::from(policy.min_query_count);
        let counts = self
            .access_counts
            .lock()
            .map_err(|_| IndexError::Other("shard loader access-count lock poisoned".into()))?;

        self.manifest
            .shards
            .iter()
            .filter(|shard| counts.get(&shard.shard_id).copied().unwrap_or(0) >= threshold)
            .filter_map(|shard| match self.cache.contains(shard.shard_id) {
                Ok(true) => None,
                Ok(false) => Some(Ok((shard.shard_id, shard.artifact_key.clone()))),
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    fn warm_hot_shards(&self) -> Result<()> {
        for (shard_id, artifact_key) in self.prefetch_candidates()? {
            let mut retained_bytes = None;
            match self.cache.get_or_load_with_status(shard_id, || {
                let started = Instant::now();
                let bytes = self.store.get(&artifact_key);
                self.metrics
                    .record_load_attempt(started.elapsed().as_nanos() as u64);
                let bytes = bytes?;
                retained_bytes = Some(bytes.len() as u64);
                Ok(Arc::new(ShardIndex::from_bytes(&bytes)?))
            }) {
                Ok((_, CacheAccess::Miss)) => {
                    if let Some(bytes) = retained_bytes {
                        self.metrics.record_retained_bytes(bytes);
                    }
                    debug!(?shard_id, "prefetch: warmed hot shard");
                }
                Ok((_, CacheAccess::Hit | CacheAccess::Raced)) => {}
                Err(error) => {
                    debug!(?shard_id, %error, "prefetch: failed to warm shard");
                }
            }
        }

        Ok(())
    }
}

impl LoadShardStage for CachedShardLoader {
    fn load(&self, shard_id: ShardId) -> Result<Arc<ShardIndex>> {
        let _span = debug_span!("shard_load", shard_id = shard_id.0).entered();
        self.record_access(shard_id)?;

        let mut retained_bytes = None;
        let (shard, access) = self.cache.get_or_load_with_status(shard_id, || {
            let shard_def = self
                .manifest
                .shards
                .iter()
                .find(|s| s.shard_id == shard_id)
                .ok_or_else(|| IndexError::Other(format!("shard {shard_id} not in manifest")))?;
            debug!("cache miss, loading from store");
            self.metrics.record_miss();
            let started = Instant::now();
            let bytes = self.store.get(&shard_def.artifact_key);
            self.metrics
                .record_load_attempt(started.elapsed().as_nanos() as u64);
            let bytes = bytes?;
            retained_bytes = Some(bytes.len() as u64);
            Ok(Arc::new(ShardIndex::from_bytes(&bytes)?))
        })?;

        match access {
            CacheAccess::Hit => {
                debug!("cache hit");
                self.metrics.record_hit();
            }
            CacheAccess::Miss => {
                if let Some(bytes) = retained_bytes {
                    self.metrics.record_retained_bytes(bytes);
                }
                self.warm_hot_shards()?;
            }
            CacheAccess::Raced => {}
        }

        Ok(shard)
    }
}

/// Minimum shard file size (in bytes) for which [`MmapShardLoader`] will
/// attempt memory-mapped I/O.  Files smaller than this threshold are loaded
/// via the regular `ObjectStore::get` path instead.
///
/// The value (1 MiB) avoids the mmap setup overhead for tiny development
/// shards while still benefiting large production artifacts.
pub const MMAP_MIN_SIZE_BYTES: u64 = 1024 * 1024;

/// Shard loader that uses memory-mapped I/O for large local shard files.
///
/// For shard artifacts stored on the local filesystem and whose on-disk size is
/// at least [`MMAP_MIN_SIZE_BYTES`], the file is memory-mapped before being
/// parsed; the mapped region is released as soon as deserialization finishes.
/// For small files, non-local stores, or any environment where memory mapping
/// is not available, the loader transparently falls back to reading the file
/// with the regular [`ObjectStore::get`] path.
///
/// Loaded shards are cached in an in-memory map so repeated loads can reuse the
/// deserialized shard instead of fetching it again after the first successful
/// load.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use shardlake_core::config::QueryConfig;
/// use shardlake_index::pipeline::{MmapShardLoader, QueryPipeline};
/// use shardlake_manifest::Manifest;
/// use shardlake_storage::LocalObjectStore;
///
/// fn run_mmap_query(store: Arc<LocalObjectStore>, manifest: Manifest) {
///     let pipeline = QueryPipeline::builder(Arc::clone(&store) as Arc<_>, manifest.clone())
///         .with_loader(Box::new(MmapShardLoader::new(store, manifest)))
///         .build();
///     let config = QueryConfig { top_k: 10, ..QueryConfig::default() };
///     let results = pipeline.run(&[1.0, 0.0], &config).unwrap();
///     println!("{} results", results.len());
/// }
/// ```
pub struct MmapShardLoader {
    store: Arc<LocalObjectStore>,
    manifest: Manifest,
    cache: Mutex<HashMap<ShardId, Arc<ShardIndex>>>,
    /// Files smaller than this threshold (in bytes) are loaded via the regular
    /// `ObjectStore::get` path instead of being memory-mapped.
    mmap_threshold: u64,
}

impl MmapShardLoader {
    /// Create a new loader backed by `store` and `manifest` using the default
    /// size threshold ([`MMAP_MIN_SIZE_BYTES`]).
    pub fn new(store: Arc<LocalObjectStore>, manifest: Manifest) -> Self {
        Self::with_threshold(store, manifest, MMAP_MIN_SIZE_BYTES)
    }

    /// Create a new loader with a custom `mmap_threshold` in bytes.
    ///
    /// Files whose on-disk size is strictly less than `mmap_threshold` are
    /// always loaded via the regular `ObjectStore::get` fallback path.
    /// Pass `0` to enable memory mapping for every file regardless of size.
    pub fn with_threshold(
        store: Arc<LocalObjectStore>,
        manifest: Manifest,
        mmap_threshold: u64,
    ) -> Self {
        Self {
            store,
            manifest,
            cache: Mutex::new(HashMap::new()),
            mmap_threshold,
        }
    }

    /// Load a shard either via mmap (if the file is large enough) or the
    /// regular byte-read fallback.  Returns the in-memory [`ShardIndex`].
    fn load_uncached(&self, artifact_key: &str) -> Result<ShardIndex> {
        // Attempt memory-mapped I/O first.
        let path = self.store.path_for(artifact_key)?;
        if let Ok(meta) = std::fs::metadata(&path) {
            if meta.len() > 0 && meta.len() >= self.mmap_threshold {
                match self.try_load_mmap(&path) {
                    Ok(idx) => return Ok(idx),
                    Err(e) => {
                        tracing::debug!(
                            key = artifact_key,
                            error = %e,
                            "mmap failed; falling back to regular read",
                        );
                    }
                }
            }
        }
        // Fallback: read the whole file into memory.
        let bytes = self.store.get(artifact_key)?;
        ShardIndex::from_bytes(&bytes)
    }

    /// Try to memory-map `path` and deserialize a [`ShardIndex`] from it.
    ///
    /// The file at `path` is opened read-only and memory-mapped for the
    /// duration of deserialization only.  The map is dropped before this
    /// function returns, so no reference to the mapped region escapes.
    ///
    /// Callers only reach this path for non-empty files; zero-length artifacts
    /// fall back to the regular read path before any mmap attempt.
    fn try_load_mmap(&self, path: &std::path::Path) -> Result<ShardIndex> {
        let file = std::fs::File::open(path).map_err(IndexError::Io)?;
        // SAFETY: The mapped region is released before this function returns
        // so no reference to it can escape.  Shard files are write-once
        // artifacts that are never truncated after creation, satisfying
        // memmap2's requirement that the file length not change while mapped.
        let mmap = unsafe { memmap2::Mmap::map(&file).map_err(IndexError::Io)? };
        ShardIndex::from_bytes(&mmap)
    }
}

impl LoadShardStage for MmapShardLoader {
    fn load(&self, shard_id: ShardId) -> Result<Arc<ShardIndex>> {
        {
            let cache = self.cache.lock().map_err(|_| {
                IndexError::Other(
                    "mmap shard loader cache lock poisoned: a panic occurred while holding the lock"
                        .into(),
                )
            })?;
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

        let idx = Arc::new(self.load_uncached(&shard_def.artifact_key)?);

        let mut cache = self.cache.lock().map_err(|_| {
            IndexError::Other(
                "mmap shard loader cache lock poisoned: a panic occurred while holding the lock"
                    .into(),
            )
        })?;
        cache.insert(shard_id, Arc::clone(&idx));
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
    ///
    /// `config` controls every aspect of query execution:
    /// - [`QueryConfig::top_k`] — number of results to return.
    /// - [`QueryConfig::fan_out`] — centroid / shard fan-out policy;
    ///   `candidate_shards` cap is applied after routing.
    /// - [`QueryConfig::rerank_limit`] — absolute cap on candidates passed to
    ///   the reranker; falls back to `top_k × rerank_oversample` when `None`.
    /// - [`QueryConfig::distance_metric`] — per-query metric override;
    ///   defaults to the manifest metric when `None`.
    pub fn run(&self, query: &[f32], config: &QueryConfig) -> Result<Vec<SearchResult>> {
        let k = config.top_k;
        let expected_dims = self.manifest.dims as usize;
        let metric = config
            .distance_metric
            .unwrap_or(self.manifest.distance_metric);

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

        let nprobe = config.fan_out.candidate_centroids as usize;
        let mut probe_shards =
            self.router
                .route(&embedded, &all_centroids, &centroid_to_shard, nprobe);

        // Apply candidate_shards cap (0 = no cap).
        if config.fan_out.candidate_shards > 0 {
            probe_shards.truncate(config.fan_out.candidate_shards as usize);
        }

        debug!(n_shards = probe_shards.len(), "Probing shards");

        // Determine how many candidates to collect per shard before merging.
        // When a reranker is active, collect more candidates so the reranker
        // has a richer pool to choose from.
        let rerank_target = if self.reranker.is_some() {
            config.rerank_limit.map_or_else(
                || k.saturating_mul(self.rerank_oversample).max(k),
                |limit| limit.max(k),
            )
        } else {
            k
        };
        let candidates_per_shard = rerank_target;

        type PerShardSearchOutput = (Vec<SearchResult>, Option<Arc<ShardIndex>>);

        let keep_probed_shards = self.reranker.is_some();
        // Rayon workers do not automatically inherit the scoped tracing
        // subscriber used by span-capture tests, so propagate it explicitly.
        let dispatch = tracing::dispatcher::get_default(|current| current.clone());
        let per_shard: Result<Vec<PerShardSearchOutput>> = probe_shards
            .par_iter()
            .map(|&shard_id| {
                let dispatch = dispatch.clone();
                tracing::dispatcher::with_default(&dispatch, || {
                    let shard = self.loader.load(shard_id)?;
                    let _span = debug_span!(
                        "ann_search",
                        shard_id = shard_id.0,
                        k = candidates_per_shard
                    )
                    .entered();
                    let results = self.candidate_search.search(
                        &embedded,
                        &shard,
                        metric,
                        candidates_per_shard,
                    )?;
                    Ok((results, keep_probed_shards.then_some(shard)))
                })
            })
            .collect();

        let mut all_results = Vec::new();
        let mut probed_shards = Vec::new();
        for (results, shard) in per_shard? {
            all_results.extend(results);
            if let Some(shard) = shard {
                probed_shards.push(shard);
            }
        }

        let merged = self.merge.merge(all_results, rerank_target);
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
            let n_candidates = merged.len();
            let _span = debug_span!("rerank", k, n_candidates).entered();
            Ok(reranker.rerank(&embedded, merged, &probed_records, metric, k))
        } else {
            Ok(merged)
        }
    }

    /// Alias for [`QueryPipeline::run`].
    pub fn search(&self, query: &[f32], config: &QueryConfig) -> Result<Vec<SearchResult>> {
        self.run(query, config)
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
    shard_cache_capacity: usize,
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
            shard_cache_capacity: DEFAULT_SHARD_CACHE_CAPACITY,
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

    /// Set the shard LRU cache capacity for the default [`CachedShardLoader`].
    ///
    /// Ignored when a custom loader is supplied via
    /// [`with_loader`](Self::with_loader).  Defaults to
    /// [`DEFAULT_SHARD_CACHE_CAPACITY`].
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is `0`.
    #[must_use]
    pub fn with_shard_cache_capacity(mut self, capacity: usize) -> Self {
        assert!(
            capacity > 0,
            "QueryPipelineBuilder shard cache capacity must be at least 1"
        );
        self.shard_cache_capacity = capacity;
        self
    }

    /// Assemble the [`QueryPipeline`].
    ///
    /// If no custom loader was provided via [`with_loader`](Self::with_loader),
    /// a [`CachedShardLoader`] backed by the store passed to
    /// [`QueryPipeline::builder`] is used with the configured cache capacity.
    pub fn build(self) -> QueryPipeline {
        let loader = self.loader.unwrap_or_else(|| {
            Arc::new(CachedShardLoader::with_cache_capacity(
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
            rerank_oversample: self.rerank_oversample,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;
    use tracing::subscriber;
    use tracing_subscriber::prelude::*;

    use super::*;
    use crate::builder::{BuildParams, IndexBuilder};
    use shardlake_core::{
        config::{QueryConfig, SystemConfig},
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
        let from_run = pipeline
            .run(
                &query,
                &QueryConfig {
                    top_k: 3,
                    ..QueryConfig::default()
                },
            )
            .unwrap();
        let from_search = pipeline
            .search(
                &query,
                &QueryConfig {
                    top_k: 3,
                    ..QueryConfig::default()
                },
            )
            .unwrap();
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
        let results = pipeline
            .run(
                &query,
                &QueryConfig {
                    top_k: 3,
                    fan_out: shardlake_core::config::FanOutPolicy {
                        candidate_centroids: 1,
                        ..Default::default()
                    },
                    ..QueryConfig::default()
                },
            )
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[derive(Clone, Default)]
    struct SpanCollector {
        names: Arc<Mutex<Vec<String>>>,
    }

    impl SpanCollector {
        fn snapshot(&self) -> Vec<String> {
            self.names.lock().unwrap().clone()
        }
    }

    impl<S> tracing_subscriber::Layer<S> for SpanCollector
    where
        S: tracing::Subscriber,
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.names
                .lock()
                .unwrap()
                .push(attrs.metadata().name().to_string());
        }
    }

    fn collect_spans<T>(f: impl FnOnce() -> T) -> (T, Vec<String>) {
        let collector = SpanCollector::default();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::TRACE)
            .with(collector.clone());
        let result = subscriber::with_default(subscriber, f);
        (result, collector.snapshot())
    }

    #[test]
    fn pipeline_run_emits_shard_load_and_ann_search_spans() {
        let records = make_records(10, 4);
        let query = records[0].data.clone();
        let pipeline = build_pipeline(records);
        let (_, span_names) = collect_spans(|| {
            pipeline
                .run(
                    &query,
                    &QueryConfig {
                        top_k: 3,
                        ..QueryConfig::default()
                    },
                )
                .unwrap()
        });

        assert!(
            span_names.iter().any(|name| name == "shard_load"),
            "expected shard_load span; got: {:?}",
            span_names
        );
        assert!(
            span_names.iter().any(|name| name == "ann_search"),
            "expected ann_search span; got: {:?}",
            span_names
        );
    }

    #[test]
    fn pipeline_run_with_reranker_emits_rerank_span() {
        let records = make_records(10, 4);
        let query = records[0].data.clone();
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
                dataset_version: DatasetVersion("ds-rerank-spans".into()),
                embedding_version: EmbeddingVersion("emb-rerank-spans".into()),
                index_version: IndexVersion("idx-rerank-spans".into()),
                metric: DistanceMetric::Euclidean,
                dims: 4,
                vectors_key: "datasets/ds-rerank-spans/vectors.jsonl".into(),
                metadata_key: "datasets/ds-rerank-spans/metadata.json".into(),
                pq_params: None,
            })
            .unwrap();
        let pipeline = QueryPipeline::builder(store, manifest)
            .rerank_stage(Arc::new(ExactRerankStage))
            .rerank_oversample(2)
            .build();
        let (_, span_names) = collect_spans(|| {
            pipeline
                .run(
                    &query,
                    &QueryConfig {
                        top_k: 3,
                        ..QueryConfig::default()
                    },
                )
                .unwrap()
        });

        assert!(
            span_names.iter().any(|name| name == "rerank"),
            "expected rerank span; got: {:?}",
            span_names
        );
    }
}
