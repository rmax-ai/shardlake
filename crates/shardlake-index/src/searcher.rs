//! Query-time shard searcher with lazy loading and in-memory LRU cache.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Instant,
};

use rayon::prelude::*;
use tracing::{debug, info};

use shardlake_core::{
    config::{FanOutPolicy, PrefetchPolicy, SystemConfig},
    error::CoreError,
    types::{DistanceMetric, SearchResult, ShardId, VectorId},
};
use shardlake_manifest::Manifest;
use shardlake_storage::ObjectStore;

use crate::{
    cache::{ShardCache, DEFAULT_SHARD_CACHE_CAPACITY},
    exact::{distance, merge_top_k},
    kmeans::top_n_centroids,
    metrics::CacheMetrics,
    plugin::{AnnPlugin, IvfFlatPlugin, IvfPqPlugin},
    pq::PqCodebook,
    query_plan::QueryPlan,
    shard::{PqShard, ShardIndex},
    IndexError, Result, MMAP_MIN_SIZE_BYTES, PQ8_CODEC,
};

/// Searcher that loads shard indexes lazily from `store`, caching them in RAM.
///
/// Raw shard indexes and PQ-encoded shards are each kept in a bounded LRU
/// cache.  Use [`IndexSearcher::with_cache_capacity`] when you need to size
/// the caches explicitly (e.g. from
/// [`SystemConfig::shard_cache_capacity`](shardlake_core::config::SystemConfig::shard_cache_capacity)).
pub struct IndexSearcher {
    store: Arc<dyn ObjectStore>,
    manifest: Manifest,
    /// LRU cache of raw shard indexes keyed by shard ID.
    cache: ShardCache<ShardIndex>,
    /// LRU cache of PQ-encoded shards keyed by shard ID.
    pq_shard_cache: ShardCache<PqShard>,
    /// PQ codebook; loaded once on first PQ search, then cached.
    codebook: Mutex<Option<Arc<PqCodebook>>>,
    metadata_cache: Mutex<Option<Arc<HashMap<String, serde_json::Value>>>>,
    mmap_threshold: u64,
    access_counts: Mutex<HashMap<ShardId, u64>>,
    prefetch: Option<PrefetchPolicy>,
    /// Cache observability counters shared with external monitoring.
    cache_metrics: Arc<CacheMetrics>,
}

/// Intermediate result of the IVF routing step, shared by [`IndexSearcher::search`]
/// and [`IndexSearcher::search_with_plan`].
struct RouteResult {
    /// Shard IDs to probe after centroid selection and deduplication.
    probe_shards: Vec<ShardId>,
    /// Centroid vectors that were selected (one per probe index, in selection
    /// order). Populated only when the caller explicitly asks to capture them.
    selected_centroids: Vec<Vec<f32>>,
    /// Distance metric derived from the manifest.
    metric: DistanceMetric,
    /// Whether PQ-encoded shards should be searched.
    pq_enabled: bool,
    /// Per-shard vector cap carried from the fan-out policy.
    max_vectors_per_shard: u32,
}

impl IndexSearcher {
    /// Create a new searcher using [`DEFAULT_SHARD_CACHE_CAPACITY`] for both
    /// the raw-shard and PQ-shard caches.
    pub fn new(store: Arc<dyn ObjectStore>, manifest: Manifest) -> Self {
        Self::with_cache_capacity(store, manifest, DEFAULT_SHARD_CACHE_CAPACITY)
    }

    /// Create a new searcher with a specific LRU cache `capacity` for both the
    /// raw-shard and PQ-shard caches.
    ///
    /// Pass `config.shard_cache_capacity` here to honour the operator's runtime
    /// configuration.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is `0`.
    pub fn with_cache_capacity(
        store: Arc<dyn ObjectStore>,
        manifest: Manifest,
        capacity: usize,
    ) -> Self {
        Self::with_cache_capacity_and_mmap_threshold(store, manifest, capacity, MMAP_MIN_SIZE_BYTES)
    }

    /// Create a new searcher with a custom raw-shard mmap threshold in bytes.
    ///
    /// Raw shard artifacts backed by a local [`ObjectStore`] path and whose
    /// size is at least `mmap_threshold` are memory-mapped for deserialization.
    /// Remote backends, unsupported environments, and mmap failures continue to
    /// use the regular `ObjectStore::get` path transparently.
    pub fn with_mmap_threshold(
        store: Arc<dyn ObjectStore>,
        manifest: Manifest,
        mmap_threshold: u64,
    ) -> Self {
        Self::with_cache_capacity_and_mmap_threshold(
            store,
            manifest,
            DEFAULT_SHARD_CACHE_CAPACITY,
            mmap_threshold,
        )
    }

    /// Create a new searcher with explicit cache capacity and mmap threshold.
    ///
    /// This combines the operator-configurable raw/PQ shard cache size with the
    /// threshold for using memory-mapped I/O when a local file path is
    /// available.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is `0`.
    pub fn with_cache_capacity_and_mmap_threshold(
        store: Arc<dyn ObjectStore>,
        manifest: Manifest,
        capacity: usize,
        mmap_threshold: u64,
    ) -> Self {
        assert!(
            capacity > 0,
            "IndexSearcher shard cache capacity must be at least 1"
        );
        info!(
            index_version = %manifest.index_version,
            shards = manifest.shards.len(),
            shard_cache_capacity = capacity,
            "IndexSearcher created"
        );
        Self {
            store,
            manifest,
            cache: ShardCache::new(capacity),
            pq_shard_cache: ShardCache::new(capacity),
            codebook: Mutex::new(None),
            metadata_cache: Mutex::new(None),
            mmap_threshold,
            access_counts: Mutex::new(HashMap::new()),
            prefetch: None,
            cache_metrics: Arc::new(CacheMetrics::new()),
        }
    }

    /// Create a new searcher configured from a [`SystemConfig`].
    pub fn from_config(
        store: Arc<dyn ObjectStore>,
        manifest: Manifest,
        config: &SystemConfig,
    ) -> Result<Self> {
        config.prefetch.validate()?;

        let mut searcher = Self::with_cache_capacity(store, manifest, config.shard_cache_capacity);
        if config.prefetch.enabled {
            searcher.prefetch = Some(config.prefetch.clone());
        }
        Ok(searcher)
    }

    /// Enable best-effort warming of hot raw shards after cache misses.
    #[must_use]
    pub fn with_prefetch(mut self, policy: PrefetchPolicy) -> Self {
        self.prefetch = Some(policy);
        self
    }

    /// Return the underlying manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Return a shared reference to the cache observability counters.
    ///
    /// The returned [`Arc`] points to the same instance used internally, so
    /// callers observe live counter updates without extra synchronisation cost.
    pub fn cache_metrics(&self) -> Arc<CacheMetrics> {
        Arc::clone(&self.cache_metrics)
    }

    /// Return the serialized size of all currently cached shard artifacts.
    ///
    /// Both raw-vector and PQ shard caches are included so the serving layer can
    /// expose an accurate scrape-time retained-bytes gauge.
    pub fn cached_shard_bytes(&self) -> Result<u64> {
        Ok(self.cache.retained_bytes(ShardIndex::encoded_len)?
            + self.pq_shard_cache.retained_bytes(PqShard::encoded_len)?)
    }

    /// Return the cache hit rate for raw-shard loads since this searcher was created.
    ///
    /// Returns a value in `[0.0, 1.0]`, or `0.0` when no shard accesses have
    /// been recorded yet.  The rate is computed from the raw-shard LRU cache;
    /// PQ-shard accesses are tracked separately and are not included.
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache.hits();
        let misses = self.cache.misses();
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Return the cumulative raw-shard cache hit and miss counts.
    ///
    /// The returned tuple is `(hits, misses)` for the raw-shard LRU cache since
    /// this searcher was created. PQ-shard accesses are tracked separately and
    /// are not included.
    pub fn cache_access_counts(&self) -> (u64, u64) {
        (self.cache.hits(), self.cache.misses())
    }

    /// Perform approximate top-k search using the provided [`FanOutPolicy`].
    ///
    /// The policy controls:
    /// - how many IVF centroids are selected for routing
    ///   ([`FanOutPolicy::candidate_centroids`]),
    /// - the maximum number of shards probed after deduplication
    ///   ([`FanOutPolicy::candidate_shards`], `0` = no cap), and
    /// - the maximum number of vectors evaluated per shard
    ///   ([`FanOutPolicy::max_vectors_per_shard`], `0` = no limit).
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        policy: &FanOutPolicy,
    ) -> Result<Vec<SearchResult>> {
        self.search_with_metric(query, k, policy, self.manifest.distance_metric)
    }

    /// Perform approximate top-k search using an explicit distance `metric`.
    ///
    /// This is primarily used by callers that expose per-query metric overrides
    /// while reusing the same loaded index artifacts.
    pub fn search_with_metric(
        &self,
        query: &[f32],
        k: usize,
        policy: &FanOutPolicy,
        metric: DistanceMetric,
    ) -> Result<Vec<SearchResult>> {
        let routed = self.route_query(query, policy, false, metric)?;
        if routed.probe_shards.is_empty() {
            return Ok(Vec::new());
        }
        self.fan_out_search(query, k, &routed)
    }

    /// Perform approximate top-k search and capture a [`QueryPlan`] with the
    /// routing decisions made during the execution.
    ///
    /// The returned [`QueryPlan`] records:
    /// - the centroid vectors that were selected during IVF routing
    ///   (`selected_centroids`),
    /// - the shard IDs probed after centroid-to-shard mapping and deduplication
    ///   (`searched_shards`), and
    /// - the candidate vectors returned by the fan-out search, before any
    ///   reranking (`candidate_vectors`).
    ///
    /// The search semantics are identical to [`IndexSearcher::search`].
    pub fn search_with_plan(
        &self,
        query: &[f32],
        k: usize,
        policy: &FanOutPolicy,
    ) -> Result<QueryPlan> {
        self.search_with_plan_with_metric(query, k, policy, self.manifest.distance_metric)
    }

    /// Perform approximate top-k search with an explicit scoring metric and
    /// capture the resulting [`QueryPlan`].
    ///
    /// The routing centroids are still selected using the index's routing
    /// configuration; `metric` affects only candidate scoring and plan output.
    pub fn search_with_plan_with_metric(
        &self,
        query: &[f32],
        k: usize,
        policy: &FanOutPolicy,
        metric: DistanceMetric,
    ) -> Result<QueryPlan> {
        let routed = self.route_query(query, policy, true, metric)?;
        if routed.probe_shards.is_empty() {
            return Ok(QueryPlan {
                selected_centroids: Vec::new(),
                searched_shards: Vec::new(),
                candidate_vectors: Vec::new(),
            });
        }
        let candidate_vectors = self.fan_out_search(query, k, &routed)?;
        Ok(QueryPlan {
            selected_centroids: routed.selected_centroids,
            searched_shards: routed.probe_shards,
            candidate_vectors,
        })
    }

    /// Resolve the IVF routing step: collect centroids, select the top-n
    /// candidates, and map them to deduplicated shard IDs.
    ///
    /// Returns an empty `probe_shards` when no centroids are present (empty
    /// index), which callers must check before invoking [`Self::fan_out_search`].
    fn route_query(
        &self,
        query: &[f32],
        policy: &FanOutPolicy,
        capture_selected_centroids: bool,
        metric: DistanceMetric,
    ) -> Result<RouteResult> {
        let expected_dims = self.manifest.dims as usize;
        if query.len() != expected_dims {
            return Err(IndexError::Core(CoreError::DimensionMismatch {
                expected: expected_dims,
                got: query.len(),
            }));
        }

        let pq_enabled =
            self.manifest.compression.enabled && self.manifest.compression.codec == PQ8_CODEC;

        // Validate the selected ANN backend once at the routing edge so
        // unsupported metric/backend combinations fail before any shard work.
        let plugin: Box<dyn AnnPlugin> = if pq_enabled {
            Box::new(IvfPqPlugin::new(self.load_codebook()?))
        } else {
            Box::new(IvfFlatPlugin)
        };
        plugin.validate(expected_dims, metric)?;

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
                // Both backends go through the same load path.
                let shard = self.load_shard_for_stage(shard_def.shard_id)?;
                for c in &shard.centroids {
                    all_centroids.push(c.clone());
                    centroid_to_shard.push(shard_def.shard_id);
                }
            }
        }

        if all_centroids.is_empty() {
            return Ok(RouteResult {
                probe_shards: Vec::new(),
                selected_centroids: Vec::new(),
                metric,
                pq_enabled,
                max_vectors_per_shard: policy.max_vectors_per_shard,
            });
        }

        // ===== ROUTING STEP =====
        // Select the top `candidate_centroids` nearest IVF centroids.
        let n_centroids = (policy.candidate_centroids as usize).min(all_centroids.len());
        let probe_indices = top_n_centroids(query, &all_centroids, n_centroids);

        let selected_centroids = if capture_selected_centroids {
            probe_indices
                .iter()
                .filter_map(|&i| all_centroids.get(i).cloned())
                .collect()
        } else {
            Vec::new()
        };

        // Map centroid indices to shard ids and deduplicate.
        let mut probe_shards: Vec<ShardId> = probe_indices
            .into_iter()
            .filter_map(|i| centroid_to_shard.get(i).copied())
            .collect();
        let mut seen = HashSet::new();
        probe_shards.retain(|shard_id| seen.insert(*shard_id));

        // Apply candidate_shards cap (0 = no cap).
        if policy.candidate_shards > 0 {
            probe_shards.truncate(policy.candidate_shards as usize);
        }

        debug!(
            n_shards = probe_shards.len(),
            candidate_centroids = policy.candidate_centroids,
            candidate_shards = policy.candidate_shards,
            max_vectors_per_shard = policy.max_vectors_per_shard,
            "Probing shards"
        );

        Ok(RouteResult {
            probe_shards,
            selected_centroids,
            metric,
            pq_enabled,
            max_vectors_per_shard: policy.max_vectors_per_shard,
        })
    }

    /// Execute the fan-out search across the probed shards described in
    /// `routed`. Callers must ensure `routed.probe_shards` is non-empty.
    fn fan_out_search(
        &self,
        query: &[f32],
        k: usize,
        routed: &RouteResult,
    ) -> Result<Vec<SearchResult>> {
        let plugin: Box<dyn AnnPlugin> = if routed.pq_enabled {
            Box::new(IvfPqPlugin::new(self.load_codebook()?))
        } else {
            Box::new(IvfFlatPlugin)
        };
        plugin.validate(self.manifest.dims as usize, routed.metric)?;
        let stage = plugin.candidate_stage();

        let shard_results: Result<Vec<Vec<SearchResult>>> = routed
            .probe_shards
            .par_iter()
            .map(|&shard_id| {
                let shard = self.load_shard_for_stage(shard_id)?;
                // Apply max_vectors_per_shard limit when requested.
                let limited: Arc<ShardIndex> = if routed.max_vectors_per_shard > 0 {
                    let limit = (routed.max_vectors_per_shard as usize).min(shard.records.len());
                    Arc::new(ShardIndex {
                        shard_id: shard.shard_id,
                        dims: shard.dims,
                        centroids: shard.centroids.clone(),
                        records: shard.records[..limit].to_vec(),
                    })
                } else {
                    Arc::clone(&shard)
                };
                stage.search(query, &limited, routed.metric, k)
            })
            .collect();

        let mut all_results: Vec<SearchResult> = shard_results?.into_iter().flatten().collect();

        // Enrich results with metadata from the metadata artifact when it exists.
        // For PQ-compressed indexes this replicates the per-result metadata that
        // was previously attached in the now-removed dedicated PQ search path.
        if routed.pq_enabled {
            if let Some(metadata_map) = self.try_load_metadata_map()? {
                for result in &mut all_results {
                    if result.metadata.is_none() {
                        result.metadata = metadata_map.get(&result.id.to_string()).cloned();
                    }
                }
            }
        }

        Ok(merge_top_k(all_results, k))
    }

    // ── Shard loading for plugin-based search ─────────────────────────────────

    /// Load a shard as a raw [`ShardIndex`] regardless of the on-disk format.
    ///
    /// For format-v1 shards (flat indexes), this delegates to [`Self::load_shard`].
    /// For format-v2 shards (PQ-compressed indexes), the [`PqShard`] is loaded via
    /// the PQ shard cache and its entries are reconstructed to approximate raw
    /// vectors using [`PqCodebook::reconstruct`].  The reconstruction is a
    /// fixed-point operation: `encode(reconstruct(codes)) == codes`, so the
    /// [`PqCandidateStage`](crate::pipeline::PqCandidateStage) produces identical
    /// results to the former `search_pq_shards` path.
    fn load_shard_for_stage(&self, shard_id: ShardId) -> Result<Arc<ShardIndex>> {
        if !self.manifest.compression.enabled || self.manifest.compression.codec != PQ8_CODEC {
            return self.load_shard(shard_id);
        }

        // PQ-encoded index (format-v2): reconstruct approximate raw vectors so
        // both backends share the same CandidateSearchStage call site.
        self.record_access(shard_id)?;
        let codebook = self.load_codebook()?;
        let pq_shard = self.load_pq_shard(shard_id)?;
        let records = pq_shard
            .entries
            .iter()
            .map(|(id, codes)| {
                let data = codebook.reconstruct(codes)?;
                Ok(shardlake_core::types::VectorRecord {
                    id: *id,
                    data,
                    metadata: None,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(ShardIndex {
            shard_id: pq_shard.shard_id,
            dims: pq_shard.dims,
            centroids: pq_shard.centroids.clone(),
            records,
        }))
    }

    // ── Metadata loading ──────────────────────────────────────────────────────

    /// Rerank ANN candidates using exact distance to `query`.
    ///
    /// Fetches the raw vectors for each candidate from the in-memory shard
    /// cache and recomputes exact distances, returning the candidates sorted by
    /// their true distances. Call this after [`IndexSearcher::search`] when a
    /// more accurate final ranking is required.
    ///
    /// Candidates whose vectors are not found in the raw-shard cache (for
    /// example, when they were produced by a different searcher instance or a
    /// PQ-only search path) retain their original score.
    ///
    /// # Errors
    ///
    /// Returns an error if the query dimensions do not match the index or if
    /// the shard cache lock is poisoned.
    pub fn rerank(
        &self,
        query: &[f32],
        candidates: Vec<SearchResult>,
    ) -> Result<Vec<SearchResult>> {
        self.rerank_with_metric(query, candidates, self.manifest.distance_metric)
    }

    /// Rerank ANN candidates using an explicit distance `metric`.
    ///
    /// This allows transport layers to honor per-query metric overrides without
    /// rebuilding the index or mutating the manifest-wide default.
    pub fn rerank_with_metric(
        &self,
        query: &[f32],
        mut candidates: Vec<SearchResult>,
        metric: DistanceMetric,
    ) -> Result<Vec<SearchResult>> {
        if candidates.is_empty() {
            return Ok(candidates);
        }

        let expected_dims = self.manifest.dims as usize;
        if query.len() != expected_dims {
            return Err(IndexError::Core(CoreError::DimensionMismatch {
                expected: expected_dims,
                got: query.len(),
            }));
        }

        let mut remaining_ids: HashSet<VectorId> =
            candidates.iter().map(|result| result.id).collect();

        let vector_lookup: HashMap<VectorId, Vec<f32>> = {
            let mut vectors = HashMap::with_capacity(remaining_ids.len());
            for shard in self.cache.cached_values()? {
                for record in &shard.records {
                    if remaining_ids.remove(&record.id) {
                        if record.data.len() != expected_dims {
                            return Err(IndexError::Core(CoreError::DimensionMismatch {
                                expected: expected_dims,
                                got: record.data.len(),
                            }));
                        }

                        vectors.insert(record.id, record.data.clone());
                        if remaining_ids.is_empty() {
                            break;
                        }
                    }
                }

                if remaining_ids.is_empty() {
                    break;
                }
            }

            vectors
        };

        for result in &mut candidates {
            if let Some(raw_vec) = vector_lookup.get(&result.id) {
                result.score = distance(query, raw_vec, metric);
            }
        }

        candidates.sort_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(candidates)
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

    fn try_load_metadata_map(&self) -> Result<Option<Arc<HashMap<String, serde_json::Value>>>> {
        {
            let guard = self
                .metadata_cache
                .lock()
                .map_err(|_| IndexError::Other("metadata cache lock poisoned".into()))?;
            if let Some(ref metadata) = *guard {
                return Ok(Some(Arc::clone(metadata)));
            }
        }

        match self.store.get(&self.manifest.metadata_key) {
            Ok(bytes) => {
                let metadata: HashMap<String, serde_json::Value> = serde_json::from_slice(&bytes)
                    .map_err(|err| {
                    IndexError::Other(format!("invalid dataset metadata map: {err}"))
                })?;
                let metadata = Arc::new(metadata);
                let mut guard = self
                    .metadata_cache
                    .lock()
                    .map_err(|_| IndexError::Other("metadata cache lock poisoned".into()))?;
                *guard = Some(Arc::clone(&metadata));
                Ok(Some(metadata))
            }
            Err(shardlake_storage::StorageError::NotFound(_)) => Ok(None),
            Err(e) => Err(IndexError::from(e)),
        }
    }

    /// Load a PQ-encoded shard from cache or store.
    fn load_pq_shard(&self, shard_id: ShardId) -> Result<Arc<PqShard>> {
        let (shard, access) = self.pq_shard_cache.get_or_load_with_status(shard_id, || {
            let shard_def = self
                .manifest
                .shards
                .iter()
                .find(|s| s.shard_id == shard_id)
                .ok_or_else(|| IndexError::Other(format!("shard {shard_id} not in manifest")))?;

            self.cache_metrics.record_miss();
            let started = Instant::now();
            let load_result = (|| {
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
                Ok(shard)
            })();
            self.cache_metrics
                .record_load_attempt(started.elapsed().as_nanos() as u64);
            load_result
        })?;

        if matches!(access, crate::cache::CacheAccess::Hit) {
            self.cache_metrics.record_hit();
        }

        Ok(shard)
    }

    // ── Raw shard path ────────────────────────────────────────────────────────

    fn record_access(&self, shard_id: ShardId) -> Result<()> {
        let mut counts = self
            .access_counts
            .lock()
            .map_err(|_| IndexError::Other("search access-count lock poisoned".into()))?;
        *counts.entry(shard_id).or_insert(0) += 1;
        Ok(())
    }

    fn prefetch_candidates(&self) -> Result<Vec<(ShardId, String)>> {
        let Some(policy) = self.prefetch.as_ref().filter(|policy| policy.enabled) else {
            return Ok(Vec::new());
        };

        let threshold = u64::from(policy.min_query_count);
        let counts = self
            .access_counts
            .lock()
            .map_err(|_| IndexError::Other("search access-count lock poisoned".into()))?;

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
            match self.cache.get_or_load_with_status(shard_id, || {
                let shard = self.load_raw_shard_uncached(&artifact_key)?;
                Ok(Arc::new(shard))
            }) {
                Ok((_, crate::cache::CacheAccess::Miss)) => {
                    debug!(?shard_id, "prefetch: warmed hot shard");
                }
                Ok((_, crate::cache::CacheAccess::Hit | crate::cache::CacheAccess::Raced)) => {}
                Err(error) => {
                    debug!(?shard_id, %error, "prefetch: failed to warm shard");
                }
            }
        }

        Ok(())
    }

    fn load_raw_shard_uncached(&self, artifact_key: &str) -> Result<ShardIndex> {
        if let Some(path) = self.store.local_path_for(artifact_key)? {
            if let Ok(meta) = std::fs::metadata(&path) {
                if meta.len() > 0 && meta.len() >= self.mmap_threshold {
                    match self.try_load_raw_shard_mmap(&path) {
                        Ok(idx) => return Ok(idx),
                        Err(error) => {
                            debug!(
                                key = artifact_key,
                                %error,
                                "mmap failed for searcher shard load; falling back to regular read",
                            );
                        }
                    }
                }
            }
        }

        let bytes = self.store.get(artifact_key)?;
        ShardIndex::from_bytes(&bytes)
    }

    fn try_load_raw_shard_mmap(&self, path: &std::path::Path) -> Result<ShardIndex> {
        let file = std::fs::File::open(path).map_err(IndexError::Io)?;
        // SAFETY: The mapped region is used only within this function and is
        // dropped immediately after deserialization, so no borrowed reference
        // can outlive the file mapping. Shard artifacts are immutable after
        // publication, so their length does not change while mapped.
        let mmap = unsafe { memmap2::Mmap::map(&file).map_err(IndexError::Io)? };
        ShardIndex::from_bytes(&mmap)
    }

    /// Load a raw shard from cache or store.
    fn load_shard(&self, shard_id: ShardId) -> Result<Arc<ShardIndex>> {
        self.record_access(shard_id)?;

        let (shard, access) = self.cache.get_or_load_with_status(shard_id, || {
            let shard_def = self
                .manifest
                .shards
                .iter()
                .find(|s| s.shard_id == shard_id)
                .ok_or_else(|| IndexError::Other(format!("shard {shard_id} not in manifest")))?;
            self.cache_metrics.record_miss();
            let started = Instant::now();
            let result = self.load_raw_shard_uncached(&shard_def.artifact_key);
            self.cache_metrics
                .record_load_attempt(started.elapsed().as_nanos() as u64);
            result.map(Arc::new)
        })?;

        match access {
            crate::cache::CacheAccess::Hit => {
                self.cache_metrics.record_hit();
            }
            crate::cache::CacheAccess::Miss => {
                self.warm_hot_shards()?;
            }
            // Raced: another thread inserted this shard between the miss
            // detection and our load completing.  The load was counted in the
            // closure above (record_miss + record_load_attempt), so no
            // additional metric update is needed here.
            crate::cache::CacheAccess::Raced => {}
        }

        Ok(shard)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use tempfile::tempdir;

    use super::*;
    use crate::builder::{BuildParams, IndexBuilder};
    use shardlake_core::{
        config::{FanOutPolicy, PrefetchPolicy, SystemConfig},
        types::{DatasetVersion, EmbeddingVersion, IndexVersion, VectorId, VectorRecord},
    };
    use shardlake_storage::{LocalObjectStore, ObjectStore, StorageError};

    struct CountingLocalPathStore {
        inner: Arc<LocalObjectStore>,
        get_calls: Arc<AtomicUsize>,
        expose_local_path: bool,
    }

    impl ObjectStore for CountingLocalPathStore {
        fn put(&self, key: &str, data: Vec<u8>) -> shardlake_storage::Result<()> {
            self.inner.put(key, data)
        }

        fn get(&self, key: &str) -> shardlake_storage::Result<Vec<u8>> {
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.get(key)
        }

        fn exists(&self, key: &str) -> shardlake_storage::Result<bool> {
            self.inner.exists(key)
        }

        fn list(&self, prefix: &str) -> shardlake_storage::Result<Vec<String>> {
            self.inner.list(prefix)
        }

        fn delete(&self, key: &str) -> shardlake_storage::Result<()> {
            self.inner.delete(key)
        }

        fn local_path_for(
            &self,
            key: &str,
        ) -> std::result::Result<Option<std::path::PathBuf>, StorageError> {
            if self.expose_local_path {
                Ok(Some(self.inner.path_for(key)?))
            } else {
                Ok(None)
            }
        }
    }

    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        shard_loads: Arc<AtomicUsize>,
    }

    impl CountingStore {
        fn new(inner: Arc<dyn ObjectStore>) -> (Arc<Self>, Arc<AtomicUsize>) {
            let shard_loads = Arc::new(AtomicUsize::new(0));
            let store = Arc::new(Self {
                inner,
                shard_loads: Arc::clone(&shard_loads),
            });
            (store, shard_loads)
        }
    }

    impl ObjectStore for CountingStore {
        fn put(&self, key: &str, data: Vec<u8>) -> std::result::Result<(), StorageError> {
            self.inner.put(key, data)
        }

        fn get(&self, key: &str) -> std::result::Result<Vec<u8>, StorageError> {
            if key.ends_with(".sidx") {
                self.shard_loads.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get(key)
        }

        fn exists(&self, key: &str) -> std::result::Result<bool, StorageError> {
            self.inner.exists(key)
        }

        fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, StorageError> {
            self.inner.list(prefix)
        }

        fn delete(&self, key: &str) -> std::result::Result<(), StorageError> {
            self.inner.delete(key)
        }
    }

    struct FailingGetStore {
        inner: Arc<dyn ObjectStore>,
    }

    impl ObjectStore for FailingGetStore {
        fn put(&self, key: &str, data: Vec<u8>) -> std::result::Result<(), StorageError> {
            self.inner.put(key, data)
        }

        fn get(&self, key: &str) -> std::result::Result<Vec<u8>, StorageError> {
            Err(StorageError::NotFound(key.to_string()))
        }

        fn exists(&self, key: &str) -> std::result::Result<bool, StorageError> {
            self.inner.exists(key)
        }

        fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, StorageError> {
            self.inner.list(prefix)
        }

        fn delete(&self, key: &str) -> std::result::Result<(), StorageError> {
            self.inner.delete(key)
        }

        fn local_path_for(
            &self,
            key: &str,
        ) -> std::result::Result<Option<std::path::PathBuf>, StorageError> {
            let _ = key;
            Ok(None)
        }
    }

    fn build_test_searcher(tmp: &tempfile::TempDir) -> IndexSearcher {
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 2,
            kmeans_iters: 2,
            nprobe: 2,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            candidate_shards: 0,
            max_vectors_per_shard: 0,
            kmeans_sample_size: None,
            ..SystemConfig::default()
        };
        let records = vec![
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
                data: vec![1.0, 1.0],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(4),
                data: vec![0.5, 0.5],
                metadata: None,
            },
        ];
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
        IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest)
    }

    fn build_metric_test_manifest(
        tmp: &tempfile::TempDir,
        index_version: &str,
        pq_params: Option<crate::pq::PqParams>,
    ) -> (Arc<LocalObjectStore>, shardlake_manifest::Manifest) {
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 1,
            kmeans_iters: 2,
            nprobe: 1,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            candidate_shards: 0,
            max_vectors_per_shard: 0,
            kmeans_sample_size: None,
            ..SystemConfig::default()
        };
        let records = vec![
            VectorRecord {
                id: VectorId(1),
                data: vec![1.0, 0.0, 0.5, 0.5],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(2),
                data: vec![0.0, 1.0, 0.5, 0.5],
                metadata: None,
            },
        ];
        let manifest = IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion(format!("ds-{index_version}")),
                embedding_version: EmbeddingVersion(format!("emb-{index_version}")),
                index_version: IndexVersion(index_version.into()),
                metric: DistanceMetric::Euclidean,
                dims: 4,
                vectors_key: format!("datasets/ds-{index_version}/vectors.jsonl"),
                metadata_key: format!("datasets/ds-{index_version}/metadata.json"),
                pq_params,
            })
            .unwrap();
        (store, manifest)
    }

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
            candidate_shards: 0,
            max_vectors_per_shard: 0,
            kmeans_sample_size: None,
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
        let policy = FanOutPolicy {
            candidate_centroids: 1,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let err = searcher.search(&[1.0, 2.0, 3.0], 1, &policy).unwrap_err();
        assert_eq!(
            err.to_string(),
            "core error: dimension mismatch: expected 2, got 3"
        );
    }

    #[test]
    fn candidate_shards_cap_limits_probed_shards() {
        let tmp = tempdir().unwrap();
        let searcher = build_test_searcher(&tmp);

        // With candidate_shards=1, only one shard is probed; results are still returned.
        let policy = FanOutPolicy {
            candidate_centroids: 4,
            candidate_shards: 1,
            max_vectors_per_shard: 0,
        };
        let results = searcher.search(&[1.0, 0.0], 2, &policy).unwrap();
        assert!(!results.is_empty(), "expected at least one result");
    }

    #[test]
    fn max_vectors_per_shard_limits_candidates() {
        let tmp = tempdir().unwrap();
        let searcher = build_test_searcher(&tmp);

        // Limit to 1 vector per shard; we still get results (just fewer candidates).
        let policy = FanOutPolicy {
            candidate_centroids: 4,
            candidate_shards: 0,
            max_vectors_per_shard: 1,
        };
        let results = searcher.search(&[1.0, 0.0], 4, &policy).unwrap();
        // With 2 shards and 1 vector/shard, we can get at most 2 results.
        assert!(results.len() <= 2);
    }

    #[test]
    fn searcher_uses_mmap_for_local_shards_when_path_is_available() {
        let tmp = tempdir().unwrap();
        let inner = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 2,
            kmeans_iters: 2,
            nprobe: 2,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            candidate_shards: 0,
            max_vectors_per_shard: 0,
            kmeans_sample_size: None,
            ..SystemConfig::default()
        };
        let records = vec![
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
                data: vec![1.0, 1.0],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(4),
                data: vec![0.5, 0.5],
                metadata: None,
            },
        ];
        let manifest = IndexBuilder::new(inner.as_ref(), &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-mmap-searcher".into()),
                embedding_version: EmbeddingVersion("emb-mmap-searcher".into()),
                index_version: IndexVersion("idx-mmap-searcher".into()),
                metric: DistanceMetric::Cosine,
                dims: 2,
                vectors_key: "datasets/ds-mmap-searcher/vectors.jsonl".into(),
                metadata_key: "datasets/ds-mmap-searcher/metadata.json".into(),
                pq_params: None,
            })
            .unwrap();
        let get_calls = Arc::new(AtomicUsize::new(0));
        let store: Arc<dyn ObjectStore> = Arc::new(CountingLocalPathStore {
            inner,
            get_calls: Arc::clone(&get_calls),
            expose_local_path: true,
        });
        let searcher = IndexSearcher::with_mmap_threshold(store, manifest, 0);
        let policy = FanOutPolicy {
            candidate_centroids: 1,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };

        let results = searcher.search(&[1.0, 0.0], 2, &policy).unwrap();

        assert!(!results.is_empty());
        assert_eq!(get_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn searcher_falls_back_to_get_without_local_path() {
        let tmp = tempdir().unwrap();
        let inner = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 2,
            kmeans_iters: 2,
            nprobe: 2,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            candidate_shards: 0,
            max_vectors_per_shard: 0,
            kmeans_sample_size: None,
            ..SystemConfig::default()
        };
        let records = vec![
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
                data: vec![1.0, 1.0],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(4),
                data: vec![0.5, 0.5],
                metadata: None,
            },
        ];
        let manifest = IndexBuilder::new(inner.as_ref(), &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-get-searcher".into()),
                embedding_version: EmbeddingVersion("emb-get-searcher".into()),
                index_version: IndexVersion("idx-get-searcher".into()),
                metric: DistanceMetric::Cosine,
                dims: 2,
                vectors_key: "datasets/ds-get-searcher/vectors.jsonl".into(),
                metadata_key: "datasets/ds-get-searcher/metadata.json".into(),
                pq_params: None,
            })
            .unwrap();
        let get_calls = Arc::new(AtomicUsize::new(0));
        let store: Arc<dyn ObjectStore> = Arc::new(CountingLocalPathStore {
            inner,
            get_calls: Arc::clone(&get_calls),
            expose_local_path: false,
        });
        let searcher = IndexSearcher::with_mmap_threshold(store, manifest, 0);
        let policy = FanOutPolicy {
            candidate_centroids: 1,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };

        let results = searcher.search(&[1.0, 0.0], 2, &policy).unwrap();

        assert!(!results.is_empty());
        assert_eq!(get_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn from_config_rejects_invalid_prefetch_policy() {
        let tmp = tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap()) as Arc<dyn ObjectStore>;
        let manifest = build_test_searcher(&tmp).manifest().clone();
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            prefetch: PrefetchPolicy {
                enabled: true,
                min_query_count: 0,
            },
            ..SystemConfig::default()
        };

        let err = match IndexSearcher::from_config(store, manifest, &config) {
            Ok(_) => panic!("expected invalid prefetch policy to be rejected"),
            Err(err) => err,
        };
        assert_eq!(
            err.to_string(),
            "core error: invalid prefetch policy: min_query_count must be ≥ 1 when prefetch is enabled"
        );
    }

    #[test]
    fn prefetch_warms_hot_evicted_shards_on_runtime_search_path() {
        let tmp = tempdir().unwrap();
        let base_store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let manifest = {
            let config = SystemConfig {
                storage_root: tmp.path().to_path_buf(),
                num_shards: 3,
                kmeans_iters: 10,
                nprobe: 2,
                kmeans_seed: SystemConfig::default_kmeans_seed(),
                ..SystemConfig::default()
            };
            let records = vec![
                VectorRecord {
                    id: VectorId(1),
                    data: vec![0.0, 0.0],
                    metadata: None,
                },
                VectorRecord {
                    id: VectorId(2),
                    data: vec![0.1, 0.0],
                    metadata: None,
                },
                VectorRecord {
                    id: VectorId(3),
                    data: vec![10.0, 10.0],
                    metadata: None,
                },
                VectorRecord {
                    id: VectorId(4),
                    data: vec![10.1, 10.0],
                    metadata: None,
                },
                VectorRecord {
                    id: VectorId(5),
                    data: vec![-10.0, -10.0],
                    metadata: None,
                },
                VectorRecord {
                    id: VectorId(6),
                    data: vec![-10.1, -10.0],
                    metadata: None,
                },
            ];

            IndexBuilder::new(base_store.as_ref(), &config)
                .build(BuildParams {
                    records,
                    dataset_version: DatasetVersion("ds-prefetch-runtime".into()),
                    embedding_version: EmbeddingVersion("emb-prefetch-runtime".into()),
                    index_version: IndexVersion("idx-prefetch-runtime".into()),
                    metric: DistanceMetric::Euclidean,
                    dims: 2,
                    vectors_key: "datasets/ds-prefetch-runtime/vectors.jsonl".into(),
                    metadata_key: "datasets/ds-prefetch-runtime/metadata.json".into(),
                    pq_params: None,
                })
                .unwrap()
        };
        let (counting_store, shard_loads) = CountingStore::new(base_store as Arc<dyn ObjectStore>);
        let searcher = IndexSearcher::with_cache_capacity(
            counting_store as Arc<dyn ObjectStore>,
            manifest.clone(),
            1,
        )
        .with_prefetch(PrefetchPolicy {
            enabled: true,
            min_query_count: 2,
        });

        let hot_shard = manifest.shards[0].shard_id;
        let cold_shard = manifest.shards[1].shard_id;

        searcher.load_shard(hot_shard).unwrap();
        searcher.load_shard(hot_shard).unwrap();

        shard_loads.store(0, Ordering::SeqCst);

        searcher.load_shard(cold_shard).unwrap();
        assert_eq!(
            shard_loads.load(Ordering::SeqCst),
            2,
            "expected one direct load and one warm-up load"
        );

        let before = shard_loads.load(Ordering::SeqCst);
        searcher.load_shard(hot_shard).unwrap();
        assert_eq!(
            shard_loads.load(Ordering::SeqCst),
            before,
            "hot shard should be cache-resident after warming"
        );
    }

    #[test]
    fn search_with_plan_captures_routing_details() {
        let tmp = tempdir().unwrap();
        let searcher = build_test_searcher(&tmp);

        let policy = FanOutPolicy {
            candidate_centroids: 2,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let plan = searcher
            .search_with_plan(&[1.0, 0.0], 1, &policy)
            .expect("search_with_plan");

        assert!(
            !plan.selected_centroids.is_empty(),
            "expected at least one selected centroid"
        );
        for centroid in &plan.selected_centroids {
            assert_eq!(centroid.len(), 2, "centroid must have index dimensionality");
        }
        assert!(
            !plan.searched_shards.is_empty(),
            "expected at least one searched shard"
        );
        assert!(
            !plan.candidate_vectors.is_empty(),
            "expected at least one candidate vector"
        );
    }

    #[test]
    fn search_with_plan_matches_search_results() {
        let tmp = tempdir().unwrap();
        let searcher = build_test_searcher(&tmp);

        let policy = FanOutPolicy {
            candidate_centroids: 2,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let query = &[1.0_f32, 0.0];
        let k = 2;

        let results = searcher.search(query, k, &policy).expect("search");
        let plan = searcher
            .search_with_plan(query, k, &policy)
            .expect("search_with_plan");

        assert_eq!(
            results.len(),
            plan.candidate_vectors.len(),
            "candidate_vectors must match search results"
        );
        for (result, candidate) in results.iter().zip(plan.candidate_vectors.iter()) {
            assert_eq!(result.id, candidate.id);
        }
    }

    #[test]
    fn search_with_plan_rejects_dimension_mismatch() {
        let tmp = tempdir().unwrap();
        let searcher = build_test_searcher(&tmp);

        let policy = FanOutPolicy {
            candidate_centroids: 1,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let err = searcher
            .search_with_plan(&[1.0, 2.0, 3.0], 1, &policy)
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "core error: dimension mismatch: expected 2, got 3"
        );
    }

    #[test]
    fn load_shard_records_load_attempt_on_storage_error() {
        let tmp = tempdir().unwrap();
        let (store, manifest) = build_metric_test_manifest(&tmp, "idx-load-error-raw", None);
        let failing_store = Arc::new(FailingGetStore {
            inner: store as Arc<dyn ObjectStore>,
        });
        let searcher = IndexSearcher::new(failing_store as Arc<dyn ObjectStore>, manifest.clone());

        let err = searcher
            .load_shard(manifest.shards[0].shard_id)
            .unwrap_err();
        assert!(err.to_string().contains("key not found"));

        let snapshot = searcher.cache_metrics().snapshot();
        assert_eq!(snapshot.misses, 1);
        assert_eq!(snapshot.total_load_count, 1);
    }

    #[test]
    fn load_pq_shard_records_load_attempt_on_storage_error() {
        let tmp = tempdir().unwrap();
        let (store, manifest) = build_metric_test_manifest(
            &tmp,
            "idx-load-error-pq",
            Some(crate::pq::PqParams {
                num_subspaces: 2,
                codebook_size: 4,
            }),
        );
        let failing_store = Arc::new(FailingGetStore {
            inner: store as Arc<dyn ObjectStore>,
        });
        let searcher = IndexSearcher::new(failing_store as Arc<dyn ObjectStore>, manifest.clone());

        let err = searcher
            .load_pq_shard(manifest.shards[0].shard_id)
            .unwrap_err();
        assert!(err.to_string().contains("key not found"));

        let snapshot = searcher.cache_metrics().snapshot();
        assert_eq!(snapshot.misses, 1);
        assert_eq!(snapshot.total_load_count, 1);
    }
}
