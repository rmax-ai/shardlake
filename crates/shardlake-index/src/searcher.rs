//! Query-time shard searcher with lazy loading and in-memory LRU cache.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use rayon::prelude::*;
use tracing::{debug, info};

use shardlake_core::{
    config::FanOutPolicy,
    error::CoreError,
    types::{DistanceMetric, SearchResult, ShardId, VectorId},
};
use shardlake_manifest::Manifest;
use shardlake_storage::ObjectStore;

use crate::{
    cache::{ShardCache, DEFAULT_SHARD_CACHE_CAPACITY},
    exact::{distance, exact_search, merge_top_k},
    kmeans::top_n_centroids,
    pq::PqCodebook,
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
        }
    }

    /// Return the underlying manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
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

        // ===== ROUTING STEP =====
        // Select the top `candidate_centroids` nearest IVF centroids.
        let n_centroids = (policy.candidate_centroids as usize).min(all_centroids.len());
        let probe_indices = top_n_centroids(query, &all_centroids, n_centroids);

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

        if pq_enabled {
            self.search_pq_shards(
                query,
                &probe_shards,
                k,
                metric,
                policy.max_vectors_per_shard,
            )
        } else {
            // Stages: Load shard + exact search run concurrently across probed shards.
            // Each task loads one shard and scores its records; results are merged below.
            // Any shard-load error short-circuits the fan-out.
            let shard_results: Result<Vec<Vec<SearchResult>>> = probe_shards
                .par_iter()
                .map(|&shard_id| {
                    let shard = self.load_shard(shard_id)?;
                    let records = if policy.max_vectors_per_shard > 0 {
                        let limit =
                            (policy.max_vectors_per_shard as usize).min(shard.records.len());
                        &shard.records[..limit]
                    } else {
                        &shard.records
                    };
                    Ok(exact_search(query, records, metric, k))
                })
                .collect();
            let all_results: Vec<SearchResult> = shard_results?.into_iter().flatten().collect();
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
        metric: DistanceMetric,
        max_vectors_per_shard: u32,
    ) -> Result<Vec<SearchResult>> {
        if metric != DistanceMetric::Euclidean {
            return Err(IndexError::Other(
                "PQ search currently supports only euclidean distance".into(),
            ));
        }

        let codebook = self.load_codebook()?;
        let table = codebook.compute_distance_table(query)?;
        let metadata_map = self.load_metadata_map()?;

        // Load and score PQ-encoded shards concurrently.  The codebook distance
        // table and metadata map are computed once and shared across tasks.
        let shard_results: Result<Vec<Vec<SearchResult>>> = probe_shards
            .par_iter()
            .map(|&shard_id| {
                let shard = self.load_pq_shard(shard_id)?;
                let max_entries = if max_vectors_per_shard > 0 {
                    (max_vectors_per_shard as usize).min(shard.entries.len())
                } else {
                    shard.entries.len()
                };
                let mut scored: Vec<SearchResult> = shard
                    .entries
                    .iter()
                    .take(max_entries)
                    .map(|(id, codes)| SearchResult {
                        id: *id,
                        score: codebook.adc_distance(codes, &table),
                        metadata: metadata_map.get(&id.to_string()).cloned(),
                    })
                    .collect();
                scored.sort_by(|a, b| {
                    a.score
                        .partial_cmp(&b.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                scored.truncate(k);
                Ok(scored)
            })
            .collect();
        let all_results: Vec<SearchResult> = shard_results?.into_iter().flatten().collect();
        Ok(merge_top_k(all_results, k))
    }

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
        mut candidates: Vec<SearchResult>,
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

        let metric = self.manifest.distance_metric;
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

    fn load_metadata_map(&self) -> Result<Arc<HashMap<String, serde_json::Value>>> {
        {
            let guard = self
                .metadata_cache
                .lock()
                .map_err(|_| IndexError::Other("metadata cache lock poisoned".into()))?;
            if let Some(ref metadata) = *guard {
                return Ok(Arc::clone(metadata));
            }
        }

        let bytes = self.store.get(&self.manifest.metadata_key)?;
        let metadata: HashMap<String, serde_json::Value> = serde_json::from_slice(&bytes)
            .map_err(|err| IndexError::Other(format!("invalid dataset metadata map: {err}")))?;
        let metadata = Arc::new(metadata);

        let mut guard = self
            .metadata_cache
            .lock()
            .map_err(|_| IndexError::Other("metadata cache lock poisoned".into()))?;
        *guard = Some(Arc::clone(&metadata));
        Ok(metadata)
    }

    /// Load a PQ-encoded shard from cache or store.
    fn load_pq_shard(&self, shard_id: ShardId) -> Result<Arc<PqShard>> {
        self.pq_shard_cache.get_or_load(shard_id, || {
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
            Ok(shard)
        })
    }

    // ── Raw shard path ────────────────────────────────────────────────────────

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
        self.cache.get_or_load(shard_id, || {
            let shard_def = self
                .manifest
                .shards
                .iter()
                .find(|s| s.shard_id == shard_id)
                .ok_or_else(|| IndexError::Other(format!("shard {shard_id} not in manifest")))?;
            Ok(Arc::new(
                self.load_raw_shard_uncached(&shard_def.artifact_key)?,
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::builder::{BuildParams, IndexBuilder};
    use shardlake_core::{
        config::{FanOutPolicy, SystemConfig},
        types::{DatasetVersion, EmbeddingVersion, IndexVersion, VectorId, VectorRecord},
    };
    use shardlake_storage::{LocalObjectStore, StorageError};

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
}
