//! LRU shard cache with metrics, prefetch policy, and memory-mapped loading.
//!
//! [`ShardCache`] is the primary entry point.  It wraps an LRU eviction policy
//! around the shard deserialization step, tracks hit/miss statistics with lock-
//! free atomic counters, and uses memory-mapped I/O when the backend is backed
//! by a local filesystem and the file is large enough to make mmap worthwhile.
//!
//! # Prefetch policy
//!
//! Call [`ShardCache::record_query_and_prefetch`] after determining which shards
//! to probe.  The method increments per-shard query counters and asynchronously
//! warms any shard whose cumulative query count exceeds the configured
//! `frequency_threshold`.  Warming is done inline on the calling thread; it is
//! expected to be called from a thread-pool context in production.

use std::{
    collections::HashMap,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

use tracing::{debug, trace};

use shardlake_core::types::ShardId;
use shardlake_storage::StorageBackend;

use crate::{shard::ShardIndex, IndexError, Result};

// ── Cache metrics ────────────────────────────────────────────────────────────

/// Snapshot of [`ShardCache`] performance statistics.
///
/// Obtained via [`ShardCache::metrics`].
#[derive(Debug, Clone)]
pub struct CacheMetrics {
    /// Number of cache lookups that found a resident shard (no I/O needed).
    pub hits: u64,
    /// Number of cache lookups that required loading from storage.
    pub misses: u64,
    /// Fraction of lookups served from cache: `hits / (hits + misses)`.
    /// Returns `0.0` when no lookups have been made.
    pub hit_rate: f64,
    /// Average time to load a shard from storage, in milliseconds.
    /// Returns `0.0` when no shards have been loaded.
    pub avg_load_latency_ms: f64,
    /// Estimated heap memory occupied by all cached shard indexes, in bytes.
    pub memory_bytes: usize,
    /// Number of shard indexes currently resident in the cache.
    pub cached_shards: usize,
}

// ── Cache configuration ───────────────────────────────────────────────────────

/// Configuration for [`ShardCache`].
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum number of shard indexes to keep in memory at once.
    ///
    /// When the cache is full the *least recently used* shard is evicted.
    pub capacity: usize,
    /// File-size threshold (bytes) above which memory-mapped I/O is used
    /// instead of reading the whole file into a heap-allocated `Vec<u8>`.
    ///
    /// Memory mapping is only attempted when the underlying backend returns a
    /// local filesystem path from [`StorageBackend::path_for_key`].  Set to
    /// `usize::MAX` to disable mmap entirely.
    pub mmap_threshold_bytes: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            capacity: 64,
            mmap_threshold_bytes: 64 * 1024 * 1024, // 64 MiB
        }
    }
}

// ── Internal LRU bookkeeping ──────────────────────────────────────────────────

/// Bounded LRU map using a generation counter.
///
/// Each entry stores an `Arc<ShardIndex>` together with the generation at which
/// it was last accessed.  A cache hit is O(1) (single HashMap lookup +
/// generation update).  Eviction only occurs on insert when the cache is full
/// and requires a linear scan to find the minimum-generation entry, but eviction
/// is rare relative to hits once the working set stabilises.
struct LruInner {
    capacity: usize,
    map: HashMap<ShardId, (Arc<ShardIndex>, u64)>,
    /// Monotonically increasing access counter.
    generation: u64,
}

impl LruInner {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "cache capacity must be > 0");
        Self {
            capacity,
            map: HashMap::new(),
            generation: 0,
        }
    }

    /// Look up `id`; returns `Some` and updates the access generation on hit.
    fn get(&mut self, id: ShardId) -> Option<Arc<ShardIndex>> {
        if let Some(entry) = self.map.get_mut(&id) {
            self.generation += 1;
            entry.1 = self.generation;
            Some(Arc::clone(&entry.0))
        } else {
            None
        }
    }

    /// Insert or replace `id`.  Evicts the LRU entry (lowest generation) when
    /// the map is at capacity.
    fn insert(&mut self, id: ShardId, shard: Arc<ShardIndex>) {
        self.generation += 1;
        if !self.map.contains_key(&id) && self.map.len() >= self.capacity {
            // Evict the entry with the smallest (oldest) generation.
            if let Some((&evict_id, _)) = self.map.iter().min_by_key(|(_, (_, gen))| gen) {
                self.map.remove(&evict_id);
                debug!(shard_id = evict_id.0, "evicted shard from cache");
            }
        }
        self.map.insert(id, (shard, self.generation));
    }

    /// Sum of [`ShardIndex::memory_bytes`] across all resident shards.
    fn memory_bytes(&self) -> usize {
        self.map.values().map(|(s, _)| s.memory_bytes()).sum()
    }

    fn len(&self) -> usize {
        self.map.len()
    }
}

// ── ShardCache ────────────────────────────────────────────────────────────────

/// LRU in-memory cache for shard indexes.
///
/// Loaded shard indexes are kept in a bounded LRU cache backed by a
/// `Mutex<`[`LruInner`]`>`.  Hit/miss counters and load-latency accumulators
/// are updated with relaxed atomics so that reads never block writers.
///
/// # Memory-mapped loading
///
/// If the storage backend returns a local filesystem path for an artifact key
/// (via [`StorageBackend::path_for_key`]) **and** the file size exceeds
/// [`CacheConfig::mmap_threshold_bytes`], the shard is deserialized directly
/// from a read-only memory mapping rather than a heap buffer.  The mapping is
/// created, used, and dropped within the load call, so no long-lived unsafe
/// region is kept.
pub struct ShardCache {
    config: CacheConfig,
    inner: Mutex<LruInner>,
    hits: AtomicU64,
    misses: AtomicU64,
    total_loads: AtomicU64,
    total_load_nanos: AtomicU64,
    /// Per-shard query-frequency counter, used by the prefetch policy.
    query_counts: Mutex<HashMap<ShardId, u64>>,
}

impl ShardCache {
    /// Create a new cache with the given [`CacheConfig`].
    pub fn new(config: CacheConfig) -> Self {
        Self {
            inner: Mutex::new(LruInner::new(config.capacity)),
            config,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            total_loads: AtomicU64::new(0),
            total_load_nanos: AtomicU64::new(0),
            query_counts: Mutex::new(HashMap::new()),
        }
    }

    /// Return a cached shard, loading from `store` on a miss.
    ///
    /// The returned `Arc<ShardIndex>` remains valid even if the cache later
    /// evicts the entry; the shard data is freed only when all `Arc`s drop.
    pub fn get_or_load(
        &self,
        shard_id: ShardId,
        artifact_key: &str,
        store: &dyn StorageBackend,
    ) -> Result<Arc<ShardIndex>> {
        // Fast path: cache hit.
        {
            let mut inner = self.inner.lock().unwrap();
            if let Some(shard) = inner.get(shard_id) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                trace!(shard_id = shard_id.0, "cache hit");
                return Ok(shard);
            }
        }

        // Slow path: load from store.
        self.misses.fetch_add(1, Ordering::Relaxed);
        trace!(
            shard_id = shard_id.0,
            key = artifact_key,
            "cache miss – loading"
        );

        let t0 = Instant::now();
        let index = self.load_from_store(artifact_key, store)?;
        let elapsed_nanos = t0.elapsed().as_nanos() as u64;

        self.total_loads.fetch_add(1, Ordering::Relaxed);
        self.total_load_nanos
            .fetch_add(elapsed_nanos, Ordering::Relaxed);

        let arc = Arc::new(index);
        self.inner
            .lock()
            .unwrap()
            .insert(shard_id, Arc::clone(&arc));
        Ok(arc)
    }

    /// Deserialize a shard from `store`, using mmap when possible.
    fn load_from_store(
        &self,
        artifact_key: &str,
        store: &dyn StorageBackend,
    ) -> Result<ShardIndex> {
        if let Some(path) = store.path_for_key(artifact_key) {
            if let Ok(meta) = std::fs::metadata(&path) {
                if meta.len() as usize >= self.config.mmap_threshold_bytes {
                    debug!(
                        key = artifact_key,
                        size_bytes = meta.len(),
                        "loading shard via mmap"
                    );
                    return Self::load_mmap(&path);
                }
            }
        }
        let bytes = store.get(artifact_key)?;
        ShardIndex::from_bytes(&bytes)
    }

    /// Deserialize a shard from a memory-mapped file.
    ///
    /// # Safety
    ///
    /// The file is opened read-only.  The mapping is created, consumed by
    /// [`ShardIndex::from_bytes`] (which copies all data into owned Vecs), and
    /// dropped before this function returns.  No reference into the mapped
    /// region escapes this call frame, so there is no dangling-pointer risk
    /// even if the file is modified on disk after the call completes.
    fn load_mmap(path: &Path) -> Result<ShardIndex> {
        let file = std::fs::File::open(path).map_err(IndexError::Io)?;
        // SAFETY: see doc comment above.
        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(IndexError::Io)?;
        ShardIndex::from_bytes(&mmap)
    }

    /// Return a snapshot of the current cache statistics.
    pub fn metrics(&self) -> CacheMetrics {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        };
        let loads = self.total_loads.load(Ordering::Relaxed);
        let total_nanos = self.total_load_nanos.load(Ordering::Relaxed);
        let avg_load_latency_ms = if loads == 0 {
            0.0
        } else {
            (total_nanos as f64 / loads as f64) / 1_000_000.0
        };
        let inner = self.inner.lock().unwrap();
        CacheMetrics {
            hits,
            misses,
            hit_rate,
            avg_load_latency_ms,
            memory_bytes: inner.memory_bytes(),
            cached_shards: inner.len(),
        }
    }

    /// Record that `queried_shards` were probed and optionally warm "hot" shards.
    ///
    /// Each shard ID in `queried_shards` has its query counter incremented.
    /// Any shard whose cumulative count reaches or exceeds `frequency_threshold`
    /// is then pre-loaded into the cache (if not already resident), so that
    /// future probes are served from RAM rather than storage.
    ///
    /// `all_shard_defs` provides the `(shard_id, artifact_key)` pairs needed to
    /// locate each shard in the store.
    pub fn record_query_and_prefetch(
        &self,
        queried_shards: &[ShardId],
        all_shard_defs: &[(ShardId, &str)],
        store: &dyn StorageBackend,
        frequency_threshold: u64,
    ) {
        // Build a key-lookup map to avoid a nested O(shards²) loop.
        let key_map: HashMap<ShardId, &str> =
            all_shard_defs.iter().map(|&(id, k)| (id, k)).collect();

        // Increment query counters and collect newly-hot shards.
        let hot: Vec<(ShardId, String)> = {
            let mut counts = self.query_counts.lock().unwrap();
            for &id in queried_shards {
                *counts.entry(id).or_insert(0) += 1;
            }
            // Only inspect shards that have been queried at least once.
            counts
                .iter()
                .filter_map(|(&id, &count)| {
                    if count >= frequency_threshold {
                        key_map.get(&id).map(|&k| (id, k.to_owned()))
                    } else {
                        None
                    }
                })
                .collect()
        };

        // Warm the hot shards (skipped if already cached).
        for (id, key) in hot {
            if let Err(e) = self.get_or_load(id, &key, store) {
                debug!(shard_id = id.0, err = %e, "prefetch failed – ignoring");
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use shardlake_core::types::{VectorId, VectorRecord};
    use shardlake_storage::{LocalObjectStore, ObjectStore};

    use crate::shard::{ShardIndex, SHARD_MAGIC};

    fn make_shard(id: u32) -> ShardIndex {
        ShardIndex {
            shard_id: ShardId(id),
            dims: 2,
            centroids: vec![vec![1.0, 0.0]],
            records: vec![VectorRecord {
                id: VectorId(id as u64),
                data: vec![1.0, 0.0],
                metadata: None,
            }],
        }
    }

    fn store_shard(store: &LocalObjectStore, shard: &ShardIndex) -> String {
        let key = format!("shards/{:04}.sidx", shard.shard_id.0);
        let bytes = shard.to_bytes().unwrap();
        store.put(&key, bytes).unwrap();
        key
    }

    fn make_store() -> (tempfile::TempDir, LocalObjectStore) {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        (tmp, store)
    }

    #[test]
    fn hit_and_miss_counted() {
        let (_tmp, store) = make_store();
        let shard = make_shard(0);
        let key = store_shard(&store, &shard);

        let cache = ShardCache::new(CacheConfig::default());
        // First access: miss.
        let _ = cache.get_or_load(ShardId(0), &key, &store).unwrap();
        // Second access: hit.
        let _ = cache.get_or_load(ShardId(0), &key, &store).unwrap();

        let m = cache.metrics();
        assert_eq!(m.hits, 1);
        assert_eq!(m.misses, 1);
        assert!((m.hit_rate - 0.5).abs() < f64::EPSILON);
        assert!(m.avg_load_latency_ms >= 0.0);
        assert_eq!(m.cached_shards, 1);
        assert!(m.memory_bytes > 0);
    }

    #[test]
    fn lru_eviction() {
        let (_tmp, store) = make_store();
        let shard0 = make_shard(0);
        let shard1 = make_shard(1);
        let shard2 = make_shard(2);
        let key0 = store_shard(&store, &shard0);
        let key1 = store_shard(&store, &shard1);
        let key2 = store_shard(&store, &shard2);

        let cache = ShardCache::new(CacheConfig {
            capacity: 2,
            mmap_threshold_bytes: usize::MAX, // disable mmap
        });

        cache.get_or_load(ShardId(0), &key0, &store).unwrap();
        cache.get_or_load(ShardId(1), &key1, &store).unwrap();
        // Promote shard 0 (so shard 1 becomes LRU).
        cache.get_or_load(ShardId(0), &key0, &store).unwrap();
        // Insert shard 2 → shard 1 should be evicted.
        cache.get_or_load(ShardId(2), &key2, &store).unwrap();

        assert_eq!(cache.metrics().cached_shards, 2);
        // Shard 1 must now be a miss again.
        let m_before = cache.metrics().misses;
        cache.get_or_load(ShardId(1), &key1, &store).unwrap();
        let m_after = cache.metrics().misses;
        assert_eq!(m_after, m_before + 1, "shard 1 should have been evicted");
    }

    #[test]
    fn prefetch_warms_hot_shard() {
        let (_tmp, store) = make_store();
        let shard = make_shard(7);
        let key = store_shard(&store, &shard);

        let cache = ShardCache::new(CacheConfig::default());
        let defs = vec![(ShardId(7), key.as_str())];

        // Query twice (threshold = 2) – should trigger prefetch on second call.
        cache.record_query_and_prefetch(&[ShardId(7)], &defs, &store, 2);
        cache.record_query_and_prefetch(&[ShardId(7)], &defs, &store, 2);

        // After prefetch the shard should be resident.
        assert_eq!(cache.metrics().cached_shards, 1);
        // Next explicit get should be a hit.
        let hits_before = cache.metrics().hits;
        cache.get_or_load(ShardId(7), &key, &store).unwrap();
        assert_eq!(cache.metrics().hits, hits_before + 1);
    }

    #[test]
    fn magic_bytes_visible_to_test() {
        // Smoke test that SHARD_MAGIC is accessible.
        assert_eq!(SHARD_MAGIC, b"SLKIDX\0\0");
    }
}
