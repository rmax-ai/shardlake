//! Bounded LRU shard index cache.
//!
//! Provides [`ShardCache`], a thread-safe bounded LRU cache for shard index
//! data, replacing bespoke per-searcher `Mutex<HashMap>` caches.  On a cache
//! miss the caller supplies a load closure that fetches the shard from
//! storage; the result is inserted and returned.  Hit and miss counts are
//! tracked atomically.

use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use shardlake_core::types::ShardId;

use crate::{IndexError, Result};

/// Default maximum number of shard indexes to retain in a [`ShardCache`].
///
/// Callers that need a different limit should construct the cache with an
/// explicit capacity, for example by reading
/// [`shardlake_core::config::SystemConfig::shard_cache_capacity`].
pub const DEFAULT_SHARD_CACHE_CAPACITY: usize = 128;

// ── internal LRU ─────────────────────────────────────────────────────────────

/// A bounded LRU eviction cache backed by a `HashMap` and a `VecDeque`.
///
/// The `VecDeque` acts as an ordered access log: the front holds the
/// least-recently-used key and the back holds the most-recently-used key.
/// Both `get` and `insert` promote the accessed key to MRU position.
///
/// Promotion is O(n) because it scans the `VecDeque` for the key's current
/// position.  This is acceptable for the expected cache sizes (≤ a few hundred
/// shard entries); the simplicity is preferred over additional heap allocations
/// or unsafe code that a doubly-linked-list approach would require.
struct LruCache<K, V> {
    capacity: usize,
    map: HashMap<K, V>,
    /// Access order: `order[0]` = LRU (next to be evicted),
    /// `order[len-1]` = MRU (most recently used).
    order: VecDeque<K>,
}

impl<K: Eq + Hash + Clone, V: Clone> LruCache<K, V> {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "LruCache capacity must be at least 1");
        Self {
            capacity,
            // Pre-allocate one extra slot to avoid reallocation on the
            // common insert-then-evict path.
            map: HashMap::with_capacity(capacity + 1),
            order: VecDeque::with_capacity(capacity + 1),
        }
    }

    /// Return a clone of the value associated with `key`, promoting it to the
    /// MRU position.  Returns `None` if `key` is not present.
    fn get(&mut self, key: &K) -> Option<V> {
        if !self.map.contains_key(key) {
            return None;
        }
        // Promote to MRU: remove from current position and push to back.
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
        }
        self.order.push_back(key.clone());
        self.map.get(key).cloned()
    }

    /// Insert `value` under `key`, evicting the LRU entry if the cache is at
    /// capacity.  Updating an existing key promotes it to MRU position.
    fn insert(&mut self, key: K, value: V) {
        if self.map.contains_key(&key) {
            // Update in-place and promote to MRU.
            if let Some(pos) = self.order.iter().position(|k| k == &key) {
                self.order.remove(pos);
            }
            self.order.push_back(key.clone());
            self.map.insert(key, value);
            return;
        }
        // Evict the LRU entry when the cache is full.
        if self.map.len() >= self.capacity {
            if let Some(lru_key) = self.order.pop_front() {
                self.map.remove(&lru_key);
            }
        }
        self.order.push_back(key.clone());
        self.map.insert(key, value);
    }

    fn len(&self) -> usize {
        self.map.len()
    }
}

// ── ShardCache ────────────────────────────────────────────────────────────────

/// Thread-safe, bounded LRU cache for shard index data.
///
/// `ShardCache<V>` stores values of type `Arc<V>` keyed by [`ShardId`].
/// On a cache miss the caller-provided load closure is invoked to fetch the
/// shard from storage; on success the result is inserted and returned.
///
/// # Eviction
///
/// When the number of cached entries reaches the configured `capacity`, the
/// least-recently-used entry is evicted before the new entry is inserted.
/// An entry's "use" time is updated on every successful
/// [`get_or_load`](ShardCache::get_or_load) call for that shard.
///
/// # Bookkeeping
///
/// Cumulative hit and miss counts are available via [`ShardCache::hits`] and
/// [`ShardCache::misses`].  Both counters are incremented atomically, making
/// them safe to read from any thread without acquiring the inner lock.
///
/// # Capacity
///
/// Construct with [`DEFAULT_SHARD_CACHE_CAPACITY`] or with the value from
/// [`shardlake_core::config::SystemConfig::shard_cache_capacity`] to respect
/// the operator's runtime configuration.
pub struct ShardCache<V> {
    inner: Mutex<LruCache<ShardId, Arc<V>>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl<V: Send + 'static> ShardCache<V> {
    /// Create a cache that holds at most `capacity` entries.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is `0`.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(LruCache::new(capacity)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Return the cached `Arc<V>` for `shard_id`, or load and cache it.
    ///
    /// On a **hit** the cached value is returned immediately and
    /// [`hits`](ShardCache::hits) is incremented.  On a **miss** `load` is
    /// called outside the lock, the result is inserted,
    /// [`misses`](ShardCache::misses) is incremented, and the value is
    /// returned.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError`] if the internal mutex is poisoned.  Any error
    /// returned by `load` is forwarded unchanged.
    pub fn get_or_load<F>(&self, shard_id: ShardId, load: F) -> Result<Arc<V>>
    where
        F: FnOnce() -> Result<Arc<V>>,
    {
        // Fast path: check the cache under the lock.
        {
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| IndexError::Other("shard cache lock poisoned".into()))?;
            if let Some(cached) = guard.get(&shard_id) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(cached);
            }
        }

        // Slow path: cache miss – load the value outside the lock.
        self.misses.fetch_add(1, Ordering::Relaxed);
        let value = load()?;

        // Re-acquire the lock and insert the loaded value.
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| IndexError::Other("shard cache lock poisoned".into()))?;
        guard.insert(shard_id, Arc::clone(&value));
        Ok(value)
    }

    /// Return the cumulative number of cache hits.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Return the cumulative number of cache misses.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Return the number of entries currently held in the cache.
    pub fn len(&self) -> usize {
        self.inner.lock().map(|g| g.len()).unwrap_or(0)
    }

    /// Return `true` if the cache contains no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return a snapshot of all currently cached values.
    ///
    /// Each `Arc<V>` in the returned `Vec` is cheaply cloned from the cache
    /// under the lock; the lock is released before this method returns.
    /// This is useful for opportunistic iteration (e.g. reranking from cached
    /// shard data) without requiring the caller to hold the lock.
    pub fn cached_values(&self) -> Vec<Arc<V>> {
        self.inner
            .lock()
            .map(|g| g.map.values().cloned().collect())
            .unwrap_or_default()
    }
}

// ─────────────────────────── tests ───────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    // ── LruCache ──────────────────────────────────────────────────────────────

    #[test]
    fn lru_cache_get_miss_on_empty() {
        let mut cache: LruCache<u32, u32> = LruCache::new(4);
        assert!(cache.get(&0).is_none());
    }

    #[test]
    fn lru_cache_insert_and_get() {
        let mut cache: LruCache<u32, u32> = LruCache::new(4);
        cache.insert(1, 100);
        cache.insert(2, 200);
        assert_eq!(cache.get(&1), Some(100));
        assert_eq!(cache.get(&2), Some(200));
        assert!(cache.get(&3).is_none());
    }

    #[test]
    fn lru_cache_evicts_lru_when_full() {
        let mut cache: LruCache<u32, u32> = LruCache::new(2);
        cache.insert(1, 10);
        cache.insert(2, 20);
        // Access key 1 → key 1 is MRU, key 2 becomes LRU.
        cache.get(&1);
        // Insert key 3: key 2 (LRU) should be evicted.
        cache.insert(3, 30);
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&2).is_none(), "key 2 should have been evicted");
        assert_eq!(cache.get(&1), Some(10));
        assert_eq!(cache.get(&3), Some(30));
    }

    #[test]
    fn lru_cache_update_promotes_existing_to_mru() {
        let mut cache: LruCache<u32, u32> = LruCache::new(2);
        cache.insert(1, 10);
        cache.insert(2, 20);
        // Re-insert key 1 → key 1 becomes MRU, key 2 becomes LRU.
        cache.insert(1, 11);
        // Insert key 3 → key 2 (LRU) should be evicted.
        cache.insert(3, 30);
        assert!(cache.get(&2).is_none(), "key 2 should have been evicted");
        assert_eq!(cache.get(&1), Some(11));
        assert_eq!(cache.get(&3), Some(30));
    }

    #[test]
    fn lru_cache_capacity_one() {
        let mut cache: LruCache<u32, u32> = LruCache::new(1);
        cache.insert(1, 10);
        assert_eq!(cache.get(&1), Some(10));
        cache.insert(2, 20);
        // Key 1 should now be evicted.
        assert!(cache.get(&1).is_none());
        assert_eq!(cache.get(&2), Some(20));
    }

    // ── ShardCache ────────────────────────────────────────────────────────────

    #[test]
    fn shard_cache_miss_invokes_load() {
        let cache: ShardCache<u64> = ShardCache::new(4);
        let load_count = AtomicUsize::new(0);

        let result = cache.get_or_load(ShardId(1), || {
            load_count.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(42u64))
        });

        assert_eq!(*result.unwrap(), 42);
        assert_eq!(load_count.load(Ordering::SeqCst), 1);
        assert_eq!(cache.misses(), 1);
        assert_eq!(cache.hits(), 0);
    }

    #[test]
    fn shard_cache_hit_returns_cached_value_without_reload() {
        let cache: ShardCache<u64> = ShardCache::new(4);
        let load_count = AtomicUsize::new(0);

        // First access: miss → loads 42.
        cache
            .get_or_load(ShardId(1), || {
                load_count.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(42u64))
            })
            .unwrap();

        // Second access: hit → should not call load (which would return 99).
        let result = cache
            .get_or_load(ShardId(1), || {
                load_count.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(99u64))
            })
            .unwrap();

        assert_eq!(*result, 42, "cached value should be returned on hit");
        assert_eq!(
            load_count.load(Ordering::SeqCst),
            1,
            "load should be called exactly once"
        );
        assert_eq!(cache.hits(), 1);
        assert_eq!(cache.misses(), 1);
    }

    #[test]
    fn shard_cache_evicts_lru_entry() {
        let cache: ShardCache<u64> = ShardCache::new(2);
        let load_count = AtomicUsize::new(0);

        // Load shards 0 and 1 to fill the cache.
        cache
            .get_or_load(ShardId(0), || {
                load_count.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(0u64))
            })
            .unwrap();
        cache
            .get_or_load(ShardId(1), || {
                load_count.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(1u64))
            })
            .unwrap();

        // Access shard 0 again → shard 0 is MRU, shard 1 becomes LRU.
        cache
            .get_or_load(ShardId(0), || Ok(Arc::new(99u64)))
            .unwrap();

        // Insert shard 2 → shard 1 (LRU) should be evicted.
        cache
            .get_or_load(ShardId(2), || {
                load_count.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(2u64))
            })
            .unwrap();

        assert_eq!(cache.len(), 2);

        // Accessing shard 1 now should trigger a reload (it was evicted).
        let before = load_count.load(Ordering::SeqCst);
        cache
            .get_or_load(ShardId(1), || {
                load_count.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(1u64))
            })
            .unwrap();
        let after = load_count.load(Ordering::SeqCst);
        assert_eq!(
            after - before,
            1,
            "shard 1 should be reloaded after eviction"
        );
    }

    #[test]
    fn shard_cache_is_empty_after_construction() {
        let cache: ShardCache<u64> = ShardCache::new(8);
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.hits(), 0);
        assert_eq!(cache.misses(), 0);
    }

    #[test]
    fn shard_cache_load_error_is_forwarded() {
        let cache: ShardCache<u64> = ShardCache::new(4);
        let result: Result<Arc<u64>> =
            cache.get_or_load(ShardId(0), || Err(IndexError::Other("load failed".into())));
        assert!(result.is_err());
        // The failed load should still count as a miss (the attempt was made).
        assert_eq!(cache.misses(), 1);
        assert_eq!(cache.hits(), 0);
        // Nothing should be in the cache.
        assert!(cache.is_empty());
    }
}
