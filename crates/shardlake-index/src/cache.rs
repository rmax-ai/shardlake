//! Bounded in-memory shard cache with LFU eviction and access-frequency
//! tracking.
//!
//! [`ShardCache`] stores deserialized [`ShardIndex`] objects keyed by their
//! [`ShardId`] and tracks how many times each shard has been accessed.  When a
//! capacity limit is configured, inserting a new entry evicts the
//! least-frequently-accessed shard that is currently in the cache (LFU policy).
//!
//! The access counter is incremented every time [`ShardCache::record_access`]
//! is called, regardless of whether the shard is in the cache at that moment.
//! This means the frequency reflects *demand*, not *cache residency*, making
//! it suitable for driving prefetch decisions.

use std::collections::HashMap;
use std::sync::Arc;

use tracing::debug;

use shardlake_core::types::ShardId;

use crate::shard::ShardIndex;

/// In-memory shard cache with access-frequency tracking and optional capacity.
///
/// # Eviction
///
/// When `capacity > 0` and the cache is at capacity, inserting a new shard
/// evicts the cached entry with the lowest access count.  If several entries
/// share the same (minimum) count the one that happens to be returned first by
/// the underlying hash-map iterator is chosen; the choice is deterministic
/// within a single process run but unspecified across runs.
///
/// # Access counting
///
/// Call [`record_access`](Self::record_access) to increment the counter for a
/// shard before consulting the cache so that counts accumulate for both hits
/// and misses.  The count is never decremented and persists even after a shard
/// is evicted; this ensures that a frequently-used shard that was evicted is
/// still recognised as "hot" and re-warmed by the prefetch policy.
pub struct ShardCache {
    entries: HashMap<ShardId, Arc<ShardIndex>>,
    access_counts: HashMap<ShardId, u64>,
    capacity: usize,
}

impl ShardCache {
    /// Create a new cache with the given capacity.
    ///
    /// `capacity` is the maximum number of shards that may be held
    /// concurrently.  A value of `0` means no limit.
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            access_counts: HashMap::new(),
            capacity,
        }
    }

    /// Return a shared reference to the cached shard, or `None` if not
    /// present.
    pub fn get(&self, shard_id: ShardId) -> Option<Arc<ShardIndex>> {
        self.entries.get(&shard_id).map(Arc::clone)
    }

    /// Return `true` if `shard_id` is currently held in the cache.
    pub fn contains(&self, shard_id: ShardId) -> bool {
        self.entries.contains_key(&shard_id)
    }

    /// Increment the access counter for `shard_id`.
    ///
    /// Should be called at the beginning of every load attempt, before
    /// checking whether the shard is already cached.
    pub fn record_access(&mut self, shard_id: ShardId) {
        *self.access_counts.entry(shard_id).or_insert(0) += 1;
    }

    /// Return the cumulative access count for `shard_id`.
    ///
    /// Returns `0` for shards that have never been accessed via
    /// [`record_access`](Self::record_access).
    pub fn access_count(&self, shard_id: ShardId) -> u64 {
        self.access_counts.get(&shard_id).copied().unwrap_or(0)
    }

    /// Insert `shard` into the cache under `shard_id`.
    ///
    /// If the shard is already present the stored entry is replaced without
    /// triggering eviction.  If the cache is at capacity the entry with the
    /// lowest access count is evicted first.
    pub fn insert(&mut self, shard_id: ShardId, shard: Arc<ShardIndex>) {
        use std::collections::hash_map::Entry;

        // Update-in-place: no eviction needed.
        if let Entry::Occupied(mut e) = self.entries.entry(shard_id) {
            e.insert(shard);
            return;
        }

        // Evict the least-frequently-used shard if the cache is at capacity.
        if self.capacity > 0 && self.entries.len() == self.capacity {
            if let Some(evict_id) = self
                .entries
                .keys()
                .min_by_key(|&id| self.access_counts.get(id).copied().unwrap_or(0))
                .copied()
            {
                self.entries.remove(&evict_id);
                debug!(shard_id = ?evict_id, "evicted cold shard from cache");
            }
        }

        self.entries.insert(shard_id, shard);
    }

    /// Number of shards currently held in the cache.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return `true` if no shards are currently held in the cache.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate over the cached shard values.
    pub fn values(&self) -> impl Iterator<Item = &Arc<ShardIndex>> {
        self.entries.values()
    }

    /// Return the maximum capacity of this cache (`0` = unlimited).
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::ShardIndex;

    fn dummy_shard() -> Arc<ShardIndex> {
        Arc::new(ShardIndex {
            shard_id: ShardId(0),
            dims: 0,
            centroids: vec![],
            records: vec![],
        })
    }

    #[test]
    fn unbounded_cache_holds_all_inserts() {
        let mut cache = ShardCache::new(0);
        for i in 0..10_u32 {
            cache.insert(ShardId(i), dummy_shard());
        }
        assert_eq!(cache.len(), 10);
    }

    #[test]
    fn bounded_cache_evicts_on_capacity() {
        let mut cache = ShardCache::new(2);
        cache.record_access(ShardId(0));
        cache.record_access(ShardId(0)); // count=2
        cache.record_access(ShardId(1)); // count=1
        cache.insert(ShardId(0), dummy_shard());
        cache.insert(ShardId(1), dummy_shard());

        assert_eq!(cache.len(), 2);

        // Inserting shard 2 must evict the coldest entry (shard 1, count=1).
        cache.record_access(ShardId(2));
        cache.insert(ShardId(2), dummy_shard());

        assert_eq!(cache.len(), 2);
        assert!(cache.contains(ShardId(0)), "hot shard must be retained");
        assert!(!cache.contains(ShardId(1)), "cold shard must be evicted");
        assert!(
            cache.contains(ShardId(2)),
            "newly inserted shard must be present"
        );
    }

    #[test]
    fn access_count_persists_after_eviction() {
        let mut cache = ShardCache::new(1);
        cache.record_access(ShardId(0));
        cache.record_access(ShardId(0));
        cache.insert(ShardId(0), dummy_shard());

        // Evict shard 0 by inserting shard 1.
        cache.record_access(ShardId(1));
        cache.insert(ShardId(1), dummy_shard());

        assert!(!cache.contains(ShardId(0)));
        assert_eq!(
            cache.access_count(ShardId(0)),
            2,
            "count must survive eviction"
        );
    }

    #[test]
    fn in_place_update_does_not_trigger_eviction() {
        let mut cache = ShardCache::new(2);
        cache.insert(ShardId(0), dummy_shard());
        cache.insert(ShardId(1), dummy_shard());
        // Re-inserting an existing entry must not evict anything.
        cache.insert(ShardId(0), dummy_shard());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn record_access_increments_count() {
        let mut cache = ShardCache::new(0);
        assert_eq!(cache.access_count(ShardId(7)), 0);
        cache.record_access(ShardId(7));
        assert_eq!(cache.access_count(ShardId(7)), 1);
        cache.record_access(ShardId(7));
        assert_eq!(cache.access_count(ShardId(7)), 2);
    }
}
