//! Cache observability metrics for the shard loading pipeline.
//!
//! [`CacheMetrics`] exposes atomic counters that track cache hit/miss events,
//! shard-load latency, and the number of bytes retained in-cache.  A single
//! instance can be shared across threads via [`std::sync::Arc`].
//!
//! Call [`CacheMetrics::snapshot`] at any time to get a best-effort
//! [`CacheMetricsSnapshot`] with derived statistics such as [`hit_rate`] and
//! [`mean_load_latency_ns`].
//!
//! [`hit_rate`]: CacheMetricsSnapshot::hit_rate
//! [`mean_load_latency_ns`]: CacheMetricsSnapshot::mean_load_latency_ns

use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic counters tracking cache and shard-load behaviour.
///
/// All counter updates use [`Ordering::Relaxed`] — the counters are
/// observability aids, not synchronisation primitives.
///
/// # Examples
///
/// ```rust
/// use std::sync::Arc;
/// use shardlake_index::metrics::CacheMetrics;
///
/// let m = Arc::new(CacheMetrics::new());
/// m.record_hit();
/// m.record_miss();
/// m.record_load_attempt(1_500_000);
/// m.record_retained_bytes(65536);
///
/// let snap = m.snapshot();
/// assert_eq!(snap.hits, 1);
/// assert_eq!(snap.misses, 1);
/// assert_eq!(snap.total_load_count, 1);
/// assert_eq!(snap.total_load_latency_ns, 1_500_000);
/// assert_eq!(snap.retained_bytes, 65536);
/// ```
pub struct CacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    total_load_count: AtomicU64,
    total_load_latency_ns: AtomicU64,
    retained_bytes: AtomicU64,
}

impl std::fmt::Debug for CacheMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheMetrics")
            .field("hits", &self.hits.load(Ordering::Relaxed))
            .field("misses", &self.misses.load(Ordering::Relaxed))
            .field(
                "total_load_count",
                &self.total_load_count.load(Ordering::Relaxed),
            )
            .field(
                "total_load_latency_ns",
                &self.total_load_latency_ns.load(Ordering::Relaxed),
            )
            .field(
                "retained_bytes",
                &self.retained_bytes.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheMetrics {
    /// Create a new, zero-initialised set of metrics.
    pub fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            total_load_count: AtomicU64::new(0),
            total_load_latency_ns: AtomicU64::new(0),
            retained_bytes: AtomicU64::new(0),
        }
    }

    /// Record one cache-hit event (shard was found in cache).
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one cache-miss event (shard was not in cache and must be loaded).
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one storage fetch attempt after a cache miss.
    ///
    /// `latency_ns` is the wall-clock duration of the storage fetch in
    /// nanoseconds. This counter is incremented even when later decode or cache
    /// insertion steps fail, as long as the fetch was attempted.
    pub fn record_load_attempt(&self, latency_ns: u64) {
        self.total_load_count.fetch_add(1, Ordering::Relaxed);
        self.total_load_latency_ns
            .fetch_add(latency_ns, Ordering::Relaxed);
    }

    /// Record the raw bytes successfully retained by the cache.
    ///
    /// Call this only after the shard has been inserted into the cache.
    pub fn record_retained_bytes(&self, bytes: u64) {
        self.retained_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Return a best-effort point-in-time snapshot of all counters.
    ///
    /// Each counter is read independently with [`Ordering::Relaxed`], so the
    /// snapshot may transiently combine values from slightly different moments
    /// when other threads are updating the metrics concurrently.
    pub fn snapshot(&self) -> CacheMetricsSnapshot {
        CacheMetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            total_load_count: self.total_load_count.load(Ordering::Relaxed),
            total_load_latency_ns: self.total_load_latency_ns.load(Ordering::Relaxed),
            retained_bytes: self.retained_bytes.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time snapshot of [`CacheMetrics`] counters.
///
/// All values are monotonically increasing totals since the metrics object was
/// created. Use the convenience methods [`hit_rate`] and
/// [`mean_load_latency_ns`] for derived statistics.
///
/// [`hit_rate`]: CacheMetricsSnapshot::hit_rate
/// [`mean_load_latency_ns`]: CacheMetricsSnapshot::mean_load_latency_ns
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheMetricsSnapshot {
    /// Total number of cache-hit events.
    pub hits: u64,
    /// Total number of cache-miss events.
    pub misses: u64,
    /// Total number of storage fetch attempts after a cache miss.
    pub total_load_count: u64,
    /// Cumulative storage-fetch wall-clock time in nanoseconds.
    pub total_load_latency_ns: u64,
    /// Total raw bytes successfully inserted into the cache.
    pub retained_bytes: u64,
}

impl CacheMetricsSnapshot {
    /// Cache hit rate in the range `[0.0, 1.0]`.
    ///
    /// Returns `0.0` when no requests have been observed yet.
    #[must_use]
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    /// Mean shard-load latency in nanoseconds.
    ///
    /// Returns `0.0` when no loads have completed yet.
    #[must_use]
    pub fn mean_load_latency_ns(&self) -> f64 {
        if self.total_load_count == 0 {
            0.0
        } else {
            self.total_load_latency_ns as f64 / self.total_load_count as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_snapshot_is_zeroed() {
        let snap = CacheMetrics::new().snapshot();
        assert_eq!(snap.hits, 0);
        assert_eq!(snap.misses, 0);
        assert_eq!(snap.total_load_count, 0);
        assert_eq!(snap.total_load_latency_ns, 0);
        assert_eq!(snap.retained_bytes, 0);
    }

    #[test]
    fn test_hit_rate_no_requests() {
        let snap = CacheMetrics::new().snapshot();
        assert_eq!(snap.hit_rate(), 0.0);
    }

    #[test]
    fn test_hit_rate_all_hits() {
        let m = CacheMetrics::new();
        m.record_hit();
        m.record_hit();
        let snap = m.snapshot();
        assert_eq!(snap.hit_rate(), 1.0);
    }

    #[test]
    fn test_hit_rate_all_misses() {
        let m = CacheMetrics::new();
        m.record_miss();
        m.record_miss();
        let snap = m.snapshot();
        assert_eq!(snap.hit_rate(), 0.0);
    }

    #[test]
    fn test_hit_rate_mixed() {
        let m = CacheMetrics::new();
        m.record_hit();
        m.record_miss();
        let snap = m.snapshot();
        assert!((snap.hit_rate() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_record_load_attempt_and_retained_bytes_accumulate() {
        let m = CacheMetrics::new();
        m.record_load_attempt(1_000);
        m.record_load_attempt(3_000);
        m.record_retained_bytes(512);
        m.record_retained_bytes(1024);
        let snap = m.snapshot();
        assert_eq!(snap.total_load_count, 2);
        assert_eq!(snap.total_load_latency_ns, 4_000);
        assert_eq!(snap.retained_bytes, 1536);
    }

    #[test]
    fn test_mean_load_latency_no_loads() {
        let snap = CacheMetrics::new().snapshot();
        assert_eq!(snap.mean_load_latency_ns(), 0.0);
    }

    #[test]
    fn test_mean_load_latency_single_load() {
        let m = CacheMetrics::new();
        m.record_load_attempt(2_000_000);
        let snap = m.snapshot();
        assert!((snap.mean_load_latency_ns() - 2_000_000.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_default_creates_zeroed_metrics() {
        let m = CacheMetrics::default();
        let snap = m.snapshot();
        assert_eq!(snap.hits, 0);
        assert_eq!(snap.misses, 0);
    }
}
