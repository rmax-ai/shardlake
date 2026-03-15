//! Cache observability metrics for the shard loading pipeline.
//!
//! [`CacheMetrics`] exposes atomic counters that track cache hit/miss events,
//! shard-load latency, and the number of bytes retained in-cache.  A single
//! instance can be shared across threads via [`std::sync::Arc`].
//!
//! Call [`CacheMetrics::snapshot`] at any time to get a consistent
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
/// m.record_load(1_500_000, 65536);
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

    /// Record one completed shard load.
    ///
    /// - `latency_ns` — wall-clock duration of the load in nanoseconds.
    /// - `bytes` — number of raw bytes loaded from storage (used as a proxy
    ///   for the memory retained in cache).
    pub fn record_load(&self, latency_ns: u64, bytes: u64) {
        self.total_load_count.fetch_add(1, Ordering::Relaxed);
        self.total_load_latency_ns
            .fetch_add(latency_ns, Ordering::Relaxed);
        self.retained_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Return a consistent point-in-time snapshot of all counters.
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
/// created (or reset).  Use the convenience methods [`hit_rate`] and
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
    /// Total number of shard-load completions (each miss triggers one load).
    pub total_load_count: u64,
    /// Cumulative shard-load wall-clock time in nanoseconds.
    pub total_load_latency_ns: u64,
    /// Total raw bytes retained in cache (sum of artifact sizes loaded so far).
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
    fn test_record_load_accumulates() {
        let m = CacheMetrics::new();
        m.record_load(1_000, 512);
        m.record_load(3_000, 1024);
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
        m.record_load(2_000_000, 0);
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
