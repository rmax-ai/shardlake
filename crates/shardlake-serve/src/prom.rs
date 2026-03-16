//! Prometheus metrics registry for the serving layer.
//!
//! [`PrometheusMetrics`] owns a private [`Registry`] with all metric
//! descriptors pre-registered. Handlers update the metrics during request
//! processing and call [`PrometheusMetrics::gather`] when the `/metrics`
//! endpoint is scraped to obtain the current Prometheus text format payload.
//!
//! Cache-related counters are refreshed from the live [`CacheMetrics`]
//! snapshot at scrape time, so they always reflect the values accumulated
//! inside [`IndexSearcher`](shardlake_index::IndexSearcher) without any
//! additional per-query overhead.

use std::sync::Arc;

use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, Registry, TextEncoder};
use shardlake_index::CacheMetrics;

/// Prometheus metrics collected by the serving layer.
///
/// Construct once at startup via [`PrometheusMetrics::new`] and store behind
/// an `Arc` in [`AppState`](crate::AppState). All handle fields are cheap to
/// clone and are safe to update from concurrent request handlers.
pub struct PrometheusMetrics {
    registry: Registry,
    /// Histogram of end-to-end query durations in seconds (includes spawn_blocking overhead).
    pub query_duration_seconds: Histogram,
    /// Total number of queries that completed successfully.
    pub queries_total: IntCounter,
    /// Total number of result vectors returned across all successful queries.
    pub query_results_total: IntCounter,
    // ── cache snapshot gauges (refreshed at scrape time) ──────────────────
    shard_cache_hits: IntGauge,
    shard_cache_misses: IntGauge,
    shard_load_count: IntGauge,
    shard_load_latency_ns_total: IntGauge,
    shard_cache_retained_bytes: IntGauge,
    /// Live cache counters sourced from the searcher's internal metrics.
    cache_metrics: Arc<CacheMetrics>,
}

impl PrometheusMetrics {
    /// Create a new metrics set backed by a private [`Registry`].
    ///
    /// `cache_metrics` must be the same [`Arc`] returned by
    /// [`IndexSearcher::cache_metrics`](shardlake_index::IndexSearcher::cache_metrics)
    /// so that cache-related gauge values are read from the live counters at
    /// scrape time.
    ///
    /// # Panics
    ///
    /// Panics if any metric name is invalid or a duplicate is registered (this
    /// cannot happen with the constant names used here).
    pub fn new(cache_metrics: Arc<CacheMetrics>) -> Self {
        let registry = Registry::new();

        let query_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "shardlake_query_duration_seconds",
                "End-to-end query duration in seconds.",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
        )
        .expect("query_duration_seconds metric");

        let queries_total =
            IntCounter::new("shardlake_queries_total", "Total completed query count.")
                .expect("queries_total metric");

        let query_results_total = IntCounter::new(
            "shardlake_query_results_total",
            "Total result vectors returned across all queries.",
        )
        .expect("query_results_total metric");

        let shard_cache_hits = IntGauge::new(
            "shardlake_shard_cache_hits_total",
            "Cumulative raw-shard cache hit count since server start.",
        )
        .expect("shard_cache_hits metric");

        let shard_cache_misses = IntGauge::new(
            "shardlake_shard_cache_misses_total",
            "Cumulative raw-shard cache miss count since server start.",
        )
        .expect("shard_cache_misses metric");

        let shard_load_count = IntGauge::new(
            "shardlake_shard_load_count_total",
            "Cumulative shard load attempt count since server start.",
        )
        .expect("shard_load_count metric");

        let shard_load_latency_ns_total = IntGauge::new(
            "shardlake_shard_load_latency_ns_total",
            "Cumulative shard load wall-clock time in nanoseconds since server start.",
        )
        .expect("shard_load_latency_ns_total metric");

        let shard_cache_retained_bytes = IntGauge::new(
            "shardlake_shard_cache_retained_bytes",
            "Total raw bytes currently retained in the in-process shard cache.",
        )
        .expect("shard_cache_retained_bytes metric");

        registry
            .register(Box::new(query_duration_seconds.clone()))
            .expect("register query_duration_seconds");
        registry
            .register(Box::new(queries_total.clone()))
            .expect("register queries_total");
        registry
            .register(Box::new(query_results_total.clone()))
            .expect("register query_results_total");
        registry
            .register(Box::new(shard_cache_hits.clone()))
            .expect("register shard_cache_hits");
        registry
            .register(Box::new(shard_cache_misses.clone()))
            .expect("register shard_cache_misses");
        registry
            .register(Box::new(shard_load_count.clone()))
            .expect("register shard_load_count");
        registry
            .register(Box::new(shard_load_latency_ns_total.clone()))
            .expect("register shard_load_latency_ns_total");
        registry
            .register(Box::new(shard_cache_retained_bytes.clone()))
            .expect("register shard_cache_retained_bytes");

        Self {
            registry,
            query_duration_seconds,
            queries_total,
            query_results_total,
            shard_cache_hits,
            shard_cache_misses,
            shard_load_count,
            shard_load_latency_ns_total,
            shard_cache_retained_bytes,
            cache_metrics,
        }
    }

    /// Refresh the cache-related gauges from the live snapshot and encode all
    /// registered metrics in Prometheus text exposition format (version 0.0.4).
    ///
    /// This is the body returned by the `GET /metrics` handler.
    pub fn gather(&self) -> String {
        let snap = self.cache_metrics.snapshot();
        self.shard_cache_hits.set(snap.hits as i64);
        self.shard_cache_misses.set(snap.misses as i64);
        self.shard_load_count.set(snap.total_load_count as i64);
        self.shard_load_latency_ns_total
            .set(snap.total_load_latency_ns as i64);
        self.shard_cache_retained_bytes
            .set(snap.retained_bytes as i64);

        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        encoder
            .encode(&self.registry.gather(), &mut buffer)
            .expect("prometheus encode");
        String::from_utf8(buffer).expect("prometheus output is valid UTF-8")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_metrics() -> PrometheusMetrics {
        PrometheusMetrics::new(Arc::new(CacheMetrics::new()))
    }

    #[test]
    fn gather_returns_expected_metric_families() {
        let m = make_metrics();
        let output = m.gather();
        assert!(
            output.contains("shardlake_query_duration_seconds"),
            "missing query_duration_seconds in:\n{output}"
        );
        assert!(
            output.contains("shardlake_queries_total"),
            "missing queries_total in:\n{output}"
        );
        assert!(
            output.contains("shardlake_query_results_total"),
            "missing query_results_total in:\n{output}"
        );
        assert!(
            output.contains("shardlake_shard_cache_hits_total"),
            "missing shard_cache_hits_total in:\n{output}"
        );
        assert!(
            output.contains("shardlake_shard_cache_misses_total"),
            "missing shard_cache_misses_total in:\n{output}"
        );
        assert!(
            output.contains("shardlake_shard_load_count_total"),
            "missing shard_load_count_total in:\n{output}"
        );
        assert!(
            output.contains("shardlake_shard_load_latency_ns_total"),
            "missing shard_load_latency_ns_total in:\n{output}"
        );
        assert!(
            output.contains("shardlake_shard_cache_retained_bytes"),
            "missing shard_cache_retained_bytes in:\n{output}"
        );
    }

    #[test]
    fn gather_reflects_cache_metric_snapshot() {
        let cache = Arc::new(CacheMetrics::new());
        cache.record_hit();
        cache.record_hit();
        cache.record_miss();
        cache.record_load_attempt(1_000_000);

        let m = PrometheusMetrics::new(Arc::clone(&cache));
        let output = m.gather();

        assert!(
            output.contains("shardlake_shard_cache_hits_total 2"),
            "expected hits=2 in:\n{output}"
        );
        assert!(
            output.contains("shardlake_shard_cache_misses_total 1"),
            "expected misses=1 in:\n{output}"
        );
    }

    #[test]
    fn gather_increments_query_counters() {
        let m = make_metrics();
        m.queries_total.inc();
        m.queries_total.inc();
        m.query_results_total.inc_by(5);

        let output = m.gather();
        assert!(
            output.contains("shardlake_queries_total 2"),
            "expected queries_total=2 in:\n{output}"
        );
        assert!(
            output.contains("shardlake_query_results_total 5"),
            "expected query_results_total=5 in:\n{output}"
        );
    }
}
