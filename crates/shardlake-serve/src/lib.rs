//! HTTP serving layer built on axum.

pub mod prom;
pub mod routes;

pub use prom::PrometheusMetrics;
pub use routes::build_router;

use std::sync::Arc;

use shardlake_core::config::FanOutPolicy;
use shardlake_index::{Bm25Index, IndexSearcher};

/// Shared application state injected into axum routes.
#[derive(Clone)]
pub struct AppState {
    pub searcher: Arc<IndexSearcher>,
    /// Default fan-out policy used when the query request does not supply
    /// per-request overrides.
    pub fan_out: FanOutPolicy,
    /// Whether diagnostic HTTP routes are exposed.
    pub debug_routes_enabled: bool,
    /// Prometheus metrics registry updated by request handlers.
    pub metrics: Arc<PrometheusMetrics>,
    /// Optional BM25 lexical index for `lexical` and `hybrid` query modes.
    ///
    /// When `None`, requests that select `QueryMode::Lexical` or
    /// `QueryMode::Hybrid` are rejected with `400 Bad Request`.
    pub bm25_index: Option<Arc<Bm25Index>>,
}
