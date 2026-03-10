//! HTTP serving layer built on axum.

pub mod routes;

pub use routes::build_router;

use std::sync::Arc;

use metrics_exporter_prometheus::PrometheusHandle;
use shardlake_index::IndexSearcher;

/// Shared application state injected into axum routes.
#[derive(Clone)]
pub struct AppState {
    pub searcher: Arc<IndexSearcher>,
    pub nprobe: usize,
    /// Handle for rendering the Prometheus metrics text at `GET /metrics`.
    pub prometheus_handle: PrometheusHandle,
}
