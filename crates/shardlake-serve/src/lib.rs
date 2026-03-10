//! HTTP serving layer built on axum.

pub mod routes;

pub use routes::build_router;

use std::sync::Arc;

use shardlake_index::{CacheMetrics, IndexSearcher};

/// Shared application state injected into axum routes.
#[derive(Clone)]
pub struct AppState {
    pub searcher: Arc<IndexSearcher>,
    pub nprobe: usize,
}

impl AppState {
    /// Return a snapshot of the current shard-cache metrics.
    pub fn cache_metrics(&self) -> CacheMetrics {
        self.searcher.cache_metrics()
    }
}
