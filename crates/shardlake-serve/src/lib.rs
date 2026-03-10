//! HTTP serving layer built on axum.

pub mod routes;

pub use routes::build_router;

use std::sync::Arc;

use shardlake_index::IndexSearcher;

/// Shared application state injected into axum routes.
#[derive(Clone)]
pub struct AppState {
    pub searcher: Arc<IndexSearcher>,
    /// Default number of candidate shards to probe when the request does not
    /// specify `candidate_shards` / `nprobe`.
    pub nprobe: usize,
    /// Default rerank limit (overridden per-request when provided).
    pub rerank_limit: Option<usize>,
}
