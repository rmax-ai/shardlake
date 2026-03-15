//! HTTP serving layer built on axum.

pub mod routes;

pub use routes::build_router;

use std::sync::Arc;

use shardlake_core::config::FanOutPolicy;
use shardlake_index::IndexSearcher;

/// Shared application state injected into axum routes.
#[derive(Clone)]
pub struct AppState {
    pub searcher: Arc<IndexSearcher>,
    /// Default fan-out policy used when the query request does not supply
    /// per-request overrides.
    pub fan_out: FanOutPolicy,
}
