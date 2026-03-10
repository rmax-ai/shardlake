use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};

use shardlake_core::types::SearchResult;
use shardlake_index::CacheMetrics;

use crate::AppState;

/// Query request payload.
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub vector: Vec<f32>,
    pub k: usize,
    pub nprobe: Option<usize>,
}

/// Query response envelope.
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub results: Vec<SearchResult>,
    pub index_version: String,
}

/// Health check response.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub index_version: String,
}

/// Cache statistics response.
#[derive(Debug, Serialize)]
pub struct CacheStatsResponse {
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
    pub avg_load_latency_ms: f64,
    pub memory_bytes: usize,
    pub cached_shards: usize,
}

impl From<CacheMetrics> for CacheStatsResponse {
    fn from(m: CacheMetrics) -> Self {
        Self {
            hits: m.hits,
            misses: m.misses,
            hit_rate: m.hit_rate,
            avg_load_latency_ms: m.avg_load_latency_ms,
            memory_bytes: m.memory_bytes,
            cached_shards: m.cached_shards,
        }
    }
}

/// Build the axum router with all routes attached to `state`.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/query", post(query_handler))
        .route("/cache-stats", get(cache_stats_handler))
        .with_state(state)
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let version = state.searcher.manifest().index_version.0.clone();
    Json(HealthResponse {
        status: "ok",
        index_version: version,
    })
}

async fn query_handler(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    if req.k == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "k must be > 0" })),
        )
            .into_response();
    }
    let nprobe = req.nprobe.unwrap_or(state.nprobe);
    match state.searcher.search(&req.vector, req.k, nprobe) {
        Ok(results) => {
            let version = state.searcher.manifest().index_version.0.clone();
            Json(QueryResponse {
                results,
                index_version: version,
            })
            .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn cache_stats_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(CacheStatsResponse::from(state.cache_metrics()))
}
