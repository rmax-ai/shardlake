use std::sync::Arc;

use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};

use shardlake_core::{query::QueryConfig, types::DistanceMetric, SearchResult};
use shardlake_index::QueryPipeline;

use crate::AppState;

/// Query request payload.
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    /// Query vector (must match the index dimensionality).
    pub vector: Vec<f32>,
    /// Number of results to return.
    pub k: usize,
    /// Number of shards to probe (overrides the server default when provided).
    pub nprobe: Option<usize>,
    /// Maximum number of candidates to gather before re-ranking.
    /// When set, the pipeline collects up to `rerank_limit` candidates from
    /// all probed shards and then trims to `k`.  A value larger than `k`
    /// improves recall at the cost of extra computation.
    pub rerank_limit: Option<usize>,
    /// Distance metric override.  When omitted the metric baked into the
    /// index manifest is used.
    pub distance_metric: Option<DistanceMetric>,
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

/// Build the axum router with all routes attached to `state`.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/query", post(query_handler))
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

    let config = QueryConfig {
        top_k: req.k,
        candidate_shards: req.nprobe.unwrap_or(state.nprobe),
        rerank_limit: req.rerank_limit.or(state.rerank_limit),
        distance_metric: req.distance_metric,
    };

    let query: Arc<[f32]> = req.vector.into();
    match QueryPipeline::run_parallel(Arc::clone(&state.searcher), query, config).await {
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
