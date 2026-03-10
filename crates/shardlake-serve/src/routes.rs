use axum::{
    extract::{Json, State},
    http::{header, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};

use shardlake_core::types::SearchResult;
use shardlake_index::QueryPlan;

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

/// Debug query-plan response.
#[derive(Debug, Serialize)]
pub struct QueryPlanResponse {
    pub plan: QueryPlan,
    pub index_version: String,
}

/// Build the axum router with all routes attached to `state`.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/query", post(query_handler))
        .route("/metrics", get(metrics_handler))
        .route("/debug/query-plan", post(query_plan_handler))
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

/// `GET /metrics` — expose Prometheus-format metrics for scraping.
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let body = state.prometheus_handle.render();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
}

/// `POST /debug/query-plan` — return the full query execution plan for debugging.
async fn query_plan_handler(
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
    match state.searcher.search_with_plan(&req.vector, req.k, nprobe) {
        Ok(plan) => {
            let version = state.searcher.manifest().index_version.0.clone();
            Json(QueryPlanResponse {
                plan,
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
