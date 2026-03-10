use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};

use shardlake_core::types::{QueryMode, SearchResult};

use crate::AppState;

/// Query request payload.
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    /// Query vector. Required for `vector` (default) and `hybrid` modes.
    pub vector: Option<Vec<f32>>,
    /// Number of results to return. Must be ≥ 1.
    pub k: usize,
    /// Number of shards to probe (vector/hybrid only). Defaults to server `--nprobe`.
    pub nprobe: Option<usize>,
    /// Retrieval mode: `"vector"` (default), `"lexical"`, or `"hybrid"`.
    pub mode: Option<QueryMode>,
    /// Query text for lexical and hybrid modes.
    pub text: Option<String>,
    /// Hybrid blending weight `[0.0, 1.0]`. `1.0` = pure vector, `0.0` = pure
    /// lexical. Default `0.5`. Ignored for non-hybrid modes.
    pub alpha: Option<f32>,
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

    let mode = req.mode.unwrap_or(QueryMode::Vector);
    let nprobe = req.nprobe.unwrap_or(state.nprobe);
    let alpha = req.alpha.unwrap_or(0.5).clamp(0.0, 1.0);

    let result = match mode {
        QueryMode::Vector => {
            let Some(ref vector) = req.vector else {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "\"vector\" field is required for mode \"vector\"" })),
                )
                    .into_response();
            };
            state.searcher.search(vector, req.k, nprobe)
        }
        QueryMode::Lexical => {
            let Some(ref text) = req.text else {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "\"text\" field is required for mode \"lexical\"" })),
                )
                    .into_response();
            };
            state.searcher.search_lexical(text, req.k)
        }
        QueryMode::Hybrid => {
            let Some(ref vector) = req.vector else {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "\"vector\" field is required for mode \"hybrid\"" })),
                )
                    .into_response();
            };
            let Some(ref text) = req.text else {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "\"text\" field is required for mode \"hybrid\"" })),
                )
                    .into_response();
            };
            state
                .searcher
                .search_hybrid(vector, text, req.k, nprobe, alpha)
        }
    };

    match result {
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
