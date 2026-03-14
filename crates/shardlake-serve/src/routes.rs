use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use tracing::error;

use shardlake_core::{error::CoreError, types::SearchResult};
use shardlake_index::IndexError;

use crate::AppState;

/// Query request payload.
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub vector: Vec<f32>,
    pub k: usize,
    pub nprobe: Option<usize>,
}

/// Query response envelope.
#[derive(Debug, Deserialize, Serialize)]
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
    let nprobe = req.nprobe.unwrap_or(state.nprobe);
    let version = state.searcher.manifest().index_version.0.clone();
    let searcher = state.searcher.clone();
    let vector = req.vector;
    match tokio::task::spawn_blocking(move || searcher.search(&vector, req.k, nprobe)).await {
        Ok(Ok(results)) => Json(QueryResponse {
            results,
            index_version: version,
        })
        .into_response(),
        Ok(Err(IndexError::Core(CoreError::DimensionMismatch { .. }))) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "query vector dimensions do not match the index"
            })),
        )
            .into_response(),
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
        Err(e) => {
            error!(error = %e, "search task failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "search task failed" })),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use shardlake_core::{
        config::SystemConfig,
        types::{
            DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
        },
    };
    use shardlake_index::{BuildParams, IndexBuilder, IndexSearcher};
    use shardlake_storage::{LocalObjectStore, ObjectStore};
    use tower::util::ServiceExt;

    use super::*;
    use crate::AppState;

    fn make_test_router() -> (Router, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(LocalObjectStore::new(tmp.path()).expect("store"));
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 2,
            kmeans_iters: 4,
            nprobe: 2,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            kmeans_sample_size: None,
            ..SystemConfig::default()
        };
        let records = vec![
            VectorRecord {
                id: VectorId(1),
                data: vec![1.0, 0.0],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(2),
                data: vec![0.0, 1.0],
                metadata: None,
            },
        ];
        let manifest = IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-test".into()),
                embedding_version: EmbeddingVersion("emb-test".into()),
                index_version: IndexVersion("idx-test".into()),
                metric: DistanceMetric::Euclidean,
                dims: 2,
                vectors_key: "datasets/ds-test/vectors.jsonl".into(),
                metadata_key: "datasets/ds-test/metadata.json".into(),
                pq_params: None,
            })
            .expect("build index");
        let searcher = Arc::new(IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest));
        let state = AppState {
            searcher,
            nprobe: 2,
        };
        (build_router(state), tmp)
    }

    #[tokio::test]
    async fn query_route_returns_results() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"vector":[1.0,0.0],"k":1}"#))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: QueryResponse = serde_json::from_slice(&body).expect("query response json");
        assert_eq!(payload.index_version, "idx-test");
        assert_eq!(payload.results.len(), 1);
        assert_eq!(payload.results[0].id, VectorId(1));
    }

    #[tokio::test]
    async fn query_route_rejects_zero_k() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"vector":[1.0,0.0],"k":0}"#))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("error json");
        assert_eq!(payload["error"], "k must be > 0");
    }

    #[tokio::test]
    async fn query_route_rejects_dimension_mismatch() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"vector":[1.0,0.0,3.0],"k":1}"#))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("error json");
        assert_eq!(
            payload["error"],
            "query vector dimensions do not match the index"
        );
    }
}
