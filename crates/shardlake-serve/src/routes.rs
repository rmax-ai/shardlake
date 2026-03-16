use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use tracing::error;

use shardlake_core::{
    config::{FanOutPolicy, QueryConfig},
    error::CoreError,
    types::{DistanceMetric, SearchResult},
};
use shardlake_index::{IndexError, QueryPlan, PQ8_CODEC};

use crate::AppState;

/// Query request payload.
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub vector: Vec<f32>,
    pub k: usize,
    /// Number of nearest centroids to select (overrides the server default).
    /// Alias for `candidate_centroids`; kept for backward compatibility.
    pub nprobe: Option<usize>,
    /// When `true`, rerank the ANN candidates by exact distance before
    /// returning results. Defaults to `false`.
    pub rerank: Option<bool>,
    /// Number of nearest centroids to select for shard routing.
    /// When present, takes precedence over `nprobe`.
    pub candidate_centroids: Option<u32>,
    /// Maximum number of shards to probe after deduplication.
    /// `0` means no cap.  Overrides the server default when present.
    pub candidate_shards: Option<u32>,
    /// Maximum number of vectors to evaluate per probed shard.
    /// `0` means no limit.  Overrides the server default when present.
    pub max_vectors_per_shard: Option<u32>,
    /// Maximum number of merged candidates passed to the reranker.
    ///
    /// When absent, the server reranks the top `k` ANN candidates.  When set,
    /// it widens the candidate pool handed to the reranker to `max(k, n)`.
    /// Must be ≥ 1 when provided.
    /// Only meaningful when `rerank` is `true`.
    pub rerank_limit: Option<usize>,
    /// Distance metric override for this query.
    ///
    /// When absent (default), the metric stored in the index manifest is
    /// used.  When provided, overrides the manifest metric for this query
    /// only.  Must be one of `"cosine"`, `"euclidean"`, or
    /// `"inner_product"`.
    pub distance_metric: Option<DistanceMetric>,
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

/// Debug query-plan response returned by `POST /debug/query-plan`.
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryPlanResponse {
    /// Routing and candidate details captured during this query execution.
    pub plan: QueryPlan,
    /// Index version used to serve this query.
    pub index_version: String,
}

/// Build the axum router with all routes attached to `state`.
pub fn build_router(state: AppState) -> Router {
    let debug_routes_enabled = state.debug_routes_enabled;
    let router = Router::new()
        .route("/health", get(health_handler))
        .route("/query", post(query_handler));
    let router = if debug_routes_enabled {
        router.route("/debug/query-plan", post(query_plan_handler))
    } else {
        router
    };
    router.with_state(state)
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let version = state.searcher.manifest().index_version.0.clone();
    Json(HealthResponse {
        status: "ok",
        index_version: version,
    })
}

/// A request-validation error that can be sent directly as an HTTP response.
///
/// Wraps a `(StatusCode, Json<serde_json::Value>)` tuple so callers can return
/// typed errors from `resolve_policy` without heap-allocating an opaque
/// `axum::response::Response`.
struct PolicyError(StatusCode, Json<serde_json::Value>);

impl IntoResponse for PolicyError {
    fn into_response(self) -> axum::response::Response {
        (self.0, self.1).into_response()
    }
}

/// Parse and validate the fan-out policy from a [`QueryRequest`], falling back
/// to the server-level defaults in `fan_out`.
///
/// Returns `Ok(QueryConfig)` on success, or a [`PolicyError`] that can be
/// returned directly as an HTTP response on invalid input.
fn resolve_policy(
    req: &QueryRequest,
    fan_out: &FanOutPolicy,
) -> std::result::Result<QueryConfig, PolicyError> {
    if req.k == 0 {
        return Err(PolicyError(
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "k must be > 0" })),
        ));
    }

    // Build per-request fan-out policy, falling back to server defaults.
    // `candidate_centroids` takes precedence over the legacy `nprobe` field.
    let legacy_candidate_centroids = match req.nprobe.map(u32::try_from).transpose() {
        Ok(value) => value,
        Err(_) => {
            return Err(PolicyError(
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": format!("nprobe must be <= {}", u32::MAX)
                })),
            ));
        }
    };
    let candidate_centroids = req
        .candidate_centroids
        .or(legacy_candidate_centroids)
        .unwrap_or(fan_out.candidate_centroids);
    let candidate_shards = req.candidate_shards.unwrap_or(fan_out.candidate_shards);
    let max_vectors_per_shard = req
        .max_vectors_per_shard
        .unwrap_or(fan_out.max_vectors_per_shard);

    let query_config = QueryConfig {
        top_k: req.k,
        fan_out: FanOutPolicy {
            candidate_centroids,
            candidate_shards,
            max_vectors_per_shard,
        },
        rerank_limit: req.rerank_limit,
        distance_metric: req.distance_metric,
    };

    if let Err(e) = query_config.validate() {
        return Err(PolicyError(
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": e.to_string() })),
        ));
    }

    Ok(query_config)
}

/// Map a blocking search task result to an HTTP error response.
fn search_error_response(e: IndexError) -> axum::response::Response {
    match e {
        IndexError::Core(CoreError::DimensionMismatch { .. }) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "query vector dimensions do not match the index"
            })),
        )
            .into_response(),
        other => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": other.to_string() })),
        )
            .into_response(),
    }
}

async fn query_handler(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let query_config = match resolve_policy(&req, &state.fan_out) {
        Ok(config) => config,
        Err(e) => return e.into_response(),
    };

    let version = state.searcher.manifest().index_version.0.clone();
    let searcher = state.searcher.clone();
    let vector = req.vector;
    let rerank = req.rerank.unwrap_or(false);
    let manifest = searcher.manifest();
    let pq_metric_override_rejected = manifest.compression.enabled
        && manifest.compression.codec == PQ8_CODEC
        && matches!(
            query_config.distance_metric,
            Some(metric) if metric != DistanceMetric::Euclidean
        );
    if pq_metric_override_rejected {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "PQ-compressed indexes currently support only euclidean distance queries"
            })),
        )
            .into_response();
    }
    let policy = query_config.fan_out.clone();
    let k = query_config.top_k;
    let metric = query_config
        .distance_metric
        .unwrap_or(manifest.distance_metric);
    let candidate_k = if rerank {
        query_config.rerank_limit.unwrap_or(k).max(k)
    } else {
        k
    };
    match tokio::task::spawn_blocking(move || {
        let ann_results = searcher.search_with_metric(&vector, candidate_k, &policy, metric)?;
        if rerank {
            let mut reranked = searcher.rerank_with_metric(&vector, ann_results, metric)?;
            reranked.truncate(k);
            Ok(reranked)
        } else {
            Ok(ann_results)
        }
    })
    .await
    {
        Ok(Ok(results)) => Json(QueryResponse {
            results,
            index_version: version,
        })
        .into_response(),
        Ok(Err(e)) => search_error_response(e),
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

async fn query_plan_handler(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let query_config = match resolve_policy(&req, &state.fan_out) {
        Ok(config) => config,
        Err(e) => return e.into_response(),
    };

    let version = state.searcher.manifest().index_version.0.clone();
    let searcher = state.searcher.clone();
    let vector = req.vector;
    let manifest = searcher.manifest();
    let pq_metric_override_rejected = manifest.compression.enabled
        && manifest.compression.codec == PQ8_CODEC
        && matches!(
            query_config.distance_metric,
            Some(metric) if metric != DistanceMetric::Euclidean
        );
    if pq_metric_override_rejected {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "PQ-compressed indexes currently support only euclidean distance queries"
            })),
        )
            .into_response();
    }
    let policy = query_config.fan_out.clone();
    let k = query_config.top_k;
    let metric = query_config
        .distance_metric
        .unwrap_or(manifest.distance_metric);
    match tokio::task::spawn_blocking(move || {
        searcher.search_with_plan_with_metric(&vector, k, &policy, metric)
    })
    .await
    {
        Ok(Ok(plan)) => Json(QueryPlanResponse {
            plan,
            index_version: version,
        })
        .into_response(),
        Ok(Err(e)) => search_error_response(e),
        Err(e) => {
            error!(error = %e, "query-plan task failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "query-plan task failed" })),
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
        config::{FanOutPolicy, SystemConfig},
        types::{
            DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
        },
    };
    use shardlake_index::{BuildParams, IndexBuilder, IndexSearcher, PqParams};
    use shardlake_storage::{LocalObjectStore, ObjectStore};
    use tower::util::ServiceExt;

    use super::*;
    use crate::AppState;

    fn make_test_router() -> (Router, tempfile::TempDir) {
        make_test_router_with_debug_routes(false)
    }

    fn make_test_router_with_debug_routes(
        debug_routes_enabled: bool,
    ) -> (Router, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(LocalObjectStore::new(tmp.path()).expect("store"));
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 2,
            kmeans_iters: 4,
            nprobe: 2,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            candidate_shards: 0,
            max_vectors_per_shard: 0,
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
            fan_out: FanOutPolicy {
                candidate_centroids: 2,
                candidate_shards: 0,
                max_vectors_per_shard: 0,
            },
            debug_routes_enabled,
        };
        (build_router(state), tmp)
    }

    fn make_distance_metric_router() -> (Router, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(LocalObjectStore::new(tmp.path()).expect("store"));
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 1,
            kmeans_iters: 4,
            nprobe: 1,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            candidate_shards: 0,
            max_vectors_per_shard: 0,
            kmeans_sample_size: None,
            ..SystemConfig::default()
        };
        let records = vec![
            VectorRecord {
                id: VectorId(1),
                data: vec![100.0, 0.0],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(2),
                data: vec![9.0, 1.0],
                metadata: None,
            },
        ];
        let manifest = IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-metric".into()),
                embedding_version: EmbeddingVersion("emb-metric".into()),
                index_version: IndexVersion("idx-metric".into()),
                metric: DistanceMetric::Euclidean,
                dims: 2,
                vectors_key: "datasets/ds-metric/vectors.jsonl".into(),
                metadata_key: "datasets/ds-metric/metadata.json".into(),
                pq_params: None,
            })
            .expect("build index");
        let searcher = Arc::new(IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest));
        let state = AppState {
            searcher,
            fan_out: FanOutPolicy {
                candidate_centroids: 1,
                candidate_shards: 0,
                max_vectors_per_shard: 0,
            },
            debug_routes_enabled: false,
        };
        (build_router(state), tmp)
    }

    fn make_pq_router() -> (Router, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(LocalObjectStore::new(tmp.path()).expect("store"));
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 1,
            kmeans_iters: 4,
            nprobe: 1,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            candidate_shards: 0,
            max_vectors_per_shard: 0,
            pq_enabled: true,
            pq_num_subspaces: 1,
            pq_codebook_size: 2,
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
            VectorRecord {
                id: VectorId(3),
                data: vec![0.5, 0.5],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(4),
                data: vec![0.25, 0.75],
                metadata: None,
            },
        ];
        let manifest = IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-pq".into()),
                embedding_version: EmbeddingVersion("emb-pq".into()),
                index_version: IndexVersion("idx-pq".into()),
                metric: DistanceMetric::Euclidean,
                dims: 2,
                vectors_key: "datasets/ds-pq/vectors.jsonl".into(),
                metadata_key: "datasets/ds-pq/metadata.json".into(),
                pq_params: Some(PqParams {
                    num_subspaces: 1,
                    codebook_size: 2,
                }),
            })
            .expect("build index");
        let searcher = Arc::new(IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest));
        let state = AppState {
            searcher,
            fan_out: FanOutPolicy {
                candidate_centroids: 1,
                candidate_shards: 0,
                max_vectors_per_shard: 0,
            },
            debug_routes_enabled: false,
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

    #[tokio::test]
    async fn query_route_rejects_zero_candidate_centroids() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"vector":[1.0,0.0],"k":1,"candidate_centroids":0}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("error json");
        assert!(
            payload["error"]
                .as_str()
                .unwrap_or("")
                .contains("candidate_centroids"),
            "expected error mentioning candidate_centroids, got: {}",
            payload["error"]
        );
    }

    #[tokio::test]
    async fn query_route_accepts_fan_out_overrides() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"vector":[1.0,0.0],"k":1,"candidate_centroids":1,"candidate_shards":1,"max_vectors_per_shard":10}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn query_route_nprobe_backward_compat() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"vector":[1.0,0.0],"k":1,"nprobe":2}"#))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn query_route_rejects_nprobe_overflow() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"vector":[1.0,0.0],"k":1,"nprobe":4294967296}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("error json");
        assert_eq!(payload["error"], format!("nprobe must be <= {}", u32::MAX));
    }

    #[tokio::test]
    async fn query_route_with_rerank_returns_correct_top_result() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"vector":[1.0,0.0],"k":1,"candidate_centroids":1,"rerank":true}"#,
                    ))
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
    async fn query_plan_route_returns_plan() {
        let (app, _tmp) = make_test_router_with_debug_routes(true);
        let response = app
            .oneshot(
                Request::post("/debug/query-plan")
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
        let payload: QueryPlanResponse =
            serde_json::from_slice(&body).expect("query plan response json");
        assert_eq!(payload.index_version, "idx-test");
        assert!(!payload.plan.selected_centroids.is_empty());
        assert!(!payload.plan.searched_shards.is_empty());
        assert!(!payload.plan.candidate_vectors.is_empty());
        assert_eq!(payload.plan.candidate_vectors[0].id, VectorId(1));
    }

    #[tokio::test]
    async fn query_plan_route_rejects_zero_k() {
        let (app, _tmp) = make_test_router_with_debug_routes(true);
        let response = app
            .oneshot(
                Request::post("/debug/query-plan")
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
    async fn query_plan_route_rejects_dimension_mismatch() {
        let (app, _tmp) = make_test_router_with_debug_routes(true);
        let response = app
            .oneshot(
                Request::post("/debug/query-plan")
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

    #[tokio::test]
    async fn query_plan_route_searched_shards_subset_of_index_shards() {
        let (app, _tmp) = make_test_router_with_debug_routes(true);
        let response = app
            .oneshot(
                Request::post("/debug/query-plan")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"vector":[1.0,0.0],"k":2}"#))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: QueryPlanResponse =
            serde_json::from_slice(&body).expect("query plan response json");
        for shard_id in &payload.plan.searched_shards {
            assert!(shard_id.0 < 2, "unexpected shard id {shard_id}");
        }
    }

    #[tokio::test]
    async fn query_plan_route_disabled_by_default() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/debug/query-plan")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"vector":[1.0,0.0],"k":1}"#))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn query_route_accepts_rerank_limit() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"vector":[1.0,0.0],"k":1,"candidate_centroids":1,"rerank":true,"rerank_limit":5}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn query_route_rejects_zero_rerank_limit() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"vector":[1.0,0.0],"k":1,"rerank_limit":0}"#))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("error json");
        assert!(
            payload["error"]
                .as_str()
                .unwrap_or("")
                .contains("rerank_limit"),
            "expected error mentioning rerank_limit, got: {}",
            payload["error"]
        );
    }

    #[tokio::test]
    async fn query_route_accepts_distance_metric_override() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"vector":[1.0,0.0],"k":1,"distance_metric":"euclidean"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn query_route_applies_distance_metric_override() {
        let (app, _tmp) = make_distance_metric_router();

        let default_response = app
            .clone()
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"vector":[10.0,0.0],"k":1}"#))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(default_response.status(), StatusCode::OK);
        let default_body = to_bytes(default_response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let default_payload: QueryResponse =
            serde_json::from_slice(&default_body).expect("query response json");
        assert_eq!(default_payload.results[0].id, VectorId(2));

        let cosine_response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"vector":[10.0,0.0],"k":1,"distance_metric":"cosine"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(cosine_response.status(), StatusCode::OK);
        let cosine_body = to_bytes(cosine_response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let cosine_payload: QueryResponse =
            serde_json::from_slice(&cosine_body).expect("query response json");
        assert_eq!(cosine_payload.results[0].id, VectorId(1));
    }

    #[tokio::test]
    async fn query_route_rejects_non_euclidean_metric_for_pq_indexes() {
        let (app, _tmp) = make_pq_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"vector":[1.0,0.0],"k":1,"distance_metric":"cosine"}"#,
                    ))
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
            "PQ-compressed indexes currently support only euclidean distance queries"
        );
    }
}
