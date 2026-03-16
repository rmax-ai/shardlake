use axum::{
    extract::{Json, State},
    http::{header, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::error;

use shardlake_core::{
    config::{FanOutPolicy, QueryConfig},
    error::CoreError,
    types::{DistanceMetric, QueryMode, SearchResult},
};
use shardlake_index::{
    bm25::tokenize,
    ranking::{rank_hybrid, HybridRankingPolicy},
    IndexError, QueryPlan, PQ8_CODEC,
};

use crate::AppState;

/// Query request payload.
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    /// Query vector.  Required for `vector` and `hybrid` modes.
    pub vector: Option<Vec<f32>>,
    pub k: usize,
    /// Retrieval mode.  Defaults to `"vector"` when absent.
    ///
    /// - `"vector"` – ANN vector search (requires `vector`).
    /// - `"lexical"` – BM25 full-text search (requires `query_text`).
    /// - `"hybrid"` – blend of vector and lexical (requires both `vector` and
    ///   `query_text`).
    #[serde(default)]
    pub query_mode: QueryMode,
    /// Query text for BM25 lexical search.  Required for `lexical` and
    /// `hybrid` modes; ignored for `vector` mode.
    pub query_text: Option<String>,
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
        .route("/query", post(query_handler))
        .route("/metrics", get(metrics_handler));
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

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.searcher.cached_shard_bytes() {
        Ok(retained_bytes) => {
            let body = state.metrics.gather(retained_bytes);
            (
                [(
                    header::CONTENT_TYPE,
                    "text/plain; version=0.0.4; charset=utf-8",
                )],
                body,
            )
                .into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to gather metrics: {error}"),
        )
            .into_response(),
    }
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
    bm25_available: bool,
) -> std::result::Result<QueryConfig, PolicyError> {
    if req.k == 0 {
        return Err(PolicyError(
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "k must be > 0" })),
        ));
    }

    // `true` when `query_text` is absent or tokenizes to no searchable terms.
    let query_text_missing = req
        .query_text
        .as_deref()
        .map(|text| tokenize(text).is_empty())
        .unwrap_or(true);

    // Validate mode-specific field requirements.
    let query_mode = req.query_mode;
    match query_mode {
        QueryMode::Vector => {
            if req.vector.is_none() {
                return Err(PolicyError(
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "vector is required for vector mode" })),
                ));
            }
        }
        QueryMode::Lexical => {
            if query_text_missing {
                return Err(PolicyError(
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "query_text is required for lexical mode" })),
                ));
            }
            if !bm25_available {
                return Err(PolicyError(
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "lexical query mode is not available: no BM25 index loaded"
                    })),
                ));
            }
        }
        QueryMode::Hybrid => {
            if req.vector.is_none() {
                return Err(PolicyError(
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "vector is required for hybrid mode" })),
                ));
            }
            if query_text_missing {
                return Err(PolicyError(
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "query_text is required for hybrid mode" })),
                ));
            }
            if !bm25_available {
                return Err(PolicyError(
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "hybrid query mode is not available: no BM25 index loaded"
                    })),
                ));
            }
        }
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
        query_mode,
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
    let query_config = match resolve_policy(&req, &state.fan_out, state.bm25_index.is_some()) {
        Ok(config) => config,
        Err(e) => return e.into_response(),
    };

    let version = state.searcher.manifest().index_version.0.clone();
    let searcher = state.searcher.clone();
    let metrics = Arc::clone(&state.metrics);
    let vector = req.vector;
    let query_text = req.query_text;
    let rerank = req.rerank.unwrap_or(false);
    let timer = metrics.query_duration_seconds.start_timer();
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
    let query_mode = query_config.query_mode;
    let metric = query_config
        .distance_metric
        .unwrap_or(manifest.distance_metric);
    let candidate_k = if rerank {
        query_config.rerank_limit.unwrap_or(k).max(k)
    } else {
        k
    };
    let bm25_index = state.bm25_index.clone();
    match tokio::task::spawn_blocking(move || -> shardlake_index::Result<Vec<SearchResult>> {
        match query_mode {
            QueryMode::Vector => {
                let v = vector.expect("vector validated above");
                let ann_results = searcher.search_with_metric(&v, candidate_k, &policy, metric)?;
                if rerank {
                    let mut reranked = searcher.rerank_with_metric(&v, ann_results, metric)?;
                    reranked.truncate(k);
                    Ok(reranked)
                } else {
                    Ok(ann_results)
                }
            }
            QueryMode::Lexical => {
                let bm25 = bm25_index.expect("bm25 index validated above");
                let text = query_text.expect("query_text validated above");
                Ok(bm25.search(&text, k))
            }
            QueryMode::Hybrid => {
                let v = vector.expect("vector validated above");
                let bm25 = bm25_index.expect("bm25 index validated above");
                let text = query_text.expect("query_text validated above");
                // Fetch up to `candidate_k` vector results so the hybrid merger
                // has a wide candidate pool when `rerank_limit` > `k`.
                let ann_results = searcher.search_with_metric(&v, candidate_k, &policy, metric)?;
                let vector_results = if rerank {
                    // Exact-rerank the ANN candidates; pass the full pool to
                    // rank_hybrid so it can select the best hybrid top-k.
                    searcher.rerank_with_metric(&v, ann_results, metric)?
                } else {
                    ann_results
                };
                let bm25_results = bm25.search(&text, candidate_k);
                let hybrid_policy = HybridRankingPolicy::default();
                Ok(rank_hybrid(vector_results, bm25_results, &hybrid_policy, k))
            }
        }
    })
    .await
    {
        Ok(Ok(results)) => {
            timer.observe_duration();
            metrics.queries_total.inc();
            metrics.query_results_total.inc_by(results.len() as u64);
            Json(QueryResponse {
                results,
                index_version: version,
            })
            .into_response()
        }
        Ok(Err(IndexError::Core(CoreError::DimensionMismatch { .. }))) => {
            timer.stop_and_discard();
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "query vector dimensions do not match the index"
                })),
            )
                .into_response()
        }
        Ok(Err(e)) => {
            timer.stop_and_discard();
            search_error_response(e)
        }
        Err(e) => {
            timer.stop_and_discard();
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
    let query_config = match resolve_policy(&req, &state.fan_out, state.bm25_index.is_some()) {
        Ok(config) => config,
        Err(e) => return e.into_response(),
    };
    if query_config.query_mode != QueryMode::Vector {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "query plan is only available for vector mode"
            })),
        )
            .into_response();
    }

    let version = state.searcher.manifest().index_version.0.clone();
    let searcher = state.searcher.clone();
    let vector = req.vector.expect("vector mode validated above");
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
    use shardlake_index::{
        bm25::{BM25Params, Bm25Index},
        BuildParams, IndexBuilder, IndexSearcher, PqParams,
    };
    use shardlake_storage::{LocalObjectStore, ObjectStore};
    use tower::util::ServiceExt;

    use super::*;
    use crate::{AppState, PrometheusMetrics};

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
                ann_family: None,
                hnsw_config: None,
            })
            .expect("build index");
        let searcher = Arc::new(IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest));
        let metrics = Arc::new(PrometheusMetrics::new(searcher.cache_metrics()));
        let state = AppState {
            searcher,
            fan_out: FanOutPolicy {
                candidate_centroids: 2,
                candidate_shards: 0,
                max_vectors_per_shard: 0,
            },
            debug_routes_enabled,
            metrics,
            bm25_index: None,
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
                ann_family: None,
                hnsw_config: None,
            })
            .expect("build index");
        let searcher = Arc::new(IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest));
        let metrics = Arc::new(PrometheusMetrics::new(searcher.cache_metrics()));
        let state = AppState {
            searcher,
            fan_out: FanOutPolicy {
                candidate_centroids: 1,
                candidate_shards: 0,
                max_vectors_per_shard: 0,
            },
            debug_routes_enabled: false,
            metrics,
            bm25_index: None,
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
                ann_family: None,
                hnsw_config: None,
            })
            .expect("build index");
        let searcher = Arc::new(IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest));
        let metrics = Arc::new(PrometheusMetrics::new(searcher.cache_metrics()));
        let state = AppState {
            searcher,
            fan_out: FanOutPolicy {
                candidate_centroids: 1,
                candidate_shards: 0,
                max_vectors_per_shard: 0,
            },
            debug_routes_enabled: false,
            metrics,
            bm25_index: None,
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
    async fn metrics_route_returns_200_with_prometheus_content_type() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::get("/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            content_type.contains("text/plain"),
            "expected text/plain content-type, got: {content_type}"
        );
    }

    #[tokio::test]
    async fn metrics_route_contains_expected_metric_families() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::get("/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let body_str = std::str::from_utf8(&body).expect("utf-8 metrics body");

        for metric in &[
            "shardlake_query_duration_seconds",
            "shardlake_queries_total",
            "shardlake_query_results_total",
            "shardlake_shard_cache_hits_total",
            "shardlake_shard_cache_misses_total",
            "shardlake_shard_load_count_total",
            "shardlake_shard_load_latency_ns_total",
            "shardlake_shard_cache_retained_bytes",
        ] {
            assert!(
                body_str.contains(metric),
                "missing metric {metric} in:\n{body_str}"
            );
        }
    }

    #[tokio::test]
    async fn metrics_route_increments_after_query() {
        let (app, _tmp) = make_test_router();

        // Issue a query first, then check the metrics endpoint.
        let response = app
            .clone()
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"vector":[1.0,0.0],"k":1}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::get("/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let body_str = std::str::from_utf8(&body).expect("utf-8 metrics body");
        assert!(
            body_str.contains("shardlake_queries_total 1"),
            "expected queries_total=1 after one query, got:\n{body_str}"
        );
        assert!(
            body_str.contains("shardlake_query_results_total 1"),
            "expected query_results_total=1 after one result, got:\n{body_str}"
        );
        assert!(
            body_str
                .lines()
                .find_map(|line| {
                    line.strip_prefix("shardlake_shard_cache_retained_bytes ")
                        .map(str::trim)
                })
                .and_then(|value| value.parse::<u64>().ok())
                .is_some_and(|value| value > 0),
            "expected shardlake_shard_cache_retained_bytes > 0 after one query, got:\n{body_str}"
        );
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

    // ── Query-mode helpers ────────────────────────────────────────────────────

    /// Build a test router that includes a BM25 lexical index alongside the
    /// vector index, enabling lexical and hybrid query mode tests.
    fn make_lexical_router() -> (Router, tempfile::TempDir) {
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
                dataset_version: DatasetVersion("ds-lex".into()),
                embedding_version: EmbeddingVersion("emb-lex".into()),
                index_version: IndexVersion("idx-lex".into()),
                metric: DistanceMetric::Euclidean,
                dims: 2,
                vectors_key: "datasets/ds-lex/vectors.jsonl".into(),
                metadata_key: "datasets/ds-lex/metadata.json".into(),
                pq_params: None,
                ann_family: None,
                hnsw_config: None,
            })
            .expect("build index");
        let searcher = Arc::new(IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest));
        let metrics = Arc::new(PrometheusMetrics::new(searcher.cache_metrics()));
        let bm25 = Bm25Index::build(
            &[
                (VectorId(1), "quick brown fox jumps over lazy dog"),
                (VectorId(2), "lazy dog sleeps all day"),
            ],
            BM25Params::default(),
        );
        let state = AppState {
            searcher,
            fan_out: FanOutPolicy {
                candidate_centroids: 2,
                candidate_shards: 0,
                max_vectors_per_shard: 0,
            },
            debug_routes_enabled: false,
            metrics,
            bm25_index: Some(Arc::new(bm25)),
        };
        (build_router(state), tmp)
    }

    fn make_hybrid_rerank_limit_router() -> (Router, tempfile::TempDir) {
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
                data: vec![0.9, 0.1],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(2),
                data: vec![1.0, 0.0],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(3),
                data: vec![0.0, 1.0],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(4),
                data: vec![0.7, 0.3],
                metadata: None,
            },
        ];
        let manifest = IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds-hybrid-rerank".into()),
                embedding_version: EmbeddingVersion("emb-hybrid-rerank".into()),
                index_version: IndexVersion("idx-hybrid-rerank".into()),
                metric: DistanceMetric::Euclidean,
                dims: 2,
                vectors_key: "datasets/ds-hybrid-rerank/vectors.jsonl".into(),
                metadata_key: "datasets/ds-hybrid-rerank/metadata.json".into(),
                pq_params: None,
                ann_family: None,
                hnsw_config: None,
            })
            .expect("build index");
        let searcher = Arc::new(IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest));
        let metrics = Arc::new(PrometheusMetrics::new(searcher.cache_metrics()));
        let bm25 = Bm25Index::build(
            &[
                (VectorId(1), "alpha alpha"),
                (VectorId(2), "beta"),
                (VectorId(3), "alpha alpha alpha"),
                (VectorId(4), "alpha"),
            ],
            BM25Params::default(),
        );
        let state = AppState {
            searcher,
            fan_out: FanOutPolicy {
                candidate_centroids: 1,
                candidate_shards: 0,
                max_vectors_per_shard: 0,
            },
            debug_routes_enabled: false,
            metrics,
            bm25_index: Some(Arc::new(bm25)),
        };
        (build_router(state), tmp)
    }

    // ── Query mode: defaults ──────────────────────────────────────────────────

    #[tokio::test]
    async fn query_mode_defaults_to_vector_when_absent() {
        // Omitting `query_mode` should behave identically to `"vector"`.
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
    }

    #[tokio::test]
    async fn query_mode_vector_explicit_returns_results() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"vector":[1.0,0.0],"k":1,"query_mode":"vector"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: QueryResponse = serde_json::from_slice(&body).expect("json");
        assert_eq!(payload.results.len(), 1);
        assert_eq!(payload.results[0].id, VectorId(1));
    }

    // ── Query mode: vector validation ─────────────────────────────────────────

    #[tokio::test]
    async fn query_mode_vector_rejects_missing_vector() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"k":1,"query_mode":"vector"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert!(
            payload["error"]
                .as_str()
                .unwrap_or("")
                .contains("vector is required for vector mode"),
            "unexpected error: {}",
            payload["error"]
        );
    }

    // ── Query mode: lexical ───────────────────────────────────────────────────

    #[tokio::test]
    async fn query_mode_lexical_returns_results() {
        let (app, _tmp) = make_lexical_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"k":2,"query_mode":"lexical","query_text":"lazy dog"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: QueryResponse = serde_json::from_slice(&body).expect("json");
        assert!(
            !payload.results.is_empty(),
            "lexical search returned no results"
        );
        // Doc 2 ("lazy dog sleeps all day") should match "lazy dog" best.
        assert_eq!(payload.results[0].id, VectorId(2));
    }

    #[tokio::test]
    async fn query_mode_lexical_rejects_missing_query_text() {
        let (app, _tmp) = make_lexical_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"k":1,"query_mode":"lexical"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert!(
            payload["error"]
                .as_str()
                .unwrap_or("")
                .contains("query_text is required for lexical mode"),
            "unexpected error: {}",
            payload["error"]
        );
    }

    #[tokio::test]
    async fn query_mode_lexical_rejects_whitespace_only_query_text() {
        let (app, _tmp) = make_lexical_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"k":1,"query_mode":"lexical","query_text":"   "}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert!(
            payload["error"]
                .as_str()
                .unwrap_or("")
                .contains("query_text is required for lexical mode"),
            "unexpected error: {}",
            payload["error"]
        );
    }

    #[tokio::test]
    async fn query_mode_lexical_rejects_no_bm25_index() {
        // Router has no BM25 index.
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"k":1,"query_mode":"lexical","query_text":"fox"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert!(
            payload["error"]
                .as_str()
                .unwrap_or("")
                .contains("no BM25 index loaded"),
            "unexpected error: {}",
            payload["error"]
        );
    }

    // ── Query mode: hybrid ────────────────────────────────────────────────────

    #[tokio::test]
    async fn query_mode_hybrid_returns_results() {
        let (app, _tmp) = make_lexical_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"vector":[1.0,0.0],"k":2,"query_mode":"hybrid","query_text":"lazy dog"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: QueryResponse = serde_json::from_slice(&body).expect("json");
        assert!(
            !payload.results.is_empty(),
            "hybrid search returned no results"
        );
        // Both docs should appear (they each appear in either the vector or lexical list).
        assert_eq!(payload.results.len(), 2);
    }

    #[tokio::test]
    async fn query_mode_hybrid_rejects_missing_vector() {
        let (app, _tmp) = make_lexical_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"k":1,"query_mode":"hybrid","query_text":"fox"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert!(
            payload["error"]
                .as_str()
                .unwrap_or("")
                .contains("vector is required for hybrid mode"),
            "unexpected error: {}",
            payload["error"]
        );
    }

    #[tokio::test]
    async fn query_mode_hybrid_rejects_missing_query_text() {
        let (app, _tmp) = make_lexical_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"vector":[1.0,0.0],"k":1,"query_mode":"hybrid"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert!(
            payload["error"]
                .as_str()
                .unwrap_or("")
                .contains("query_text is required for hybrid mode"),
            "unexpected error: {}",
            payload["error"]
        );
    }

    #[tokio::test]
    async fn query_mode_hybrid_rejects_punctuation_only_query_text() {
        let (app, _tmp) = make_lexical_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"vector":[1.0,0.0],"k":1,"query_mode":"hybrid","query_text":"!!!"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert!(
            payload["error"]
                .as_str()
                .unwrap_or("")
                .contains("query_text is required for hybrid mode"),
            "unexpected error: {}",
            payload["error"]
        );
    }

    #[tokio::test]
    async fn query_mode_hybrid_rejects_no_bm25_index() {
        // Router has no BM25 index.
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"vector":[1.0,0.0],"k":1,"query_mode":"hybrid","query_text":"fox"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert!(
            payload["error"]
                .as_str()
                .unwrap_or("")
                .contains("no BM25 index loaded"),
            "unexpected error: {}",
            payload["error"]
        );
    }

    #[tokio::test]
    async fn query_mode_hybrid_rerank_limit_widens_bm25_candidates() {
        let (app, _tmp) = make_hybrid_rerank_limit_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"vector":[1.0,0.0],"k":1,"query_mode":"hybrid","query_text":"alpha","rerank":true,"rerank_limit":3}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let payload: QueryResponse = serde_json::from_slice(&body).expect("json");
        assert_eq!(payload.results.len(), 1);
        assert_eq!(payload.results[0].id, VectorId(1));
    }

    #[tokio::test]
    async fn query_mode_invalid_value_rejected() {
        let (app, _tmp) = make_test_router();
        let response = app
            .oneshot(
                Request::post("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"vector":[1.0,0.0],"k":1,"query_mode":"invalid_mode"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }
}
