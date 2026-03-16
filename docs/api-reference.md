# HTTP API Reference

The `shardlake serve` command exposes a minimal JSON HTTP API built on
[axum](https://github.com/tokio-rs/axum).

**Base URL:** `http://<bind-address>` (default `http://0.0.0.0:8080`)

All request bodies must be `Content-Type: application/json`. All responses are JSON.

---

## `GET /health`

Returns the current health status and the index version being served.

### Response

```json
{
  "status": "ok",
  "index_version": "idx-v1"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | Always `"ok"` when the server is running |
| `index_version` | string | Version string from the loaded manifest |

### Example

```bash
curl -s http://localhost:8080/health
```

---

## `POST /query`

Performs approximate nearest-neighbour (ANN) search and optionally reranks
the top candidates by exact distance computation before returning results.

### Request body

```json
{
  "vector": [0.1, 0.2, 0.3],
  "k": 10,
  "candidate_centroids": 3,
  "candidate_shards": 2,
  "max_vectors_per_shard": 500,
  "rerank": true,
  "rerank_limit": 50,
  "distance_metric": "cosine"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `vector` | `float[]` | Yes | Query vector. Must have the same number of dimensions as the index. |
| `k` | integer | Yes | Number of results to return. Must be ≥ 1. |
| `nprobe` | integer | No | Backward-compatible alias for `candidate_centroids`. Ignored when `candidate_centroids` is also provided. Defaults to the server's `--nprobe` value. |
| `candidate_centroids` | integer | No | Number of nearest IVF centroids to select for shard routing. Must be ≥ 1 when provided. Takes precedence over `nprobe`. Defaults to the server's `--nprobe` value. |
| `candidate_shards` | integer | No | Maximum number of shards to probe after centroid-to-shard deduplication. `0` means no cap. Defaults to the server's `--candidate-shards` value. |
| `max_vectors_per_shard` | integer | No | Maximum number of vectors evaluated inside each probed shard. `0` means no limit. Defaults to the server's `--max-vectors-per-shard` value. |
| `rerank` | boolean | No | When `true`, the ANN candidates are re-scored against their raw vectors using exact distance computation before the final ranking is returned. Defaults to `false`. See [Exact reranking](#exact-reranking) below. |
| `rerank_limit` | integer | No | Maximum number of merged candidates passed to the reranker. Must be ≥ 1 when provided. When absent, the server reranks the top `k` ANN candidates. Only meaningful when `rerank` is `true`. See [Rerank candidate limit](#rerank-candidate-limit) below. |
| `distance_metric` | string | No | Distance metric override for this query. One of `"cosine"`, `"euclidean"`, or `"inner_product"`. When absent, the metric stored in the index manifest is used. See [Per-query distance metric](#per-query-distance-metric) below. |

### Success response — `200 OK`

```json
{
  "results": [
    {
      "id": 42,
      "score": 0.0031,
      "metadata": {"label": "dog"}
    },
    {
      "id": 7,
      "score": 0.0157,
      "metadata": null
    }
  ],
  "index_version": "idx-v1"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `results` | array | Ordered list of nearest neighbours, best match first |
| `results[].id` | integer | Numeric vector id as provided at ingest time |
| `results[].score` | float | Distance score. Lower is better for `cosine` and `euclidean`; lower is also better for `inner_product` (scores are negated internally). |
| `results[].metadata` | object or null | Metadata attached to this vector at ingest time, or `null` if none was provided |
| `index_version` | string | Index version used to serve this query |

### Error responses

| Status | Body | Cause |
|--------|------|-------|
| `400 Bad Request` | `{"error": "k must be > 0"}` | `k` field is 0 |
| `400 Bad Request` | `{"error": "invalid fan-out policy: candidate_centroids must be ≥ 1"}` | `candidate_centroids` (or `nprobe`) is 0 |
| `400 Bad Request` | `{"error": "invalid query config: rerank_limit must be ≥ 1 when set"}` | `rerank_limit` is 0 |
| `400 Bad Request` | `{"error": "query vector dimensions do not match the index"}` | Query vector length differs from the manifest `dims` |
| `400 Bad Request` | `{"error": "PQ-compressed indexes currently support only euclidean distance queries"}` | Request overrides `distance_metric` to a non-euclidean value against a PQ-compressed index |
| `500 Internal Server Error` | `{"error": "<message>"}` | Internal search failure |

### Example

```bash
curl -s -X POST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{
    "vector": [0.5, 0.3, 0.8, 0.1],
    "k": 5,
    "nprobe": 3
  }' | python3 -m json.tool
```

### Example with exact reranking

```bash
curl -s -X POST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{
    "vector": [0.5, 0.3, 0.8, 0.1],
    "k": 5,
    "candidate_centroids": 3,
    "rerank": true
  }' | python3 -m json.tool
```

## Exact reranking

When `"rerank": true` is set, the server applies a two-stage pipeline:

1. **ANN stage** – routes the query through the configured fan-out policy and
   retrieves the top-k candidate vectors from the probed shards.
2. **Rerank stage** – fetches the raw float vectors for the ANN candidates
   already loaded in the in-memory raw-shard cache, recomputes exact distances,
   and returns the candidates sorted by those exact scores.

This ensures that the final ordering reflects the true distance metric even
when the ANN stage is only an approximate first pass.

**When to use reranking:**
- When ranking accuracy matters more than raw throughput.
- When you want exact final scores after an approximate routing stage.

**Performance note:** reranking reads the raw vectors for the returned
candidates from the in-memory shard cache, so the extra cost is proportional to
the number of candidates reranked and their dimensionality.

## Weighted hybrid ranking

The `shardlake_index::ranking` module provides `rank_hybrid` and
`HybridRankingPolicy` for blending vector-distance and BM25 lexical scores
into a single deterministically-ordered result list.  The function is
reusable from any query or search path and is independent of transport layers.

### How it works

1. **Normalize** — each candidate list (vector and BM25) is independently
   min–max normalized to the \[0, 1\] range so that scores are on a common
   scale before combining.  Because both score types follow the
   **lower-is-better** convention (vector distances and negated BM25 scores),
   a normalized value of `0.0` means the best candidate in that list and
   `1.0` means the worst.

2. **Combine** — the hybrid score for each candidate is:

    ```
    hybrid_score =
      (vector_weight × vector_norm + bm25_weight × bm25_norm)
      / (vector_weight + bm25_weight)
    ```

    This keeps the final score in the `[0, 1]` range even when the weights do
    not sum to `1.0`. A lower `hybrid_score` remains the best result.

3. **Missing-signal handling** — a candidate that appears in only one list
   receives a normalized score of `1.0` (worst) for the missing signal.
   Its final rank depends on both the weight given to the missing signal and
   its actual score on the available signal.

4. **Tie-breaking** — when two candidates produce an identical `hybrid_score`
   the one with the lower `VectorId` wins, giving a fully deterministic order
   that is stable across runs.

### `HybridRankingPolicy`

| Field | Type | Description |
|-------|------|-------------|
| `vector_weight` | `f32` | Weight applied to the normalized vector-distance score. Must be ≥ 0. |
| `bm25_weight` | `f32` | Weight applied to the normalized BM25 lexical score. Must be ≥ 0. |

At least one weight must be positive.  Weights do not need to sum to 1.

### Examples

**Equal blend (default policy)**

```rust
use shardlake_index::ranking::{HybridRankingPolicy, rank_hybrid};

let policy = HybridRankingPolicy::default(); // vector_weight: 0.5, bm25_weight: 0.5
```

**Pure vector (ignore BM25)**

```rust
let policy = HybridRankingPolicy { vector_weight: 1.0, bm25_weight: 0.0 };
```

**Pure lexical (ignore vector)**

```rust
let policy = HybridRankingPolicy { vector_weight: 0.0, bm25_weight: 1.0 };
```

**Emphasise lexical signal**

```rust
let policy = HybridRankingPolicy { vector_weight: 0.3, bm25_weight: 0.7 };
```

## Rerank candidate limit

By default, when `rerank` is `true`, the server reranks the top `k` ANN
candidates. The `rerank_limit` field widens that candidate pool to an explicit
absolute cap.

When `rerank_limit` is set, the server collects `max(k, rerank_limit)` merged
candidates and passes them all to the reranker, which then returns the final
top-`k`.  A larger pool gives the reranker more candidates to choose from,
improving recall at the cost of reranking more vectors.

| Value | Effect |
|-------|--------|
| absent (default) | Candidate pool is `k` |
| `n` (≥ 1) | Candidate pool is `max(k, n)` |

**Validation:** `rerank_limit` must be ≥ 1 when provided. A value of `0` is
rejected with `"invalid query config: rerank_limit must be ≥ 1 when set"`.

## Per-query distance metric

The `distance_metric` field overrides the distance metric stored in the index
manifest for a single query.  This is useful when comparing the effect of
different metrics without rebuilding the index, or when the index was built with
one metric but you want to evaluate candidates under another.

| Value | Description |
|-------|-------------|
| absent (default) | Use the metric recorded in the index manifest |
| `"cosine"` | Cosine distance: `1 - cosine_similarity` |
| `"euclidean"` | Euclidean (L2) distance: `sqrt(sum((a_i - b_i)²))` |
| `"inner_product"` | Negated dot product: `-dot(a, b)` |

**Note:** Overriding the metric does not affect the IVF shard routing centroid
distances, which always use the routing metric baked into the index at build
time.  Only the per-shard candidate scoring and any subsequent reranking use the
overridden metric.

**PQ limitation:** PQ-compressed indexes currently support only euclidean
distance scoring. Requests that override `distance_metric` to `"cosine"` or
`"inner_product"` against a PQ index are rejected with `400 Bad Request`.

## Notes on scoring

- **Cosine** distance: `score = 1 - cosine_similarity`. Range [0, 2]; 0 means identical direction.
- **Euclidean** distance: `score = sqrt(sum((a_i - b_i)^2))`. Range [0, ∞).
- **Inner product**: `score = -dot(a, b)`. Negated so that lower is always better; the most similar vector has the most negative raw dot product but the smallest (most negative → closest to zero) reported score.
- **BM25**: raw BM25 scores are negated before being returned, so lower is still better and the convention is consistent with all other scorers.

In all cases, results are sorted ascending by score (best match first).

---

## `POST /debug/query-plan`

Returns routing details captured during a single query execution. Intended for
offline debugging and introspection; not recommended for production query
traffic.

This route is exposed only when the server is started with
`shardlake serve --enable-debug-routes`.

Accepts the same request body as `POST /query`, except that `rerank` is ignored.
The response includes the selected IVF centroids, the probed shard IDs, and the
candidate vectors returned by the fan-out search before any reranking would
occur.

### Request body

Identical to [`POST /query`](#post-query), except that `"rerank": true` does
not change the response for this endpoint.

### Success response — `200 OK`

```json
{
  "plan": {
    "selected_centroids": [
      [0.12, 0.45, 0.89],
      [0.33, 0.71, 0.02]
    ],
    "searched_shards": [0, 2],
    "candidate_vectors": [
      { "id": 42, "score": 0.0031, "metadata": {"label": "dog"} },
      { "id": 7,  "score": 0.0157, "metadata": null }
    ]
  },
  "index_version": "idx-v1"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `plan.selected_centroids` | `float[][]` | Centroid vectors selected during IVF routing, in selection order |
| `plan.searched_shards` | `integer[]` | Shard IDs probed after centroid-to-shard mapping and deduplication, in probe order |
| `plan.candidate_vectors` | array | Candidate vectors returned by the fan-out search, before any reranking |
| `plan.candidate_vectors[].id` | integer | Numeric vector id |
| `plan.candidate_vectors[].score` | float | Distance score (lower is better) |
| `plan.candidate_vectors[].metadata` | object or null | Metadata attached to this vector at ingest time |
| `index_version` | string | Index version used to serve this query |

### Error responses

Same error codes and bodies as [`POST /query`](#post-query).

### Example

```bash
curl -s -X POST http://localhost:8080/debug/query-plan \
  -H 'Content-Type: application/json' \
  -d '{
    "vector": [0.5, 0.3, 0.8, 0.1],
    "k": 5,
    "candidate_centroids": 3
  }' | python3 -m json.tool
```

## `GET /metrics`

Returns a Prometheus text-format metrics payload (version 0.0.4) suitable for
scraping by a Prometheus server or any compatible metrics collector.

### Response

The response body is plain text following the
[Prometheus exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/#text-based-format).

**Content-Type:** `text/plain; version=0.0.4; charset=utf-8`

### Exposed metric families

| Metric name | Type | Description |
|-------------|------|-------------|
| `shardlake_query_duration_seconds` | histogram | End-to-end query duration in seconds, bucketed at 1 ms–10 s. |
| `shardlake_queries_total` | counter | Total number of successfully completed queries. |
| `shardlake_query_results_total` | counter | Total number of result vectors returned across all queries (recall-oriented signal). |
| `shardlake_shard_cache_hits_total` | gauge | Cumulative raw-shard cache hit count since server start. |
| `shardlake_shard_cache_misses_total` | gauge | Cumulative raw-shard cache miss count since server start. |
| `shardlake_shard_load_count_total` | gauge | Cumulative shard load attempt count since server start. |
| `shardlake_shard_load_latency_ns_total` | gauge | Cumulative shard-load wall-clock time in nanoseconds since server start. |
| `shardlake_shard_cache_retained_bytes` | gauge | Total raw bytes currently retained in the in-process shard cache. |

> **Note:** Cache hit/miss/load metrics (`shardlake_shard_cache_hits_total`,
> `shardlake_shard_cache_misses_total`, and `shardlake_shard_load_*`) are
> exposed as gauges because they are read from monotonically increasing atomic
> counters inside the searcher at scrape time. By contrast,
> `shardlake_shard_cache_retained_bytes` is recomputed from the live shard
> caches on each scrape and reflects the bytes currently resident in-process.

### Example

```bash
curl -s http://localhost:8080/metrics
```

Example output (truncated):

```
# HELP shardlake_query_duration_seconds End-to-end query duration in seconds.
# TYPE shardlake_query_duration_seconds histogram
shardlake_query_duration_seconds_bucket{le="0.001"} 3
shardlake_query_duration_seconds_bucket{le="0.005"} 5
...
shardlake_query_duration_seconds_count 5
shardlake_query_duration_seconds_sum 0.012

# HELP shardlake_queries_total Total completed query count.
# TYPE shardlake_queries_total counter
shardlake_queries_total 5

# HELP shardlake_shard_cache_hits_total Cumulative raw-shard cache hit count since server start.
# TYPE shardlake_shard_cache_hits_total gauge
shardlake_shard_cache_hits_total 8
```
