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
  "rerank": true
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
| `400 Bad Request` | `{"error": "query vector dimensions do not match the index"}` | Query vector length differs from the manifest `dims` |
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

## Notes on scoring

- **Cosine** distance: `score = 1 - cosine_similarity`. Range [0, 2]; 0 means identical direction.
- **Euclidean** distance: `score = sqrt(sum((a_i - b_i)^2))`. Range [0, ∞).
- **Inner product**: `score = -dot(a, b)`. Negated so that lower is always better; the most similar vector has the most negative raw dot product but the smallest (most negative → closest to zero) reported score.

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
