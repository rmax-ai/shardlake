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

Performs approximate nearest-neighbour (ANN) search and returns the top-k most similar
vector ids with their scores.

### Request body

```json
{
  "vector": [0.1, 0.2, 0.3],
  "k": 10,
  "nprobe": 3,
  "rerank_limit": 50,
  "distance_metric": "cosine"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `vector` | `float[]` | Yes | Query vector. Must have the same number of dimensions as the index. |
| `k` | integer | Yes | Number of results to return. Must be ≥ 1. |
| `nprobe` | integer | No | Number of shards to probe (candidate_shards). Defaults to the value set via `--nprobe` when the server was started. Higher values improve recall at the cost of latency. |
| `rerank_limit` | integer | No | When set, the pipeline gathers up to `rerank_limit` candidates from all probed shards, re-sorts them by score, and then returns the best `k`. Use a value larger than `k` to improve recall without growing the result set (e.g. `rerank_limit: 50` with `k: 10`). Defaults to the server-level `--rerank-limit` if configured, otherwise no extra candidates are gathered. |
| `distance_metric` | string | No | Distance metric override: `"cosine"`, `"euclidean"`, or `"inner_product"`. When omitted the metric baked into the index manifest at build time is used. |

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
| `500 Internal Server Error` | `{"error": "<message>"}` | Internal search failure (e.g. dimension mismatch) |

### Example

```bash
curl -s -X POST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{
    "vector": [0.5, 0.3, 0.8, 0.1],
    "k": 5,
    "nprobe": 3,
    "rerank_limit": 20,
    "distance_metric": "cosine"
  }' | python3 -m json.tool
```

## Notes on scoring

- **Cosine** distance: `score = 1 - cosine_similarity`. Range [0, 2]; 0 means identical direction.
- **Euclidean** distance: `score = sqrt(sum((a_i - b_i)^2))`. Range [0, ∞).
- **Inner product**: `score = -dot(a, b)`. Negated so that lower is always better; the most similar vector has the most negative raw dot product but the smallest (most negative → closest to zero) reported score.

In all cases, results are sorted ascending by score (best match first).
