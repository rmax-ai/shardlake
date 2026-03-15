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
  "candidate_centroids": 3,
  "candidate_shards": 2,
  "max_vectors_per_shard": 500
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

## Notes on scoring

- **Cosine** distance: `score = 1 - cosine_similarity`. Range [0, 2]; 0 means identical direction.
- **Euclidean** distance: `score = sqrt(sum((a_i - b_i)^2))`. Range [0, ∞).
- **Inner product**: `score = -dot(a, b)`. Negated so that lower is always better; the most similar vector has the most negative raw dot product but the smallest (most negative → closest to zero) reported score.

In all cases, results are sorted ascending by score (best match first).
