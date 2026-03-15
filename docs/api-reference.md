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
  "nprobe": 3,
  "rerank": true
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `vector` | `float[]` | Yes | Query vector. Must have the same number of dimensions as the index. |
| `k` | integer | Yes | Number of results to return. Must be ≥ 1. |
| `nprobe` | integer | No | Number of shards to probe. Defaults to the value set via `--nprobe` when the server was started. Higher values improve recall at the cost of latency. |
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
    "nprobe": 3,
    "rerank": true
  }' | python3 -m json.tool
```

## Exact reranking

When `"rerank": true` is set, the server applies a two-stage pipeline:

1. **ANN stage** – probes the `nprobe` nearest shards using centroid routing
   and retrieves the top-k candidate vectors (same as the default path).
2. **Rerank stage** – fetches the raw float vectors for every ANN candidate
   from the in-memory shard cache and recomputes the exact distance between
   the query and each candidate.  The candidates are then re-sorted by their
   exact scores and the final top-k list is returned.

This ensures that the returned scores and ranking accurately reflect the
true distances between the query and each result, even when the ANN phase
would otherwise return a slightly suboptimal ordering (e.g. when future
compression such as product quantisation is introduced).

**When to use reranking:**
- When ranking accuracy matters more than raw throughput.
- When the index was built with a lossy compression method and you want
  the final scores to reflect exact distances.

**Performance note:** reranking loads and scans the raw vectors of the ANN
candidates from the in-memory shard cache.  The overhead is proportional to
`k × dims` and is typically small relative to the ANN stage.

## Notes on scoring

- **Cosine** distance: `score = 1 - cosine_similarity`. Range [0, 2]; 0 means identical direction.
- **Euclidean** distance: `score = sqrt(sum((a_i - b_i)^2))`. Range [0, ∞).
- **Inner product**: `score = -dot(a, b)`. Negated so that lower is always better; the most similar vector has the most negative raw dot product but the smallest (most negative → closest to zero) reported score.

In all cases, results are sorted ascending by score (best match first).
