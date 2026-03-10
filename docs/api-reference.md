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
  "nprobe": 3
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `vector` | `float[]` | Yes | Query vector. Must have the same number of dimensions as the index. |
| `k` | integer | Yes | Number of results to return. Must be ≥ 1. |
| `nprobe` | integer | No | Number of shards to probe. Defaults to the value set via `--nprobe` when the server was started. Higher values improve recall at the cost of latency. |

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
    "nprobe": 3
  }' | python3 -m json.tool
```

## Notes on scoring

- **Cosine** distance: `score = 1 - cosine_similarity`. Range [0, 2]; 0 means identical direction.
- **Euclidean** distance: `score = sqrt(sum((a_i - b_i)^2))`. Range [0, ∞).
- **Inner product**: `score = -dot(a, b)`. Negated so that lower is always better; the most similar vector has the most negative raw dot product but the smallest (most negative → closest to zero) reported score.

In all cases, results are sorted ascending by score (best match first).

---

## `GET /metrics`

Exposes runtime metrics in [Prometheus text format (version 0.0.4)](https://prometheus.io/docs/instrumenting/exposition_formats/) for scraping by a Prometheus server or compatible tool.

### Response

Plain text in Prometheus exposition format, e.g.:

```
# HELP query_latency_seconds Wall-clock time for a complete ANN search
# TYPE query_latency_seconds histogram
query_latency_seconds_bucket{le="0.005"} 12
...
# HELP shard_cache_hits_total Number of shard loads served from the in-memory cache
# TYPE shard_cache_hits_total counter
shard_cache_hits_total 37
...
```

| Metric | Type | Description |
|--------|------|-------------|
| `query_latency_seconds` | histogram | Wall-clock time (seconds) per `POST /query` call |
| `shard_cache_hits_total` | counter | Shard loads fulfilled from the in-memory cache |
| `shard_load_latency_seconds` | histogram | Time (seconds) to load a shard from storage (cache misses only) |
| `recall_at_k` | gauge | Mean recall@k from the most recent `shardlake benchmark` run |

### Response headers

| Header | Value |
|--------|-------|
| `Content-Type` | `text/plain; version=0.0.4` |

### Example

```bash
curl -s http://localhost:8080/metrics
```

---

## `POST /debug/query-plan`

Returns the full query execution plan for a given query vector. Useful for debugging
shard selection, centroid probing, and reranking behaviour.

### Request body

Same as `POST /query`:

```json
{
  "vector": [0.1, 0.2, 0.3],
  "k": 5,
  "nprobe": 2
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `vector` | `float[]` | Yes | Query vector |
| `k` | integer | Yes | Number of results to return. Must be ≥ 1. |
| `nprobe` | integer | No | Number of shards to probe. Defaults to `--nprobe` server value. |

### Success response — `200 OK`

```json
{
  "plan": {
    "selected_centroids": [[0.1, 0.2], [0.5, 0.8]],
    "searched_shards": [0, 1],
    "candidate_vectors": [
      {"id": 3, "score": 0.01, "metadata": null},
      {"id": 7, "score": 0.04, "metadata": null}
    ],
    "results": [
      {"id": 3, "score": 0.01, "metadata": null}
    ]
  },
  "index_version": "idx-v1"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `plan.selected_centroids` | `float[][]` | Centroid vectors chosen for probing (one per selected centroid index) |
| `plan.searched_shards` | `integer[]` | Shard IDs that were probed |
| `plan.candidate_vectors` | array | All results gathered across probed shards before the final rerank |
| `plan.results` | array | Final top-k results after merging and reranking |
| `index_version` | string | Index version used to serve this query |

### Error responses

Same as `POST /query`.

### Example

```bash
curl -s -X POST http://localhost:8080/debug/query-plan \
  -H 'Content-Type: application/json' \
  -d '{"vector": [0.5, 0.3, 0.8, 0.1], "k": 3, "nprobe": 2}' | python3 -m json.tool
```
