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

Performs retrieval and returns the top-k results. The retrieval signal is
controlled by the `mode` field.

### Request body

```json
{
  "vector": [0.1, 0.2, 0.3],
  "k": 10,
  "nprobe": 3,
  "mode": "hybrid",
  "text": "quick brown fox",
  "alpha": 0.6
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `vector` | `float[]` | Yes for `vector`/`hybrid` | Query vector. Must have the same dimensionality as the index. |
| `k` | integer | Yes | Number of results to return. Must be ≥ 1. |
| `nprobe` | integer | No | Number of shards to probe (vector/hybrid only). Defaults to the server's `--nprobe` value. Higher values improve recall at the cost of latency. |
| `mode` | string | No | Retrieval mode: `"vector"` (default), `"lexical"`, or `"hybrid"`. |
| `text` | string | Yes for `lexical`/`hybrid` | Query text for BM25 scoring. Ignored for mode `"vector"`. |
| `alpha` | float | No | Hybrid blending weight `[0.0, 1.0]`. `1.0` = pure vector, `0.0` = pure lexical. Default `0.5`. Only used for mode `"hybrid"`. |

### Query modes

| Mode | Signal | Requirements |
|------|--------|--------------|
| `vector` | ANN (K-means + exact search within shards) | `vector` required; server needs no BM25 |
| `lexical` | BM25 inverted index | `text` required; server must be started with `--enable-bm25` |
| `hybrid` | Blended vector + BM25 | `vector` and `text` required; server must be started with `--enable-bm25` |

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
| `results[].score` | float | Distance/hybrid score. Lower is always better. |
| `results[].metadata` | object or null | Metadata attached to this vector at ingest time, or `null` if none was provided |
| `index_version` | string | Index version used to serve this query |

### Error responses

| Status | Body | Cause |
|--------|------|-------|
| `400 Bad Request` | `{"error": "k must be > 0"}` | `k` field is 0 |
| `400 Bad Request` | `{"error": "\"vector\" field is required for mode \"vector\""}` | Missing `vector` for vector/hybrid mode |
| `400 Bad Request` | `{"error": "\"text\" field is required for mode \"lexical\""}` | Missing `text` for lexical/hybrid mode |
| `500 Internal Server Error` | `{"error": "<message>"}` | Internal search failure (e.g. dimension mismatch, BM25 index not loaded) |

### Examples

**Vector search (default)**
```bash
curl -s -X POST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{
    "vector": [0.5, 0.3, 0.8, 0.1],
    "k": 5,
    "nprobe": 3
  }' | python3 -m json.tool
```

**Lexical search** (requires `--enable-bm25` at serve time)
```bash
curl -s -X POST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{
    "mode": "lexical",
    "text": "quick brown fox",
    "k": 5
  }' | python3 -m json.tool
```

**Hybrid search** (requires `--enable-bm25` at serve time)
```bash
curl -s -X POST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{
    "mode": "hybrid",
    "vector": [0.5, 0.3, 0.8, 0.1],
    "text": "quick brown fox",
    "k": 5,
    "alpha": 0.6
  }' | python3 -m json.tool
```

## Notes on scoring

- **Vector mode (`cosine`)**: `score = 1 - cosine_similarity`. Range [0, 2]; 0 means identical direction.
- **Vector mode (`euclidean`)**: `score = sqrt(sum((a_i - b_i)^2))`. Range [0, ∞).
- **Vector mode (`inner_product`)**: `score = -dot(a, b)`. Negated so lower is always better.
- **Lexical mode**: `score = -bm25_score`. Negated BM25 so lower is always better.
- **Hybrid mode**: `score = alpha × v_norm + (1 − alpha) × (1 − bm25_norm)`, where both signals are normalised to `[0, 1]`. Range [0, 1]; 0 is best.

In all cases, results are sorted ascending by score (best match first).
