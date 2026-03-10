# Configuration

Shardlake is configured primarily through CLI flags. A `config/default.toml` file
documents the default values for the fields that map to `SystemConfig`, but it is not
loaded automatically—CLI flags always take precedence and are the authoritative way to
set configuration today.

## `SystemConfig` fields

These values are passed to the index builder and serve as defaults when not overridden by
individual command flags.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `storage_root` | path | `./data` | Root directory for all artifact storage. Equivalent to the global `--storage` CLI flag. |
| `num_shards` | u32 | `4` | Number of K-means clusters (shards) to create at index build time. Equivalent to `--num-shards`. |
| `kmeans_iters` | u32 | `20` | Maximum number of K-means iterations. Equivalent to `--kmeans-iters`. |
| `nprobe` | u32 | `2` | Number of shard centroids to probe during a query. Equivalent to `--nprobe` on both `build-index` (recorded in the manifest) and `serve` (runtime default). |

### `config/default.toml` (reference)

```toml
storage_root = "./data"
num_shards = 4
kmeans_iters = 20
nprobe = 2
```

---

## `QueryConfig` fields

Per-query configuration that drives the query pipeline. All fields can be overridden
per-request via the HTTP `/query` endpoint (see [API Reference](api-reference.md)).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `top_k` | usize | *(from request `k`)* | Number of results to return. |
| `candidate_shards` | usize | server `--nprobe` | Number of shards to probe during centroid routing. Corresponds to `nprobe` in the request and `--nprobe` on the CLI. |
| `rerank_limit` | usize or null | server `--rerank-limit` | When set, the pipeline gathers up to `rerank_limit` candidates from all probed shards, re-sorts them, and returns the best `top_k`. Use a value larger than `top_k` to improve recall. |
| `distance_metric` | enum or null | manifest metric | Per-query metric override (`cosine`, `euclidean`, `inner_product`). When `null` the metric stored in the index manifest is used. |

### Pipeline stages

The query engine executes the following stages in order:

1. **embed_query** – accept a pre-computed query vector (pass-through for raw vectors).
2. **route_centroids** – find the `candidate_shards` nearest shard centroids and select
   their shard IDs.
3. **load_shards** – retrieve shard indexes from the in-memory cache or object store.
4. **search_shards** – run exact ANN candidate search on each selected shard.  When
   `candidate_shards > 1` the HTTP server runs these searches concurrently using
   `tokio::task::spawn_blocking`.
5. **merge** – combine shard-local results into a global top-k list, deduplicating by
   vector id.
6. **rerank** – re-sort the merged candidates and trim to `top_k`.  A no-op when
   `rerank_limit` is not configured.

## Choosing `num_shards`

`num_shards` controls the coarseness of the IVF-style partition:

| Scenario | Suggested `num_shards` |
|----------|----------------------|
| < 1 000 vectors | 2–4 |
| 1 000 – 100 000 vectors | 8–32 |
| > 100 000 vectors | 64–256 |

A good rule of thumb: aim for roughly 100–1 000 vectors per shard. Too few shards reduces
the benefit of partitioning; too many shards increases overhead and hurts recall if
`nprobe` is not increased proportionally.

## Choosing `nprobe`

`nprobe` is the number of shards probed per query. It controls the recall–latency
trade-off:

- **nprobe = 1**: fastest queries, lowest recall
- **nprobe = num_shards**: equivalent to exact brute-force (perfect recall, maximum latency)

A typical starting point is `nprobe ≈ sqrt(num_shards)`. Measure recall@k with
`shardlake benchmark` and increase `nprobe` until the recall target is met.

## Logging

Shardlake uses the [`tracing`](https://docs.rs/tracing) crate. Log verbosity is
controlled via the `RUST_LOG` environment variable (parsed by
[`tracing-subscriber`](https://docs.rs/tracing-subscriber)):

```bash
# Show all INFO-and-above logs from every crate
RUST_LOG=info shardlake serve

# Show DEBUG logs only from the index crate
RUST_LOG=shardlake_index=debug shardlake build-index --dataset-version ds-v1

# Suppress all log output
RUST_LOG=off shardlake benchmark
```

Default filter when `RUST_LOG` is unset: `shardlake=info` (shows Shardlake crates at
INFO, everything else at WARN).
