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
| `candidate_centroids` | u32 | `0` | Number of top centroids to evaluate per query during routing. `0` = same as `nprobe`. Equivalent to `--candidate-centroids`. |
| `candidate_shards` | u32 | `0` | Maximum unique shards to search after centroid routing. `0` = same as `nprobe`. Equivalent to `--candidate-shards`. |
| `max_vectors_per_shard` | u32 | `0` | Cap on vectors per shard at build time. Overflow vectors are re-assigned to the next-nearest centroid. `0` = unlimited. Equivalent to `--max-vectors-per-shard`. |
| `kmeans_sample_size` | u32 | `0` | If non-zero, K-means centroids are trained on a random sample of this many vectors instead of the full corpus. `0` = use all vectors. Equivalent to `--kmeans-sample-size`. |

### `config/default.toml` (reference)

```toml
storage_root = "./data"
num_shards = 4
kmeans_iters = 20
nprobe = 2
candidate_centroids = 0
candidate_shards = 0
max_vectors_per_shard = 0
kmeans_sample_size = 0
```

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

## Choosing `candidate_centroids` and `candidate_shards`

These parameters refine the query fan-out policy (Epic 2):

- **`candidate_centroids`** controls how many centroid distances are evaluated
  before selecting shards.  Setting it higher than `nprobe` can improve routing
  quality without necessarily searching more shards (because multiple centroids
  may map to the same shard).
- **`candidate_shards`** caps the number of unique shards actually searched
  after centroid deduplication.

When both are `0` (the default), the effective value for both is `nprobe`,
preserving backward-compatible behaviour.  A typical tuning flow:

1. Set `candidate_centroids = 2 * nprobe` to cast a wider centroid net.
2. Keep `candidate_shards = nprobe` to control actual search fan-out.
3. Verify recall improvement with `shardlake evaluate-partitioning`.

## Choosing `kmeans_sample_size`

For large corpora (> 100 000 vectors), running K-means on the full set can be
expensive.  Setting `kmeans_sample_size` to a few thousand vectors speeds up
centroid training with negligible recall impact, because K-means converges on a
representative sample.

A good heuristic: `kmeans_sample_size ≈ max(10 000, num_shards * 100)`.

## Choosing `max_vectors_per_shard`

`max_vectors_per_shard` prevents extremely imbalanced shards.  When a shard
would exceed this limit, overflow vectors (those furthest from the centroid) are
re-assigned to their next-nearest centroid.

Set to `0` (unlimited) unless you observe severe imbalance in the shard size
distribution reported by `shardlake evaluate-partitioning`.  A reasonable cap
is `2 × (total_vectors / num_shards)` (twice the average shard size).

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
