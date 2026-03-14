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
| `nprobe` | u32 | `2` | Number of shard centroids to probe during a query (maps to `candidate_centroids`). Equivalent to `--nprobe` on both `build-index` (recorded in the manifest) and `serve` (runtime default). |
| `kmeans_seed` | u64 | `3735928559` (0xdeadbeef) | RNG seed for K-means centroid initialisation. Recorded in `algorithm.params.kmeans_seed` in the manifest. Two builds with the same seed and all other inputs identical produce the same shard layout and fingerprints. Equivalent to `--kmeans-seed`. |
| `candidate_shards` | u32 | `0` | Maximum number of shards to probe after centroid-to-shard deduplication. `0` means no cap. Equivalent to `--candidate-shards` on `serve` and `benchmark`. |
| `max_vectors_per_shard` | u32 | `0` | Maximum number of vectors evaluated inside each probed shard. `0` means no limit. Equivalent to `--max-vectors-per-shard` on `serve` and `benchmark`. |

### `config/default.toml` (reference)

```toml
storage_root = "./data"
num_shards = 4
kmeans_iters = 20
nprobe = 2
kmeans_seed = 3735928559
candidate_shards = 0
max_vectors_per_shard = 0
```

## Fan-out policy

The **fan-out policy** groups the three query routing controls into a single concept.
Together they let you tune the recall–latency trade-off without changing the index itself.

### `candidate_centroids` (alias: `nprobe`)

`candidate_centroids` is the number of IVF centroids ranked nearest to the query vector.
These centroids are mapped to their shards, which are then probed in parallel.

- **Higher** → more shards checked → better recall → higher latency
- **Lower** → fewer shards → faster queries → lower recall

Equivalent to the existing `nprobe` flag; the two names refer to the same field.

### `candidate_shards`

After mapping centroids to shards and deduplicating, `candidate_shards` caps the total
number of shards that are actually probed.  `0` (the default) means no cap—all shards
selected by `candidate_centroids` are probed.

Use this to hard-limit fan-out width independently of how many centroids were selected.

| Value | Effect |
|-------|--------|
| `0` | No cap; all shards selected by `candidate_centroids` are probed |
| `1` | At most one shard is probed regardless of centroid selection |
| `N` | At most N distinct shards are probed |

### `max_vectors_per_shard`

`max_vectors_per_shard` limits how many vectors are scored inside each probed shard.
`0` (the default) means no limit—all vectors in the shard are evaluated.

This knob is useful when shards are large and exact per-shard search is expensive.
Setting a non-zero value reduces per-shard latency at the cost of potentially missing
the true nearest neighbours in the tail of each shard.

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

## Choosing `nprobe` / `candidate_centroids`

`nprobe` is the number of shards probed per query. It controls the recall–latency
trade-off:

- **nprobe = 1**: fastest queries, lowest recall
- **nprobe = num_shards**: equivalent to exact brute-force (perfect recall, maximum latency)

A typical starting point is `nprobe ≈ sqrt(num_shards)`. Measure recall@k with
`shardlake benchmark` and increase `nprobe` until the recall target is met.

## Validation

Invalid fan-out settings are rejected at startup (for `serve` and `benchmark`) and at
request time (for per-request HTTP overrides).  The following invariants are enforced:

- `candidate_centroids` (or `nprobe`) must be ≥ 1. A value of `0` would cause every
  query to return no results and is rejected with:
  `"invalid fan-out policy: candidate_centroids must be ≥ 1"`.
- `candidate_shards` and `max_vectors_per_shard` accept any value including `0`
  (meaning no limit).

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
