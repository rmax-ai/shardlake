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

## Shard cache (`CacheConfig`)

The shard cache is configured programmatically via `CacheConfig` (used by
`IndexSearcher::with_cache_config`). The defaults are suitable for most workloads.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `capacity` | usize | `64` | Maximum number of shard indexes kept in RAM simultaneously. When the cache is full the least recently used shard is evicted. |
| `mmap_threshold_bytes` | usize | `67_108_864` (64 MiB) | File-size threshold above which shard files are loaded via `mmap(2)` instead of `read(2)`. Memory-mapped loading avoids copying shard data into a heap buffer and is only available for `LocalFilesystemBackend`; it is ignored for `S3CompatibleBackend`. Set to `usize::MAX` to disable mmap entirely. |

### Tuning guidance

- **`capacity`**: Set to at least `num_shards` to allow the full index to fit in RAM. A
  larger value (e.g. `num_shards * 2`) is useful when multiple index versions are served
  concurrently.
- **`mmap_threshold_bytes`**: Lower this value to use mmap for smaller shards. Set to `0`
  to always use mmap when a local path is available. The default of 64 MiB is a
  conservative starting point; production workloads with large shard files (> 256 MiB)
  should lower this threshold to reduce heap pressure.

## Storage backends

Shardlake's storage layer is abstracted behind the `StorageBackend` trait (which extends
`ObjectStore` with optional filesystem-path access for mmap support).

| Backend | Type | Description |
|---------|------|-------------|
| `LocalFilesystemBackend` | `LocalObjectStore` alias | Stores artifacts as files under `--storage` root. Supports mmap via `path_for_key`. |
| `S3CompatibleBackend` | stub | Placeholder for AWS S3 / MinIO. All operations return an error. `path_for_key` returns `None`. |

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
