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
| `kmeans_seed` | u64 | `3735928559` (0xdeadbeef) | RNG seed for K-means centroid initialisation. Recorded in `algorithm.params.kmeans_seed` in the manifest. Two builds with the same seed and all other inputs identical produce the same shard layout and fingerprints. Equivalent to `--kmeans-seed`. |
| `kmeans_sample_size` | u32 or absent | absent (`None`) | Maximum number of vectors used to train K-means centroids. When absent, all vectors are used. When set to a positive `n` smaller than the dataset size, a reproducible random sample of up to `n` vectors is drawn (using `kmeans_seed`) before running K-means. All vectors—including those not in the sample—are still assigned to the nearest centroid after training, so no data is lost. Recorded in `algorithm.params.kmeans_sample_size` in the manifest only when bounded sampling actually occurs. Equivalent to `--kmeans-sample-size`. |

### `config/default.toml` (reference)

```toml
storage_root = "./data"
num_shards = 4
kmeans_iters = 20
nprobe = 2
kmeans_seed = 3735928559
# kmeans_sample_size is absent by default (all vectors used for training).
# Set to a positive integer to limit centroid training to a sample:
# kmeans_sample_size = 50000
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

## Storage backends

Shardlake uses the `shardlake_storage::ObjectStore` trait to abstract artifact
persistence.  All storage-key layout helpers live in
[`shardlake_storage::paths`](../crates/shardlake-storage/src/paths.rs) and must be
used instead of hand-constructed strings (see *Artifact storage layout* in
[`data-formats.md`](data-formats.md)).

### Local filesystem backend (`LocalObjectStore`)

The default, production-ready backend. All keys map to paths under the configured
`--storage` root.

Today, the CLI and server always construct `LocalObjectStore` at runtime; there is
not yet a user-facing flag or config setting to select a different backend.

### S3-compatible backend (`S3CompatibleBackend`) — **stub / not yet functional**

> **⚠ This backend is a compile-time stub only.**  Every operation returns an error.
> It exists so that downstream code can target the `ObjectStore` abstraction and compile;
> real S3 network I/O will be added in a follow-up PR.

`S3Config` fields:

| Field | Type | Description |
|-------|------|-------------|
| `endpoint` | string | HTTP(S) endpoint URL, e.g. `https://s3.amazonaws.com` or a MinIO base URL such as `http://localhost:9000`. |
| `bucket` | string | Target bucket name. |
| `region` | string | AWS-style region identifier (e.g. `us-east-1`). For non-AWS S3-compatible services this may be any non-empty string. |
| `access_key_id` | string | AWS access key ID (or equivalent credential). **Do not log this value.** |
| `secret_access_key` | string | AWS secret access key (or equivalent credential). **Do not log this value.** |

**Non-goals for the current stub** (will be addressed in follow-up work):

- Actual HTTP requests to any S3-compatible service
- Authentication / credential refresh
- Multipart upload for large objects
- Presigned URL generation
- Streaming / range-request `get`
- List pagination beyond a single response
- Server-side encryption (SSE)
- Object versioning
- Retry / back-off logic

## Query-time centroid shard routing

`IndexSearcher` implements centroid-based routing rather than a naive fan-out to every
shard.  At search time:

1. **Centroid lookup** — each shard's centroid is read directly from the in-memory
   manifest (`ShardDef.centroid`, present in manifest v2 and later).  No shard bodies
   are loaded during this phase.
2. **Top-`nprobe` selection** — the `nprobe` shards whose centroids are nearest to the
   query vector (by squared Euclidean distance) are selected.
3. **Lazy shard loading** — only the selected probe shards are deserialized from storage
   and cached.  Non-selected shards are never touched during a given query.
4. **Merge** — exact nearest-neighbour search is run within each probed shard and the
   per-shard top-k results are merged into a single ordered list.

For indexes built from a legacy manifest v1 (no `centroid` field in `ShardDef`) the
searcher falls back to loading every shard body to extract its centroid on first use.
Rebuilding the index with the current builder produces a v4 manifest (which still
includes the `centroid` field introduced in v2), restoring the zero-I/O routing path.

The routing centroids are stored in the manifest and verified by
`shardlake validate` (check `ShardCentroidMismatch` in the validation report).

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
