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
| `pq_enabled` | bool | `false` | Enables PQ-compressed shard builds. When `true`, `build-index` trains a PQ codebook, stores `pq_codebook.bin`, and emits format-v2 `.sidx` shards with `compression.codec = "pq8"` in the manifest. |
| `pq_num_subspaces` | u32 | `8` | Number of PQ sub-spaces (`M`) used when `pq_enabled` is `true`. Must be at least 1 and divide the embedding dimension evenly. |
| `pq_codebook_size` | u32 | `256` | Number of centroids (`K`) per PQ sub-space when `pq_enabled` is `true`. Must be in the range `1..=256`. |
| `kmeans_sample_size` | u32 or absent | absent (`None`) | Maximum number of vectors used to train K-means centroids. When absent, all vectors are used. When set to a positive `n` smaller than the dataset size, a reproducible random sample of up to `n` vectors is drawn (using `kmeans_seed`) before running K-means. All vectors—including those not in the sample—are still assigned to the nearest centroid after training, so no data is lost. Recorded in `algorithm.params.kmeans_sample_size` in the manifest only when bounded sampling actually occurs. Equivalent to `--kmeans-sample-size`. |

### `config/default.toml` (reference)

```toml
storage_root = "./data"
num_shards = 4
kmeans_iters = 20
nprobe = 2
kmeans_seed = 3735928559
candidate_shards = 0
max_vectors_per_shard = 0
pq_enabled = false
pq_num_subspaces = 8
pq_codebook_size = 256
# kmeans_sample_size is absent by default (all vectors used for training).
# Set to a positive integer to limit centroid training to a sample:
# kmeans_sample_size = 50000
```

## Fan-out policy

The **fan-out policy** groups the three query routing controls into a single concept.
Together they let you tune the recall–latency trade-off without changing the index itself.

### `candidate_centroids` (alias: `nprobe`)

`candidate_centroids` is the number of IVF centroids ranked nearest to the query vector.
Those centroids determine which shards are eligible to be probed; the final shard set can
still be reduced by `candidate_shards`.

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

`nprobe` is the legacy name for `candidate_centroids`: the number of nearest centroids
selected per query. In practice, the number of probed shards can be lower after
deduplication and any `candidate_shards` cap. It controls the recall–latency trade-off:

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

## Shard loading

The query pipeline's shard-loading stage is pluggable via the
`LoadShardStage` trait.  Two built-in implementations are provided.

### `CachedShardLoader` (default)

The default loader fetches the entire shard file into a `Vec<u8>` via
`ObjectStore::get`, deserializes it, and caches the result in a
`Mutex<HashMap>`.  Compatible with every `ObjectStore` backend.

### `MmapShardLoader` (memory-mapped, local only)

`MmapShardLoader` uses `memmap2` to memory-map large shard files directly
from the local filesystem, avoiding an extra heap allocation for the raw
bytes.  The mapped region is released as soon as deserialization finishes, so
only the deserialized `ShardIndex` is retained in memory.

**Threshold.** Files whose on-disk size is strictly less than
`MMAP_MIN_SIZE_BYTES` (1 MiB by default) are loaded via the regular
`ObjectStore::get` fallback path instead.  Pass a custom threshold to
`MmapShardLoader::with_threshold` to override this.

**Fallback.** If memory mapping fails for any reason (e.g. the OS returns an
error, the platform does not support anonymous mmaps), the loader retries the
load via `ObjectStore::get` automatically and logs a DEBUG message.

**Caching.** Like `CachedShardLoader`, loaded shards are cached in an
in-memory map keyed by shard ID.  Each unique shard is fetched (and
memory-mapped) at most once per loader instance.

**Usage.**  Inject the loader via `QueryPipelineBuilder::with_loader`:

```rust,no_run
use std::sync::Arc;
use shardlake_index::pipeline::{MmapShardLoader, QueryPipeline};
use shardlake_manifest::Manifest;
use shardlake_storage::LocalObjectStore;

fn run(store: Arc<LocalObjectStore>, manifest: Manifest) {
    let pipeline = QueryPipeline::builder(Arc::clone(&store) as Arc<_>, manifest.clone())
        .with_loader(Box::new(MmapShardLoader::new(store, manifest)))
        .build();
    let results = pipeline.run(&[1.0, 0.0], 10, 2).unwrap();
    println!("{} results", results.len());
}
```

`MmapShardLoader` is only useful with `LocalObjectStore`.  For any other
backend, use `CachedShardLoader` (the default).

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
