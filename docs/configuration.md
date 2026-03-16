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
| `shard_cache_capacity` | usize | `128` | Maximum number of shard indexes kept in the in-memory LRU cache at query time. When more than `shard_cache_capacity` distinct shards have been loaded, the least-recently-used shard is evicted to bound memory usage. Set this to at least as large as `nprobe` (or `candidate_shards` when non-zero) so that all shards probed in a single query can stay hot in cache simultaneously. This value must be greater than or equal to `1`. Equivalent to `--shard-cache-capacity` on `serve`. |
| `prefetch.enabled` | bool | `false` | Enable optional shard warming for programmatic consumers that construct an `IndexSearcher` or `CachedShardLoader` with prefetch support. Lazy loading remains unchanged when disabled. |
| `prefetch.min_query_count` | u32 | `3` | Minimum number of probe events a shard must accumulate before it becomes eligible for warming. Must be ≥ 1 when `prefetch.enabled` is `true`. |

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
shard_cache_capacity = 128
# kmeans_sample_size is absent by default (all vectors used for training).
# Set to a positive integer to limit centroid training to a sample:
# kmeans_sample_size = 50000

[prefetch]
enabled = false
min_query_count = 3
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

## Prefetch policy

`PrefetchPolicy` adds optional shard warming on top of the normal lazy-loading
path used by `IndexSearcher` and `CachedShardLoader`.

When `prefetch.enabled = true`, a shard becomes eligible for warming once it
has been probed at least `prefetch.min_query_count` times. On a later cache
miss, eligible hot shards that are not currently resident in the bounded LRU
cache are loaded proactively in the background of that miss-handling path.

This keeps cold shards lazy while reducing follow-up I/O for repeatedly probed
hot shards.

### `prefetch.enabled`

Turns proactive warming on or off. Disabled by default.

### `prefetch.min_query_count`

Controls how many probe events are required before a shard is considered hot.

| Value | Effect |
|-------|--------|
| `0` | Invalid when `enabled = true`; rejected with `"min_query_count must be ≥ 1 when prefetch is enabled"` |
| `1` | A shard becomes eligible for warming after its first probe |
| `3` (default) | A shard becomes eligible after three probes |

## ANN plugin interface

Shardlake exposes a shared ANN plugin interface in `shardlake_index::plugin` that lets
multiple ANN backends be built, queried, and compared through a single extensible
framework.

### `AnnFamily`

`AnnFamily` is the type-safe identifier for an ANN backend. It lives in
`shardlake_core` and is serialisable so it can be embedded in manifests and config.

| Variant | String key | Description |
|---------|-----------|-------------|
| `IvfFlat` | `"ivf_flat"` | Exact (brute-force) distance scoring within each probed shard. Supports all distance metrics. |
| `IvfPq` | `"ivf_pq"` | Product-quantised scoring with asymmetric distance computation. Euclidean metric only. |
| `DiskAnn` | `"diskann"` | Experimental strided-probe backend loosely inspired by DiskANN. Euclidean metric only. See [DiskANN experiment](#diskann-experiment) below. |

Parse from a string with `"ivf_flat".parse::<AnnFamily>()`. Unknown names return a
`CoreError::Other` with the list of valid choices.

### `AnnPlugin` trait

Every ANN backend implements `AnnPlugin`:

```rust
pub trait AnnPlugin: Send + Sync {
    /// Human-readable family identifier, e.g. `"ivf_flat"`.
    fn family(&self) -> &str;

    /// Validate compatibility with a vector dimension and distance metric.
    fn validate(&self, dims: usize, metric: DistanceMetric) -> Result<()>;

    /// Create a `CandidateSearchStage` for use in the query pipeline.
    fn candidate_stage(&self) -> Arc<dyn CandidateSearchStage>;
}
```

Call `plugin.validate(dims, metric)` before building or querying to surface
incompatible configurations early—before any pipeline or artifact is
constructed.  Then call `plugin.candidate_stage()` to wire the backend into a
`QueryPipeline` without algorithm-specific branching:

```rust
// No branching—all families use the same interface.
let plugin: &dyn AnnPlugin = &IvfFlatPlugin;
plugin.validate(dims, DistanceMetric::Cosine).unwrap();
let pipeline = QueryPipeline::builder(store, manifest)
    .candidate_stage(plugin.candidate_stage())
    .build();
```

### Built-in plugin structs

| Struct | Family | Notes |
|--------|--------|-------|
| `IvfFlatPlugin` | `"ivf_flat"` | No extra data needed; constructed directly. |
| `IvfPqPlugin::new(codebook)` | `"ivf_pq"` | Requires a pre-trained `PqCodebook` loaded from storage. |
| `DiskAnnPlugin::new(beam_width)` | `"diskann"` | Experiment; constructed directly with a beam width. |

### `AnnRegistry`

`AnnRegistry` is a stateless helper that enumerates built-in families and
validates family names:

```rust
// Enumerate all built-in families.
for name in AnnRegistry::families() { println!("{name}"); }

// Check if a name is known.
assert!(AnnRegistry::exists("ivf_flat"));
assert!(AnnRegistry::exists("diskann"));
assert!(!AnnRegistry::exists("hnsw"));

// Get a plugin that needs no runtime artifact.
let plugin = AnnRegistry::get_flat("ivf_flat").unwrap();
let plugin = AnnRegistry::get_flat("diskann").unwrap();
```

For `"ivf_pq"`, `AnnRegistry::get_flat` returns a helpful error message
explaining that the codebook must be supplied, which guides callers to
construct `IvfPqPlugin::new(codebook)` directly.

### DiskANN experiment

`DiskAnnPlugin` and its `DiskAnnCandidateStage` implement an experimental
bounded-probe approximation over each shard's flat vector list. The algorithm
uses deterministic strided sampling across each shard; it is only loosely
inspired by DiskANN and does not implement DiskANN's navigable graph or greedy
best-first search.

**Query semantics**

For each probed shard the stage scores exactly `probe_count = max(k, beam_width).min(shard_size)` records,
selected at evenly-spaced positions across the shard, including both endpoints
when more than one record is probed. This bound means:

- Per-shard work is `O(probe_count)`, not `O(shard_size)`.
- When the shard has at least `k` records, the stage always returns exactly `k`
  candidates — even when `k > beam_width`.
- When `probe_count == shard_size` (small shards or large `k`/`beam_width`)
  the stage falls back to an exact flat scan.

**Constraints**

- Only `DistanceMetric::Euclidean` is accepted.  Supplying a different metric
  causes `validate()` to return an error before any pipeline is constructed.

**Beam width**

The `beam_width` parameter controls the candidate budget:

| Scenario | Behaviour |
|---|---|
| `k ≤ beam_width` (typical) | `probe_count = beam_width` — bounded exploration independent of `k` |
| `k > beam_width` | `probe_count = k` — probe set grows to satisfy the result count |
| `probe_count ≥ shard_size` | Degrades to exact flat scan, matching `"ivf_flat"` quality |

The default beam width used by `AnnRegistry::get_flat("diskann")` is
`DISKANN_DEFAULT_BEAM_WIDTH` (64).  Override it by constructing the plugin
directly:

```rust
use shardlake_index::DiskAnnPlugin;

let plugin = DiskAnnPlugin::new(128);
plugin.validate(dims, DistanceMetric::Euclidean)?;
let pipeline = QueryPipeline::builder(store, manifest)
    .candidate_stage(plugin.candidate_stage())
    .build();
```

### Extending with a new backend

1. Implement `AnnPlugin` for a new struct.
2. Call `plugin.validate()` and `plugin.candidate_stage()` from your build or
   query integration code—no changes to the orchestration layer are needed.

## Query execution configuration (`QueryConfig`)

`QueryConfig` is the per-query runtime configuration struct that bundles all
the knobs for a single ANN search.  It is the primary way to express query
behaviour in library code that calls `QueryPipeline::run` directly.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `top_k` | usize | `10` | Number of results to return. Must be ≥ 1. |
| `fan_out` | `FanOutPolicy` | See *Fan-out policy* | Controls centroid and shard selection. |
| `rerank_limit` | usize or absent | absent | Absolute cap on the number of merged candidates passed to the reranker. When absent, falls back to `top_k × rerank_oversample`. Must be ≥ 1 when set. |
| `distance_metric` | `DistanceMetric` or absent | absent | Per-query distance metric override. When absent, the metric stored in the index manifest is used. |

### `rerank_limit`

`rerank_limit` is an alternative to the pipeline-level `rerank_oversample`
multiplier.  Where `rerank_oversample` scales the candidate pool by a factor of
`top_k`, `rerank_limit` sets an absolute ceiling.  When both are configured in
the pipeline builder, the `rerank_limit` in the per-query `QueryConfig` takes
precedence at call time.

- Higher values → more vectors evaluated during reranking → better recall
- Lower values → fewer vectors evaluated → lower reranking latency
- `None` (default) → use `top_k × rerank_oversample`

### `distance_metric` override

When set, `distance_metric` replaces the metric recorded in the index manifest
for candidate scoring and reranking within this query.  The IVF routing phase
(centroid-to-shard assignment) is not affected.

| Value | Distance function |
|-------|-------------------|
| `Cosine` | `1 - cosine_similarity(a, b)` |
| `Euclidean` | `sqrt(sum((a_i - b_i)²))` |
| `InnerProduct` | `-dot(a, b)` (negated so lower = more similar) |

## Validation

Invalid fan-out settings are rejected at startup (for `serve` and `benchmark`) and at
request time (for per-request HTTP overrides). The following invariants are enforced:

- `candidate_centroids` (or `nprobe`) must be ≥ 1. A value of `0` would cause every
  query to return no results and is rejected with:
  `"invalid fan-out policy: candidate_centroids must be ≥ 1"`.
- `candidate_shards` and `max_vectors_per_shard` accept any value including `0`
  (meaning no limit).
- `shard_cache_capacity` must be ≥ 1. A value of `0` is rejected during config
  deserialisation instead of panicking later during cache construction.
- `prefetch.min_query_count` must be ≥ 1 when `prefetch.enabled = true`.
- `QueryConfig::top_k` must be ≥ 1. A value of `0` is rejected with:
  `"invalid query config: top_k must be ≥ 1"`.
- `QueryConfig::rerank_limit` must be ≥ 1 when set. A value of `0` is rejected with:
  `"invalid query config: rerank_limit must be ≥ 1 when set"`.

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

Query-time raw shard loading supports both the default `IndexSearcher` path
and the composable `QueryPipeline` `LoadShardStage` abstraction.

### Default behavior

For any backend that does not expose a validated local filesystem path, raw
shards are fetched through `ObjectStore::get`, deserialized, and cached in
memory. This remains the universal fallback for remote stores and unsupported
environments.

`QueryPipeline` uses `CachedShardLoader` by default, and `IndexSearcher`
automatically follows the same fallback path whenever mmap is unavailable.

### `CachedShardLoader`

`CachedShardLoader` is the explicit query-pipeline implementation of the
default behavior above: it fetches the entire shard file into a `Vec<u8>` via
`ObjectStore::get`, deserializes it, and caches the result in a
`Mutex<HashMap>`. Compatible with every `ObjectStore` backend.

### `MmapShardLoader` (memory-mapped, local only)

`MmapShardLoader` uses `memmap2` to memory-map large shard files directly
from the local filesystem, avoiding an extra heap allocation for the raw
bytes. `IndexSearcher` uses the same mmap fast path automatically when its
store exposes a validated local path. The mapped region is released as soon as
deserialization finishes, so only the deserialized `ShardIndex` is retained in
memory.

**Threshold.** Files whose on-disk size is strictly less than
`MMAP_MIN_SIZE_BYTES` (1 MiB by default) are loaded via the regular
`ObjectStore::get` fallback path instead.  Pass a custom threshold to
`MmapShardLoader::with_threshold` to override this.

**Fallback.** If memory mapping fails for any reason (e.g. the OS returns an
error, file-backed memory mapping is unavailable), the loader retries the
load via `ObjectStore::get` automatically and logs a DEBUG message.

**Caching.** Like `CachedShardLoader`, loaded shards are cached in an
in-memory map keyed by shard ID. Repeated loads can reuse the cached shard
after the first successful fetch.

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

## Cache metrics

`CachedShardLoader` (the default shard-load stage in the query pipeline) maintains
in-process observability counters accessible through the
`shardlake_index::CacheMetrics` type.

### What is tracked

| Counter | Description |
|---------|-------------|
| `hits` | Cumulative number of shard-load requests that were served from cache. |
| `misses` | Cumulative number of shard-load requests that resolved to a known shard and required a storage fetch attempt. |
| `total_load_count` | Total number of storage fetch attempts after cache misses (matches `misses`). |
| `total_load_latency_ns` | Cumulative wall-clock time spent in storage fetch attempts, in nanoseconds, including fetches whose bytes later fail to decode. |
| `retained_bytes` | Total raw artifact bytes successfully inserted into cache after successful decode/load. |

### Accessing metrics

```rust
use std::sync::Arc;
use shardlake_index::{pipeline::CachedShardLoader, CacheMetrics};
use shardlake_manifest::Manifest;
use shardlake_storage::ObjectStore;

fn inspect(store: Arc<dyn ObjectStore>, manifest: Manifest) {
    let loader = CachedShardLoader::new(store, manifest);
    let metrics: Arc<CacheMetrics> = loader.metrics();

    // … run queries through the pipeline …

    let snap = metrics.snapshot();
    println!("hit rate:           {:.1}%", snap.hit_rate() * 100.0);
    println!("mean load latency:  {:.0} µs", snap.mean_load_latency_ns() / 1_000.0);
    println!("retained bytes:     {} B", snap.retained_bytes);
}
```

### Derived statistics

`CacheMetricsSnapshot` exposes two derived statistics:

| Method | Formula | Description |
|--------|---------|-------------|
| `hit_rate()` | `hits / (hits + misses)` | Cache hit rate in `[0.0, 1.0]`. Returns `0.0` when no requests have been observed. |
| `mean_load_latency_ns()` | `total_load_latency_ns / total_load_count` | Mean wall-clock time per storage fetch attempt, in nanoseconds. Returns `0.0` when no loads have occurred. |

### Transport independence

`CacheMetrics` deliberately uses atomic counters rather than emitting to any specific
metrics backend (e.g. Prometheus, StatsD, OpenTelemetry).  Applications that need to
export metrics can poll `CacheMetrics::snapshot()` on a timer and push the values to
whatever collection system they use.
