# CLI Reference

`shardlake` is the single binary that drives the entire pipeline: ingest → build-index →
publish → serve → benchmark.

## Global flags

These flags apply to every subcommand and must be placed **before** the subcommand name.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--storage <PATH>` | path | `./data` | Root directory used for all artifact storage |

**Example:**

```bash
shardlake --storage /mnt/fast-disk ingest --input vectors.jsonl --dataset-version ds-v1
```

---

## `shardlake ingest`

Reads a JSONL file of vector records and writes versioned dataset artifacts to storage.

### Usage

```
shardlake [--storage <PATH>] ingest --input <FILE> [OPTIONS]
```

### Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--input <FILE>` | path | *(required)* | Path to the input JSONL file |
| `--dataset-version <STRING>` | string | `ds-<timestamp>` | Version tag for the ingested dataset |
| `--embedding-version <STRING>` | string | same as `--dataset-version` | Version tag for the embedding artifact |

### Output

Writes to `<storage>/datasets/<dataset-version>/`:

| File | Description |
|------|-------------|
| `vectors.jsonl` | Re-serialised vector records (id + data + metadata) |
| `metadata.json` | Map of id → metadata for all records that carry metadata |
| `info.json` | Pointer containing `dataset_version`, `dims`, `count`, and storage keys |

### Example

```bash
shardlake ingest \
  --input /tmp/vectors.jsonl \
  --dataset-version ds-v1 \
  --embedding-version emb-v1
```

---

## `shardlake build-index`

Builds a K-means shard-based ANN index from a previously ingested dataset.

### Usage

```
shardlake [--storage <PATH>] build-index --dataset-version <STRING> [OPTIONS]
```

### Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dataset-version <STRING>` | string | *(required)* | Dataset version to index (must match a prior `ingest` run) |
| `--embedding-version <STRING>` | string | same as `--dataset-version` | Embedding version to record in the manifest |
| `--index-version <STRING>` | string | `idx-<timestamp>` | Version tag for the index artifact |
| `--metric <METRIC>` | enum | `cosine` | Distance metric: `cosine`, `euclidean`, or `inner-product` |
| `--num-shards <N>` | u32 | `4` | Number of K-means clusters / shards |
| `--kmeans-iters <N>` | u32 | `20` | Number of K-means iterations |
| `--nprobe <N>` | u32 | `2` | Default number of shards to probe at query time (recorded in manifest) |

### Output

Writes to `<storage>/indexes/<index-version>/`:

| File | Description |
|------|-------------|
| `manifest.json` | Full manifest JSON (see [Data Formats](data-formats.md)) |
| `shards/shard-NNNN.sidx` | Binary shard index file for each non-empty shard |

### Example

```bash
shardlake build-index \
  --dataset-version ds-v1 \
  --index-version idx-v1 \
  --num-shards 8 \
  --kmeans-iters 30 \
  --metric cosine \
  --nprobe 3
```

---

## `shardlake publish`

Creates or updates an alias pointer that maps a human-readable name (e.g. `latest`) to a
specific index version. The serving layer resolves aliases at startup.

### Usage

```
shardlake [--storage <PATH>] publish --index-version <STRING> [OPTIONS]
```

### Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--index-version <STRING>` | string | *(required)* | Index version to point the alias at |
| `--alias <STRING>` | string | `latest` | Alias name to create or update |

### Output

Writes `<storage>/aliases/<alias>.json` containing the alias → index-version mapping.

### Example

```bash
# Promote idx-v2 to "latest"
shardlake publish --index-version idx-v2

# Create a named alias for a specific release
shardlake publish --index-version idx-v1 --alias stable
```

---

## `shardlake serve`

Starts the HTTP query server. Loads the manifest identified by `--alias` at startup;
individual shard artifacts are loaded lazily on first use and cached in RAM.

### Usage

```
shardlake [--storage <PATH>] serve [OPTIONS]
```

### Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--alias <STRING>` | string | `latest` | Alias name to resolve at startup |
| `--bind <ADDR:PORT>` | string | `0.0.0.0:8080` | TCP address to listen on |
| `--nprobe <N>` | usize | `2` | Default shard probe count for queries that omit `nprobe` |

### Example

```bash
# Serve the "stable" alias on a non-default port
shardlake serve --alias stable --bind 127.0.0.1:9090 --nprobe 4
```

See [API Reference](api-reference.md) for the HTTP endpoints.

---

## `shardlake benchmark`

Measures approximate-search quality (Recall@k), latency, throughput, and cost by comparing
the index output against an exact brute-force baseline over a sample of the corpus.

### Usage

```
shardlake [--storage <PATH>] benchmark [OPTIONS]
```

### Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--alias <STRING>` | string | `latest` | Alias to benchmark |
| `--k <N>` | usize | `10` | Number of nearest neighbours to retrieve |
| `--nprobe <N>` | usize | `2` | Number of shards to probe per query |
| `--max-queries <N>` | usize | `0` | Maximum query vectors to use (0 = min(corpus size, 100)) |
| `--workload <KIND>` | enum | `warm` | Query workload: `cold`, `warm`, or `mixed` |

#### Workload kinds

| Kind | Behaviour |
|------|-----------|
| `warm` | Shard cache is pre-warmed; all measurements are cache hits |
| `cold` | Shard cache is cleared before every query (simulates cold start) |
| `mixed` | Even-indexed queries are cold, odd-indexed are warm (50/50 split) |

### Output

Printed to stdout:

```
=== Benchmark Report ===
  Workload:          warm
  Queries:           100
  k:                 10
  nprobe:            2
  Recall@10:         0.9400
  Mean latency:      42.3 µs
  P99  latency:      210.0 µs
  Throughput:        23616.1 QPS

=== Cost Estimates ===
  Index size:        184320 bytes
  Raw vectors size:  512000 bytes
  Memory (est.):     184320 bytes
  Compression ratio: 2.778x
```

### Example

```bash
# Full precision warm benchmark with a larger query sample
shardlake benchmark --k 10 --nprobe 4 --max-queries 500

# Measure cold-start latency
shardlake benchmark --k 10 --nprobe 4 --workload cold

# Simulate a realistic mixed workload
shardlake benchmark --k 10 --nprobe 4 --workload mixed --max-queries 200
```

---

## `shardlake generate-dataset`

Generates a synthetic JSONL vector dataset suitable for use with `shardlake ingest`.

Vectors are drawn from Gaussian distributions centred on randomly placed cluster
centroids. All parameters influence the random generator; results are reproducible
given the same `--seed`.

### Usage

```
shardlake generate-dataset [OPTIONS]
```

### Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dims <N>` | usize | `128` | Number of dimensions per vector |
| `--num-vectors <N>` | usize | `10000` | Total number of vectors to generate |
| `--num-clusters <N>` | usize | `10` | Number of clusters (controls cluster structure) |
| `--cluster-spread <F>` | f32 | `0.1` | Standard deviation of each component around its centroid |
| `--seed <N>` | u64 | `42` | Random seed for reproducible generation |
| `--output <FILE>` | path | `generated.jsonl` | Output JSONL file path |

### Output

Writes a JSONL file where each line is:

```json
{"id": 1, "vector": [0.12, -0.45, ...], "metadata": {"cluster": 0}}
```

The `metadata.cluster` field records which cluster centroid each vector was drawn
from, useful for ground-truth recall analysis.

### Example

```bash
# Generate 50 000 vectors in 64 dims with 20 clusters, reproducible from seed 99
shardlake generate-dataset \
  --dims 64 \
  --num-vectors 50000 \
  --num-clusters 20 \
  --seed 99 \
  --output /tmp/bench_vectors.jsonl

# Immediately ingest the result
shardlake ingest --input /tmp/bench_vectors.jsonl --dataset-version synth-v1
```
