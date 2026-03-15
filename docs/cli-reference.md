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

### Validation

- Every non-empty input row must contain a numeric `id` and a numeric `vector` array.
- All vectors in the input file must have the same dimensionality. Ingest fails with the offending line number and vector id when a row does not match the dataset dimension.

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
| `--embedding-version <STRING>` | string | dataset manifest `embedding_version` | Embedding version to record in the manifest |
| `--index-version <STRING>` | string | `idx-<timestamp>` | Version tag for the index artifact |
| `--metric <METRIC>` | enum | `cosine` | Distance metric: `cosine`, `euclidean`, or `inner-product` |
| `--num-shards <N>` | u32 | `4` | Number of K-means clusters / shards. Must be greater than 0. |
| `--kmeans-iters <N>` | u32 | `20` | Number of K-means iterations |
| `--kmeans-seed <N>` | u64 | `3735928559` | RNG seed for K-means centroid initialisation. Use the same seed with identical inputs to reproduce shard layout and manifest fingerprints. |
| `--kmeans-sample-size <N>` | u32 | use all vectors | Maximum number of vectors to use for K-means centroid training. Values must be greater than 0. When set below the dataset size, `build-index` draws a reproducible random sample using `--kmeans-seed` before training centroids, then still assigns every vector to its nearest centroid. |
| `--nprobe <N>` | u32 | `2` | Default number of shards to probe at query time (recorded in manifest) |

### Validation

- `--num-shards` must be greater than 0.
- `--kmeans-sample-size`, when provided, must be greater than 0 and is capped to the dataset size before sampling.
- The stored dataset must contain vectors whose dimensions match the dataset metadata written during `ingest`; index building fails if any record is inconsistent.

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
  --kmeans-seed 3735928559 \
  --kmeans-sample-size 50000 \
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

Measures approximate-search quality (Recall@k) and latency by comparing the index output
against an exact brute-force baseline over a sample of the corpus.

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

### Output

Printed to stdout:

```
=== Benchmark Report ===
  Queries:           100
  k:                 10
  nprobe:            2
  Recall@10:         0.9400
  Mean latency:      42.3 µs
  P99  latency:      210.0 µs
  Artifact size:     184320 bytes
```

### Example

```bash
# Full precision benchmark with a larger query sample
shardlake benchmark --k 10 --nprobe 4 --max-queries 500
```

---

## `shardlake validate-manifest`

Validates the integrity of dataset and/or index manifests against their stored
artifacts.  At least one of `--index-version` or `--dataset-version` must be
provided; both may be supplied to validate them together in a single run.

Exits with a **non-zero status code** when any validation failure is found,
making it safe to use as a CI gate.

### Usage

```
shardlake [--storage <PATH>] validate-manifest [OPTIONS]
```

### Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--index-version <STRING>` | string | *(optional)* | Index version to validate; checks the index manifest and every referenced shard artifact |
| `--dataset-version <STRING>` | string | *(optional)* | Dataset version to validate; checks the dataset manifest and its referenced artifact files |

At least one of `--index-version` or `--dataset-version` is required.

### Checks performed

**Index manifest** (`--index-version`):

1. Structural validation of the manifest document.
2. Existence of `vectors_key` and `metadata_key` artifacts.
3. Per-shard: artifact existence, FNV-1a fingerprint match, dimension consistency, vector-count consistency, and centroid consistency.

**Dataset manifest** (`--dataset-version`):

1. Structural validation of the manifest document.
2. Existence of `vectors_key` and `metadata_key` artifacts.

### Exit codes

| Code | Meaning |
|------|---------|
| `0` | All requested manifests are valid |
| non-zero | One or more validation failures were detected (details printed to stderr) |

### Output

On success:

```
index manifest 'idx-v1': OK
dataset manifest 'ds-v1': OK
```

On failure (stderr):

```
index manifest 'idx-v1': 2 failure(s)
  - artifact missing: indexes/idx-v1/shards/shard-0000.sidx
  - fingerprint mismatch for indexes/idx-v1/shards/shard-0001.sidx: expected abc123, actual deadbeef
```

### Example

```bash
# Validate only the index
shardlake validate-manifest --index-version idx-v1

# Validate only the dataset
shardlake validate-manifest --dataset-version ds-v1

# Validate both together (e.g. in a CI pipeline)
shardlake --storage /mnt/data validate-manifest \
  --index-version idx-v1 \
  --dataset-version ds-v1
```

---

## `shardlake evaluate-partitioning`

Evaluates the quality of an existing index partition.  Reports shard size
distribution, routing accuracy, recall impact, and per-shard hotness.

Requires a previously built index (created with `build-index` and optionally
published with `publish`).

### Usage

```
shardlake [--storage <PATH>] evaluate-partitioning [OPTIONS]
```

### Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--index-version <STRING>` | string | *(optional)* | Index version to evaluate; takes precedence over `--alias` when both are supplied |
| `--alias <STRING>` | string | `latest` | Alias to resolve when `--index-version` is not provided |
| `--k <N>` | usize | `10` | Number of nearest neighbours for recall@k evaluation. Must be ≥ 1. |
| `--nprobe <N>` | usize | `2` | Number of shards to probe per query. Must be ≥ 1. |
| `--max-queries <N>` | usize | `0` | Maximum query vectors to use from the corpus (0 = min(corpus size, 100)) |

### Output

Printed to stdout:

```
=== Partition Evaluation Report ===
  Index version:       idx-v1
  Total vectors:       10000
  Shards:              4
  k:                   10
  nprobe:              2
  Queries:             100

Shard Size Distribution:
  shard-0000:      2543 vectors  ( 25.4%)
  shard-0001:      2501 vectors  ( 25.0%)
  shard-0002:      2456 vectors  ( 24.6%)
  shard-0003:      2500 vectors  ( 25.0%)
  Min:         2456    Max: 2543
  Mean:        2500.0  Std dev: 31.1
  Imbalance:   1.017  (max / mean)

Routing & Recall (nprobe=2):
  Routing accuracy:    0.9800
  Recall@10:           0.9400

Shard Hotness (fraction of queries that probe each shard, nprobe=2):
  shard-0000:  0.5100
  shard-0001:  0.4900
  shard-0002:  0.4800
  shard-0003:  0.5200
```

**Shard Size Distribution** fields:

| Field | Description |
|-------|-------------|
| `shard-NNNN: N vectors (PP.P%)` | Per-shard vector count and percentage of total |
| `Min` / `Max` | Smallest and largest shard sizes |
| `Mean` / `Std dev` | Population mean and standard deviation of shard sizes |
| `Imbalance` | `max / mean`; 1.0 = perfectly balanced, higher = more skewed |

**Routing & Recall** fields (only printed when query vectors are available):

| Field | Description |
|-------|-------------|
| `Routing accuracy` | Fraction of queries where the exact top-1 neighbour's assigned shard is among the `nprobe` probed shards. Printed as `n/a` when the manifest was built without centroid metadata (manifest_version < 2). |
| `Recall@k` | Fraction of ground-truth top-k ids returned by approximate search with the specified `nprobe` setting (recall impact of the current partition). |

**Shard Hotness** (only printed when query vectors and centroid metadata are available):

| Field | Description |
|-------|-------------|
| `shard-NNNN: F` | Fraction of evaluated queries that probed this shard. Fractions sum to `nprobe` (each query probes exactly `nprobe` shards). |

### Example

```bash
# Evaluate the "latest" index with default settings
shardlake evaluate-partitioning

# Evaluate a specific version with a larger query sample and more probes
shardlake evaluate-partitioning \
  --index-version idx-v1 \
  --k 10 \
  --nprobe 4 \
  --max-queries 500
```
