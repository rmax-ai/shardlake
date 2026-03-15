# CLI Reference

`shardlake` is the single binary that drives the entire pipeline: ingest → build-index →
publish → serve → benchmark → eval-ann.

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

### Validation

- `--num-vectors`, `--dims`, and `--num-clusters` must each be greater than 0.
- `--cluster-spread` must be finite and non-negative.

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

## `shardlake generate`

Generates a reproducible synthetic benchmark dataset and writes versioned
dataset artifacts to storage.  Cluster centroids are drawn uniformly from
`[-1, 1]^dims`; each vector is a randomly selected centroid perturbed by
uniform noise in `[-cluster-spread, cluster-spread]^dims`.  Identical
arguments and `--seed` values always produce identical `vectors.jsonl`
content, making repeated benchmark evaluations reproducible. The generated
`info.json` manifest still records the current generation timestamp.

### Usage

```
shardlake [--storage <PATH>] generate [OPTIONS]
```

### Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--num-vectors <N>` | usize | `1000` | Total number of vectors to generate |
| `--dims <N>` | usize | `128` | Dimensionality of each generated vector |
| `--num-clusters <N>` | usize | `10` | Number of synthetic clusters |
| `--seed <N>` | u64 | `3735928559` | RNG seed; the same seed always produces the same dataset |
| `--cluster-spread <F>` | f32 | `0.1` | Half-range of uniform noise added per dimension around each cluster centroid |
| `--dataset-version <STRING>` | string | `ds-<timestamp>` | Version tag for the generated dataset |
| `--embedding-version <STRING>` | string | same as `--dataset-version` | Version tag for the embedding artifact |

### Validation

- `--num-vectors`, `--dims`, and `--num-clusters` must each be greater than 0.
- `--cluster-spread` must be finite and non-negative.

### Output

Writes to `<storage>/datasets/<dataset-version>/`:

| File | Description |
|------|-------------|
| `vectors.jsonl` | Generated vector records (id + data, no metadata) |
| `metadata.json` | Empty JSON object `{}` (no per-record metadata) |
| `info.json` | Dataset manifest with `dims`, `vector_count`, and storage keys |

### Example

```bash
# Generate a 10,000-vector, 64-dimensional corpus with 8 clusters
shardlake generate \
  --num-vectors 10000 \
  --dims 64 \
  --num-clusters 8 \
  --seed 3735928559 \
  --dataset-version bench-ds-v1

# Then build an index directly from it
shardlake build-index \
  --dataset-version bench-ds-v1 \
  --index-version bench-idx-v1 \
  --num-shards 8
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

## `shardlake eval-ann`

Evaluates ANN quality by comparing the index output against an exact brute-force baseline
over a sample of the corpus. Reports recall@k, precision@k, and latency metrics in either
human-readable text or machine-readable JSON (for regression tracking).

### Usage

```
shardlake [--storage <PATH>] eval-ann [OPTIONS]
```

### Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--alias <STRING>` | string | `latest` | Alias to evaluate |
| `--k <N>` | usize | `10` | Number of nearest neighbours to retrieve per query |
| `--nprobe <N>` | usize | `2` | Number of shards to probe per query |
| `--max-queries <N>` | usize | `0` | Maximum query vectors to evaluate (0 = min(corpus size, 100)) |
| `--output <FORMAT>` | enum | `text` | Output format: `text` or `json` |

### Metrics

| Metric | Description |
|--------|-------------|
| Recall@k | Fraction of true top-k neighbours that appear in the retrieved results |
| Precision@k | Fraction of retrieved results that are true top-k neighbours |
| Mean latency | Average per-query ANN search time in microseconds |
| P99 latency | 99th-percentile per-query ANN search time in microseconds |

### Output

**Text (default):**

```
=== ANN Evaluation Report ===
  Queries:           100
  k:                 10
  nprobe:            2
  Recall@10:         0.9400
  Precision@10:      0.9400
  Mean latency:      42.3 µs
  P99  latency:      210.0 µs
```

**JSON (`--output json`):**

```json
{
  "num_queries": 100,
  "k": 10,
  "nprobe": 2,
  "recall_at_k": 0.94,
  "precision_at_k": 0.94,
  "mean_latency_us": 42.3,
  "p99_latency_us": 210.0
}
```

### Example

```bash
# Default text output
shardlake eval-ann --k 10 --nprobe 4 --max-queries 500

# Machine-readable JSON for CI regression tracking
shardlake eval-ann --k 10 --nprobe 4 --max-queries 500 --output json
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
