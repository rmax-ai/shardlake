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

## `shardlake build-index-worker`

Distributed build worker mode.  Splits the index-build workload across
multiple independent workers, each responsible for a subset of shards.  Use
this instead of `build-index` when the dataset is too large to build on a
single machine, or when you want to parallelize shard construction.

The workflow has two phases: **`plan`** and **`execute`**.

### Phase 1 – `plan`

Trains the IVF coarse quantizer, assigns all dataset vectors to shards, and
partitions shards round-robin across `--num-workers` workers.  Writes a
`worker_plan.json` file and the coarse-quantizer artifact to storage.

Run this **once** before launching individual workers.

#### Usage

```
shardlake [--storage <PATH>] build-index-worker --mode plan \
  --dataset-version <STRING> [OPTIONS]
```

#### Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dataset-version <STRING>` | string | *(required)* | Dataset version to index (must match a prior `ingest` run) |
| `--index-version <STRING>` | string | `idx-<timestamp>` | Version tag for the index artifact |
| `--embedding-version <STRING>` | string | dataset manifest `embedding_version` | Embedding version to record in the manifest |
| `--metric <METRIC>` | enum | `cosine` | Distance metric: `cosine`, `euclidean`, or `inner-product` |
| `--num-shards <N>` | u32 | `4` | Number of K-means clusters / shards |
| `--kmeans-iters <N>` | u32 | `20` | Number of K-means iterations |
| `--kmeans-seed <N>` | u64 | `3735928559` | RNG seed for reproducible shard layout |
| `--kmeans-sample-size <N>` | u32 | use all vectors | Maximum vectors for centroid training |
| `--num-workers <N>` | usize | `1` | Number of workers to distribute shards across |

#### Output

Writes to `<storage>/indexes/<index-version>/`:

| File | Description |
|------|-------------|
| `worker_plan.json` | Worker plan JSON (shard assignments, centroids, and dataset keys) |
| `coarse_quantizer.cq` | Trained IVF coarse-quantizer artifact |

#### Example

```bash
# Plan a 4-worker distributed build
shardlake build-index-worker --mode plan \
  --dataset-version ds-v1 \
  --index-version idx-v1 \
  --num-shards 8 \
  --kmeans-seed 3735928559 \
  --num-workers 4
```

---

### Phase 2 – `execute`

Loads the plan for a given index version, reads the dataset vectors, assigns
each vector to its globally nearest shard using the centroids from the plan,
builds the shards assigned to `--worker-id`, and writes shard artifacts and
an `output.json` metadata file to storage.

Run one `execute` invocation per worker ID (`0` through `num_workers - 1`).

#### Usage

```
shardlake [--storage <PATH>] build-index-worker --mode execute \
  --index-version <STRING> --worker-id <N>
```

#### Arguments

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--index-version <STRING>` | string | *(required)* | Index version whose plan to load |
| `--worker-id <N>` | usize | *(required)* | Zero-based worker index |

#### Output

Writes to `<storage>/indexes/<index-version>/`:

| File | Description |
|------|-------------|
| `shards/shard-NNNN.sidx` | Binary shard file for each shard assigned to this worker |
| `workers/<worker-id>/output.json` | Intermediate output metadata (shard IDs, artifact keys, vector counts, fingerprints, centroids) |

The `output.json` file is consumed by the future merge step to assemble the
final `manifest.json` without re-reading shard artifact bytes.

#### Reproducibility

Given the same dataset, `--num-shards`, and `--kmeans-seed`, the plan phase
always produces identical shard centroids and assignments.  Execute workers
therefore always produce identical artifact bytes and fingerprints for the
same inputs, enabling deterministic distributed builds.

#### Example

```bash
# Run all 4 workers (can be parallelised across machines)
for WORKER_ID in 0 1 2 3; do
  shardlake build-index-worker --mode execute \
    --index-version idx-v1 \
    --worker-id $WORKER_ID
done
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
| `--nprobe <N>` | u32 | `2` | Default shard probe count for queries that omit `nprobe` |
| `--candidate-shards <N>` | u32 | `0` | Cap the number of distinct shards probed after centroid-to-shard deduplication; `0` means no cap |
| `--max-vectors-per-shard <N>` | u32 | `0` | Limit how many vectors are scored inside each probed shard; `0` means score the full shard |
| `--shard-cache-capacity <N>` | usize | `128` | Maximum number of loaded shard indexes retained in the in-memory LRU cache |

### Validation

- `--nprobe` must be greater than or equal to 1.
- `--candidate-shards` and `--max-vectors-per-shard` may be `0` to disable their respective caps.
- `--shard-cache-capacity` must be greater than or equal to 1.

### Example

```bash
# Serve the "stable" alias on a non-default port
shardlake serve \
  --alias stable \
  --bind 127.0.0.1:9090 \
  --nprobe 4 \
  --shard-cache-capacity 256
```

See [API Reference](api-reference.md) for the HTTP endpoints.

---

## `shardlake benchmark`

Measures approximate-search quality (Recall@k), throughput, and latency by comparing the index output
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
| `--nprobe <N>` | u32 | `2` | Number of nearest centroids to select per query (`candidate_centroids`) |
| `--candidate-shards <N>` | u32 | `0` | Maximum number of shards to probe after centroid-to-shard deduplication (`0` = no cap) |
| `--max-vectors-per-shard <N>` | u32 | `0` | Maximum number of vectors to score inside each probed shard (`0` = no limit) |
| `--max-queries <N>` | usize | `0` | Maximum query vectors to use (0 = min(corpus size, 100)) |
| `--output <FORMAT>` | enum | `text` | Output format: `text` or `json` |

### Metrics

| Metric | Description |
|--------|-------------|
| Recall@k | Fraction of true top-k neighbours that appear in the retrieved results |
| Mean latency | Average per-query ANN search time in microseconds |
| P99 latency | 99th-percentile per-query ANN search time in microseconds |
| Throughput | Wall-clock query throughput in queries per second (qps) |
| Artifact size | Total size of all index artifact files in bytes |

### Output

**Text (default):**

```
=== Benchmark Report ===
  Queries:           100
  k:                 10
  nprobe:            2
  Recall@10:         0.9400
  Mean latency:      42.3 µs
  P99  latency:      210.0 µs
  Throughput:        23800.0 qps
  Artifact size:     184320 bytes
```

**JSON (`--output json`):**

```json
{
  "num_queries": 100,
  "k": 10,
  "nprobe": 2,
  "recall_at_k": 0.94,
  "mean_latency_us": 42.3,
  "p99_latency_us": 210.0,
  "throughput_qps": 23800.0,
  "artifact_size_bytes": 184320
}
```

### Example

```bash
# Full precision benchmark with a larger query sample
shardlake benchmark --k 10 --nprobe 4 --max-queries 500

# Machine-readable JSON for CI regression tracking
shardlake benchmark --k 10 --nprobe 4 --max-queries 500 --output json
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
| `shard-NNNN: F` | Fraction of evaluated queries that probed this shard. Fractions sum to `min(nprobe, number of shards)` because each query probes at most that many shards. |

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
