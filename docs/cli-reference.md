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

When `--enable-bm25` is passed the server also loads the full corpus at startup to
build a BM25 inverted index, enabling `lexical` and `hybrid` query modes.

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
| `--enable-bm25` | flag | `false` | Load corpus at startup and build a BM25 index for lexical/hybrid queries |

### Example

```bash
# Serve the "stable" alias on a non-default port
shardlake serve --alias stable --bind 127.0.0.1:9090 --nprobe 4

# Enable hybrid retrieval (loads BM25 index at startup)
shardlake serve --alias latest --enable-bm25
```

See [API Reference](api-reference.md) for the HTTP endpoints.

---

## `shardlake benchmark`

Measures retrieval quality (Recall@k) and latency by comparing the index output
against an exact brute-force baseline over a sample of the corpus.  Supports
`vector`, `lexical`, and `hybrid` query modes for side-by-side recall comparison.

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
| `--mode <MODE>` | enum | `vector` | Query mode: `vector`, `lexical`, or `hybrid` |
| `--alpha <F>` | float | `0.5` | Hybrid blending weight `[0.0, 1.0]`; `1.0` = pure vector, `0.0` = pure lexical. Only used when `--mode hybrid`. |

### Output

Printed to stdout:

```
=== Benchmark Report ===
  Queries:           100
  k:                 10
  nprobe:            2
  Mode:              vector
  Recall@10:         0.9400
  Mean latency:      42.3 µs
  P99  latency:      210.0 µs
  Artifact size:     184320 bytes
```

When `--mode hybrid`, an additional `Alpha` line is printed:

```
  Mode:              hybrid
  Alpha:             0.50
```

### Example

```bash
# Vector-only benchmark
shardlake benchmark --k 10 --nprobe 4 --max-queries 500

# Hybrid benchmark (alpha=0.6 means 60% vector, 40% lexical)
shardlake benchmark --mode hybrid --alpha 0.6 --k 10

# Pure lexical benchmark
shardlake benchmark --mode lexical --k 10
```
