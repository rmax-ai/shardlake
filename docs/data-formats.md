# Data Formats

This document describes every file format produced and consumed by Shardlake.

---

## Input: Vector JSONL

The `shardlake ingest` command reads a UTF-8 JSONL file where each non-empty line is one
JSON object:

```json
{"id": 1, "vector": [0.1, 0.2, 0.3, 0.4], "metadata": {"label": "cat"}}
{"id": 2, "vector": [0.9, 0.8, 0.7, 0.6], "metadata": null}
{"id": 3, "vector": [0.5, 0.5, 0.5, 0.5]}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | unsigned integer | Yes | Unique identifier for this vector. Must be a non-negative integer. |
| `vector` | array of numbers | Yes | Embedding values (f32 precision). All records in a file must have the same length. |
| `metadata` | any JSON value | No | Arbitrary JSON attached to this vector. Returned verbatim in search results. |

**Rules:**
- Blank lines are skipped.
- `id` values must be unique across the file (duplicates are not detected at ingest time but will produce undefined search results).
- All vectors in a single ingest run must have the same dimension; mismatches cause an error.

---

## Artifact storage layout

All artifacts are stored under the configured `--storage` root. The layout uses
forward-slash-delimited keys, which map directly to filesystem paths:

```
<storage-root>/
├── datasets/
│   └── <dataset-version>/
│       ├── vectors.jsonl      # re-serialised vector records
│       ├── metadata.json      # id → metadata map (JSON object)
│       └── info.json          # dataset pointer / summary
├── indexes/
│   └── <index-version>/
│       ├── manifest.json      # full manifest (see below)
│       └── shards/
│           ├── shard-0000.sidx
│           ├── shard-0001.sidx
│           └── ...
└── aliases/
    └── <alias-name>.json      # alias pointer (see below)
```

---

## Dataset info pointer (`datasets/<version>/info.json`)

Written by `shardlake ingest`. Contains a quick summary of the dataset for use by
`shardlake build-index` without re-reading the full JSONL file.

```json
{
  "dataset_version": "ds-v1",
  "embedding_version": "emb-v1",
  "dims": 128,
  "count": 10000,
  "vectors_key": "datasets/ds-v1/vectors.jsonl",
  "metadata_key": "datasets/ds-v1/metadata.json"
}
```

---

## Manifest (`indexes/<version>/manifest.json`)

Written by `shardlake build-index`. Ties together the dataset version, embedding version,
and index version and describes every shard artifact.

```json
{
  "manifest_version": 1,
  "dataset_version": "ds-v1",
  "embedding_version": "emb-v1",
  "index_version": "idx-v1",
  "alias": "latest",
  "dims": 128,
  "distance_metric": "cosine",
  "vectors_key": "datasets/ds-v1/vectors.jsonl",
  "metadata_key": "datasets/ds-v1/metadata.json",
  "total_vector_count": 10000,
  "shards": [
    {
      "shard_id": 0,
      "artifact_key": "indexes/idx-v1/shards/shard-0000.sidx",
      "vector_count": 2504,
      "sha256": "a1b2c3d4e5f60708"
    }
  ],
  "build_metadata": {
    "built_at": "2026-03-10T17:44:00Z",
    "builder_version": "0.1.0",
    "num_kmeans_iters": 20,
    "nprobe_default": 2
  }
}
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `manifest_version` | integer | Schema version. Currently always `1`. |
| `dataset_version` | string | Version tag of the source dataset. |
| `embedding_version` | string | Version tag of the embedding generation run. |
| `index_version` | string | Version tag of this index build. |
| `alias` | string | Alias name this manifest was last published under. |
| `dims` | integer | Vector dimension. |
| `distance_metric` | string | `"cosine"`, `"euclidean"`, or `"inner_product"`. |
| `vectors_key` | string | Storage key of the raw vectors JSONL file. |
| `metadata_key` | string | Storage key of the metadata JSON file. |
| `total_vector_count` | integer | Total number of vectors indexed. Must equal the sum of `shards[*].vector_count`. |
| `shards` | array | One entry per non-empty shard (see below). |
| `build_metadata.built_at` | ISO 8601 datetime | When the index was built (UTC). |
| `build_metadata.builder_version` | string | Semver version of the `shardlake` binary that built this index. |
| `build_metadata.num_kmeans_iters` | integer | K-means iterations used. |
| `build_metadata.nprobe_default` | integer | Default nprobe recorded at build time. |

### Shard definition fields

| Field | Type | Description |
|-------|------|-------------|
| `shard_id` | integer | Zero-based shard index. |
| `artifact_key` | string | Storage key of the `.sidx` file for this shard. |
| `vector_count` | integer | Number of vectors stored in this shard. |
| `sha256` | string | FNV-1a fingerprint of the raw shard bytes (prototype; not cryptographic SHA-256). |

---

## Alias pointer (`aliases/<name>.json`)

Written by `shardlake publish`. Maps a human-readable alias to a specific index version.

```json
{
  "alias": "latest",
  "index_version": "idx-v1"
}
```

Updating this file (via `shardlake publish --index-version idx-v2`) is the recommended
way to upgrade the index served by a running or restarting server without hard-coding
version strings.

---

## Shard index binary format (`.sidx`)

Written by `shardlake build-index` for each shard. Binary, little-endian throughout.

```
Offset   Size    Field
------   ----    -----
0        8       Magic bytes: 0x534C4B4944580000 ("SLKIDX\0\0")
8        4       Format version (u32) — currently 1
12       4       Shard ID (u32)
16       4       Vector dimension `dims` (u32)
20       4       Number of centroids `C` (u32)
24       8       Number of vectors `N` (u64)

--- Centroids (C entries) ---
per centroid:
  dims * 4       Centroid coordinates (f32 × dims)

--- Vectors (N entries) ---
per vector:
  8              Vector ID (u64)
  dims * 4       Vector data (f32 × dims)
```

The magic bytes and format version allow the reader to detect corrupt or incompatible
artifacts before parsing any vector data.
