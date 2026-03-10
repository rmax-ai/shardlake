# Shardlake Architecture

## Problem statement

Vector search at large scale requires decoupling raw data storage from serving.
Shardlake implements this pattern at personal scale using:
- immutable versioned artifacts
- offline index building
- stateless query serving

## Design goals

1. **Reproducibility**: same raw embeddings always produce the same index
2. **Decoupling**: ingest, build, and serve are separate execution paths
3. **Manifest-driven lifecycle**: the serving layer only knows about what the manifest says
4. **Testability**: each module is independently testable
5. **Simplicity**: the ANN approach (K-means sharding) is simple enough to fully validate

## Component diagram

```
┌─────────────────────────────────────────────────────────────┐
│                       Storage Layer                          │
│                   (LocalObjectStore)                         │
│      key/value interface over local filesystem              │
└──────────────┬───────────────────┬────────────────┬─────────┘
               │                   │                │
        ┌──────▼────┐       ┌──────▼────┐    ┌──────▼────┐
        │  Datasets │       │  Indexes  │    │  Aliases  │
        │  /datasets│       │  /indexes │    │  /aliases │
        └──────┬────┘       └──────┬────┘    └──────┬────┘
               │                   │                │
        ┌──────▼────────────────────▼────────────────▼────┐
        │                   Manifest                       │
        │           schema tying all artifacts together    │
        └──────────────────────┬───────────────────────────┘
                               │
           ┌───────────────────┴───────────────┐
           │                                   │
    ┌──────▼──────┐                    ┌───────▼──────┐
    │IndexBuilder │                    │ IndexSearcher│
    │  K-means    │                    │  nprobe lazy │
    │  sharding   │                    │  load+cache  │
    └─────────────┘                    └──────┬───────┘
                                              │
                                       ┌──────▼──────┐
                                       │  HTTP API   │
                                       │  axum       │
                                       │  /query     │
                                       └─────────────┘
```

## Data flow

### Ingest
1. User provides JSONL file with `{id, vector, metadata}` per line
2. Vectors stored under `datasets/{version}/vectors.jsonl`
3. Metadata stored under `datasets/{version}/metadata.json`
4. Info pointer stored under `datasets/{version}/info.json`

### Build index
1. Load vectors from dataset artifact
2. Run K-means to compute `k` centroids
3. Assign each vector to nearest centroid → shard assignment
4. For each shard: write `ShardIndex` binary artifact (`indexes/{version}/shards/shard-NNNN.sidx`)
5. Write manifest JSON (`indexes/{version}/manifest.json`)

### Publish
1. Load manifest by index version
2. Write alias pointer (`aliases/{alias}.json` → index version)

### Serve
1. Load alias pointer → load manifest
2. For each query: find top-nprobe centroids, load those shards (lazy+cache), brute-force within shards, merge and return top-k

### Benchmark
1. Load corpus + manifest
2. For each query: compute exact top-k (brute force), compute approximate top-k (index), measure recall@k and latency

## Artifact lifecycle

```
raw input (JSONL)
  → ingest → dataset artifact (versioned JSONL + metadata)
    → build-index → shard artifacts (binary .sidx) + manifest (JSON)
      → publish → alias pointer (JSON)
        → serve → query results (JSON HTTP)
```

## Trade-offs

| Choice | Trade-off |
|--------|-----------|
| K-means + brute-force | Simple, fully correct within shard; lower recall than HNSW |
| JSONL for raw vectors | Human-readable, easy to inspect; not optimal at large scale |
| `Mutex<HashMap>` cache | Simple; not optimal for high-concurrency production |
| Local FS object store | Zero external dependencies; not cloud-native |
| Single-pass K-means | Fast; not globally optimal clustering |
