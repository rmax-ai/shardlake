# Shardlake

Shardlake is a Rust prototype of a decoupled, billion-scale-inspired vector search system built for personal-scale experimentation.

It demonstrates the architecture pattern of:
- raw embeddings stored separately from serving
- offline index build pipeline (K-means sharding + brute-force within shards)
- immutable, versioned index artifacts
- stateless HTTP query workers with lazy shard loading
- manifest/version-driven artifact lifecycle
- reproducible benchmarking

## Architecture overview

```
┌─────────────────┐     ┌──────────────────┐     ┌───────────────────┐
│  Ingest plane   │────▶│  Index build      │────▶│  Serving plane    │
│  shardlake ingest│    │  shardlake build- │     │  shardlake serve  │
│  raw vectors    │     │  index            │     │  axum HTTP API    │
│  JSONL format   │     │  K-means + shard  │     │  lazy shard cache │
└─────────────────┘     └──────────────────┘     └───────────────────┘
                                │
                         ┌──────▼──────┐
                         │  Manifest   │
                         │  shardlake  │
                         │  publish    │
                         └─────────────┘
                                │
                         ┌──────▼──────┐
                         │  Benchmark  │
                         │  shardlake  │
                         │  benchmark  │
                         └─────────────┘
```

## Quickstart

### Prerequisites

- Rust stable (≥ 1.75)

### Build

```bash
cargo build --release
```

### Generate sample data

```bash
# Use the built-in synthetic dataset generator
./target/release/shardlake generate \
  --num-vectors 1000 \
  --dims 64 \
  --num-clusters 8 \
  --dataset-version ds-generated-v1

# Or generate a standalone JSONL fixture
python3 tools/gen_sample.py --count 1000 --dims 64 > /tmp/vectors.jsonl
```

### Run end-to-end

```bash
# 1. Ingest
./target/release/shardlake ingest --input fixtures/sample_10.jsonl --dataset-version ds-v1

# 2. Build index
./target/release/shardlake build-index \
  --dataset-version ds-v1 \
  --index-version idx-v1 \
  --num-shards 2 \
  --metric cosine

# 3. Publish alias
./target/release/shardlake publish --index-version idx-v1

# 4. Serve
./target/release/shardlake serve &

# 5. Query
curl -s -X POST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{"vector": [0.1, 0.2], "k": 3}'

# 6. Benchmark
./target/release/shardlake benchmark --k 5 --nprobe 2
```

Or use the Makefile:

```bash
make demo
```

## Commands

| Command | Description |
|---------|-------------|
| `shardlake generate` | Generate a reproducible synthetic dataset in versioned storage |
| `shardlake ingest` | Read JSONL vectors into versioned artifact storage |
| `shardlake build-index` | Build K-means shard index from ingested dataset |
| `shardlake publish` | Create/update alias pointer (e.g. `latest`) |
| `shardlake serve` | Start HTTP query server |
| `shardlake benchmark` | Measure recall@k and latency |

Full command reference: [docs/cli-reference.md](docs/cli-reference.md)

## Documentation

| Document | Description |
|----------|-------------|
| [docs/getting-started.md](docs/getting-started.md) | Step-by-step installation and first-run walkthrough |
| [docs/cli-reference.md](docs/cli-reference.md) | All CLI commands, flags, and defaults |
| [docs/api-reference.md](docs/api-reference.md) | HTTP endpoints, request/response schemas |
| [docs/data-formats.md](docs/data-formats.md) | Input JSONL, manifest schema, `.sidx` binary format |
| [docs/configuration.md](docs/configuration.md) | Config fields, tuning guidance, logging |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Component diagram, data flow, trade-offs |
| [DECISIONS.md](DECISIONS.md) | Design decisions and rationale |
| [ROADMAP.md](ROADMAP.md) | Planned improvements |

## Workspace crates

| Crate | Purpose |
|-------|---------|
| `shardlake-core` | Shared types, errors, config |
| `shardlake-storage` | `ObjectStore` trait + local filesystem backend |
| `shardlake-manifest` | Manifest schema and alias lifecycle |
| `shardlake-index` | K-means builder, shard format, ANN searcher |
| `shardlake-serve` | axum HTTP API |
| `shardlake-bench` | Recall@k and latency benchmark harness |
| `shardlake-cli` | CLI binary (`shardlake`) |

## Limitations

- Single-node only (no distributed execution)
- Index is rebuilt offline; no online updates
- K-means sharding is simple; not HNSW or DiskANN
- No authentication or multitenancy
- Vectors stored as JSONL for simplicity (not optimal for large scale)

## Future evolution

See [ROADMAP.md](ROADMAP.md).
