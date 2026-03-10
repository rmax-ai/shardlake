# Decisions

## Major assumptions

1. Vectors fit in RAM for indexing (personal-scale prototype, not billion-scale)
2. Queries are read-only; no online writes
3. Metadata is optional and stored as arbitrary JSON
4. A single node serves all queries
5. The K-means seed is fixed (`0xdeadbeef`) for reproducibility

## ANN approach: K-means IVF (coarse clustering + brute-force)

**Chosen:** K-means partitioning with per-shard brute-force search (nprobe parameter controls recall vs speed trade-off).

**Why:** Simple, correct within each shard, easy to validate, doesn't require external libraries. The recall@k benchmark directly measures quality.

**What's next:** Replace per-shard brute-force with HNSW (see ROADMAP).

## Why local object-store abstraction first

Cloud storage (S3, MinIO) adds operational complexity not needed for prototyping. The `ObjectStore` trait is designed to make a future backend drop-in. All artifact I/O goes through this interface.

## Cache design

In-memory `Mutex<HashMap<ShardId, Arc<ShardIndex>>>` per `IndexSearcher` instance. Simple, correct, no external dependencies. For production: consider `DashMap` or `moka` for better concurrency.

## Manifest/versioning choices

- Manifests are immutable after publication
- Aliases are mutable pointers; they can be updated to roll out new index versions
- Manifest version field allows future format evolution
- FNV-1a fingerprint per shard for artifact identity (prototype; replace with SHA-256 for production)

## Serialisation: JSONL for vectors, custom binary for shard indexes

- JSONL for raw vectors: easy to inspect, write, and debug
- Custom binary (`.sidx`) for shard indexes: explicit magic bytes + version field, memory-efficient for large shards

## HTTP framework: axum

Chosen over actix-web because axum's tower-based middleware composes cleanly, has a simpler state-injection model, and integrates naturally with tokio.

## `BuildParams` struct

`IndexBuilder::build` takes a `BuildParams` struct rather than 9 positional arguments to comply with clippy's `too_many_arguments` lint and improve call-site readability.

## Known limitations

- K-means uses a simple random initialisation (not k-means++)
- FNV-1a hash is used instead of real SHA-256 (acceptable for a prototype)
- No streaming ingest; vectors loaded fully into RAM for indexing
- No shard eviction from cache (unbounded memory growth under load)
- JSONL not suitable for very large datasets (no random access)
