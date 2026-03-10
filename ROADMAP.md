# Roadmap

## v0.1.0 (done)

- [x] Workspace with 7 crates
- [x] Local filesystem object store
- [x] Manifest schema with alias pointers
- [x] K-means shard partitioning
- [x] Per-shard brute-force search
- [x] Exact search baseline
- [x] HTTP query API (axum)
- [x] Lazy shard loading with in-memory cache
- [x] CLI: ingest, build-index, publish, serve, benchmark
- [x] Recall@k + latency benchmark harness
- [x] Unit and integration tests
- [x] Documentation

## v0.2.0 (next)

- [ ] HNSW per-shard index (replace brute-force)
- [ ] Streaming vector ingest (avoid loading entire corpus into RAM)
- [ ] Shard eviction from cache (LRU)
- [ ] Real SHA-256 shard fingerprints
- [ ] K-means++ initialisation
- [ ] Config file support (TOML)
- [ ] Prometheus metrics endpoint

## Future: object storage

- [ ] S3/MinIO `ObjectStore` implementation
- [ ] Presigned URL support for shard artifacts
- [ ] Artifact versioning with content-addressable storage

## Future: DiskANN/HNSW alternatives

- [ ] Per-shard HNSW index (using pure Rust `hnsw_rs`)
- [ ] Disk-mapped index files (mmap) for serving without RAM loading
- [ ] Compressed vectors (product quantisation)

## Future: hybrid lexical + vector retrieval

- [ ] BM25 inverted index alongside ANN index
- [ ] Score fusion (reciprocal rank fusion or learned scorer)
- [ ] Unified query API: `{"vector": [...], "text": "...", "mode": "hybrid"}`
