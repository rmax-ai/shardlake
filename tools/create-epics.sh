#!/usr/bin/env bash
# create-epics.sh — Create the Shardlake v1.x roadmap parent issue and 10 epic sub-issues.
#
# Usage:
#   ./tools/create-epics.sh [--repo owner/repo] [--dry-run]
#
# Requirements:
#   - gh CLI authenticated with 'issues: write' permission
#   - The target repository must exist
#
# The script:
#   1. Creates labels "epic" and "roadmap" if they do not exist
#   2. Creates a parent umbrella issue with all epic titles listed
#   3. Creates one detailed child issue per epic (10 total)
#   4. Links each child issue to the parent as a sub-issue (GitHub sub-issues API)

set -euo pipefail

REPO="${GH_REPO:-$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || echo "")}"
DRY_RUN=false

# ── argument parsing ────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)   REPO="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    *) echo "Unknown flag: $1"; exit 1 ;;
  esac
done

if [[ -z "$REPO" ]]; then
  echo "ERROR: could not determine repository. Pass --repo owner/repo or run from inside the repo." >&2
  exit 1
fi

echo "Repository : $REPO"
echo "Dry run    : $DRY_RUN"
echo

# ── helpers ─────────────────────────────────────────────────────────────────
create_issue() {
  local title="$1"
  local body="$2"
  local labels="${3:-epic}"
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN] Would create: $title"
    echo "0"
    return
  fi
  local url
  url=$(gh issue create \
    --repo "$REPO" \
    --title "$title" \
    --body "$body" \
    --label "$labels")
  echo "$url" | grep -oE '[0-9]+$'
}

link_sub_issue() {
  local parent="$1"
  local child="$2"
  if [[ "$DRY_RUN" == "true" || "$parent" == "0" || "$child" == "0" ]]; then return; fi
  gh api --method POST "/repos/$REPO/issues/$parent/sub-issues" \
    -f sub_issue_id="$child" 2>/dev/null || true
}

# ── create labels ────────────────────────────────────────────────────────────
echo "Creating labels..."
gh label create "epic"    --color "8B5CF6" --description "Large feature epic" --repo "$REPO" 2>/dev/null || true
gh label create "roadmap" --color "0EA5E9" --description "Roadmap item"       --repo "$REPO" 2>/dev/null || true

# ── parent umbrella issue ─────────────────────────────────────────────────────
echo "Creating parent umbrella issue..."
PARENT_BODY=$(cat <<'EOF'
## Shardlake v1.x Roadmap — 10 Engineering Epics

This umbrella issue tracks the full product roadmap for Shardlake v1.x.
Each child issue below is a self-contained engineering epic with detailed tasks.

### Epics

| # | Epic | Theme |
|---|------|-------|
| 1 | Artifact and Dataset Lifecycle | Immutable, reproducible artifacts |
| 2 | Shard Routing and Partitioning | Reduce fan-out, improve latency |
| 3 | ANN Index Algorithms | IVF, PQ, candidate pipelines |
| 4 | Shard Cache and Storage Layer | Object storage + cache architecture |
| 5 | Query Engine Improvements | Production-like query pipeline |
| 6 | Benchmarking and Evaluation | Systematic measurement tools |
| 7 | Observability | Structured logs, metrics, traces |
| 8 | Hybrid Retrieval | Lexical + vector hybrid search |
| 9 | Distributed Index Build | Parallel index building |
| 10 | Future ANN Research Integration | Research testbed |

### Architecture Evolution

```
v0 (current)
┌─────────────────────────────────────────────────────────┐
│  ingest → build-index → manifest → serve → benchmark   │
│  K-means sharding + brute-force ANN + local storage     │
└─────────────────────────────────────────────────────────┘

v1 (these epics)
┌─────────────────────────────────────────────────────────┐
│  dataset manifest + index manifest + checksum integrity │
│  IVF + PQ compression + centroid routing                │
│  shard cache LRU + mmap + prefetch                      │
│  modular query pipeline + parallel shard search         │
│  observability (prometheus + tracing + debug endpoint)  │
│  hybrid lexical + vector retrieval (BM25)               │
│  parallel / distributed index builds                    │
└─────────────────────────────────────────────────────────┘
```

### Related files

- `crates/shardlake-manifest/` — manifest schema and validation
- `crates/shardlake-index/` — ANN index builder and searcher
- `crates/shardlake-storage/` — storage backend abstraction
- `crates/shardlake-serve/` — HTTP query server
- `crates/shardlake-bench/` — benchmarking harness
- `crates/shardlake-cli/` — command-line interface
- `docs/` — user-facing documentation
EOF
)
PARENT=$(create_issue "Shardlake v1.x Roadmap — 10 Engineering Epics" "$PARENT_BODY" "epic,roadmap")
echo "Parent issue: #$PARENT"

# ── epic 1 ────────────────────────────────────────────────────────────────────
echo "Creating Epic 1..."
E1=$(create_issue "Epic 1 — Artifact and Dataset Lifecycle" "$(cat <<'EOF'
## Epic 1 — Artifact and Dataset Lifecycle

**Goal:** Make dataset, embeddings, and index artifacts first-class immutable objects
with deterministic reproducibility.

### Task 1.1 — Dataset manifest specification

Define canonical schema for dataset manifests (extend `crates/shardlake-manifest/`).

Required fields: `dataset_id`, `dataset_version`, `vector_dimension`, `distance_metric`,
`embedding_model`, `embedding_version`, `shard_count`, `record_count`,
`artifact_locations`, `checksum`.

### Task 1.2 — Index manifest specification

Fields: `index_version`, `dataset_version`, `build_timestamp`, `algorithm`,
`shard_metadata`, `compression_method`, `quantization_parameters`, `recall_estimates`,
`build_duration`. Add `check_compatibility()` for dimension/version/algorithm checks.

### Task 1.3 — Manifest integrity validation

CLI: `shardlake validate-manifest --index-version <v>`
Verify shard files, metadata consistency, dimension match, and artifact checksums.

### Task 1.4 — Deterministic artifact builds

Add `--seed` flag to `build-index`. Pin RNG seed in K-means initialisation.
Log all build parameters. Write build metadata to manifest.

### Task 1.5 — Artifact registry abstraction

Layout: `artifacts/datasets/`, `artifacts/indexes/`, `artifacts/manifests/`.
Define `ArtifactRegistry` trait; implement `LocalArtifactRegistry`.
EOF
)")
link_sub_issue "$PARENT" "$E1"
echo "Epic 1: #$E1"

# ── epic 2 ────────────────────────────────────────────────────────────────────
echo "Creating Epic 2..."
E2=$(create_issue "Epic 2 — Shard Routing and Partitioning" "$(cat <<'EOF'
## Epic 2 — Shard Routing and Partitioning

**Goal:** Improve the vector partitioning strategy to reduce query fan-out and improve latency.

### Task 2.1 — Implement vector clustering

Extend `crates/shardlake-index/src/kmeans.rs` with `fit_sample()` method.
Train centroids on a configurable sample fraction. Store centroid metadata in manifest.

### Task 2.2 — Centroid routing logic

Add `route_to_shards(query, candidate_centroids)` to `IndexSearcher`.
Compute query-to-centroid distances, return top-N shard IDs.

### Task 2.3 — Shard metadata format

```rust
pub struct ShardMeta {
    pub shard_id: ShardId,
    pub centroid_id: usize,
    pub vector_count: usize,
    pub index_type: IndexAlgorithm,
    pub file_location: String,
    pub size_bytes: u64,
    pub checksum: String,
}
```

### Task 2.4 — Query fan-out policy

Config fields: `candidate_centroids`, `candidate_shards`, `max_vectors_per_shard`.
Support per-query override in HTTP request body.

### Task 2.5 — Partition evaluation harness

CLI: `shardlake evaluate-partitioning --dataset-version <v> --index-version <v>`
Metrics: shard size distribution, routing accuracy, recall impact, shard hotness.
EOF
)")
link_sub_issue "$PARENT" "$E2"
echo "Epic 2: #$E2"

# ── epic 3 ────────────────────────────────────────────────────────────────────
echo "Creating Epic 3..."
E3=$(create_issue "Epic 3 — ANN Index Algorithms" "$(cat <<'EOF'
## Epic 3 — ANN Index Algorithms

**Goal:** Introduce real approximate nearest neighbor methods beyond brute-force shards.

### Task 3.1 — Implement IVF index

New file: `crates/shardlake-index/src/ivf.rs`.
`IvfIndex::build(vectors, k, metric)` and `IvfIndex::search(query, top_k, nprobe)`.
Unit tests: recall@10 ≥ 0.8 with nprobe = k/2.

### Task 3.2 — Implement PQ compression

New file: `crates/shardlake-index/src/pq.rs`.
`ProductQuantizer` with `train/encode/decode/asymmetric_distance`.
Compression ratio ≥ 8× vs raw f32 for default params.

### Task 3.3 — Candidate retrieval pipeline

New file: `crates/shardlake-index/src/pipeline.rs`.
Pipeline: centroid routing → IVF candidate selection → PQ approximate distance → top-K.

### Task 3.4 — Exact reranking

`ExactReranker` fetches raw vectors for top-M candidates, computes exact distance.
`--rerank-limit` flag on `serve` command.

### Task 3.5 — Recall evaluation command

CLI: `shardlake eval-ann --k 10 --nprobe 8 --queries <file>`
Metrics: recall@k, precision@k, P50/P95/P99 latency. JSON report output.
EOF
)")
link_sub_issue "$PARENT" "$E3"
echo "Epic 3: #$E3"

# ── epic 4 ────────────────────────────────────────────────────────────────────
echo "Creating Epic 4..."
E4=$(create_issue "Epic 4 — Shard Cache and Storage Layer" "$(cat <<'EOF'
## Epic 4 — Shard Cache and Storage Layer

**Goal:** Simulate the object storage + cache architecture found in production vector systems.

### Task 4.1 — Storage abstraction

Rename/extend `LocalObjectStore` to `LocalFilesystemBackend`. Add `S3CompatibleBackend` stub.
Config: `storage.backend = "local" | "s3"`.

### Task 4.2 — Shard cache system

New file: `crates/shardlake-storage/src/cache.rs`.
`ShardCache` with LRU eviction (`lru` crate), thread-safe via `Mutex`.
`get_or_load(shard_id, backend)` loads shard on miss, returns `Arc<ShardIndex>`.

### Task 4.3 — Cache metrics

```rust
pub struct CacheMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
}
```
Exposed via `GET /metrics` (see Epic 7).

### Task 4.4 — Prefetch policy

`--prefetch-shards N` flag on `serve`. Track per-shard query frequency.
Background `tokio::spawn` task warms cache on startup.

### Task 4.5 — Memory-mapped shard loading

Use `memmap2` crate. `ShardIndex::from_file_mmap()` for large shards.
Configurable mmap threshold in `SystemConfig`.
EOF
)")
link_sub_issue "$PARENT" "$E4"
echo "Epic 4: #$E4"

# ── epic 5 ────────────────────────────────────────────────────────────────────
echo "Creating Epic 5..."
E5=$(create_issue "Epic 5 — Query Engine Improvements" "$(cat <<'EOF'
## Epic 5 — Query Engine Improvements

**Goal:** Turn the basic search service into a production-like query pipeline.

### Task 5.1 — Query pipeline abstraction

New file: `crates/shardlake-index/src/pipeline.rs`.
`PipelineStage` trait (typed input/output). `QueryPipeline` composing stages.
Stages: embedding → centroid routing → shard loading → ANN search → merge → rerank.

### Task 5.2 — Parallel shard search

Use `tokio::task::JoinSet` for concurrent shard searches.
Configurable `max_parallel_shards`. Measurable latency improvement in benchmarks.

### Task 5.3 — Result merge logic

New file: `crates/shardlake-index/src/merge.rs`.
`merge_results(shard_results, top_k)` using BinaryHeap. O(n log k) complexity.
Property-based test: output always sorted, no duplicates, length ≤ k.

### Task 5.4 — Distance metrics

All three metrics in `DistanceMetric::compute(a, b)`: cosine, dot product, euclidean.
Metric selected from `DatasetManifest` at serve time.

### Task 5.5 — Query configuration

Extend `QueryRequest` with optional fields: `candidate_shards`, `rerank_limit`, `distance_metric`.
Server applies per-query overrides over system defaults.
EOF
)")
link_sub_issue "$PARENT" "$E5"
echo "Epic 5: #$E5"

# ── epic 6 ────────────────────────────────────────────────────────────────────
echo "Creating Epic 6..."
E6=$(create_issue "Epic 6 — Benchmarking and Evaluation" "$(cat <<'EOF'
## Epic 6 — Benchmarking and Evaluation

**Goal:** Build systematic measurement tools to track performance regressions and
evaluate retrieval quality.

### Task 6.1 — Benchmark dataset generator

Add `--clusters` and `--cluster-std` flags to `tools/gen_sample.py`.
Add `shardlake generate-dataset` CLI command with same parameters.

### Task 6.2 — Benchmark harness

Extend `shardlake benchmark` with P50/P95/P99 latency (use `hdrhistogram`),
throughput (QPS), and `--output benchmark-report.json` flag.

### Task 6.3 — Workload simulation

Add `--workload cold|warm|mixed` flag. Cold mode clears cache between queries.
Benchmark report includes workload mode.

### Task 6.4 — Cost estimation

CLI: `shardlake estimate-cost --dataset-version <v> --index-version <v>`
Output: dataset size, index size, compression ratio, memory footprint.
Human-readable and JSON output modes.
EOF
)")
link_sub_issue "$PARENT" "$E6"
echo "Epic 6: #$E6"

# ── epic 7 ────────────────────────────────────────────────────────────────────
echo "Creating Epic 7..."
E7=$(create_issue "Epic 7 — Observability" "$(cat <<'EOF'
## Epic 7 — Observability

**Goal:** Expose internal system behavior through structured logs, metrics, and query traces.

### Task 7.1 — Structured logging

Add `#[tracing::instrument]` to all public async functions in `shardlake-index`.
Span fields: shard_id, size_bytes, latency_ms, top_k, candidates_found.
`RUST_LOG=shardlake=trace` produces per-query traces.

### Task 7.2 — Metrics endpoint

New file: `crates/shardlake-serve/src/metrics.rs`.
`GET /metrics` returns Prometheus text format.
Metrics: `shardlake_query_duration_seconds`, `shardlake_shard_cache_hits_total`,
`shardlake_shard_cache_misses_total`, `shardlake_shard_load_duration_seconds`,
`shardlake_recall_at_k`.

### Task 7.3 — Query tracing debug endpoint

`POST /debug/query-plan` returns: centroids selected, shards searched,
candidates per shard, total candidates, top_k returned, latency_ms.
Disabled in production via `debug.enable_query_plan = false` config flag.
EOF
)")
link_sub_issue "$PARENT" "$E7"
echo "Epic 7: #$E7"

# ── epic 8 ────────────────────────────────────────────────────────────────────
echo "Creating Epic 8..."
E8=$(create_issue "Epic 8 — Hybrid Retrieval" "$(cat <<'EOF'
## Epic 8 — Hybrid Retrieval

**Goal:** Add lexical + vector hybrid search for queries where keyword matching
complements semantic similarity.

### Task 8.1 — BM25 inverted index

New file: `crates/shardlake-index/src/bm25.rs`.
`Bm25Index::build(records)` from metadata fields.
`Bm25Index::search(query_terms, top_k)` returning scored results.

### Task 8.2 — Hybrid ranking

New file: `crates/shardlake-index/src/hybrid.rs`.
`HybridRanker::rank(vector_results, bm25_results, top_k)`.
Score formula: `hybrid = α * vector_score + (1-α) * bm25_score`.
Configurable `alpha` (default 0.5) in `QueryRequest`.

### Task 8.3 — Query mode selection

Extend `QueryRequest` with `mode: "vector" | "lexical" | "hybrid"` and `query_text`.
Validate: hybrid requires both `vector` and `query_text` fields.

### Task 8.4 — Hybrid recall evaluation

CLI: `shardlake eval-hybrid --k 10 --alpha 0.5 --queries <file>`
Side-by-side recall comparison: vector vs. lexical vs. hybrid.
EOF
)")
link_sub_issue "$PARENT" "$E8"
echo "Epic 8: #$E8"

# ── epic 9 ────────────────────────────────────────────────────────────────────
echo "Creating Epic 9..."
E9=$(create_issue "Epic 9 — Distributed Index Build" "$(cat <<'EOF'
## Epic 9 — Distributed Index Build

**Goal:** Allow parallel index building to scale to larger datasets.

### Task 9.1 — Parallel shard building

Use `rayon::par_iter()` in `IndexBuilder::build_parallel()`.
`--parallel` flag on `build-index` (default: true).
Benchmark: ≥ 2× faster than sequential on 4+ cores. Deterministic with fixed seed.

### Task 9.2 — Distributed build mode

New `build-shard` sub-command for single-shard builds.
Coordinator spawns N worker processes, waits for all to complete,
then assembles final manifest.

### Task 9.3 — Merge index shards

CLI: `shardlake merge-shards --dataset-version <v> --index-version <v> --shard-dir <dir>`
Validates completeness (no missing shards), assembles manifest, publishes atomically.
Idempotent: same inputs produce same manifest.

### Task 9.4 — Build scheduler CLI

`shardlake build-index --parallel --num-workers 4 --algorithm ivfpq`
Progress tracking per shard. Build duration written to manifest. `--dry-run` flag.
EOF
)")
link_sub_issue "$PARENT" "$E9"
echo "Epic 9: #$E9"

# ── epic 10 ───────────────────────────────────────────────────────────────────
echo "Creating Epic 10..."
E10=$(create_issue "Epic 10 — Future ANN Research Integration" "$(cat <<'EOF'
## Epic 10 — Future ANN Research Integration

**Goal:** Make Shardlake a research testbed for evaluating and comparing ANN algorithms.

### Task 10.1 — DiskANN experiment

New file: `crates/shardlake-index/src/diskann.rs`.
Graph-based index on disk via `memmap2`. Greedy beam search traversal.
Memory usage bounded by `beam_width × vector_size`.

### Task 10.2 — HNSW integration

New file: `crates/shardlake-index/src/hnsw.rs`.
`HnswIndex::build(vectors, m, ef)` and `HnswIndex::search(query, top_k, ef)`.
Recall@10 ≥ 0.95 at ef=50. Serialisable to/from disk.

### Task 10.3 — ANN plugin interface

New file: `crates/shardlake-index/src/ann.rs`.
`AnnIndex` trait: `algorithm()`, `dimension()`, `search()`, `save()`, `load()`, `stats()`.
All index types implement `AnnIndex`. Factory: `AnnIndex::from_algorithm(algo, store, key)`.

### Task 10.4 — ANN benchmark comparison

CLI: `shardlake benchmark-ann --algorithms ivfpq,hnsw,diskann,exact --k 10 --queries 1000`
Side-by-side: recall@k, mean/p99 latency, index size, build time.
JSON report and terminal table output.
EOF
)")
link_sub_issue "$PARENT" "$E10"
echo "Epic 10: #$E10"

# ── summary ───────────────────────────────────────────────────────────────────
echo
echo "═══════════════════════════════════════════════"
echo "  Issue creation complete"
echo "═══════════════════════════════════════════════"
echo "  Parent  : #$PARENT"
echo "  Epic 1  : #$E1"
echo "  Epic 2  : #$E2"
echo "  Epic 3  : #$E3"
echo "  Epic 4  : #$E4"
echo "  Epic 5  : #$E5"
echo "  Epic 6  : #$E6"
echo "  Epic 7  : #$E7"
echo "  Epic 8  : #$E8"
echo "  Epic 9  : #$E9"
echo "  Epic 10 : #$E10"
echo "═══════════════════════════════════════════════"
