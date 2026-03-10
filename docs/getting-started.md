# Getting Started with Shardlake

This guide walks you through installing Shardlake, generating sample data, running the
full pipeline, and querying the HTTP API for the first time.

## Prerequisites

- [Rust stable toolchain](https://rustup.rs) (≥ 1.75)
- `cargo` available in `PATH`
- Python 3 (optional — only needed to generate larger synthetic datasets)

## Build

```bash
git clone <repo-url>
cd shardlake
cargo build --release
```

The compiled binary is placed at `./target/release/shardlake`. You can add it to `PATH`
or invoke it with the relative path throughout this guide.

For a quicker development build (slower at runtime, faster to compile):

```bash
cargo build
# binary at ./target/debug/shardlake
```

## One-command demo

The repository ships with a 10-vector 2D fixture and a `Makefile` target that runs the
complete pipeline automatically:

```bash
make demo
```

Expected output:

```
=== Shardlake demo ===
Ingested 10 vectors (dims=2) → dataset_version=ds-v1
Index built → index_version=idx-v1 (2 shards, 10 vectors)
Published alias 'latest' → index_version=idx-v1
=== Benchmark Report ===
  Queries:           10
  k:                 5
  nprobe:            2
  Recall@5:         0.9200
  Mean latency:      ...
  Artifact size:     ... bytes
=== Demo complete ===
```

## Step-by-step walkthrough

### 1. Ingest

```bash
./target/release/shardlake ingest \
  --input fixtures/sample_10.jsonl \
  --dataset-version ds-v1
```

This reads `fixtures/sample_10.jsonl`, parses each vector record, and writes versioned
artifacts under `./data/datasets/ds-v1/`.

To use a custom storage root:

```bash
./target/release/shardlake --storage /tmp/mydata ingest \
  --input fixtures/sample_10.jsonl \
  --dataset-version ds-v1
```

### 2. Build index

```bash
./target/release/shardlake build-index \
  --dataset-version ds-v1 \
  --index-version idx-v1 \
  --num-shards 2 \
  --metric cosine
```

This partitions the vectors into shards using K-means and writes binary `.sidx` shard
artifacts plus a `manifest.json` under `./data/indexes/idx-v1/`.

### 3. Publish alias

```bash
./target/release/shardlake publish --index-version idx-v1
```

This creates `./data/aliases/latest.json` pointing to `idx-v1`. The server reads this
alias at startup; updating it to a new version is how you roll out index upgrades without
restarting with a hard-coded version string.

### 4. Serve

```bash
./target/release/shardlake serve &
```

The server binds to `0.0.0.0:8080` by default. It loads the manifest at startup and
serves queries with lazy shard loading.

### 5. Query

```bash
curl -s -X POST http://localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{"vector": [0.1, 0.2], "k": 3}' | python3 -m json.tool
```

Sample response:

```json
{
  "results": [
    {"id": 1, "score": 0.0, "metadata": null},
    {"id": 10, "score": 0.0077, "metadata": null},
    {"id": 8, "score": 0.0100, "metadata": null}
  ],
  "index_version": "idx-v1"
}
```

### 6. Benchmark

```bash
./target/release/shardlake benchmark --k 5 --nprobe 2
```

Compares approximate (index) search against exact brute-force ground truth and prints
Recall@k, mean latency, P99 latency, and total artifact size.

## Working with larger datasets

Generate synthetic data with the bundled Python script:

```bash
python3 tools/gen_sample.py --count 10000 --dims 128 --seed 42 > /tmp/vectors.jsonl
```

Then run the same pipeline with more shards:

```bash
./target/release/shardlake ingest \
  --input /tmp/vectors.jsonl \
  --dataset-version ds-128d

./target/release/shardlake build-index \
  --dataset-version ds-128d \
  --index-version idx-128d-v1 \
  --num-shards 16 \
  --kmeans-iters 30 \
  --metric cosine \
  --nprobe 4

./target/release/shardlake publish --index-version idx-128d-v1

./target/release/shardlake benchmark --k 10 --nprobe 4 --max-queries 200
```

## Logging

Shardlake uses the `tracing` crate for structured logs. Control verbosity via the
`RUST_LOG` environment variable:

```bash
RUST_LOG=debug ./target/release/shardlake serve
RUST_LOG=shardlake_index=trace ./target/release/shardlake build-index --dataset-version ds-v1
```

## Running tests

```bash
cargo test
```

All unit and integration tests live alongside the code they test (unit tests in `#[cfg(test)]`
blocks; integration tests in `crates/*/tests/`).
