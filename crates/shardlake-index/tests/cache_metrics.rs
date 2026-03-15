//! Integration tests for cache metrics on the `CachedShardLoader`.
//!
//! These tests build a real index, drive the loader through hit, miss, and
//! repeated-load scenarios, and assert that the metric counters update
//! correctly.

use std::sync::Arc;

use shardlake_core::{
    config::SystemConfig,
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
    },
};
use shardlake_index::{pipeline::CachedShardLoader, BuildParams, IndexBuilder, LoadShardStage};
use shardlake_storage::{paths, LocalObjectStore, ObjectStore};

// ── helpers ───────────────────────────────────────────────────────────────────

fn make_records(n: usize, dims: usize) -> Vec<VectorRecord> {
    (0..n)
        .map(|i| VectorRecord {
            id: VectorId(i as u64),
            data: (0..dims).map(|d| (i * dims + d) as f32 / 100.0).collect(),
            metadata: None,
        })
        .collect()
}

fn build_index(
    store: &dyn ObjectStore,
    tmp: &std::path::Path,
    tag: &str,
) -> shardlake_manifest::Manifest {
    let config = SystemConfig {
        storage_root: tmp.to_path_buf(),
        num_shards: 2,
        kmeans_iters: 10,
        nprobe: 1,
        kmeans_seed: SystemConfig::default_kmeans_seed(),
        ..SystemConfig::default()
    };
    let records = make_records(20, 4);
    IndexBuilder::new(store, &config)
        .build(BuildParams {
            records,
            dataset_version: DatasetVersion(format!("ds-{tag}")),
            embedding_version: EmbeddingVersion(format!("emb-{tag}")),
            index_version: IndexVersion(format!("idx-{tag}")),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key(&format!("ds-{tag}")),
            metadata_key: paths::dataset_metadata_key(&format!("ds-{tag}")),
            pq_params: None,
        })
        .unwrap()
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// First load of a shard records a miss and increments load count / retained bytes.
#[test]
fn test_first_load_records_miss_and_load_metrics() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let manifest = build_index(store.as_ref(), tmp.path(), "miss");

    let loader =
        CachedShardLoader::new(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest.clone());
    let metrics = loader.metrics();

    let shard_id = manifest.shards[0].shard_id;
    loader.load(shard_id).unwrap();

    let snap = metrics.snapshot();
    assert_eq!(snap.misses, 1, "first load must record one miss");
    assert_eq!(snap.hits, 0, "no hits yet");
    assert_eq!(snap.total_load_count, 1, "one load must be recorded");
    assert!(
        snap.total_load_latency_ns > 0,
        "load latency must be non-zero"
    );
    assert!(
        snap.retained_bytes > 0,
        "retained bytes must be non-zero after a load"
    );
}

/// A second load of the same shard increments the hit counter and leaves the
/// load count unchanged.
#[test]
fn test_second_load_records_hit() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let manifest = build_index(store.as_ref(), tmp.path(), "hit");

    let loader =
        CachedShardLoader::new(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest.clone());
    let metrics = loader.metrics();

    let shard_id = manifest.shards[0].shard_id;
    loader.load(shard_id).unwrap(); // miss
    loader.load(shard_id).unwrap(); // hit

    let snap = metrics.snapshot();
    assert_eq!(snap.misses, 1, "exactly one miss (first load)");
    assert_eq!(snap.hits, 1, "exactly one hit (second load)");
    assert_eq!(
        snap.total_load_count, 1,
        "only one actual storage fetch should occur"
    );
}

/// Loading multiple distinct shards records a miss and load for each one.
#[test]
fn test_multiple_shards_record_individual_misses() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let manifest = build_index(store.as_ref(), tmp.path(), "multi");

    let loader =
        CachedShardLoader::new(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest.clone());
    let metrics = loader.metrics();

    for shard_def in &manifest.shards {
        loader.load(shard_def.shard_id).unwrap();
    }

    let snap = metrics.snapshot();
    let num_shards = manifest.shards.len() as u64;
    assert_eq!(
        snap.misses, num_shards,
        "each distinct shard triggers a miss"
    );
    assert_eq!(snap.hits, 0, "no hits when every shard is loaded once");
    assert_eq!(snap.total_load_count, num_shards, "one load per shard");
}

/// After loading all shards once (cold), a second full sweep should record all hits.
#[test]
fn test_warm_cache_produces_only_hits() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let manifest = build_index(store.as_ref(), tmp.path(), "warm");

    let loader =
        CachedShardLoader::new(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest.clone());
    let metrics = loader.metrics();

    // Cold sweep.
    for shard_def in &manifest.shards {
        loader.load(shard_def.shard_id).unwrap();
    }
    // Warm sweep.
    for shard_def in &manifest.shards {
        loader.load(shard_def.shard_id).unwrap();
    }

    let snap = metrics.snapshot();
    let num_shards = manifest.shards.len() as u64;
    assert_eq!(snap.misses, num_shards, "misses from cold sweep");
    assert_eq!(snap.hits, num_shards, "hits from warm sweep");
    assert_eq!(
        snap.total_load_count, num_shards,
        "storage is only accessed once per shard"
    );
}

/// Hit rate is `0.0` before any load, then converges toward `1.0` as the cache warms.
#[test]
fn test_hit_rate_progression() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let manifest = build_index(store.as_ref(), tmp.path(), "rate");

    let loader =
        CachedShardLoader::new(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest.clone());
    let metrics = loader.metrics();

    // No requests yet.
    assert_eq!(
        metrics.snapshot().hit_rate(),
        0.0,
        "initial hit rate must be 0.0"
    );

    let shard_id = manifest.shards[0].shard_id;

    // First load → miss.
    loader.load(shard_id).unwrap();
    assert_eq!(metrics.snapshot().hit_rate(), 0.0, "all misses → 0.0");

    // Second load → hit.
    loader.load(shard_id).unwrap();
    let snap = metrics.snapshot();
    assert!(
        (snap.hit_rate() - 0.5).abs() < f64::EPSILON,
        "one hit one miss → 0.5 hit rate, got {}",
        snap.hit_rate()
    );
}

/// Retained bytes grow monotonically as new shards are loaded.
#[test]
fn test_retained_bytes_accumulate() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let manifest = build_index(store.as_ref(), tmp.path(), "bytes");

    let loader =
        CachedShardLoader::new(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest.clone());
    let metrics = loader.metrics();

    let mut prev_bytes = 0u64;
    for shard_def in &manifest.shards {
        loader.load(shard_def.shard_id).unwrap();
        let snap = metrics.snapshot();
        assert!(
            snap.retained_bytes > prev_bytes,
            "retained bytes must grow after loading shard {}",
            shard_def.shard_id
        );
        prev_bytes = snap.retained_bytes;
    }
}

/// Metrics shared via `Arc` are observable from another handle without cloning.
#[test]
fn test_metrics_arc_reflects_live_updates() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let manifest = build_index(store.as_ref(), tmp.path(), "arc");

    let loader =
        CachedShardLoader::new(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest.clone());
    let observer = loader.metrics(); // observer Arc, not the loader

    let shard_id = manifest.shards[0].shard_id;
    loader.load(shard_id).unwrap();

    // The observer sees the miss without holding any reference to the loader.
    assert_eq!(observer.snapshot().misses, 1, "observer must see the miss");
}
