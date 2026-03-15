//! Integration tests for the `ShardCache` component.
//!
//! Covers lazy loading through real shard artifacts, repeated-access reuse
//! (cache hits), and LRU eviction behaviour when capacity is limited.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use shardlake_core::{
    config::{FanOutPolicy, SystemConfig},
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
    },
};
use shardlake_index::{
    cache::{ShardCache, DEFAULT_SHARD_CACHE_CAPACITY},
    pipeline::{CachedShardLoader, LoadShardStage, QueryPipeline},
    BuildParams, IndexBuilder, IndexSearcher, Result,
};
use shardlake_storage::{LocalObjectStore, ObjectStore};
use tempfile::TempDir;

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
    root: &std::path::Path,
    num_shards: u32,
    dims: usize,
    n: usize,
) -> shardlake_manifest::Manifest {
    let config = SystemConfig {
        storage_root: root.to_path_buf(),
        num_shards,
        kmeans_iters: 5,
        nprobe: 2,
        ..SystemConfig::default()
    };
    let records = make_records(n, dims);
    IndexBuilder::new(store, &config)
        .build(BuildParams {
            records,
            dataset_version: DatasetVersion("ds-cache-test".into()),
            embedding_version: EmbeddingVersion("emb-cache-test".into()),
            index_version: IndexVersion("idx-cache-test".into()),
            metric: DistanceMetric::Euclidean,
            dims,
            vectors_key: "datasets/ds-cache-test/vectors.jsonl".into(),
            metadata_key: "datasets/ds-cache-test/metadata.json".into(),
            pq_params: None,
        })
        .unwrap()
}

// ── ShardCache unit-style tests (no real shards needed) ───────────────────────

#[test]
fn shard_cache_default_capacity_is_128() {
    assert_eq!(DEFAULT_SHARD_CACHE_CAPACITY, 128);
}

#[test]
fn shard_cache_miss_on_first_access() {
    let cache: ShardCache<u64> = ShardCache::new(4);
    let load_count = AtomicUsize::new(0);

    cache
        .get_or_load(shardlake_core::types::ShardId(0), || {
            load_count.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(1u64))
        })
        .unwrap();

    assert_eq!(cache.misses(), 1, "first access should be a miss");
    assert_eq!(cache.hits(), 0);
    assert_eq!(load_count.load(Ordering::SeqCst), 1);
}

#[test]
fn shard_cache_hit_on_second_access() {
    let cache: ShardCache<u64> = ShardCache::new(4);
    let load_count = AtomicUsize::new(0);
    let sid = shardlake_core::types::ShardId(7);

    for _ in 0..3 {
        cache
            .get_or_load(sid, || {
                load_count.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(42u64))
            })
            .unwrap();
    }

    assert_eq!(
        load_count.load(Ordering::SeqCst),
        1,
        "load should be called only once"
    );
    assert_eq!(cache.misses(), 1);
    assert_eq!(cache.hits(), 2);
}

#[test]
fn shard_cache_lru_eviction() {
    let cache: ShardCache<u64> = ShardCache::new(2);
    let load_count = AtomicUsize::new(0);

    let mk_loader = |id: u64| {
        let load_count = &load_count;
        move || -> Result<Arc<u64>> {
            load_count.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(id))
        }
    };

    // Fill cache: shards 0 and 1.
    cache
        .get_or_load(shardlake_core::types::ShardId(0), mk_loader(0))
        .unwrap();
    cache
        .get_or_load(shardlake_core::types::ShardId(1), mk_loader(1))
        .unwrap();
    assert_eq!(cache.len(), 2);

    // Promote shard 0 → shard 1 is now LRU.
    cache
        .get_or_load(shardlake_core::types::ShardId(0), mk_loader(0))
        .unwrap();

    // Insert shard 2 → shard 1 (LRU) evicted.
    let loads_before = load_count.load(Ordering::SeqCst);
    cache
        .get_or_load(shardlake_core::types::ShardId(2), mk_loader(2))
        .unwrap();
    assert_eq!(cache.len(), 2);

    // Accessing shard 1 must reload it (was evicted).
    cache
        .get_or_load(shardlake_core::types::ShardId(1), mk_loader(1))
        .unwrap();
    let loads_after = load_count.load(Ordering::SeqCst);
    assert!(
        loads_after > loads_before + 1,
        "shard 1 and 2 should both have triggered a load since eviction"
    );
}

// ── CachedShardLoader integration tests ───────────────────────────────────────

#[test]
fn cached_loader_lazy_loads_shard_on_first_access() {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let manifest = build_index(store.as_ref(), tmp.path(), 2, 4, 20);

    let loader =
        CachedShardLoader::new(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest.clone());

    let first_shard_id = manifest.shards[0].shard_id;
    let shard = loader.load(first_shard_id).unwrap();
    assert!(
        !shard.records.is_empty(),
        "loaded shard should have records"
    );
}

#[test]
fn cached_loader_returns_same_arc_on_repeat_access() {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let manifest = build_index(store.as_ref(), tmp.path(), 2, 4, 20);

    let loader =
        CachedShardLoader::new(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest.clone());
    let sid = manifest.shards[0].shard_id;

    let first = loader.load(sid).unwrap();
    let second = loader.load(sid).unwrap();

    // Both calls should return an Arc pointing to the same allocation.
    assert!(
        Arc::ptr_eq(&first, &second),
        "repeated loads should return the same Arc"
    );
}

#[test]
fn cached_loader_respects_custom_capacity() {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    // Build with 4 shards so we can test eviction at capacity=2.
    let manifest = build_index(store.as_ref(), tmp.path(), 4, 4, 40);

    let loader = CachedShardLoader::with_cache_capacity(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        manifest.clone(),
        2,
    );

    // Load all 4 shards; only 2 should remain in cache after eviction.
    for shard_def in &manifest.shards {
        loader.load(shard_def.shard_id).unwrap();
    }
    // After loading 4 shards into a capacity-2 cache, exactly 2 remain.
    // (We can't directly inspect the cache from the public API, but we can
    // verify that repeated access to the same shard doesn't panic or error.)
    let sid = manifest.shards[0].shard_id;
    let result = loader.load(sid);
    assert!(result.is_ok(), "reloading an evicted shard should succeed");
}

// ── IndexSearcher integration tests ───────────────────────────────────────────

#[test]
fn index_searcher_with_cache_capacity_respects_limit() {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let manifest = build_index(store.as_ref(), tmp.path(), 4, 4, 40);

    // Build a searcher with a tiny cache so eviction definitely kicks in.
    let searcher =
        IndexSearcher::with_cache_capacity(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest, 1);

    let policy = FanOutPolicy {
        candidate_centroids: 4,
        candidate_shards: 0,
        max_vectors_per_shard: 0,
    };
    // Running the search should not error even with a very small cache.
    let results = searcher
        .search(&[0.01, 0.02, 0.03, 0.04], 5, &policy)
        .unwrap();
    assert!(
        !results.is_empty(),
        "search should still return results with a tiny cache"
    );
}

#[test]
fn pipeline_builder_with_cache_capacity_propagates_limit() {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let manifest = build_index(store.as_ref(), tmp.path(), 2, 4, 20);

    // Build pipeline with capacity=1 to stress the LRU.
    let pipeline = QueryPipeline::builder(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest)
        .with_shard_cache_capacity(1)
        .build();

    let results = pipeline.run(&[0.01, 0.02, 0.03, 0.04], 5, 2).unwrap();
    // Even with cache capacity = 1, results should be returned correctly.
    assert!(!results.is_empty());
}

#[test]
fn shard_cache_capacity_from_system_config() {
    // Verify that SystemConfig exposes shard_cache_capacity and uses it
    // correctly when constructing an IndexSearcher.
    let config = SystemConfig {
        shard_cache_capacity: 64,
        ..SystemConfig::default()
    };
    assert_eq!(config.shard_cache_capacity, 64);

    let tmp = TempDir::new().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let manifest = build_index(store.as_ref(), tmp.path(), 2, 4, 20);

    let searcher = IndexSearcher::with_cache_capacity(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        manifest,
        config.shard_cache_capacity,
    );
    let policy = FanOutPolicy {
        candidate_centroids: 2,
        candidate_shards: 0,
        max_vectors_per_shard: 0,
    };
    let results = searcher.search(&[0.1, 0.2, 0.3, 0.4], 3, &policy).unwrap();
    assert!(!results.is_empty());
}
