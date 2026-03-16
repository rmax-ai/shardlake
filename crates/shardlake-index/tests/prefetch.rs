//! Focused tests for the shard prefetch policy and cache eviction.
//!
//! These tests exercise:
//! - `CachedShardLoader` with prefetching disabled (lazy cold-path).
//! - `CachedShardLoader` with prefetching enabled (hot-shard warming).
//! - Interaction between the prefetch policy and cache capacity limits.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use shardlake_core::{
    config::{PrefetchPolicy, SystemConfig},
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
    },
};
use shardlake_index::{
    pipeline::{CachedShardLoader, LoadShardStage},
    BuildParams, IndexBuilder,
};
use shardlake_storage::{paths, LocalObjectStore, ObjectStore, StorageError};

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

/// Wraps a `LocalObjectStore` and counts `.sidx` shard artifact fetches.
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    shard_loads: Arc<AtomicUsize>,
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> (Arc<Self>, Arc<AtomicUsize>) {
        let counter = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(Self {
            inner,
            shard_loads: Arc::clone(&counter),
        });
        (store, counter)
    }
}

impl ObjectStore for CountingStore {
    fn put(&self, key: &str, data: Vec<u8>) -> Result<(), StorageError> {
        self.inner.put(key, data)
    }
    fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        if key.ends_with(".sidx") {
            self.shard_loads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key)
    }
    fn exists(&self, key: &str) -> Result<bool, StorageError> {
        self.inner.exists(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.inner.delete(key)
    }
}

/// Build a small index and return the manifest plus a fresh counting store.
fn build_index(
    tmp: &tempfile::TempDir,
    num_shards: usize,
    tag: &str,
) -> (
    shardlake_manifest::Manifest,
    Arc<CountingStore>,
    Arc<AtomicUsize>,
) {
    let base = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = SystemConfig {
        storage_root: tmp.path().to_path_buf(),
        num_shards: num_shards as u32,
        kmeans_iters: 10,
        nprobe: 2,
        kmeans_seed: SystemConfig::default_kmeans_seed(),
        ..SystemConfig::default()
    };
    let records = make_records(num_shards * 10, 4);
    let manifest = IndexBuilder::new(base.as_ref(), &config)
        .build(BuildParams {
            records,
            dataset_version: DatasetVersion(tag.into()),
            embedding_version: EmbeddingVersion(format!("emb-{tag}")),
            index_version: IndexVersion(format!("idx-{tag}")),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key(tag),
            metadata_key: paths::dataset_metadata_key(tag),
            pq_params: None,
            ann_family: None,
            hnsw_config: None,
        })
        .unwrap();
    let (counting, counter) = CountingStore::new(base as Arc<dyn ObjectStore>);
    (manifest, counting, counter)
}

// ── policy validation ─────────────────────────────────────────────────────────

/// A disabled policy with any `min_query_count` value is valid.
#[test]
fn prefetch_policy_validate_disabled_always_ok() {
    let policy = PrefetchPolicy {
        enabled: false,
        min_query_count: 0,
    };
    assert!(policy.validate().is_ok());
}

/// An enabled policy with `min_query_count = 0` must be rejected.
#[test]
fn prefetch_policy_validate_enabled_zero_count_is_invalid() {
    let policy = PrefetchPolicy {
        enabled: true,
        min_query_count: 0,
    };
    assert!(policy.validate().is_err());
}

/// An enabled policy with `min_query_count >= 1` is valid.
#[test]
fn prefetch_policy_validate_enabled_nonzero_count_is_ok() {
    let policy = PrefetchPolicy {
        enabled: true,
        min_query_count: 1,
    };
    assert!(policy.validate().is_ok());
}

// ── disabled prefetch ─────────────────────────────────────────────────────────

/// With no prefetch policy, only the explicitly requested shard is loaded from
/// the store; no extra loads occur.
#[test]
fn loader_no_policy_loads_only_requested_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let (manifest, store, counter) = build_index(&tmp, 3, "no-policy");

    let loader =
        CachedShardLoader::new(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest.clone());

    let shard_id = manifest.shards[0].shard_id;
    loader.load(shard_id).unwrap();

    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "exactly one store read expected when prefetch is disabled"
    );
}

/// With an explicitly disabled policy the loader must not warm extra shards
/// even when shards have high access counts.
#[test]
fn loader_disabled_policy_no_extra_loads() {
    let tmp = tempfile::tempdir().unwrap();
    let (manifest, store, counter) = build_index(&tmp, 3, "disabled-policy");

    let policy = PrefetchPolicy {
        enabled: false,
        min_query_count: 1,
    };
    let loader = CachedShardLoader::with_cache_capacity(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        manifest.clone(),
        1,
    )
    .with_prefetch(policy);

    let s0 = manifest.shards[0].shard_id;
    let s1 = manifest.shards[1].shard_id;

    loader.load(s0).unwrap(); // miss → 1 load
    loader.load(s1).unwrap(); // miss → 1 load; s0 evicted, not warmed back

    // Exactly two store reads: no warming took place.
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

// ── enabled prefetch ──────────────────────────────────────────────────────────

/// When prefetch is enabled and a shard's access count exceeds the threshold
/// but it is no longer in cache (evicted), it must be warmed back on the next
/// cache-miss event.
#[test]
fn loader_enabled_policy_warms_evicted_hot_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let (manifest, store, counter) = build_index(&tmp, 3, "enabled-policy");

    let policy = PrefetchPolicy {
        enabled: true,
        min_query_count: 2,
    };
    let loader = CachedShardLoader::with_cache_capacity(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        manifest.clone(),
        1,
    )
    .with_prefetch(policy);

    let s0 = manifest.shards[0].shard_id;
    let s1 = manifest.shards[1].shard_id;

    // Access shard 0 twice so its count reaches the threshold (2).
    loader.load(s0).unwrap(); // miss → load #1; count[s0]=1; cache={s0}
    loader.load(s0).unwrap(); // hit;  count[s0]=2; cache={s0}

    // Reset counter so we can measure loads from this point.
    counter.store(0, Ordering::SeqCst);

    // Load shard 1 (miss): s0 gets evicted (capacity=1), s1 is inserted.
    // Post-insert warming detects s0 (count=2 >= 2) is hot but uncached
    // and reloads it.
    loader.load(s1).unwrap(); // miss → load s1 (#1) + warm s0 (#2); cache={s1→evict s0? wait...}

    // After s1 is inserted (evicting s0), warming re-inserts s0 (evicting s1).
    // Net result: the cache ends with the re-warmed s0.

    // Total loads since reset: 1 (s1) + 1 (warming s0) = 2.
    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "expected one explicit load + one prefetch warm"
    );

    // Now loading s0 must be a cache hit (no additional store reads).
    let before = counter.load(Ordering::SeqCst);
    loader.load(s0).unwrap();
    let after = counter.load(Ordering::SeqCst);
    assert_eq!(
        after, before,
        "s0 must be served from cache after warming; no extra store reads expected"
    );
}

/// A prefetch policy with a threshold above the current access counts must NOT
/// warm any shards.
#[test]
fn loader_enabled_policy_threshold_not_met_no_warming() {
    let tmp = tempfile::tempdir().unwrap();
    let (manifest, store, counter) = build_index(&tmp, 2, "threshold-not-met");

    let policy = PrefetchPolicy {
        enabled: true,
        min_query_count: 5, // high threshold
    };
    let loader = CachedShardLoader::with_cache_capacity(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        manifest.clone(),
        1,
    )
    .with_prefetch(policy);

    let s0 = manifest.shards[0].shard_id;
    let s1 = manifest.shards[1].shard_id;

    loader.load(s0).unwrap(); // count[s0]=1; cache={s0}

    counter.store(0, Ordering::SeqCst);

    loader.load(s1).unwrap(); // miss; s0 evicted; count[s1]=1 < 5 → no warm
                              // count[s0]=1 < 5 → no warm either.

    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "threshold not met: only the explicit load should occur"
    );
}

// ── cache capacity / eviction ─────────────────────────────────────────────────

/// A loader whose capacity exceeds the shard count must retain all loaded shards.
#[test]
fn loader_unbounded_cache_retains_all_loaded_shards() {
    let tmp = tempfile::tempdir().unwrap();
    let (manifest, store, counter) = build_index(&tmp, 3, "unbounded");

    let loader = CachedShardLoader::with_cache_capacity(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        manifest.clone(),
        manifest.shards.len() + 1,
    );

    // Load all shards once.
    for shard_def in &manifest.shards {
        loader.load(shard_def.shard_id).unwrap();
    }
    let total = counter.load(Ordering::SeqCst);

    // Loading them again must produce zero additional store reads.
    for shard_def in &manifest.shards {
        loader.load(shard_def.shard_id).unwrap();
    }
    assert_eq!(
        counter.load(Ordering::SeqCst),
        total,
        "all shards must be cache-resident after the first pass"
    );
}

/// With a bounded cache, successive loads of different shards cause evictions
/// but the total number of cache residents never exceeds the capacity.
#[test]
fn loader_bounded_cache_does_not_exceed_capacity() {
    let tmp = tempfile::tempdir().unwrap();
    let (manifest, store, _counter) = build_index(&tmp, 4, "bounded");

    let loader = CachedShardLoader::with_cache_capacity(
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        manifest.clone(),
        2,
    );

    let s0 = manifest.shards[0].shard_id;
    let s1 = manifest.shards[1].shard_id;
    let s2 = manifest.shards[2].shard_id;
    let s3 = manifest.shards[3].shard_id;

    loader.load(s0).unwrap();
    loader.load(s0).unwrap();
    loader.load(s1).unwrap();
    loader.load(s2).unwrap();
    loader.load(s3).unwrap();

    // With capacity=2 and shard 0 accessed twice, shard 1 must have been
    // evicted once shards 2 and 3 were inserted.
    let first_count = store.shard_loads.load(Ordering::SeqCst);
    loader.load(s1).unwrap();
    let second_count = store.shard_loads.load(Ordering::SeqCst);
    assert!(
        second_count > first_count,
        "cold shard 1 must have been evicted from the bounded cache"
    );
}
