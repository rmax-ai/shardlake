//! Integration test: build index and verify search results.

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
use shardlake_index::{BuildParams, IndexBuilder, IndexSearcher};
use shardlake_storage::{paths, LocalObjectStore, ObjectStore, StorageError};

fn make_records(n: usize, dims: usize) -> Vec<VectorRecord> {
    (0..n)
        .map(|i| VectorRecord {
            id: VectorId(i as u64),
            data: (0..dims).map(|d| (i * dims + d) as f32 / 100.0).collect(),
            metadata: None,
        })
        .collect()
}

/// Wraps an [`ObjectStore`] and counts how many times each `.sidx` shard key is fetched.
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    shard_get_count: Arc<AtomicUsize>,
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> (Arc<Self>, Arc<AtomicUsize>) {
        let counter = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(Self {
            inner,
            shard_get_count: Arc::clone(&counter),
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
            self.shard_get_count.fetch_add(1, Ordering::Relaxed);
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

#[test]
fn test_build_and_search() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = SystemConfig {
        storage_root: tmp.path().to_path_buf(),
        num_shards: 2,
        kmeans_iters: 10,
        nprobe: 2,
        kmeans_seed: SystemConfig::default_kmeans_seed(),
        kmeans_sample_size: None,
        ..SystemConfig::default()
    };

    let records = make_records(20, 4);
    let builder = IndexBuilder::new(store.as_ref(), &config);
    let manifest = builder
        .build(BuildParams {
            records: records.clone(),
            dataset_version: DatasetVersion("ds-test".into()),
            embedding_version: EmbeddingVersion("emb-test".into()),
            index_version: IndexVersion("idx-test".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-test"),
            metadata_key: paths::dataset_metadata_key("ds-test"),
        })
        .unwrap();

    assert!(manifest.total_vector_count > 0);
    assert!(!manifest.shards.is_empty());
    let shard_sum: u64 = manifest.shards.iter().map(|s| s.vector_count).sum();
    assert_eq!(shard_sum, manifest.total_vector_count);
    assert!(manifest
        .shards
        .iter()
        .all(|shard| !shard.fingerprint.is_empty()));
    // v4 builder must populate routing metadata for every shard.
    assert!(manifest.shards.iter().all(|shard| {
        let r = shard.routing.as_ref().expect("routing must be populated");
        !r.centroid_id.is_empty() && !r.index_type.is_empty() && !r.file_location.is_empty()
    }));

    let searcher = IndexSearcher::new(
        Arc::clone(&store) as Arc<dyn shardlake_storage::ObjectStore>,
        manifest,
    );
    let query = records[0].data.clone();
    let policy = FanOutPolicy {
        candidate_centroids: 2,
        candidate_shards: 0,
        max_vectors_per_shard: 0,
    };
    let results = searcher.search(&query, 5, &policy).unwrap();
    assert!(!results.is_empty());
    // The closest vector to itself should be id 0.
    assert_eq!(results[0].id, VectorId(0));
}

/// Verify that with nprobe=1 and 4 shards only the single nearest shard is
/// deserialized; the other 3 must not be loaded during the routing phase.
#[test]
fn test_search_does_not_load_non_probed_shards() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());

    // Build with 4 shards so there are clearly non-probed shards when nprobe=1.
    let config = SystemConfig {
        storage_root: tmp.path().to_path_buf(),
        num_shards: 4,
        kmeans_iters: 10,
        nprobe: 1,
        kmeans_seed: SystemConfig::default_kmeans_seed(),
        kmeans_sample_size: None,
        ..SystemConfig::default()
    };

    let records = make_records(40, 4);
    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: records.clone(),
            dataset_version: DatasetVersion("ds-lazy".into()),
            embedding_version: EmbeddingVersion("emb-lazy".into()),
            index_version: IndexVersion("idx-lazy".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-lazy"),
            metadata_key: paths::dataset_metadata_key("ds-lazy"),
        })
        .unwrap();

    // Manifest v4 embeds centroids so routing requires zero shard loads.
    assert_eq!(manifest.manifest_version, 4);
    assert!(manifest.shards.iter().all(|s| !s.centroid.is_empty()));

    let (counting_store, counter) = CountingStore::new(Arc::clone(&store) as Arc<dyn ObjectStore>);

    let searcher = IndexSearcher::new(counting_store, manifest);

    // candidate_centroids=1: only 1 of the 4 shards should be loaded.
    let query = records[0].data.clone();
    let policy = FanOutPolicy {
        candidate_centroids: 1,
        candidate_shards: 0,
        max_vectors_per_shard: 0,
    };
    let results = searcher.search(&query, 5, &policy).unwrap();
    assert!(!results.is_empty());

    let loads = counter.load(Ordering::Relaxed);
    assert_eq!(
        loads, 1,
        "expected exactly 1 shard load with candidate_centroids=1, got {loads}"
    );
}

/// Verify that two builds with identical inputs (same records, same config including
/// seed) produce bit-for-bit identical shard fingerprints.
///
/// Timestamps (`built_at`, `build_duration_secs`) are wall-clock values and are
/// explicitly **excluded** from the determinism contract; this test does not compare
/// them.  Everything else — centroid assignments, shard artifact bytes, and therefore
/// `ShardDef.fingerprint` — must be identical.
#[test]
fn test_build_is_deterministic() {
    let records = make_records(20, 4);

    let build_once = |idx_ver: &str| {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 2,
            kmeans_iters: 10,
            nprobe: 2,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            kmeans_sample_size: None,
            ..SystemConfig::default()
        };
        IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records: records.clone(),
                dataset_version: DatasetVersion("ds-det".into()),
                embedding_version: EmbeddingVersion("emb-det".into()),
                index_version: IndexVersion(idx_ver.into()),
                metric: DistanceMetric::Euclidean,
                dims: 4,
                vectors_key: paths::dataset_vectors_key("ds-det"),
                metadata_key: paths::dataset_metadata_key("ds-det"),
            })
            .unwrap()
    };

    let m1 = build_once("idx-det-1");
    let m2 = build_once("idx-det-2");

    // Shard count and per-shard vector counts must match.
    assert_eq!(m1.shards.len(), m2.shards.len());
    for (s1, s2) in m1.shards.iter().zip(m2.shards.iter()) {
        assert_eq!(
            s1.vector_count, s2.vector_count,
            "shard {} vector count differs between builds",
            s1.shard_id
        );
        assert_eq!(
            s1.fingerprint, s2.fingerprint,
            "shard {} artifact fingerprint differs between builds",
            s1.shard_id
        );
        assert_eq!(
            s1.centroid, s2.centroid,
            "shard {} centroid differs between builds",
            s1.shard_id
        );
        // Routing metadata must be consistent between builds (centroid_id and
        // index_type are deterministic; file_location varies by index_version
        // just as index_version itself does).
        let r1 = s1.routing.as_ref().expect("routing must be populated");
        let r2 = s2.routing.as_ref().expect("routing must be populated");
        assert_eq!(
            r1.centroid_id, r2.centroid_id,
            "shard {} routing.centroid_id differs between builds",
            s1.shard_id
        );
        assert_eq!(
            r1.index_type, r2.index_type,
            "shard {} routing.index_type differs between builds",
            s1.shard_id
        );
    }

    // The seed must be recorded in algorithm.params so the build can be reproduced.
    let seed_param = m1
        .algorithm
        .params
        .get("kmeans_seed")
        .expect("kmeans_seed must be recorded in algorithm.params");
    assert_eq!(
        seed_param.as_u64().unwrap(),
        SystemConfig::default_kmeans_seed(),
    );
}

/// Verify that using `kmeans_sample_size` assigns all vectors to a shard
/// (no records are dropped) and that repeated builds with the same seed and
/// sample size yield identical shard layouts.
#[test]
fn test_build_with_sample_size_assigns_all_and_is_deterministic() {
    let records = make_records(60, 4);

    let build_once = |idx_ver: &str| {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 3,
            kmeans_iters: 10,
            nprobe: 2,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            // Train centroids on 20 of the 60 vectors.
            kmeans_sample_size: Some(20),
        };
        IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records: records.clone(),
                dataset_version: DatasetVersion("ds-samp".into()),
                embedding_version: EmbeddingVersion("emb-samp".into()),
                index_version: IndexVersion(idx_ver.into()),
                metric: DistanceMetric::Euclidean,
                dims: 4,
                vectors_key: paths::dataset_vectors_key("ds-samp"),
                metadata_key: paths::dataset_metadata_key("ds-samp"),
            })
            .unwrap()
    };

    let m1 = build_once("idx-samp-1");
    let m2 = build_once("idx-samp-2");

    // All vectors must be assigned regardless of sampling.
    assert_eq!(
        m1.total_vector_count,
        records.len() as u64,
        "all vectors must be assigned when kmeans_sample_size is set"
    );
    let shard_sum: u64 = m1.shards.iter().map(|s| s.vector_count).sum();
    assert_eq!(shard_sum, records.len() as u64);

    // Builds with the same seed and sample_size must be deterministic.
    assert_eq!(m1.shards.len(), m2.shards.len());
    for (s1, s2) in m1.shards.iter().zip(m2.shards.iter()) {
        assert_eq!(
            s1.fingerprint, s2.fingerprint,
            "shard {} fingerprint differs between sample-based builds",
            s1.shard_id
        );
        assert_eq!(
            s1.centroid, s2.centroid,
            "shard {} centroid differs between sample-based builds",
            s1.shard_id
        );
    }

    // kmeans_sample_size must be recorded in algorithm.params.
    let param = m1
        .algorithm
        .params
        .get("kmeans_sample_size")
        .expect("kmeans_sample_size must be in algorithm.params when set");
    assert_eq!(param.as_u64().unwrap(), 20);
}

#[test]
fn test_build_with_large_sample_size_uses_full_dataset_without_manifest_override() {
    let records = make_records(12, 4);
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = SystemConfig {
        storage_root: tmp.path().to_path_buf(),
        num_shards: 3,
        kmeans_iters: 10,
        nprobe: 2,
        kmeans_seed: SystemConfig::default_kmeans_seed(),
        kmeans_sample_size: Some(100),
    };

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records,
            dataset_version: DatasetVersion("ds-full-sample".into()),
            embedding_version: EmbeddingVersion("emb-full-sample".into()),
            index_version: IndexVersion("idx-full-sample".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-full-sample"),
            metadata_key: paths::dataset_metadata_key("ds-full-sample"),
        })
        .unwrap();

    assert!(
        !manifest.algorithm.params.contains_key("kmeans_sample_size"),
        "manifest should omit kmeans_sample_size when centroid training used the full dataset"
    );
}
