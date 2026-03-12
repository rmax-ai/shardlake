//! Integration test: build index and verify search results.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use shardlake_core::{
    config::SystemConfig,
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
    },
};
use shardlake_index::{BuildParams, IndexBuilder, IndexSearcher};
use shardlake_storage::{LocalObjectStore, ObjectStore, StorageError};

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
            vectors_key: "datasets/ds-test/vectors.jsonl".into(),
            metadata_key: "datasets/ds-test/metadata.json".into(),
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

    let searcher = IndexSearcher::new(
        Arc::clone(&store) as Arc<dyn shardlake_storage::ObjectStore>,
        manifest,
    );
    let query = records[0].data.clone();
    let results = searcher.search(&query, 5, 2).unwrap();
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
            vectors_key: "datasets/ds-lazy/vectors.jsonl".into(),
            metadata_key: "datasets/ds-lazy/metadata.json".into(),
        })
        .unwrap();

    // Manifest v2 embeds centroids so routing requires zero shard loads.
    assert_eq!(manifest.manifest_version, 2);
    assert!(manifest.shards.iter().all(|s| !s.centroid.is_empty()));

    let (counting_store, counter) = CountingStore::new(Arc::clone(&store) as Arc<dyn ObjectStore>);

    let searcher = IndexSearcher::new(counting_store, manifest);

    // nprobe=1: only 1 of the 4 shards should be loaded.
    let query = records[0].data.clone();
    let results = searcher.search(&query, 5, 1).unwrap();
    assert!(!results.is_empty());

    let loads = counter.load(Ordering::Relaxed);
    assert_eq!(
        loads, 1,
        "expected exactly 1 shard load with nprobe=1, got {loads}"
    );
}
