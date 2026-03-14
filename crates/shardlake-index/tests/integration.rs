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
use shardlake_index::{BuildParams, IndexBuilder, IndexSearcher, PqParams};
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
            pq_params: None,
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
        kmeans_seed: SystemConfig::default_kmeans_seed(),
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
            pq_params: None,
        })
        .unwrap();

    // Manifest v3 embeds centroids so routing requires zero shard loads.
    assert_eq!(manifest.manifest_version, 3);
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
                pq_params: None,
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

/// Build a PQ-compressed index and verify that:
/// 1. The manifest records PQ compression metadata.
/// 2. The codebook artifact is persisted.
/// 3. Search returns results (top-1 is the query vector itself).
#[test]
fn test_pq_build_and_search() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = SystemConfig {
        storage_root: tmp.path().to_path_buf(),
        num_shards: 2,
        kmeans_iters: 10,
        nprobe: 2,
        kmeans_seed: SystemConfig::default_kmeans_seed(),
        ..SystemConfig::default()
    };

    // Use 8-dimensional vectors so we can split into 4 sub-spaces of 2 dims.
    let records = make_records(40, 8);
    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: records.clone(),
            dataset_version: DatasetVersion("ds-pq".into()),
            embedding_version: EmbeddingVersion("emb-pq".into()),
            index_version: IndexVersion("idx-pq".into()),
            metric: DistanceMetric::Euclidean,
            dims: 8,
            vectors_key: paths::dataset_vectors_key("ds-pq"),
            metadata_key: paths::dataset_metadata_key("ds-pq"),
            pq_params: Some(PqParams {
                num_subspaces: 4,
                codebook_size: 8,
            }),
        })
        .unwrap();

    // Manifest must record PQ compression.
    assert!(manifest.compression.enabled);
    assert_eq!(manifest.compression.codec, "pq8");
    assert_eq!(manifest.compression.pq_num_subspaces, 4);
    assert_eq!(manifest.compression.pq_codebook_size, 8);
    let codebook_key = manifest
        .compression
        .codebook_key
        .as_deref()
        .expect("codebook_key must be set for PQ indexes");
    assert_eq!(codebook_key, "indexes/idx-pq/pq_codebook.bin");

    // Codebook artifact must exist in storage.
    assert!(
        store.exists(codebook_key).unwrap(),
        "PQ codebook artifact must be persisted"
    );

    // Shard artifacts must exist and use format version 2.
    for shard_def in &manifest.shards {
        let bytes = store.get(&shard_def.artifact_key).unwrap();
        let pq_shard = shardlake_index::PqShard::from_bytes(&bytes)
            .expect("shard must deserialise as PqShard");
        assert_eq!(pq_shard.dims, 8);
        assert_eq!(pq_shard.pq_m, 4);
        assert_eq!(pq_shard.pq_k, 8);
        assert_eq!(pq_shard.entries.len() as u64, shard_def.vector_count);
    }

    // Search must return results and the top-1 result must be the query vector.
    let searcher = IndexSearcher::new(
        Arc::clone(&store) as Arc<dyn shardlake_storage::ObjectStore>,
        manifest,
    );
    let query = records[0].data.clone();
    let results = searcher.search(&query, 5, 2).unwrap();
    assert!(
        !results.is_empty(),
        "PQ search must return at least one result"
    );
    assert_eq!(
        results[0].id,
        VectorId(0),
        "top-1 result must be the query vector itself"
    );
}

/// Build a PQ-compressed index via SystemConfig (pq_enabled = true) and verify
/// that the manifest records PQ metadata without explicit PqParams in BuildParams.
#[test]
fn test_pq_build_via_config() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = SystemConfig {
        storage_root: tmp.path().to_path_buf(),
        num_shards: 2,
        kmeans_iters: 5,
        nprobe: 2,
        kmeans_seed: SystemConfig::default_kmeans_seed(),
        pq_enabled: true,
        pq_num_subspaces: 2,
        pq_codebook_size: 4,
    };

    let records = make_records(20, 4);
    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records,
            dataset_version: DatasetVersion("ds-pqcfg".into()),
            embedding_version: EmbeddingVersion("emb-pqcfg".into()),
            index_version: IndexVersion("idx-pqcfg".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-pqcfg"),
            metadata_key: paths::dataset_metadata_key("ds-pqcfg"),
            pq_params: None, // rely on config
        })
        .unwrap();

    assert!(manifest.compression.enabled);
    assert_eq!(manifest.compression.codec, "pq8");
    assert_eq!(manifest.compression.pq_num_subspaces, 2);
    assert_eq!(manifest.compression.pq_codebook_size, 4);
    assert!(manifest.compression.codebook_key.is_some());
}

/// Two PQ builds with identical inputs (same records, config, seed) must
/// produce identical shard fingerprints (PQ encoding is deterministic).
#[test]
fn test_pq_build_is_deterministic() {
    let records = make_records(20, 8);

    let build_once = |idx_ver: &str| {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 2,
            kmeans_iters: 10,
            nprobe: 2,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            ..SystemConfig::default()
        };
        IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records: records.clone(),
                dataset_version: DatasetVersion("ds-pqdet".into()),
                embedding_version: EmbeddingVersion("emb-pqdet".into()),
                index_version: IndexVersion(idx_ver.into()),
                metric: DistanceMetric::Euclidean,
                dims: 8,
                vectors_key: paths::dataset_vectors_key("ds-pqdet"),
                metadata_key: paths::dataset_metadata_key("ds-pqdet"),
                pq_params: Some(PqParams {
                    num_subspaces: 4,
                    codebook_size: 8,
                }),
            })
            .unwrap()
    };

    let m1 = build_once("idx-pqdet-1");
    let m2 = build_once("idx-pqdet-2");

    assert_eq!(m1.shards.len(), m2.shards.len());
    for (s1, s2) in m1.shards.iter().zip(m2.shards.iter()) {
        assert_eq!(
            s1.fingerprint, s2.fingerprint,
            "PQ shard fingerprints must be identical across deterministic builds"
        );
    }
}
