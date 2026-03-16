//! Tests covering parallel shard-build execution.
//!
//! These tests verify that:
//! - Shard artifacts and deterministic manifest shard definitions are identical
//!   across repeated builds with the same input and configuration.
//! - Multiple shards are written concurrently without data corruption or
//!   missing artifacts.
//! - A storage error during a shard write propagates cleanly as a build error.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use shardlake_core::{
    config::SystemConfig,
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
    },
};
use shardlake_index::{BuildParams, IndexBuilder};
use shardlake_storage::{LocalObjectStore, ObjectStore, StorageError};
use tempfile::tempdir;

// ── helpers ──────────────────────────────────────────────────────────────────

fn make_records(n: usize, dims: usize) -> Vec<VectorRecord> {
    (0..n)
        .map(|i| VectorRecord {
            id: VectorId(i as u64),
            data: (0..dims).map(|d| (i * dims + d) as f32 / 100.0).collect(),
            metadata: None,
        })
        .collect()
}

fn default_config(tmp: &std::path::Path, num_shards: u32) -> SystemConfig {
    SystemConfig {
        storage_root: tmp.to_path_buf(),
        num_shards,
        kmeans_iters: 2,
        nprobe: 1,
        kmeans_seed: SystemConfig::default_kmeans_seed(),
        kmeans_sample_size: None,
        ..SystemConfig::default()
    }
}

fn build_params(records: Vec<VectorRecord>, dims: usize, idx_ver: &str) -> BuildParams {
    BuildParams {
        records,
        dataset_version: DatasetVersion("ds-par".into()),
        embedding_version: EmbeddingVersion("emb-par".into()),
        index_version: IndexVersion(idx_ver.into()),
        metric: DistanceMetric::Euclidean,
        dims,
        vectors_key: shardlake_storage::paths::dataset_vectors_key("ds-par"),
        metadata_key: shardlake_storage::paths::dataset_metadata_key("ds-par"),
        pq_params: None,
    }
}

// ── ObjectStore wrapper that counts concurrent writes ────────────────────────

/// Tracks the peak number of concurrent `put` calls in progress.
struct ConcurrencyTrackingStore {
    inner: Arc<dyn ObjectStore>,
    in_flight: Arc<AtomicUsize>,
    peak_concurrent: Arc<AtomicUsize>,
    /// Artificial per-write delay in microseconds (0 = no delay).
    delay_us: u64,
}

impl ConcurrencyTrackingStore {
    fn new(inner: Arc<dyn ObjectStore>, delay_us: u64) -> (Arc<Self>, Arc<AtomicUsize>) {
        let peak = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(Self {
            inner,
            in_flight: Arc::new(AtomicUsize::new(0)),
            peak_concurrent: Arc::clone(&peak),
            delay_us,
        });
        (store, peak)
    }
}

impl ObjectStore for ConcurrencyTrackingStore {
    fn put(&self, key: &str, data: Vec<u8>) -> Result<(), StorageError> {
        let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_concurrent.fetch_max(current, Ordering::SeqCst);
        if self.delay_us > 0 {
            std::thread::sleep(std::time::Duration::from_micros(self.delay_us));
        }
        let result = self.inner.put(key, data);
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        result
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
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

// ── ObjectStore wrapper that fails on shard writes ───────────────────────────

/// Fails the first shard `.sidx` write it encounters.
struct FailingShardStore {
    inner: Arc<dyn ObjectStore>,
    failed: Mutex<bool>,
}

impl FailingShardStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            failed: Mutex::new(false),
        })
    }
}

impl ObjectStore for FailingShardStore {
    fn put(&self, key: &str, data: Vec<u8>) -> Result<(), StorageError> {
        if key.ends_with(".sidx") {
            let mut failed = self.failed.lock().expect("lock poisoned");
            if !*failed {
                *failed = true;
                return Err(StorageError::Other("injected shard write failure".into()));
            }
        }
        self.inner.put(key, data)
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
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

// ── tests ─────────────────────────────────────────────────────────────────────

/// Two builds with the same input and configuration must produce identical
/// shard definitions and total vector counts. Time-based build metadata is
/// intentionally excluded from this assertion.
#[test]
fn parallel_build_is_deterministic() {
    let records = make_records(60, 4);
    let dims = 4;
    let num_shards = 4;
    let index_version = "idx-par-det";

    let build_once = || {
        let tmp = tempdir().unwrap();
        let inner = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = default_config(tmp.path(), num_shards);
        let manifest = IndexBuilder::new(inner.as_ref(), &config)
            .build(build_params(records.clone(), dims, index_version))
            .unwrap();
        manifest
    };

    let m1 = build_once();
    let m2 = build_once();

    assert_eq!(
        m1.shards.len(),
        m2.shards.len(),
        "shard count must be identical"
    );
    assert_eq!(
        m1.total_vector_count, m2.total_vector_count,
        "total_vector_count must be identical"
    );

    for (s1, s2) in m1.shards.iter().zip(m2.shards.iter()) {
        assert_eq!(
            s1.shard_id, s2.shard_id,
            "shard IDs must appear in consistent order"
        );
        assert_eq!(
            s1.artifact_key, s2.artifact_key,
            "shard {} artifact key must be identical across parallel builds",
            s1.shard_id
        );
        assert_eq!(
            s1.vector_count, s2.vector_count,
            "shard {} vector_count must be identical across parallel builds",
            s1.shard_id
        );
        assert_eq!(
            s1.fingerprint, s2.fingerprint,
            "shard {} fingerprint must be identical across parallel builds",
            s1.shard_id
        );
        assert_eq!(
            s1.centroid, s2.centroid,
            "shard {} centroid must be identical across parallel builds",
            s1.shard_id
        );
        assert_eq!(
            s1.routing, s2.routing,
            "shard {} routing metadata must be identical across parallel builds",
            s1.shard_id
        );
    }
}

/// All shard artifact keys produced by a parallel build must be present in the
/// store and must round-trip without data corruption (non-empty bytes).
#[test]
fn parallel_build_writes_all_shard_artifacts() {
    let tmp = tempdir().unwrap();
    let inner = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let num_shards = 4u32;
    let config = default_config(tmp.path(), num_shards);
    let records = make_records(80, 4);

    let manifest = IndexBuilder::new(inner.as_ref(), &config)
        .build(build_params(records, 4, "idx-par-artifacts"))
        .unwrap();

    assert!(
        !manifest.shards.is_empty(),
        "at least one shard must be produced"
    );
    for shard in &manifest.shards {
        let bytes = inner
            .get(&shard.artifact_key)
            .expect("shard artifact must be present in store");
        assert!(
            !bytes.is_empty(),
            "shard {} artifact must not be empty",
            shard.shard_id
        );
        assert!(
            !shard.fingerprint.is_empty(),
            "shard {} fingerprint must not be empty",
            shard.shard_id
        );
    }
}

/// Shard IDs in the manifest must be assigned in ascending order regardless of
/// the order in which worker threads finish.
#[test]
fn parallel_build_shard_ids_are_sorted() {
    let tmp = tempdir().unwrap();
    let inner = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = default_config(tmp.path(), 6);
    let records = make_records(120, 4);

    let manifest = IndexBuilder::new(inner.as_ref(), &config)
        .build(build_params(records, 4, "idx-par-sorted"))
        .unwrap();

    let ids: Vec<u32> = manifest.shards.iter().map(|s| s.shard_id.0).collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "shard IDs must be in ascending order");
}

/// A storage error on any shard write must cause the build to return an error
/// rather than silently producing a partial artifact set.
#[test]
fn parallel_build_propagates_shard_write_error() {
    let tmp = tempdir().unwrap();
    let inner = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let failing = FailingShardStore::new(inner);
    let config = default_config(tmp.path(), 4);
    let records = make_records(80, 4);

    let err = IndexBuilder::new(failing.as_ref(), &config)
        .build(build_params(records, 4, "idx-par-err"))
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("injected shard write failure") || msg.contains("storage error"),
        "expected storage error, got: {msg}"
    );
}

/// Artificial write delay (µs) used to make overlapping shard writes observable.
const SHARD_WRITE_DELAY_US: u64 = 5_000;

/// With an artificial write delay and multiple shards, verify that at least two
/// shard writes overlap in time (i.e. the build actually runs them concurrently
/// when more than one Rayon worker thread is available).
///
/// This test is best-effort: on a single-core machine or when Rayon picks a
/// pool size of 1 the peak may be 1.  We therefore only assert it on machines
/// where `rayon::current_num_threads() > 1`.
#[test]
fn parallel_build_executes_shards_concurrently() {
    let tmp = tempdir().unwrap();
    let inner = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let (tracking, peak) = ConcurrencyTrackingStore::new(inner, SHARD_WRITE_DELAY_US);
    let config = default_config(tmp.path(), 6);
    let records = make_records(120, 4);

    IndexBuilder::new(tracking.as_ref(), &config)
        .build(build_params(records, 4, "idx-par-conc"))
        .unwrap();

    if rayon::current_num_threads() > 1 {
        let observed_peak = peak.load(Ordering::SeqCst);
        assert!(
            observed_peak >= 2,
            "expected at least 2 concurrent shard writes on a multi-threaded pool, got {observed_peak}"
        );
    }
}
