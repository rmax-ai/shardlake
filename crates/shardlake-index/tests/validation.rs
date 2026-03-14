//! Integration tests for the manifest integrity validation engine.

use std::sync::Arc;

use shardlake_core::{
    config::SystemConfig,
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
    },
};
use shardlake_index::{
    validator::{validate_dataset, validate_index, ValidationFailure},
    BuildParams, IndexBuilder,
};
use shardlake_manifest::{
    AlgorithmMetadata, BuildMetadata, CompressionConfig, DatasetManifest, IngestMetadata, Manifest,
    ShardDef, ShardSummary, DATASET_MANIFEST_VERSION,
};
use shardlake_storage::{paths, LocalObjectStore, ObjectStore};

use chrono::Utc;
use shardlake_core::types::ShardId;

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

fn default_config(tmp: &std::path::Path) -> SystemConfig {
    SystemConfig {
        storage_root: tmp.to_path_buf(),
        num_shards: 2,
        kmeans_iters: 5,
        nprobe: 2,
        ..SystemConfig::default()
    }
}

/// Build a real index in a temp store and return (store, manifest).
fn build_index(tmp: &std::path::Path) -> (Arc<LocalObjectStore>, shardlake_manifest::Manifest) {
    let store = Arc::new(LocalObjectStore::new(tmp).unwrap());
    let config = default_config(tmp);
    let records = make_records(20, 4);
    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records,
            dataset_version: DatasetVersion("ds-v1".into()),
            embedding_version: EmbeddingVersion("emb-v1".into()),
            index_version: IndexVersion("idx-v1".into()),
            metric: DistanceMetric::Cosine,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-v1"),
            metadata_key: paths::dataset_metadata_key("ds-v1"),
            pq_params: None,
        })
        .unwrap();
    (store, manifest)
}

/// Store placeholder bytes for the dataset artifacts (vectors + metadata).
fn store_dataset_artifacts(store: &dyn ObjectStore, dataset_version: &str) {
    store
        .put(
            &paths::dataset_vectors_key(dataset_version),
            b"placeholder-vectors".to_vec(),
        )
        .unwrap();
    store
        .put(
            &paths::dataset_metadata_key(dataset_version),
            b"placeholder-metadata".to_vec(),
        )
        .unwrap();
}

fn sample_dataset_manifest() -> DatasetManifest {
    DatasetManifest {
        manifest_version: DATASET_MANIFEST_VERSION,
        dataset_version: DatasetVersion("ds-v1".into()),
        embedding_version: EmbeddingVersion("emb-v1".into()),
        dims: 4,
        vector_count: 100,
        vectors_key: paths::dataset_vectors_key("ds-v1"),
        metadata_key: paths::dataset_metadata_key("ds-v1"),
        ingest_metadata: Some(IngestMetadata {
            ingested_at: Utc::now(),
            ingester_version: "0.1.0".into(),
        }),
    }
}

fn sample_manifest() -> Manifest {
    Manifest {
        manifest_version: 3,
        dataset_version: DatasetVersion("ds-v1".into()),
        embedding_version: EmbeddingVersion("emb-v1".into()),
        index_version: IndexVersion("idx-v1".into()),
        alias: "latest".into(),
        dims: 4,
        distance_metric: DistanceMetric::Cosine,
        vectors_key: paths::dataset_vectors_key("ds-v1"),
        metadata_key: paths::dataset_metadata_key("ds-v1"),
        total_vector_count: 5,
        shards: vec![ShardDef {
            shard_id: ShardId(0),
            artifact_key: paths::index_shard_key("idx-v1", 0),
            vector_count: 5,
            fingerprint: "abc123".into(),
            centroid: vec![0.1, 0.2, 0.3, 0.4],
        }],
        build_metadata: BuildMetadata {
            built_at: Utc::now(),
            builder_version: "0.1.0".into(),
            num_kmeans_iters: 20,
            nprobe_default: 2,
            build_duration_secs: 1.0,
        },
        algorithm: AlgorithmMetadata {
            algorithm: "kmeans-flat".into(),
            variant: None,
            params: std::collections::BTreeMap::new(),
        },
        shard_summary: Some(ShardSummary {
            num_shards: 1,
            min_shard_vector_count: 5,
            max_shard_vector_count: 5,
        }),
        compression: CompressionConfig::default(),
        recall_estimate: None,
    }
}

// ── validate_index: passing scenarios ─────────────────────────────────────────

/// A freshly-built index with all artifacts present must pass validation.
#[test]
fn test_validate_index_passes_with_all_artifacts_present() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, manifest) = build_index(tmp.path());

    // The builder writes shard artifacts but not the dataset vectors/metadata.
    store_dataset_artifacts(store.as_ref(), "ds-v1");

    let report = validate_index(&manifest, store.as_ref());
    assert!(
        report.is_valid(),
        "expected no failures, got: {:?}",
        report.failures
    );
}

/// `into_result` returns `Ok(())` on a valid index.
#[test]
fn test_validate_index_into_result_ok() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, manifest) = build_index(tmp.path());
    store_dataset_artifacts(store.as_ref(), "ds-v1");

    let report = validate_index(&manifest, store.as_ref());
    assert!(report.into_result().is_ok());
}

// ── validate_index: failing scenarios ─────────────────────────────────────────

/// A missing shard artifact must produce an `ArtifactMissing` failure.
#[test]
fn test_validate_index_detects_missing_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, manifest) = build_index(tmp.path());
    store_dataset_artifacts(store.as_ref(), "ds-v1");

    // Delete one of the shard artifacts.
    let shard_key = manifest.shards[0].artifact_key.clone();
    store.delete(&shard_key).unwrap();

    let report = validate_index(&manifest, store.as_ref());
    assert!(!report.is_valid());
    assert!(report.failures.iter().any(|f| matches!(
        f,
        ValidationFailure::ArtifactMissing { key } if key == &shard_key
    )));
}

/// A missing `vectors_key` artifact must produce an `ArtifactMissing` failure.
#[test]
fn test_validate_index_detects_missing_vectors_artifact() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, manifest) = build_index(tmp.path());
    // Only store metadata, omit vectors.
    store
        .put(
            &paths::dataset_metadata_key("ds-v1"),
            b"placeholder".to_vec(),
        )
        .unwrap();

    let report = validate_index(&manifest, store.as_ref());
    assert!(!report.is_valid());

    let missing_keys: Vec<_> = report
        .failures
        .iter()
        .filter_map(|f| {
            if let ValidationFailure::ArtifactMissing { key } = f {
                Some(key.as_str())
            } else {
                None
            }
        })
        .collect();
    assert!(
        missing_keys.contains(&paths::dataset_vectors_key("ds-v1").as_str()),
        "expected vectors_key in missing failures, got: {missing_keys:?}"
    );
}

/// A missing `metadata_key` artifact must produce an `ArtifactMissing` failure.
#[test]
fn test_validate_index_detects_missing_metadata_artifact() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, manifest) = build_index(tmp.path());
    // Only store vectors, omit metadata.
    store
        .put(
            &paths::dataset_vectors_key("ds-v1"),
            b"placeholder".to_vec(),
        )
        .unwrap();

    let report = validate_index(&manifest, store.as_ref());
    assert!(!report.is_valid());

    let missing_keys: Vec<_> = report
        .failures
        .iter()
        .filter_map(|f| {
            if let ValidationFailure::ArtifactMissing { key } = f {
                Some(key.as_str())
            } else {
                None
            }
        })
        .collect();
    assert!(
        missing_keys.contains(&paths::dataset_metadata_key("ds-v1").as_str()),
        "expected metadata_key in missing failures, got: {missing_keys:?}"
    );
}

/// Overwriting a shard with corrupted bytes must produce a `FingerprintMismatch`.
#[test]
fn test_validate_index_detects_fingerprint_mismatch() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, manifest) = build_index(tmp.path());
    store_dataset_artifacts(store.as_ref(), "ds-v1");

    // Corrupt the first shard artifact.
    let shard_key = manifest.shards[0].artifact_key.clone();
    store.put(&shard_key, b"corrupted bytes".to_vec()).unwrap();

    let report = validate_index(&manifest, store.as_ref());
    assert!(!report.is_valid());
    assert!(report.failures.iter().any(|f| matches!(
        f,
        ValidationFailure::FingerprintMismatch { key, .. } if key == &shard_key
    )));
    assert!(report.failures.iter().any(|f| matches!(
        f,
        ValidationFailure::ShardParseError { key, .. } if key == &shard_key
    )));
}

/// A manifest with an incorrect fingerprint field (not matching stored bytes)
/// must produce a `FingerprintMismatch` failure.
#[test]
fn test_validate_index_detects_fingerprint_field_mismatch() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, mut manifest) = build_index(tmp.path());
    store_dataset_artifacts(store.as_ref(), "ds-v1");

    // Tamper with the fingerprint in the manifest (stored bytes are untouched).
    manifest.shards[0].fingerprint = "000000000000dead".into();

    let report = validate_index(&manifest, store.as_ref());
    assert!(!report.is_valid());
    assert!(report
        .failures
        .iter()
        .any(|f| matches!(f, ValidationFailure::FingerprintMismatch { .. })));
}

/// A shard artifact whose binary dimension mismatches the manifest must produce
/// a `ShardDimensionMismatch` failure.
#[test]
fn test_validate_index_detects_dimension_mismatch() {
    use shardlake_index::ShardIndex;

    let tmp = tempfile::tempdir().unwrap();
    let (store, mut manifest) = build_index(tmp.path());
    store_dataset_artifacts(store.as_ref(), "ds-v1");

    // Build a replacement shard with a different dimension.
    let shard_key = manifest.shards[0].artifact_key.clone();
    let different_dims = 8usize;
    let replacement = ShardIndex {
        shard_id: ShardId(0),
        dims: different_dims,
        centroids: vec![vec![0.0f32; different_dims]],
        records: vec![VectorRecord {
            id: VectorId(0),
            data: vec![0.0f32; different_dims],
            metadata: None,
        }],
    };
    let bytes = replacement.to_bytes().unwrap();

    // Recompute fingerprint so we isolate the dimension-mismatch failure.
    let new_fp = shardlake_index::artifact_fingerprint(&bytes);
    manifest.shards[0].fingerprint = new_fp;
    manifest.shards[0].vector_count = 1;
    // Keep total_vector_count consistent so structural validate() passes.
    let other_count: u64 = manifest.shards[1..].iter().map(|s| s.vector_count).sum();
    manifest.total_vector_count = 1 + other_count;

    store.put(&shard_key, bytes).unwrap();

    let report = validate_index(&manifest, store.as_ref());
    assert!(!report.is_valid());
    assert!(
        report.failures.iter().any(|f| matches!(
            f,
            ValidationFailure::ShardDimensionMismatch {
                shard_id,
                expected,
                actual,
            } if *shard_id == ShardId(0) && *expected == 4 && *actual == 8
        )),
        "expected ShardDimensionMismatch, got: {:?}",
        report.failures
    );
}

/// A shard artifact whose vector count mismatches the manifest must produce a
/// `ShardVectorCountMismatch` failure.
#[test]
fn test_validate_index_detects_vector_count_mismatch() {
    use shardlake_index::ShardIndex;

    let tmp = tempfile::tempdir().unwrap();
    let (store, mut manifest) = build_index(tmp.path());
    store_dataset_artifacts(store.as_ref(), "ds-v1");

    let shard_key = manifest.shards[0].artifact_key.clone();
    let dims = 4usize;
    // Build a replacement shard with 3 vectors instead of what the manifest says.
    let replacement = ShardIndex {
        shard_id: ShardId(0),
        dims,
        centroids: vec![vec![0.0f32; dims]],
        records: vec![
            VectorRecord {
                id: VectorId(0),
                data: vec![0.0f32; dims],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(1),
                data: vec![1.0f32; dims],
                metadata: None,
            },
            VectorRecord {
                id: VectorId(2),
                data: vec![2.0f32; dims],
                metadata: None,
            },
        ],
    };
    let bytes = replacement.to_bytes().unwrap();

    let new_fp = shardlake_index::artifact_fingerprint(&bytes);
    manifest.shards[0].fingerprint = new_fp;
    // Leave vector_count as the original to cause the mismatch.
    store.put(&shard_key, bytes).unwrap();

    let report = validate_index(&manifest, store.as_ref());
    assert!(!report.is_valid());
    assert!(
        report.failures.iter().any(|f| matches!(
            f,
            ValidationFailure::ShardVectorCountMismatch {
                shard_id,
                actual: 3,
                ..
            } if *shard_id == ShardId(0)
        )),
        "expected ShardVectorCountMismatch, got: {:?}",
        report.failures
    );
}

/// A manifest centroid that diverges from shard bytes must produce a
/// `ShardCentroidMismatch` failure.
#[test]
fn test_validate_index_detects_centroid_mismatch() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, mut manifest) = build_index(tmp.path());
    store_dataset_artifacts(store.as_ref(), "ds-v1");

    manifest.shards[0].centroid[0] += 1.0;

    let report = validate_index(&manifest, store.as_ref());
    assert!(!report.is_valid());
    assert!(
        report.failures.iter().any(|f| matches!(
            f,
            ValidationFailure::ShardCentroidMismatch { shard_id, .. } if *shard_id == manifest.shards[0].shard_id
        )),
        "expected ShardCentroidMismatch, got: {:?}",
        report.failures
    );
}

/// A structurally invalid manifest (empty shards list) must produce a
/// `ManifestInvalid` failure even without artifact checks.
#[test]
fn test_validate_index_detects_structural_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();

    let mut manifest = sample_manifest();
    manifest.shards.clear();
    manifest.total_vector_count = 0;

    let report = validate_index(&manifest, &store);
    assert!(!report.is_valid());
    assert!(report
        .failures
        .iter()
        .any(|f| matches!(f, ValidationFailure::ManifestInvalid(_))));
}

/// All failures are collected (not just the first one).
#[test]
fn test_validate_index_collects_all_failures() {
    let tmp = tempfile::tempdir().unwrap();
    let (store, manifest) = build_index(tmp.path());
    // Do not write dataset artifacts AND do not write shard artifacts.
    // Delete all shards.
    for shard in &manifest.shards {
        store.delete(&shard.artifact_key).unwrap();
    }
    // metadata and vectors are also missing.

    let report = validate_index(&manifest, store.as_ref());
    assert!(!report.is_valid());
    // Expect failures for both dataset artifacts + each shard.
    let missing_count = report
        .failures
        .iter()
        .filter(|f| matches!(f, ValidationFailure::ArtifactMissing { .. }))
        .count();
    assert!(
        missing_count >= manifest.shards.len() + 2,
        "expected failures for shards ({}) + vectors + metadata, got {missing_count}",
        manifest.shards.len()
    );
}

/// `into_result` returns `Err` containing all failures when validation fails.
#[test]
fn test_validate_index_into_result_err() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let manifest = sample_manifest();
    // No artifacts written.

    let result = validate_index(&manifest, &store).into_result();
    assert!(result.is_err());
    let failures = result.unwrap_err();
    assert!(!failures.is_empty());
}

// ── validate_dataset: passing scenarios ───────────────────────────────────────

/// A dataset manifest with both artifacts present must pass validation.
#[test]
fn test_validate_dataset_passes_with_artifacts_present() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let dm = sample_dataset_manifest();
    store_dataset_artifacts(&store, "ds-v1");

    let report = validate_dataset(&dm, &store);
    assert!(
        report.is_valid(),
        "expected no failures, got: {:?}",
        report.failures
    );
}

/// `into_result` returns `Ok(())` on a valid dataset manifest.
#[test]
fn test_validate_dataset_into_result_ok() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let dm = sample_dataset_manifest();
    store_dataset_artifacts(&store, "ds-v1");

    assert!(validate_dataset(&dm, &store).into_result().is_ok());
}

// ── validate_dataset: failing scenarios ───────────────────────────────────────

/// A missing vectors artifact must produce an `ArtifactMissing` failure.
#[test]
fn test_validate_dataset_detects_missing_vectors_artifact() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let dm = sample_dataset_manifest();
    // Store only metadata, not vectors.
    store
        .put(&paths::dataset_metadata_key("ds-v1"), b"meta".to_vec())
        .unwrap();

    let report = validate_dataset(&dm, &store);
    assert!(!report.is_valid());
    assert!(report.failures.iter().any(|f| matches!(
        f,
        ValidationFailure::ArtifactMissing { key } if key == &paths::dataset_vectors_key("ds-v1")
    )));
}

/// A missing metadata artifact must produce an `ArtifactMissing` failure.
#[test]
fn test_validate_dataset_detects_missing_metadata_artifact() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let dm = sample_dataset_manifest();
    // Store only vectors, not metadata.
    store
        .put(&paths::dataset_vectors_key("ds-v1"), b"vecs".to_vec())
        .unwrap();

    let report = validate_dataset(&dm, &store);
    assert!(!report.is_valid());
    assert!(report.failures.iter().any(|f| matches!(
        f,
        ValidationFailure::ArtifactMissing { key } if key == &paths::dataset_metadata_key("ds-v1")
    )));
}

/// Invalid storage keys must surface as `StorageError` failures.
#[test]
fn test_validate_dataset_reports_storage_error_for_invalid_key() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let mut dm = sample_dataset_manifest();
    dm.metadata_key = "../escape".into();
    store.put(&dm.vectors_key, b"placeholder".to_vec()).unwrap();

    let report = validate_dataset(&dm, &store);
    assert!(!report.is_valid());
    assert!(report.failures.iter().any(|f| matches!(
        f,
        ValidationFailure::StorageError { key, .. } if key == "../escape"
    )));
}

/// A structurally invalid dataset manifest (dims=0) must produce a
/// `ManifestInvalid` failure.
#[test]
fn test_validate_dataset_detects_structural_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let mut dm = sample_dataset_manifest();
    dm.dims = 0; // invalid

    let report = validate_dataset(&dm, &store);
    assert!(!report.is_valid());
    assert!(report
        .failures
        .iter()
        .any(|f| matches!(f, ValidationFailure::ManifestInvalid(_))));
}

/// When both artifacts are missing the report contains two `ArtifactMissing`
/// failures.
#[test]
fn test_validate_dataset_collects_both_missing_artifacts() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let dm = sample_dataset_manifest();
    // Nothing stored.

    let report = validate_dataset(&dm, &store);
    assert!(!report.is_valid());
    let missing_count = report
        .failures
        .iter()
        .filter(|f| matches!(f, ValidationFailure::ArtifactMissing { .. }))
        .count();
    assert_eq!(missing_count, 2, "expected 2 ArtifactMissing failures");
}

// ── ValidationFailure Display ─────────────────────────────────────────────────

/// `ValidationFailure` variants must produce human-readable Display output.
#[test]
fn test_validation_failure_display() {
    let cases: Vec<(ValidationFailure, &str)> = vec![
        (
            ValidationFailure::ManifestInvalid("dims must be > 0".into()),
            "manifest invalid: dims must be > 0",
        ),
        (
            ValidationFailure::ArtifactMissing { key: "datasets/ds-v1/vectors.jsonl".into() },
            "artifact missing: datasets/ds-v1/vectors.jsonl",
        ),
        (
            ValidationFailure::FingerprintMismatch {
                key: "indexes/idx-v1/shards/shard-0000.sidx".into(),
                expected: "aabbcc".into(),
                actual: "112233".into(),
            },
            "fingerprint mismatch for indexes/idx-v1/shards/shard-0000.sidx: expected aabbcc, actual 112233",
        ),
        (
            ValidationFailure::ShardDimensionMismatch {
                shard_id: ShardId(0),
                expected: 4,
                actual: 8,
            },
            "shard shard-0000 dimension mismatch: expected 4, actual 8",
        ),
        (
            ValidationFailure::ShardVectorCountMismatch {
                shard_id: ShardId(1),
                expected: 100,
                actual: 99,
            },
            "shard shard-0001 vector count mismatch: expected 100, actual 99",
        ),
    ];

    for (failure, expected) in cases {
        assert_eq!(failure.to_string(), expected);
    }
}
