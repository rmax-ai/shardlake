use chrono::Utc;
use shardlake_core::types::{
    DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, ShardId,
};
use shardlake_manifest::{
    validate_manifest_integrity, ArtifactLocations, ArtifactRegistry, BuildMetadata,
    LocalArtifactRegistry, Manifest, ShardDef,
};
use shardlake_storage::{LocalObjectStore, ObjectStore};
use std::sync::Arc;

fn sample_manifest() -> Manifest {
    Manifest {
        manifest_version: 1,
        dataset_id: "test-dataset".into(),
        dataset_version: DatasetVersion("ds-v1".into()),
        embedding_model: "test-model".into(),
        embedding_version: EmbeddingVersion("emb-v1".into()),
        index_version: IndexVersion("idx-v1".into()),
        alias: "latest".into(),
        dims: 4,
        distance_metric: DistanceMetric::Cosine,
        vectors_key: "datasets/ds-v1/vectors.jsonl".into(),
        metadata_key: "datasets/ds-v1/metadata.json".into(),
        shard_count: 2,
        total_vector_count: 10,
        checksum: String::new(),
        shards: vec![
            ShardDef {
                shard_id: ShardId(0),
                artifact_key: "indexes/idx-v1/shards/shard-0000.sidx".into(),
                vector_count: 5,
                sha256: "abc".into(),
            },
            ShardDef {
                shard_id: ShardId(1),
                artifact_key: "indexes/idx-v1/shards/shard-0001.sidx".into(),
                vector_count: 5,
                sha256: "def".into(),
            },
        ],
        build_metadata: BuildMetadata {
            built_at: Utc::now(),
            builder_version: "0.1.0".into(),
            num_kmeans_iters: 20,
            nprobe_default: 2,
            algorithm: "kmeans".into(),
            compression_method: "none".into(),
            quantization_parameters: None,
            recall_estimates: None,
            build_duration_ms: 42,
        },
    }
}

#[test]
fn test_save_load_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let mut m = sample_manifest();
    m.save(&store).unwrap();
    let loaded = Manifest::load(&store, &m.index_version).unwrap();
    assert_eq!(loaded.index_version, m.index_version);
    assert_eq!(loaded.total_vector_count, 10);
    assert_eq!(loaded.dataset_id, "test-dataset");
    assert_eq!(loaded.embedding_model, "test-model");
    assert_eq!(loaded.shard_count, 2);
    // Checksum must be populated after save
    assert!(!loaded.checksum.is_empty());
}

#[test]
fn test_validate_bad_count() {
    let mut m = sample_manifest();
    m.shards[0].vector_count = 999;
    assert!(m.validate().is_err());
}

#[test]
fn test_validate_shard_count_mismatch() {
    let mut m = sample_manifest();
    m.shard_count = 99;
    assert!(m.validate().is_err());
}

#[test]
fn test_alias_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let mut m = sample_manifest();
    m.save(&store).unwrap();
    m.publish_alias(&store).unwrap();
    let loaded = Manifest::load_alias(&store, "latest").unwrap();
    assert_eq!(loaded.index_version, m.index_version);
}

#[test]
fn test_artifact_locations() {
    let m = sample_manifest();
    let locs: ArtifactLocations = m.artifact_locations();
    assert_eq!(locs.vectors_key, "datasets/ds-v1/vectors.jsonl");
    assert_eq!(locs.shard_keys.len(), 2);
    assert!(locs.manifest_key.contains("idx-v1"));
}

#[test]
fn test_compatibility_checks() {
    let m = sample_manifest();

    // Dimension match
    assert!(m.check_dimension_compatibility(4).is_ok());
    assert!(m.check_dimension_compatibility(8).is_err());

    // Dataset version match
    assert!(m
        .check_dataset_version_compatibility(&DatasetVersion("ds-v1".into()))
        .is_ok());
    assert!(m
        .check_dataset_version_compatibility(&DatasetVersion("ds-v2".into()))
        .is_err());

    // Algorithm match
    assert!(m.check_algorithm_compatibility("kmeans").is_ok());
    assert!(m.check_algorithm_compatibility("hnsw").is_err());
}

#[test]
fn test_integrity_validation_missing_shards() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let m = sample_manifest();
    // Do NOT write shard files; only write the manifest.
    let mut m2 = m.clone();
    m2.save(&store).unwrap();

    let report = validate_manifest_integrity(&m2, &store);
    assert!(
        !report.ok,
        "should fail because shard artifacts are missing"
    );
    assert!(report.messages.iter().any(|msg| msg.contains("not found")));
}

#[test]
fn test_integrity_validation_checksum_mismatch() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();

    let mut m = sample_manifest();
    // Write shard files with known content
    store
        .put(
            "indexes/idx-v1/shards/shard-0000.sidx",
            b"shard0_data".to_vec(),
        )
        .unwrap();
    store
        .put(
            "indexes/idx-v1/shards/shard-0001.sidx",
            b"shard1_data".to_vec(),
        )
        .unwrap();
    m.save(&store).unwrap();

    // Tamper with expected sha256 in shard def
    m.shards[0].sha256 = "deadbeef".into();

    let report = validate_manifest_integrity(&m, &store);
    assert!(!report.ok, "should fail due to checksum mismatch");
    assert!(report
        .messages
        .iter()
        .any(|msg| msg.contains("checksum mismatch")));
}

#[test]
fn test_integrity_validation_passes() {
    use shardlake_manifest::fingerprint_hex;

    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();

    let shard0_bytes = b"shard0_data".to_vec();
    let shard1_bytes = b"shard1_data".to_vec();
    store
        .put(
            "indexes/idx-v1/shards/shard-0000.sidx",
            shard0_bytes.clone(),
        )
        .unwrap();
    store
        .put(
            "indexes/idx-v1/shards/shard-0001.sidx",
            shard1_bytes.clone(),
        )
        .unwrap();

    let mut m = sample_manifest();
    m.shards[0].sha256 = fingerprint_hex(&shard0_bytes);
    m.shards[1].sha256 = fingerprint_hex(&shard1_bytes);
    m.save(&store).unwrap();

    let report = validate_manifest_integrity(&m, &store);
    assert!(report.ok, "validation should pass: {:?}", report.messages);
}

#[test]
fn test_local_artifact_registry_save_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let registry = LocalArtifactRegistry::new(store.clone());

    let mut m = sample_manifest();
    registry.save_manifest(&mut m).unwrap();

    // Primary copy
    assert!(store.exists("indexes/idx-v1/manifest.json").unwrap());
    // Mirror copy
    assert!(store.exists("manifests/idx-v1.json").unwrap());

    // Can load back
    let loaded = registry
        .load_manifest(&IndexVersion("idx-v1".into()))
        .unwrap();
    assert_eq!(loaded.index_version, m.index_version);
}
