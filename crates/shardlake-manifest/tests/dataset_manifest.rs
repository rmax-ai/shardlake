use chrono::{DateTime, TimeZone, Utc};
use shardlake_core::types::{DatasetVersion, EmbeddingVersion};
use shardlake_manifest::{
    DatasetManifest, IngestMetadata, ManifestError, DATASET_MANIFEST_VERSION,
};
use shardlake_storage::{paths, LocalObjectStore, ObjectStore};

fn fixed_ts() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 15, 10, 0, 0).unwrap()
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
            ingested_at: fixed_ts(),
            ingester_version: "0.1.0".into(),
        }),
    }
}

#[test]
fn test_dataset_manifest_save_load_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let dm = sample_dataset_manifest();

    dm.save(&store).unwrap();

    // The key written must match the canonical path helper.
    let expected_key = DatasetManifest::storage_key(&dm.dataset_version);
    assert_eq!(expected_key, paths::dataset_info_key("ds-v1"));

    let loaded = DatasetManifest::load(&store, &dm.dataset_version).unwrap();
    assert_eq!(loaded.dataset_version, dm.dataset_version);
    assert_eq!(loaded.embedding_version, dm.embedding_version);
    assert_eq!(loaded.dims, 4);
    assert_eq!(loaded.vector_count, 100);
    assert_eq!(loaded.vectors_key, dm.vectors_key);
    assert_eq!(loaded.metadata_key, dm.metadata_key);
    assert_eq!(loaded.manifest_version, DATASET_MANIFEST_VERSION);
    assert!(loaded.ingest_metadata.is_some());
    let meta = loaded.ingest_metadata.unwrap();
    assert_eq!(meta.ingester_version, "0.1.0");
}

#[test]
fn test_dataset_manifest_json_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let dm = sample_dataset_manifest();
    dm.save(&store).unwrap();

    let key = DatasetManifest::storage_key(&dm.dataset_version);
    let bytes = store.get(&key).unwrap();
    let json = String::from_utf8(bytes).unwrap();

    // New schema writes `vector_count`, not the legacy `count`.
    assert!(json.contains("\"vector_count\""));
    assert!(!json.contains("\"count\""));
    assert!(json.contains("\"manifest_version\""));
    assert!(json.contains("\"ingest_metadata\""));
}

#[test]
fn test_dataset_manifest_validate_rejects_zero_dims() {
    let mut dm = sample_dataset_manifest();
    dm.dims = 0;
    let err = dm.validate().unwrap_err();
    assert!(err.to_string().contains("dims must be > 0"));
}

#[test]
fn test_dataset_manifest_validate_rejects_zero_vector_count() {
    let mut dm = sample_dataset_manifest();
    dm.vector_count = 0;
    let err = dm.validate().unwrap_err();
    assert!(err.to_string().contains("vector_count must be > 0"));
}

#[test]
fn test_dataset_manifest_validate_rejects_empty_vectors_key() {
    let mut dm = sample_dataset_manifest();
    dm.vectors_key.clear();
    let err = dm.validate().unwrap_err();
    assert!(err.to_string().contains("vectors_key must not be empty"));
}

#[test]
fn test_dataset_manifest_validate_rejects_empty_metadata_key() {
    let mut dm = sample_dataset_manifest();
    dm.metadata_key.clear();
    let err = dm.validate().unwrap_err();
    assert!(err.to_string().contains("metadata_key must not be empty"));
}

#[test]
fn test_dataset_manifest_validate_rejects_unsupported_version() {
    let mut dm = sample_dataset_manifest();
    dm.manifest_version = 99;
    let err = dm.validate().unwrap_err();
    assert!(err
        .to_string()
        .contains("unsupported dataset manifest_version 99"));
}

/// Loading a dataset that was never ingested must return a storage error.
#[test]
fn test_dataset_manifest_load_missing_returns_error() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let err = DatasetManifest::load(&store, &DatasetVersion("nonexistent".into())).unwrap_err();
    assert!(matches!(err, ManifestError::Storage(_)));
}

#[test]
fn test_dataset_manifest_load_rejects_dataset_version_mismatch() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();

    let dm = sample_dataset_manifest();
    store
        .put(
            &DatasetManifest::storage_key(&DatasetVersion("other-dataset".into())),
            serde_json::to_vec_pretty(&dm).unwrap(),
        )
        .unwrap();

    let err = DatasetManifest::load(&store, &DatasetVersion("other-dataset".into())).unwrap_err();
    assert!(err
        .to_string()
        .contains("dataset manifest: dataset_version mismatch"));
}

/// Backward compatibility: old `info.json` files written before the versioned
/// schema are still accepted by `DatasetManifest::load`.
///
/// Legacy files use `"count"` instead of `"vector_count"` and do not include
/// `manifest_version` or `ingest_metadata`.
#[test]
fn test_dataset_manifest_loads_legacy_info_json() {
    let legacy = serde_json::json!({
        "dataset_version": "ds-v1",
        "embedding_version": "emb-v1",
        "dims": 128,
        "count": 10000,
        "vectors_key": "datasets/ds-v1/vectors.jsonl",
        "metadata_key": "datasets/ds-v1/metadata.json"
    });

    let dm: DatasetManifest = serde_json::from_value(legacy).unwrap();
    // manifest_version defaults to 0 for legacy files.
    assert_eq!(dm.manifest_version, 0);
    // `count` is accepted as alias for `vector_count`.
    assert_eq!(dm.vector_count, 10000);
    assert_eq!(dm.dims, 128);
    // ingest_metadata is absent in legacy files.
    assert!(dm.ingest_metadata.is_none());
    // validate() must accept legacy (version 0) manifests.
    dm.validate().unwrap();
}

/// Legacy `info.json` can be loaded from storage end-to-end.
#[test]
fn test_dataset_manifest_loads_legacy_info_json_from_store() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();

    let legacy = serde_json::json!({
        "dataset_version": "ds-v1",
        "embedding_version": "emb-v1",
        "dims": 8,
        "count": 50,
        "vectors_key": "datasets/ds-v1/vectors.jsonl",
        "metadata_key": "datasets/ds-v1/metadata.json"
    });
    store
        .put(
            &paths::dataset_info_key("ds-v1"),
            serde_json::to_vec_pretty(&legacy).unwrap(),
        )
        .unwrap();

    let loaded = DatasetManifest::load(&store, &DatasetVersion("ds-v1".into())).unwrap();
    assert_eq!(loaded.vector_count, 50);
    assert_eq!(loaded.dims, 8);
    assert!(loaded.ingest_metadata.is_none());
}
