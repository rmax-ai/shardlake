use chrono::Utc;
use shardlake_core::types::{
    DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, ShardId,
};
use shardlake_manifest::{
    AlgorithmMetadata, BuildMetadata, CompressionConfig, Manifest, ManifestError, RecallEstimate,
    RoutingMetadata, ShardDef, ShardSummary,
};
use shardlake_storage::{paths, LocalObjectStore, ObjectStore};

fn sample_manifest() -> Manifest {
    Manifest {
        manifest_version: 4,
        dataset_version: DatasetVersion("ds-v1".into()),
        embedding_version: EmbeddingVersion("emb-v1".into()),
        index_version: IndexVersion("idx-v1".into()),
        alias: "latest".into(),
        dims: 4,
        distance_metric: DistanceMetric::Cosine,
        vectors_key: paths::dataset_vectors_key("ds-v1"),
        metadata_key: paths::dataset_metadata_key("ds-v1"),
        total_vector_count: 10,
        shards: vec![
            ShardDef {
                shard_id: ShardId(0),
                artifact_key: paths::index_shard_key("idx-v1", 0),
                vector_count: 5,
                fingerprint: "abc".into(),
                centroid: vec![0.1, 0.2, 0.3, 0.4],
                routing: Some(RoutingMetadata {
                    centroid_id: "shard-0000".into(),
                    index_type: "flat".into(),
                    file_location: paths::index_shard_key("idx-v1", 0),
                }),
            },
            ShardDef {
                shard_id: ShardId(1),
                artifact_key: paths::index_shard_key("idx-v1", 1),
                vector_count: 5,
                fingerprint: "def".into(),
                centroid: vec![0.9, 0.8, 0.7, 0.6],
                routing: Some(RoutingMetadata {
                    centroid_id: "shard-0001".into(),
                    index_type: "flat".into(),
                    file_location: paths::index_shard_key("idx-v1", 1),
                }),
            },
        ],
        build_metadata: BuildMetadata {
            built_at: Utc::now(),
            builder_version: "0.1.0".into(),
            num_kmeans_iters: 20,
            nprobe_default: 2,
            build_duration_secs: 1.5,
        },
        algorithm: AlgorithmMetadata {
            algorithm: "kmeans-flat".into(),
            variant: None,
            params: {
                let mut p = std::collections::BTreeMap::new();
                p.insert("num_shards".into(), serde_json::json!(2));
                p.insert("kmeans_iters".into(), serde_json::json!(20));
                p.insert(
                    "kmeans_seed".into(),
                    serde_json::json!(shardlake_core::config::DEFAULT_KMEANS_SEED),
                );
                p
            },
        },
        shard_summary: Some(ShardSummary {
            num_shards: 2,
            min_shard_vector_count: 5,
            max_shard_vector_count: 5,
        }),
        compression: CompressionConfig::default(),
        recall_estimate: None,
        coarse_quantizer_key: None,
    }
}

/// A genuine v2 manifest with centroid data but without v3 lifecycle fields.
/// Used to verify that v3 readers can still round-trip v2 documents.
fn sample_v2_manifest() -> serde_json::Value {
    serde_json::json!({
        "manifest_version": 2,
        "dataset_version": "ds-v1",
        "embedding_version": "emb-v1",
        "index_version": "idx-v1",
        "alias": "latest",
        "dims": 4,
        "distance_metric": "cosine",
        "vectors_key": "datasets/ds-v1/vectors.jsonl",
        "metadata_key": "datasets/ds-v1/metadata.json",
        "total_vector_count": 10,
        "shards": [
            {
                "shard_id": 0,
                "artifact_key": "indexes/idx-v1/shards/shard-0000.sidx",
                "vector_count": 5,
                "sha256": "abc",
                "centroid": [0.1, 0.2, 0.3, 0.4]
            },
            {
                "shard_id": 1,
                "artifact_key": "indexes/idx-v1/shards/shard-0001.sidx",
                "vector_count": 5,
                "sha256": "def",
                "centroid": [0.9, 0.8, 0.7, 0.6]
            }
        ],
        "build_metadata": {
            "built_at": "2026-03-10T17:44:00Z",
            "builder_version": "0.1.0",
            "num_kmeans_iters": 20,
            "nprobe_default": 2
        }
    })
}

#[test]
fn test_save_load_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let m = sample_manifest();
    m.save(&store).unwrap();
    let manifest_key = Manifest::storage_key(&m.index_version);
    let saved = store.get(&manifest_key).unwrap();
    let saved_json = String::from_utf8(saved).unwrap();
    assert!(saved_json.contains("\"sha256\""));
    assert!(!saved_json.contains("\"fingerprint\""));
    let loaded = Manifest::load(&store, &m.index_version).unwrap();
    assert_eq!(loaded.index_version, m.index_version);
    assert_eq!(loaded.total_vector_count, 10);
    assert_eq!(loaded.shards[0].fingerprint, "abc");
}

#[test]
fn test_validate_bad_count() {
    let mut m = sample_manifest();
    m.shards[0].vector_count = 999;
    assert!(m.validate().is_err());
}

#[test]
fn test_validate_rejects_empty_fingerprint() {
    let mut m = sample_manifest();
    m.shards[0].fingerprint.clear();

    let err = m.validate().unwrap_err();
    assert!(err
        .to_string()
        .contains("shard shard-0000 fingerprint must not be empty"));
}

#[test]
fn test_alias_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let m = sample_manifest();
    m.save(&store).unwrap();
    m.publish_alias(&store).unwrap();
    let loaded = Manifest::load_alias(&store, "latest").unwrap();
    assert_eq!(loaded.index_version, m.index_version);
}

#[test]
fn test_load_accepts_compat_fingerprint_field() {
    let compat = serde_json::json!({
        "manifest_version": 1,
        "dataset_version": "ds-v1",
        "embedding_version": "emb-v1",
        "index_version": "idx-v1",
        "alias": "latest",
        "dims": 4,
        "distance_metric": "cosine",
        "vectors_key": "datasets/ds-v1/vectors.jsonl",
        "metadata_key": "datasets/ds-v1/metadata.json",
        "total_vector_count": 5,
        "shards": [
            {
                "shard_id": 0,
                "artifact_key": "indexes/idx-v1/shards/shard-0000.sidx",
                "vector_count": 5,
                "fingerprint": "compat-fingerprint"
            }
        ],
        "build_metadata": {
            "built_at": "2026-03-10T17:44:00Z",
            "builder_version": "0.1.0",
            "num_kmeans_iters": 20,
            "nprobe_default": 2
        }
    });

    let manifest: Manifest = serde_json::from_value(compat).unwrap();
    assert_eq!(manifest.shards[0].fingerprint, "compat-fingerprint");
    // v1 manifests have no centroid data; the field defaults to an empty Vec.
    assert!(manifest.shards[0].centroid.is_empty());
}

#[test]
fn test_v2_centroid_round_trips() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();

    // Deserialize a genuine v2 document (no v3 lifecycle fields).
    let v2_value = sample_v2_manifest();
    let m: Manifest = serde_json::from_value(v2_value).unwrap();
    assert_eq!(m.manifest_version, 2);
    m.save(&store).unwrap();

    let manifest_key = Manifest::storage_key(&m.index_version);
    let saved = store.get(&manifest_key).unwrap();
    let saved_json = String::from_utf8(saved).unwrap();
    // Legacy manifests are upgraded to the current schema on write.
    assert!(saved_json.contains("\"manifest_version\": 4"));
    assert!(saved_json.contains("\"algorithm\""));
    assert!(saved_json.contains("\"compression\""));
    // Centroid is re-serialised into the saved JSON.
    assert!(saved_json.contains("\"centroid\""));

    let loaded = Manifest::load(&store, &m.index_version).unwrap();
    assert_eq!(loaded.manifest_version, 4);
    assert_eq!(loaded.shards[0].centroid, vec![0.1, 0.2, 0.3, 0.4]);
    assert_eq!(loaded.shards[1].centroid, vec![0.9, 0.8, 0.7, 0.6]);
    // v2 shards have no routing metadata; the field must remain None on upgrade.
    assert!(loaded.shards[0].routing.is_none());
    assert!(loaded.shards[1].routing.is_none());
}

#[test]
fn test_validate_rejects_unsupported_manifest_version() {
    let mut m = sample_manifest();
    m.manifest_version = 99;
    let err = m.validate().unwrap_err();
    assert!(err.to_string().contains("unsupported manifest_version 99"));
}

#[test]
fn test_validate_rejects_inconsistent_shard_summary() {
    let mut m = sample_manifest();
    m.shard_summary = Some(ShardSummary {
        num_shards: 99,
        min_shard_vector_count: 5,
        max_shard_vector_count: 5,
    });

    let err = m.validate().unwrap_err();
    assert!(err
        .to_string()
        .contains("shard_summary.num_shards mismatch: expected 2, found 99"));
}

#[test]
fn test_validate_rejects_invalid_recall_estimate() {
    let mut m = sample_manifest();
    m.recall_estimate = Some(RecallEstimate {
        k: 10,
        recall_at_k: 1.5,
        sample_size: 100,
    });

    let err = m.validate().unwrap_err();
    assert!(err
        .to_string()
        .contains("recall_estimate.recall_at_k must be finite and within [0, 1]"));
}

#[test]
fn test_validate_rejects_negative_build_duration() {
    let mut m = sample_manifest();
    m.build_metadata.build_duration_secs = -0.25;

    let err = m.validate().unwrap_err();
    assert!(err
        .to_string()
        .contains("build_metadata.build_duration_secs must be finite and >= 0"));
}

#[test]
fn test_validate_rejects_ivf_flat_without_coarse_quantizer_key() {
    let mut m = sample_manifest();
    m.algorithm.algorithm = "ivf-flat".into();

    let err = m.validate().unwrap_err();
    assert!(err
        .to_string()
        .contains("algorithm 'ivf-flat' requires coarse_quantizer_key"));
}

// ── manifest v4 routing metadata + lifecycle fields ───────────────────────────

#[test]
fn test_v4_round_trips_lifecycle_and_routing_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let m = sample_manifest();
    assert_eq!(m.manifest_version, 4);
    m.save(&store).unwrap();

    let saved = store.get(&Manifest::storage_key(&m.index_version)).unwrap();
    let saved_json = String::from_utf8(saved).unwrap();

    // Lifecycle fields must be present in the serialised output.
    assert!(saved_json.contains("\"algorithm\""));
    assert!(saved_json.contains("\"shard_summary\""));
    assert!(saved_json.contains("\"compression\""));
    assert!(saved_json.contains("\"build_duration_secs\""));
    // recall_estimate is None, so it must be absent.
    assert!(!saved_json.contains("\"recall_estimate\""));
    // Routing metadata must be serialised.
    assert!(saved_json.contains("\"routing\""));
    assert!(saved_json.contains("\"centroid_id\""));
    assert!(saved_json.contains("\"index_type\""));
    assert!(saved_json.contains("\"file_location\""));

    let loaded = Manifest::load(&store, &m.index_version).unwrap();
    assert_eq!(loaded.manifest_version, 4);
    assert_eq!(loaded.algorithm.algorithm, "kmeans-flat");
    assert!(loaded.algorithm.params.contains_key("num_shards"));
    assert!(
        loaded.algorithm.params.contains_key("kmeans_seed"),
        "kmeans_seed must be recorded in algorithm.params for reproducibility"
    );
    assert_eq!(
        loaded
            .algorithm
            .params
            .get("kmeans_seed")
            .unwrap()
            .as_u64()
            .unwrap(),
        shardlake_core::config::DEFAULT_KMEANS_SEED,
    );
    let summary = loaded.shard_summary.as_ref().unwrap();
    assert_eq!(summary.num_shards, 2);
    assert_eq!(summary.min_shard_vector_count, 5);
    assert_eq!(summary.max_shard_vector_count, 5);
    assert!(!loaded.compression.enabled);
    assert_eq!(loaded.compression.codec, "none");
    assert_eq!(loaded.build_metadata.build_duration_secs, 1.5);
    assert!(loaded.recall_estimate.is_none());

    // Routing metadata round-trips correctly.
    let r0 = loaded.shards[0].routing.as_ref().unwrap();
    assert_eq!(r0.centroid_id, "shard-0000");
    assert_eq!(r0.index_type, "flat");
    assert_eq!(r0.file_location, paths::index_shard_key("idx-v1", 0));

    let r1 = loaded.shards[1].routing.as_ref().unwrap();
    assert_eq!(r1.centroid_id, "shard-0001");
    assert_eq!(r1.index_type, "flat");
    assert_eq!(r1.file_location, paths::index_shard_key("idx-v1", 1));
}

#[test]
fn test_v1_manifest_defaults_new_fields() {
    // A v1 manifest (no algorithm / shard_summary / compression / build_duration_secs)
    // must deserialize with sensible defaults for all v3 fields.
    let compat = serde_json::json!({
        "manifest_version": 1,
        "dataset_version": "ds-v1",
        "embedding_version": "emb-v1",
        "index_version": "idx-v1",
        "alias": "latest",
        "dims": 4,
        "distance_metric": "cosine",
        "vectors_key": "datasets/ds-v1/vectors.jsonl",
        "metadata_key": "datasets/ds-v1/metadata.json",
        "total_vector_count": 5,
        "shards": [{
            "shard_id": 0,
            "artifact_key": "indexes/idx-v1/shards/shard-0000.sidx",
            "vector_count": 5,
            "fingerprint": "compat-fingerprint"
        }],
        "build_metadata": {
            "built_at": "2026-03-10T17:44:00Z",
            "builder_version": "0.1.0",
            "num_kmeans_iters": 20,
            "nprobe_default": 2
        }
    });

    let manifest: Manifest = serde_json::from_value(compat).unwrap();
    // Algorithm defaults to kmeans-flat.
    assert_eq!(manifest.algorithm.algorithm, "kmeans-flat");
    assert!(manifest.algorithm.variant.is_none());
    assert!(manifest.algorithm.params.is_empty());
    // ShardSummary is absent for old manifests.
    assert!(manifest.shard_summary.is_none());
    // Compression defaults to disabled / none.
    assert!(!manifest.compression.enabled);
    assert_eq!(manifest.compression.codec, "none");
    // build_duration_secs defaults to 0.0.
    assert_eq!(manifest.build_metadata.build_duration_secs, 0.0);
    // recall_estimate is absent.
    assert!(manifest.recall_estimate.is_none());
    // routing is absent for v1 manifests.
    assert!(manifest.shards[0].routing.is_none());
}

// ── compatibility checks ──────────────────────────────────────────────────────

#[test]
fn test_check_dimension_compat_ok() {
    let m = sample_manifest();
    assert!(m.check_dimension_compat(4).is_ok());
}

#[test]
fn test_check_dimension_compat_mismatch() {
    let m = sample_manifest();
    let err = m.check_dimension_compat(128).unwrap_err();
    assert!(matches!(err, ManifestError::Compatibility(_)));
    assert!(err.to_string().contains("dimension mismatch"));
    assert!(err.to_string().contains("manifest has 4"));
    assert!(err.to_string().contains("requested 128"));
}

#[test]
fn test_check_dataset_version_compat_ok() {
    let m = sample_manifest();
    assert!(m
        .check_dataset_version_compat(&DatasetVersion("ds-v1".into()))
        .is_ok());
}

#[test]
fn test_check_dataset_version_compat_mismatch() {
    let m = sample_manifest();
    let err = m
        .check_dataset_version_compat(&DatasetVersion("ds-v2".into()))
        .unwrap_err();
    assert!(matches!(err, ManifestError::Compatibility(_)));
    assert!(err.to_string().contains("dataset version mismatch"));
    assert!(err.to_string().contains("manifest has ds-v1"));
    assert!(err.to_string().contains("requested ds-v2"));
}

#[test]
fn test_check_algorithm_compat_ok() {
    let m = sample_manifest();
    assert!(m.check_algorithm_compat("kmeans-flat").is_ok());
}

#[test]
fn test_check_algorithm_compat_mismatch() {
    let m = sample_manifest();
    let err = m.check_algorithm_compat("hnsw").unwrap_err();
    assert!(matches!(err, ManifestError::Compatibility(_)));
    assert!(err.to_string().contains("algorithm mismatch"));
    assert!(err.to_string().contains("manifest has kmeans-flat"));
    assert!(err.to_string().contains("requested hnsw"));
}

#[test]
fn test_check_algorithm_compat_uses_v1_default() {
    // A v1/v2 manifest has no algorithm field; it deserializes to the
    // default "kmeans-flat", so the compatibility check should pass for
    // "kmeans-flat" and fail for anything else.
    let compat = serde_json::json!({
        "manifest_version": 2,
        "dataset_version": "ds-v1",
        "embedding_version": "emb-v1",
        "index_version": "idx-v1",
        "alias": "latest",
        "dims": 4,
        "distance_metric": "cosine",
        "vectors_key": "datasets/ds-v1/vectors.jsonl",
        "metadata_key": "datasets/ds-v1/metadata.json",
        "total_vector_count": 5,
        "shards": [{
            "shard_id": 0,
            "artifact_key": "indexes/idx-v1/shards/shard-0000.sidx",
            "vector_count": 5,
            "sha256": "abc"
        }],
        "build_metadata": {
            "built_at": "2026-03-10T17:44:00Z",
            "builder_version": "0.1.0",
            "num_kmeans_iters": 20,
            "nprobe_default": 2
        }
    });
    let manifest: Manifest = serde_json::from_value(compat).unwrap();
    assert!(manifest.check_algorithm_compat("kmeans-flat").is_ok());
    assert!(manifest.check_algorithm_compat("hnsw").is_err());
}

// ── routing metadata validation ───────────────────────────────────────────────

/// A v4 manifest with a shard whose `routing.centroid_id` is empty must fail
/// validation.
#[test]
fn test_validate_rejects_empty_routing_centroid_id() {
    let mut m = sample_manifest();
    if let Some(routing) = m.shards[0].routing.as_mut() {
        routing.centroid_id.clear();
    }
    let err = m.validate().unwrap_err();
    assert!(err
        .to_string()
        .contains("routing.centroid_id must not be empty"));
}

/// A v4 manifest with a shard whose `routing.index_type` is empty must fail
/// validation.
#[test]
fn test_validate_rejects_empty_routing_index_type() {
    let mut m = sample_manifest();
    if let Some(routing) = m.shards[0].routing.as_mut() {
        routing.index_type.clear();
    }
    let err = m.validate().unwrap_err();
    assert!(err
        .to_string()
        .contains("routing.index_type must not be empty"));
}

/// A v4 manifest with a shard whose `routing.file_location` is empty must fail
/// validation.
#[test]
fn test_validate_rejects_empty_routing_file_location() {
    let mut m = sample_manifest();
    if let Some(routing) = m.shards[0].routing.as_mut() {
        routing.file_location.clear();
    }
    let err = m.validate().unwrap_err();
    assert!(err
        .to_string()
        .contains("routing.file_location must not be empty"));
}

/// A v3 manifest without routing metadata must still pass validation (backward
/// compatibility — `routing` is optional).
#[test]
fn test_v3_manifest_without_routing_passes_validation() {
    let compat = serde_json::json!({
        "manifest_version": 3,
        "dataset_version": "ds-v1",
        "embedding_version": "emb-v1",
        "index_version": "idx-v1",
        "alias": "latest",
        "dims": 4,
        "distance_metric": "cosine",
        "vectors_key": "datasets/ds-v1/vectors.jsonl",
        "metadata_key": "datasets/ds-v1/metadata.json",
        "total_vector_count": 5,
        "shards": [{
            "shard_id": 0,
            "artifact_key": "indexes/idx-v1/shards/shard-0000.sidx",
            "vector_count": 5,
            "sha256": "abc",
            "centroid": [0.1, 0.2, 0.3, 0.4]
        }],
        "build_metadata": {
            "built_at": "2026-03-10T17:44:00Z",
            "builder_version": "0.1.0",
            "num_kmeans_iters": 20,
            "nprobe_default": 2,
            "build_duration_secs": 1.0
        },
        "algorithm": { "algorithm": "kmeans-flat" },
        "compression": { "enabled": false, "codec": "none" }
    });
    let manifest: Manifest = serde_json::from_value(compat).unwrap();
    assert!(manifest.validate().is_ok());
    // routing must default to None for older manifests.
    assert!(manifest.shards[0].routing.is_none());
}
