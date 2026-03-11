use chrono::Utc;
use shardlake_core::types::{
    DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, ShardId,
};
use shardlake_manifest::{BuildMetadata, Manifest, ShardDef};
use shardlake_storage::{LocalObjectStore, ObjectStore};

fn sample_manifest() -> Manifest {
    Manifest {
        manifest_version: 2,
        dataset_version: DatasetVersion("ds-v1".into()),
        embedding_version: EmbeddingVersion("emb-v1".into()),
        index_version: IndexVersion("idx-v1".into()),
        alias: "latest".into(),
        dims: 4,
        distance_metric: DistanceMetric::Cosine,
        vectors_key: "datasets/ds-v1/vectors.jsonl".into(),
        metadata_key: "datasets/ds-v1/metadata.json".into(),
        total_vector_count: 10,
        shards: vec![
            ShardDef {
                shard_id: ShardId(0),
                artifact_key: "indexes/idx-v1/shards/shard-0000.sidx".into(),
                vector_count: 5,
                fingerprint: "abc".into(),
                centroid: vec![0.1, 0.2, 0.3, 0.4],
            },
            ShardDef {
                shard_id: ShardId(1),
                artifact_key: "indexes/idx-v1/shards/shard-0001.sidx".into(),
                vector_count: 5,
                fingerprint: "def".into(),
                centroid: vec![0.9, 0.8, 0.7, 0.6],
            },
        ],
        build_metadata: BuildMetadata {
            built_at: Utc::now(),
            builder_version: "0.1.0".into(),
            num_kmeans_iters: 20,
            nprobe_default: 2,
        },
    }
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
    let m = sample_manifest();
    assert_eq!(m.manifest_version, 2);
    m.save(&store).unwrap();

    let manifest_key = Manifest::storage_key(&m.index_version);
    let saved = store.get(&manifest_key).unwrap();
    let saved_json = String::from_utf8(saved).unwrap();
    // Centroid is serialised into the JSON for v2 manifests.
    assert!(saved_json.contains("\"centroid\""));

    let loaded = Manifest::load(&store, &m.index_version).unwrap();
    assert_eq!(loaded.manifest_version, 2);
    assert_eq!(loaded.shards[0].centroid, vec![0.1, 0.2, 0.3, 0.4]);
    assert_eq!(loaded.shards[1].centroid, vec![0.9, 0.8, 0.7, 0.6]);
}

#[test]
fn test_validate_rejects_unsupported_manifest_version() {
    let mut m = sample_manifest();
    m.manifest_version = 99;
    let err = m.validate().unwrap_err();
    assert!(err.to_string().contains("unsupported manifest_version 99"));
}
