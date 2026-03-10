use chrono::Utc;
use shardlake_core::types::{
    DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, ShardId,
};
use shardlake_manifest::{BuildMetadata, Manifest, ShardDef};
use shardlake_storage::LocalObjectStore;

fn sample_manifest() -> Manifest {
    Manifest {
        manifest_version: 1,
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
        },
    }
}

#[test]
fn test_save_load_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let m = sample_manifest();
    m.save(&store).unwrap();
    let loaded = Manifest::load(&store, &m.index_version).unwrap();
    assert_eq!(loaded.index_version, m.index_version);
    assert_eq!(loaded.total_vector_count, 10);
}

#[test]
fn test_validate_bad_count() {
    let mut m = sample_manifest();
    m.shards[0].vector_count = 999;
    assert!(m.validate().is_err());
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
