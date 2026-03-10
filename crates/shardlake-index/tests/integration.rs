//! Integration test: build index and verify search results.

use std::sync::Arc;

use shardlake_core::{
    config::SystemConfig,
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
    },
};
use shardlake_index::{BuildParams, IndexBuilder, IndexSearcher};
use shardlake_storage::LocalObjectStore;

fn make_records(n: usize, dims: usize) -> Vec<VectorRecord> {
    (0..n)
        .map(|i| VectorRecord {
            id: VectorId(i as u64),
            data: (0..dims).map(|d| (i * dims + d) as f32 / 100.0).collect(),
            metadata: None,
        })
        .collect()
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
        candidate_centroids: 0,
        candidate_shards: 0,
        max_vectors_per_shard: 0,
        kmeans_sample_size: 0,
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
