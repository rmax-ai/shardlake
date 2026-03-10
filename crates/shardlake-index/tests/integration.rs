//! Integration test: build index and verify search results.

use std::sync::Arc;

use shardlake_core::{
    config::SystemConfig,
    query::QueryConfig,
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
    },
};
use shardlake_index::{BuildParams, IndexBuilder, IndexSearcher, QueryPipeline};
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

fn build_test_index(
    store: Arc<LocalObjectStore>,
    records: Vec<VectorRecord>,
    metric: DistanceMetric,
) -> shardlake_manifest::Manifest {
    let config = SystemConfig {
        storage_root: std::path::PathBuf::new(),
        num_shards: 2,
        kmeans_iters: 10,
        nprobe: 2,
    };
    let builder = IndexBuilder::new(store.as_ref(), &config);
    builder
        .build(BuildParams {
            records,
            dataset_version: DatasetVersion("ds-test".into()),
            embedding_version: EmbeddingVersion("emb-test".into()),
            index_version: IndexVersion("idx-test".into()),
            metric,
            dims: 4,
            vectors_key: "datasets/ds-test/vectors.jsonl".into(),
            metadata_key: "datasets/ds-test/metadata.json".into(),
        })
        .unwrap()
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

// ---------------------------------------------------------------------------
// Pipeline stage tests
// ---------------------------------------------------------------------------

#[test]
fn test_pipeline_run_sync() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let records = make_records(20, 4);
    let manifest = build_test_index(
        Arc::clone(&store),
        records.clone(),
        DistanceMetric::Euclidean,
    );

    let searcher = IndexSearcher::new(
        Arc::clone(&store) as Arc<dyn shardlake_storage::ObjectStore>,
        manifest,
    );

    let pipeline = QueryPipeline::new(&searcher);
    let config = QueryConfig {
        top_k: 5,
        candidate_shards: 2,
        rerank_limit: None,
        distance_metric: None,
    };

    let results = pipeline.run(&records[0].data, &config).unwrap();
    assert!(!results.is_empty());
    assert_eq!(
        results[0].id,
        VectorId(0),
        "closest vector to itself should be id 0"
    );
}

#[test]
fn test_pipeline_run_with_rerank() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let records = make_records(20, 4);
    let manifest = build_test_index(
        Arc::clone(&store),
        records.clone(),
        DistanceMetric::Euclidean,
    );

    let searcher = IndexSearcher::new(
        Arc::clone(&store) as Arc<dyn shardlake_storage::ObjectStore>,
        manifest,
    );

    let pipeline = QueryPipeline::new(&searcher);
    let config = QueryConfig {
        top_k: 3,
        candidate_shards: 2,
        rerank_limit: Some(10),
        distance_metric: None,
    };

    let results = pipeline.run(&records[0].data, &config).unwrap();
    // With rerank_limit > top_k the pipeline gathers 10 candidates and trims to 3.
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].id, VectorId(0));
}

#[test]
fn test_pipeline_distance_metric_override() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let records = make_records(20, 4);
    // Index built with Euclidean …
    let manifest = build_test_index(
        Arc::clone(&store),
        records.clone(),
        DistanceMetric::Euclidean,
    );

    let searcher = IndexSearcher::new(
        Arc::clone(&store) as Arc<dyn shardlake_storage::ObjectStore>,
        manifest,
    );

    let pipeline = QueryPipeline::new(&searcher);
    // … but queried with Cosine override.
    let config = QueryConfig {
        top_k: 5,
        candidate_shards: 2,
        rerank_limit: None,
        distance_metric: Some(DistanceMetric::Cosine),
    };

    let results = pipeline.run(&records[0].data, &config).unwrap();
    assert!(!results.is_empty());
}

#[test]
fn test_pipeline_stage_merge() {
    use shardlake_core::types::{SearchResult, VectorId};
    use shardlake_index::exact::merge_top_k;

    let candidates: Vec<SearchResult> = vec![
        SearchResult {
            id: VectorId(3),
            score: 0.3,
            metadata: None,
        },
        SearchResult {
            id: VectorId(1),
            score: 0.1,
            metadata: None,
        },
        SearchResult {
            id: VectorId(2),
            score: 0.2,
            metadata: None,
        },
        SearchResult {
            id: VectorId(1),
            score: 0.15,
            metadata: None,
        }, // duplicate
    ];
    let merged = merge_top_k(candidates.clone(), 2);
    assert_eq!(merged.len(), 2);
    assert_eq!(merged[0].id, VectorId(1));
    assert_eq!(merged[1].id, VectorId(2));

    // Rerank: same ordering but explicit stage call
    let reranked = QueryPipeline::rerank(candidates, 2);
    assert_eq!(reranked.len(), 2);
    assert_eq!(reranked[0].id, VectorId(1));
}

#[tokio::test]
async fn test_pipeline_run_parallel() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let records = make_records(20, 4);
    let manifest = build_test_index(
        Arc::clone(&store),
        records.clone(),
        DistanceMetric::Euclidean,
    );

    let searcher = Arc::new(IndexSearcher::new(
        Arc::clone(&store) as Arc<dyn shardlake_storage::ObjectStore>,
        manifest,
    ));

    let config = QueryConfig {
        top_k: 5,
        candidate_shards: 2,
        rerank_limit: None,
        distance_metric: None,
    };

    let query: Arc<[f32]> = records[0].data.clone().into();
    let results = QueryPipeline::run_parallel(Arc::clone(&searcher), query, config)
        .await
        .unwrap();

    assert!(!results.is_empty());
    assert_eq!(
        results[0].id,
        VectorId(0),
        "parallel search: closest to itself should be id 0"
    );
}

#[tokio::test]
async fn test_pipeline_run_parallel_with_rerank() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let records = make_records(20, 4);
    let manifest = build_test_index(Arc::clone(&store), records.clone(), DistanceMetric::Cosine);

    let searcher = Arc::new(IndexSearcher::new(
        Arc::clone(&store) as Arc<dyn shardlake_storage::ObjectStore>,
        manifest,
    ));

    let config = QueryConfig {
        top_k: 3,
        candidate_shards: 2,
        rerank_limit: Some(10),
        distance_metric: None,
    };

    let query: Arc<[f32]> = records[0].data.clone().into();
    let results = QueryPipeline::run_parallel(Arc::clone(&searcher), query, config)
        .await
        .unwrap();

    assert_eq!(results.len(), 3, "should return exactly top_k=3 results");
    assert_eq!(results[0].id, VectorId(0));
}
