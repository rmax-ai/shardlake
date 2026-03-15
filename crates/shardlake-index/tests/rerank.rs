//! Integration tests for exact reranking of ANN search candidates.

use std::{collections::HashMap, sync::Arc};

use shardlake_core::{
    config::SystemConfig,
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, SearchResult, VectorId,
        VectorRecord,
    },
};
use shardlake_index::{BuildParams, IndexBuilder, IndexSearcher};
use shardlake_storage::{LocalObjectStore, ObjectStore};

/// Build a small searcher with known 2-D vectors and a given distance metric.
fn make_searcher(
    tmp: &tempfile::TempDir,
    records: Vec<VectorRecord>,
    metric: DistanceMetric,
) -> IndexSearcher {
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = SystemConfig {
        storage_root: tmp.path().to_path_buf(),
        num_shards: 1,
        kmeans_iters: 2,
        nprobe: 1,
        kmeans_seed: SystemConfig::default_kmeans_seed(),
        kmeans_sample_size: None,
    };
    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records,
            dataset_version: DatasetVersion("ds-rr".into()),
            embedding_version: EmbeddingVersion("emb-rr".into()),
            index_version: IndexVersion("idx-rr".into()),
            metric,
            dims: 2,
            vectors_key: "datasets/ds-rr/vectors.jsonl".into(),
            metadata_key: "datasets/ds-rr/metadata.json".into(),
        })
        .unwrap();
    IndexSearcher::new(store as Arc<dyn ObjectStore>, manifest)
}

fn records_2d() -> Vec<VectorRecord> {
    vec![
        VectorRecord {
            id: VectorId(1),
            data: vec![1.0, 0.0],
            metadata: None,
        },
        VectorRecord {
            id: VectorId(2),
            data: vec![0.0, 1.0],
            metadata: None,
        },
        VectorRecord {
            id: VectorId(3),
            data: vec![1.0, 1.0],
            metadata: None,
        },
    ]
}

/// Run a search to warm the shard cache so `rerank` has raw vectors available.
fn warm_cache(searcher: &IndexSearcher, query: &[f32]) {
    let _ = searcher.search(query, 3, 1).unwrap();
}

/// Rerank must correct scores when candidates are injected with wrong scores.
///
/// Query [1.0, 0.1] is closest to ID 1 by Euclidean distance.  If we inject
/// a candidate list where ID 2 has an artificially low (better) score, rerank
/// should restore the correct ordering based on the raw vectors.
#[test]
fn rerank_corrects_approximate_scores() {
    let tmp = tempfile::tempdir().unwrap();
    let searcher = make_searcher(&tmp, records_2d(), DistanceMetric::Euclidean);
    let query = [1.0f32, 0.1];

    warm_cache(&searcher, &query);

    // Inject candidates with intentionally wrong (swapped) scores.
    let wrong_candidates = vec![
        SearchResult {
            id: VectorId(2),
            score: 0.0001,
            metadata: None,
        }, // wrong: ID2 is far
        SearchResult {
            id: VectorId(1),
            score: 9999.0,
            metadata: None,
        }, // wrong: ID1 is close
    ];

    let reranked = searcher.rerank(&query, wrong_candidates).unwrap();

    // After reranking, ID1 must come first because dist([1,0.1], [1,0]) < dist([1,0.1], [0,1]).
    assert_eq!(reranked.len(), 2);
    assert_eq!(
        reranked[0].id,
        VectorId(1),
        "ID1 should be ranked first after exact reranking"
    );
    assert_eq!(reranked[1].id, VectorId(2));
}

/// Reranked scores must match manually computed exact Euclidean distances.
#[test]
fn rerank_scores_match_exact_euclidean_distance() {
    let tmp = tempfile::tempdir().unwrap();
    let searcher = make_searcher(&tmp, records_2d(), DistanceMetric::Euclidean);
    let query = [0.5f32, 0.5];

    // search() both warms the cache and provides the initial candidate list.
    let ann_results = searcher.search(&query, 3, 1).unwrap();
    let reranked = searcher.rerank(&query, ann_results).unwrap();
    assert!(!reranked.is_empty());

    // Manually compute expected Euclidean distances from query [0.5, 0.5].
    let expected: HashMap<VectorId, f32> = [
        (
            VectorId(1),
            ((0.5f32 - 1.0).powi(2) + (0.5f32 - 0.0).powi(2)).sqrt(),
        ),
        (
            VectorId(2),
            ((0.5f32 - 0.0).powi(2) + (0.5f32 - 1.0).powi(2)).sqrt(),
        ),
        (
            VectorId(3),
            ((0.5f32 - 1.0).powi(2) + (0.5f32 - 1.0).powi(2)).sqrt(),
        ),
    ]
    .into_iter()
    .collect();

    for result in &reranked {
        let exp = expected[&result.id];
        assert!(
            (result.score - exp).abs() < 1e-5,
            "score for {:?} was {}, expected {}",
            result.id,
            result.score,
            exp
        );
    }

    // Results must be in ascending score order.
    let scores: Vec<f32> = reranked.iter().map(|r| r.score).collect();
    for window in scores.windows(2) {
        assert!(
            window[0] <= window[1],
            "reranked results are not sorted ascending: {:?}",
            scores
        );
    }
}

/// Reranking with cosine distance must produce correct order.
#[test]
fn rerank_cosine_distance_ordering() {
    let tmp = tempfile::tempdir().unwrap();
    // Use vectors where cosine similarity gives a clear ordering.
    // Query [1, 0]:
    //   cosine dist to [1, 0] = 0 (identical direction)
    //   cosine dist to [0, 1] = 1 (orthogonal)
    //   cosine dist to [1, 1] = 1 - 1/sqrt(2) ≈ 0.293
    let searcher = make_searcher(&tmp, records_2d(), DistanceMetric::Cosine);
    let query = [1.0f32, 0.0];

    let ann_results = searcher.search(&query, 3, 1).unwrap();
    let reranked = searcher.rerank(&query, ann_results).unwrap();

    assert_eq!(
        reranked[0].id,
        VectorId(1),
        "ID1=[1,0] is most similar to query [1,0]"
    );
    // ID3=[1,1] should rank before ID2=[0,1].
    assert_eq!(reranked[1].id, VectorId(3));
    assert_eq!(reranked[2].id, VectorId(2));
}

/// When rerank=false results come directly from ANN; when rerank=true they are
/// re-scored.  In the IVF-flat case with a single shard both should agree on
/// the top result but the scores after reranking must reflect exact distances.
#[test]
fn rerank_top_result_is_stable_for_exact_ann() {
    let tmp = tempfile::tempdir().unwrap();
    let searcher = make_searcher(&tmp, records_2d(), DistanceMetric::Euclidean);
    let query = [0.9f32, 0.0];

    let ann = searcher.search(&query, 3, 1).unwrap();
    let reranked = searcher.rerank(&query, ann.clone()).unwrap();

    // The best result must be the same.
    assert_eq!(
        ann[0].id, reranked[0].id,
        "top-1 should be the same candidate with and without reranking"
    );
}
