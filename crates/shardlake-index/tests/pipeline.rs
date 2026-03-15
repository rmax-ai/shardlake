//! Integration tests for the composable query pipeline.
//!
//! Covers routing behaviour, PQ approximate scoring, exact reranking, and
//! top-k result shaping.

use std::sync::Arc;

use shardlake_core::{
    config::{FanOutPolicy, SystemConfig},
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, ShardId, VectorId,
        VectorRecord,
    },
};
use shardlake_index::{
    pipeline::{
        CandidateSearchStage, ExactCandidateStage, ExactRerankStage, PqCandidateStage,
        QueryPipeline, RerankStage,
    },
    pq::{PqCodebook, PqParams},
    shard::ShardIndex,
    BuildParams, IndexBuilder, IndexSearcher,
};
use shardlake_storage::{LocalObjectStore, ObjectStore};

// ── Helpers ──────────────────────────────────────────────────────────────────

fn make_records(n: usize, dims: usize) -> Vec<VectorRecord> {
    (0..n)
        .map(|i| VectorRecord {
            id: VectorId(i as u64),
            data: (0..dims).map(|d| (i * dims + d) as f32 / 100.0).collect(),
            metadata: None,
        })
        .collect()
}

fn build_index(
    records: Vec<VectorRecord>,
    num_shards: u32,
    dims: usize,
    dataset_tag: &str,
) -> (
    Arc<dyn ObjectStore>,
    shardlake_manifest::Manifest,
    tempfile::TempDir,
) {
    build_index_with_metric(
        records,
        num_shards,
        dims,
        dataset_tag,
        DistanceMetric::Euclidean,
    )
}

fn build_index_with_metric(
    records: Vec<VectorRecord>,
    num_shards: u32,
    dims: usize,
    dataset_tag: &str,
    metric: DistanceMetric,
) -> (
    Arc<dyn ObjectStore>,
    shardlake_manifest::Manifest,
    tempfile::TempDir,
) {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = SystemConfig {
        storage_root: tmp.path().to_path_buf(),
        num_shards,
        kmeans_iters: 10,
        nprobe: num_shards,
        kmeans_seed: SystemConfig::default_kmeans_seed(),
        kmeans_sample_size: None,
        ..SystemConfig::default()
    };
    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records,
            dataset_version: DatasetVersion(dataset_tag.into()),
            embedding_version: EmbeddingVersion("emb".into()),
            index_version: IndexVersion("idx".into()),
            metric,
            dims,
            vectors_key: format!("datasets/{dataset_tag}/vectors.jsonl"),
            metadata_key: format!("datasets/{dataset_tag}/metadata.json"),
            pq_params: None,
        })
        .unwrap();
    (store, manifest, tmp)
}

// ── Routing tests ─────────────────────────────────────────────────────────────

/// With nprobe=1 and 4 shards the pipeline must probe exactly one shard.
#[test]
fn pipeline_nprobe_limits_shards_probed() {
    use shardlake_storage::StorageError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        shard_loads: Arc<AtomicUsize>,
    }

    impl ObjectStore for CountingStore {
        fn put(&self, key: &str, data: Vec<u8>) -> Result<(), StorageError> {
            self.inner.put(key, data)
        }
        fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
            if key.ends_with(".sidx") {
                self.shard_loads.fetch_add(1, Ordering::Relaxed);
            }
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

    let records = make_records(40, 4);
    let (store, manifest, _tmp) = build_index(records.clone(), 4, 4, "ds-route");

    let counter = Arc::new(AtomicUsize::new(0));
    let counting: Arc<dyn ObjectStore> = Arc::new(CountingStore {
        inner: Arc::clone(&store),
        shard_loads: Arc::clone(&counter),
    });

    let pipeline = QueryPipeline::builder(counting, manifest).build();

    let query = records[0].data.clone();
    let results = pipeline.search(&query, 5, 1).unwrap();
    assert!(!results.is_empty());

    let loads = counter.load(Ordering::Relaxed);
    assert_eq!(
        loads, 1,
        "nprobe=1 should load exactly 1 shard; got {loads}"
    );
}

/// The first result should be the query vector itself when it is part of the
/// index.
#[test]
fn pipeline_returns_self_as_nearest_neighbor() {
    let records = make_records(20, 4);
    let query = records[3].data.clone();
    let (store, manifest, _tmp) = build_index(records, 2, 4, "ds-self");

    let pipeline = QueryPipeline::builder(store, manifest).build();
    let results = pipeline.search(&query, 1, 2).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].id,
        VectorId(3),
        "nearest to record 3 must be record 3 itself"
    );
}

/// The pipeline must return at most k results regardless of nprobe.
#[test]
fn pipeline_result_count_bounded_by_k() {
    let records = make_records(20, 4);
    let query = records[0].data.clone();
    let (store, manifest, _tmp) = build_index(records, 2, 4, "ds-bound");

    let pipeline = QueryPipeline::builder(store, manifest).build();
    for k in [1usize, 3, 5, 20] {
        let results = pipeline.search(&query, k, 2).unwrap();
        assert!(
            results.len() <= k,
            "k={k}: expected ≤ {k} results, got {}",
            results.len()
        );
    }
}

// ── Candidate retrieval correctness ──────────────────────────────────────────

/// The default exact pipeline and the IndexSearcher must agree on results.
#[test]
fn pipeline_matches_searcher_results() {
    let records = make_records(20, 4);
    let (store, manifest, _tmp) = build_index(records.clone(), 2, 4, "ds-match");

    let searcher = IndexSearcher::new(Arc::clone(&store), manifest.clone());
    let pipeline = QueryPipeline::builder(Arc::clone(&store), manifest).build();

    for i in [0, 5, 10, 15] {
        let query = records[i].data.clone();
        let policy = FanOutPolicy {
            candidate_centroids: 2,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let s_res = searcher.search(&query, 5, &policy).unwrap();
        let p_res = pipeline.search(&query, 5, 2).unwrap();
        assert_eq!(
            s_res.len(),
            p_res.len(),
            "query {i}: result count differs between searcher and pipeline"
        );
        let s_ids: Vec<VectorId> = s_res.iter().map(|r| r.id).collect();
        let p_ids: Vec<VectorId> = p_res.iter().map(|r| r.id).collect();
        assert_eq!(
            s_ids, p_ids,
            "query {i}: result order differs between searcher and pipeline"
        );
    }
}

// ── PQ scoring behaviour ──────────────────────────────────────────────────────

/// [`PqCandidateStage`] must rank the identical vector above distant vectors.
#[test]
fn pq_candidate_stage_ranks_identical_vector_first() {
    let records = make_records(30, 4);
    let vecs: Vec<Vec<f32>> = records.iter().map(|r| r.data.clone()).collect();

    let pq = Arc::new(
        PqCodebook::train(
            &vecs,
            PqParams {
                num_subspaces: 2,
                codebook_size: 16,
            },
            0xdead_beef,
            20,
        )
        .unwrap(),
    );
    let stage = PqCandidateStage::new(Arc::clone(&pq));

    let shard = ShardIndex {
        shard_id: ShardId(0),
        dims: 4,
        centroids: vec![records[0].data.clone()],
        records: records.clone(),
    };

    let query = records[0].data.clone();
    let results = stage
        .search_shard(&query, &shard, 5, DistanceMetric::Euclidean)
        .unwrap();

    assert!(
        results.iter().any(|r| r.id == VectorId(0)),
        "PQ stage must include the identical vector (id=0) in top-5"
    );
    // The ADC score for the identical vector should be the minimum.
    let id0_score = results
        .iter()
        .find(|r| r.id == VectorId(0))
        .map(|r| r.score)
        .unwrap();
    let min_score = results.iter().map(|r| r.score).fold(f32::MAX, f32::min);
    assert!(
        (id0_score - min_score).abs() < 1e-5 || id0_score == min_score,
        "id=0 should have the lowest PQ score; got {id0_score} vs min {min_score}"
    );
}

/// PQ top-k recall should be non-trivial against the exact ground truth.
#[test]
fn pq_candidate_stage_has_reasonable_recall() {
    use shardlake_index::exact::exact_search;

    let records = make_records(50, 4);
    let vecs: Vec<Vec<f32>> = records.iter().map(|r| r.data.clone()).collect();

    let pq = Arc::new(
        PqCodebook::train(
            &vecs,
            PqParams {
                num_subspaces: 2,
                codebook_size: 16,
            },
            42,
            30,
        )
        .unwrap(),
    );
    let stage = PqCandidateStage::new(Arc::clone(&pq));

    let shard = ShardIndex {
        shard_id: ShardId(0),
        dims: 4,
        centroids: vec![records[0].data.clone()],
        records: records.clone(),
    };

    let k = 5;
    let mut total_recall = 0.0f64;
    let num_queries = 10usize;

    for i in 0..num_queries {
        let query = records[i * 4].data.clone();

        let exact = exact_search(&query, &records, DistanceMetric::Euclidean, k);
        let approx = stage
            .search_shard(&query, &shard, k, DistanceMetric::Euclidean)
            .unwrap();

        let exact_ids: Vec<VectorId> = exact.iter().map(|r| r.id).collect();
        let approx_ids: Vec<VectorId> = approx.iter().map(|r| r.id).collect();
        let hits = approx_ids
            .iter()
            .filter(|id| exact_ids.contains(id))
            .count();
        total_recall += hits as f64 / k as f64;
    }

    let avg_recall = total_recall / num_queries as f64;
    assert!(
        avg_recall >= 0.4,
        "PQ recall@{k} should be ≥ 0.4 over {num_queries} queries; got {avg_recall:.3}"
    );
}

/// PQ-backed pipelines must reject non-Euclidean metrics explicitly.
#[test]
fn pipeline_with_pq_stage_rejects_non_euclidean_metrics() {
    let records = make_records(30, 4);
    let vecs: Vec<Vec<f32>> = records.iter().map(|r| r.data.clone()).collect();
    let pq = Arc::new(
        PqCodebook::train(
            &vecs,
            PqParams {
                num_subspaces: 2,
                codebook_size: 16,
            },
            0xdead_beef,
            20,
        )
        .unwrap(),
    );

    for metric in [DistanceMetric::Cosine, DistanceMetric::InnerProduct] {
        let (store, manifest, _tmp) =
            build_index_with_metric(records.clone(), 2, 4, "ds-pq-metric", metric);
        let pipeline = QueryPipeline::builder(store, manifest)
            .candidate_stage(Arc::new(PqCandidateStage::new(Arc::clone(&pq))))
            .build();

        let err = pipeline.search(&records[0].data, 5, 2).unwrap_err();
        assert!(
            err.to_string()
                .contains("PQ search currently supports only euclidean distance"),
            "expected explicit PQ metric rejection for {metric:?}, got {err}"
        );
    }
}

// ── Reranking correctness ─────────────────────────────────────────────────────

/// [`ExactRerankStage`] must correctly rescore candidates that had wrong
/// approximate scores.
#[test]
fn exact_rerank_corrects_wrong_scores() {
    let records = make_records(10, 4);
    let reranker = ExactRerankStage;

    let k = 3;
    let query = records[0].data.clone();

    // Flip the scores so the worst vector gets the best approximate score.
    let inverted_candidates: Vec<_> = records
        .iter()
        .map(|r| shardlake_core::types::SearchResult {
            id: r.id,
            // Assign inverse rank as score — exact reranking must correct this.
            score: (records.len() - r.id.0 as usize) as f32,
            metadata: None,
        })
        .collect();

    let reranked = reranker.rerank(
        &query,
        inverted_candidates,
        &records,
        DistanceMetric::Euclidean,
        k,
    );

    assert_eq!(reranked.len(), k);
    // After exact rescoring the nearest vector (id=0) must be ranked first.
    assert_eq!(
        reranked[0].id,
        VectorId(0),
        "exact reranker must place the true nearest vector first"
    );
}

/// With PQ + exact reranking the final result must equal the exact-only
/// pipeline result.
#[test]
fn pipeline_pq_plus_rerank_equals_exact_result() {
    let records = make_records(20, 4);
    let vecs: Vec<Vec<f32>> = records.iter().map(|r| r.data.clone()).collect();

    let (store, manifest, _tmp) = build_index(records.clone(), 2, 4, "ds-pqrr");

    let pq = Arc::new(
        PqCodebook::train(
            &vecs,
            PqParams {
                num_subspaces: 2,
                codebook_size: 16,
            },
            0xdead_beef,
            20,
        )
        .unwrap(),
    );

    let exact_pipeline = QueryPipeline::builder(Arc::clone(&store), manifest.clone()).build();
    let pq_rerank_pipeline = QueryPipeline::builder(Arc::clone(&store), manifest)
        .candidate_stage(Arc::new(PqCandidateStage::new(Arc::clone(&pq))))
        .rerank_stage(Arc::new(ExactRerankStage))
        .rerank_oversample(10)
        .build();

    let k = 3;
    let query = records[5].data.clone();

    let exact_results = exact_pipeline.search(&query, k, 2).unwrap();
    let pq_rr_results = pq_rerank_pipeline.search(&query, k, 2).unwrap();

    // With sufficient oversample the PQ+rerank result set must match exact.
    assert_eq!(
        exact_results.len(),
        pq_rr_results.len(),
        "result counts must match"
    );
    let mut exact_ids: Vec<VectorId> = exact_results.iter().map(|r| r.id).collect();
    let mut pq_rr_ids: Vec<VectorId> = pq_rr_results.iter().map(|r| r.id).collect();
    // Sort both sets before comparing — ordering may differ when distances tie.
    exact_ids.sort_by_key(|id| id.0);
    pq_rr_ids.sort_by_key(|id| id.0);
    assert_eq!(
        exact_ids, pq_rr_ids,
        "PQ+rerank with high oversample must recover the exact top-k id set"
    );
}

// ── Top-k selection behaviour ─────────────────────────────────────────────────

/// Results must be sorted ascending by score (best match first).
#[test]
fn pipeline_results_are_sorted_ascending_by_score() {
    let records = make_records(20, 4);
    let query = records[7].data.clone();
    let (store, manifest, _tmp) = build_index(records, 2, 4, "ds-sort");

    let pipeline = QueryPipeline::builder(store, manifest).build();
    let results = pipeline.search(&query, 10, 2).unwrap();

    for window in results.windows(2) {
        assert!(
            window[0].score <= window[1].score,
            "results not sorted: score[i]={} > score[i+1]={}",
            window[0].score,
            window[1].score
        );
    }
}

/// Each vector id must appear at most once in the result set.
#[test]
fn pipeline_results_have_no_duplicate_ids() {
    let records = make_records(20, 4);
    let query = records[2].data.clone();
    let (store, manifest, _tmp) = build_index(records, 2, 4, "ds-dedup");

    let pipeline = QueryPipeline::builder(store, manifest).build();
    let results = pipeline.search(&query, 20, 2).unwrap();

    let mut seen = std::collections::HashSet::new();
    for r in &results {
        assert!(
            seen.insert(r.id),
            "duplicate id {} in pipeline results",
            r.id.0
        );
    }
}

// ── ExactCandidateStage unit tests ────────────────────────────────────────────

#[test]
fn exact_candidate_stage_returns_k_nearest() {
    let stage = ExactCandidateStage;
    let records = make_records(10, 2);
    let shard = ShardIndex {
        shard_id: ShardId(0),
        dims: 2,
        centroids: vec![records[0].data.clone()],
        records: records.clone(),
    };
    let query = records[0].data.clone();
    let results = stage
        .search_shard(&query, &shard, 3, DistanceMetric::Euclidean)
        .unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0].id,
        VectorId(0),
        "exact stage must rank id=0 first"
    );
    // Results must be sorted.
    for window in results.windows(2) {
        assert!(window[0].score <= window[1].score);
    }
}

#[test]
fn exact_candidate_stage_handles_k_larger_than_shard() {
    let stage = ExactCandidateStage;
    let records = make_records(3, 2);
    let shard = ShardIndex {
        shard_id: ShardId(0),
        dims: 2,
        centroids: vec![records[0].data.clone()],
        records: records.clone(),
    };
    let query = records[0].data.clone();
    let results = stage
        .search_shard(&query, &shard, 100, DistanceMetric::Euclidean)
        .unwrap();
    assert_eq!(
        results.len(),
        3,
        "should return all 3 records when k > shard size"
    );
}
