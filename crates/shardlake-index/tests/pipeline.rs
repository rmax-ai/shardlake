//! Integration tests for the composable ANN query pipeline.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use shardlake_core::{
    config::{FanOutPolicy, SystemConfig},
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, SearchResult, ShardId,
        VectorId, VectorRecord,
    },
};
use shardlake_index::{
    pipeline::{
        CandidateSearchStage, ExactRerankStage, LoadShardStage, PqCandidateStage, QueryPipeline,
        RerankStage,
    },
    pq::{PqCodebook, PqParams},
    shard::ShardIndex,
    BuildParams, IndexBuilder, IndexError, IndexSearcher,
};
use shardlake_storage::{LocalObjectStore, ObjectStore, StorageError};

fn make_records(n: usize, dims: usize) -> Vec<VectorRecord> {
    (0..n)
        .map(|index| VectorRecord {
            id: VectorId(index as u64),
            data: (0..dims)
                .map(|dimension| (index * dims + dimension) as f32 / 100.0)
                .collect(),
            metadata: None,
        })
        .collect()
}

fn build_index(
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

struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    shard_loads: Arc<std::sync::atomic::AtomicUsize>,
}

impl ObjectStore for CountingStore {
    fn put(&self, key: &str, data: Vec<u8>) -> Result<(), StorageError> {
        self.inner.put(key, data)
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        if key.ends_with(".sidx") {
            self.shard_loads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

#[test]
fn pipeline_nprobe_limits_shards_probed() {
    let records = make_records(40, 4);
    let (store, manifest, _tmp) =
        build_index(records.clone(), 4, 4, "ds-route", DistanceMetric::Euclidean);

    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counting: Arc<dyn ObjectStore> = Arc::new(CountingStore {
        inner: Arc::clone(&store),
        shard_loads: Arc::clone(&counter),
    });

    let pipeline = QueryPipeline::builder(counting, manifest).build();
    let results = pipeline.run(&records[0].data, 5, 1).unwrap();
    assert!(!results.is_empty());
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 1);
}

#[test]
fn pipeline_matches_searcher_results() {
    let records = make_records(20, 4);
    let (store, manifest, _tmp) =
        build_index(records.clone(), 2, 4, "ds-match", DistanceMetric::Euclidean);

    let searcher = IndexSearcher::new(Arc::clone(&store), manifest.clone());
    let pipeline = QueryPipeline::builder(Arc::clone(&store), manifest).build();

    for index in [0, 5, 10, 15] {
        let query = records[index].data.clone();
        let policy = FanOutPolicy {
            candidate_centroids: 2,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        };
        let searcher_results = searcher.search(&query, 5, &policy).unwrap();
        let pipeline_results = pipeline.run(&query, 5, 2).unwrap();
        let mut searcher_ids: Vec<VectorId> =
            searcher_results.iter().map(|result| result.id).collect();
        let mut pipeline_ids: Vec<VectorId> =
            pipeline_results.iter().map(|result| result.id).collect();
        // Sort both before comparing: the two implementations may break ties
        // between equidistant vectors differently due to floating-point
        // precision, so we only verify that the same candidate set is found.
        searcher_ids.sort();
        pipeline_ids.sort();
        assert_eq!(
            searcher_ids, pipeline_ids,
            "query {index} should match searcher results"
        );
    }
}

#[test]
fn pq_candidate_stage_ranks_identical_vector_first() {
    let records = make_records(30, 4);
    let vectors: Vec<Vec<f32>> = records.iter().map(|record| record.data.clone()).collect();
    let codebook = Arc::new(
        PqCodebook::train(
            &vectors,
            PqParams {
                num_subspaces: 2,
                codebook_size: 16,
            },
            0xdead_beef,
            20,
        )
        .unwrap(),
    );
    let stage = PqCandidateStage::new(Arc::clone(&codebook));
    let shard = ShardIndex {
        shard_id: ShardId(0),
        dims: 4,
        centroids: vec![records[0].data.clone()],
        records: records.clone(),
    };

    let results = stage
        .search(&records[0].data, &shard, DistanceMetric::Euclidean, 5)
        .unwrap();

    assert_eq!(results[0].id, VectorId(0));
}

#[test]
fn pq_candidate_stage_rejects_non_euclidean_queries() {
    let records = make_records(20, 4);
    let vectors: Vec<Vec<f32>> = records.iter().map(|record| record.data.clone()).collect();
    let codebook = Arc::new(
        PqCodebook::train(
            &vectors,
            PqParams {
                num_subspaces: 2,
                codebook_size: 16,
            },
            7,
            20,
        )
        .unwrap(),
    );
    let stage = PqCandidateStage::new(codebook);
    let shard = ShardIndex {
        shard_id: ShardId(0),
        dims: 4,
        centroids: vec![records[0].data.clone()],
        records,
    };

    let err = stage
        .search(&[0.0, 0.0, 0.0, 0.0], &shard, DistanceMetric::Cosine, 5)
        .unwrap_err();
    assert!(err.to_string().contains("euclidean"));
}

#[test]
fn exact_rerank_stage_rescores_candidates() {
    let records = make_records(10, 4);
    let reranker = ExactRerankStage;
    let candidates: Vec<SearchResult> = records
        .iter()
        .map(|record| SearchResult {
            id: record.id,
            score: 999.0,
            metadata: None,
        })
        .collect();

    let results = reranker.rerank(
        &records[0].data,
        candidates,
        &records,
        DistanceMetric::Euclidean,
        3,
    );

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].id, VectorId(0));
    assert!(results[0].score < 999.0);
}

#[test]
fn pipeline_with_pq_stage_and_reranking_finds_correct_top1() {
    let records = make_records(20, 4);
    let vectors: Vec<Vec<f32>> = records.iter().map(|record| record.data.clone()).collect();
    let (store, manifest, _tmp) =
        build_index(records.clone(), 2, 4, "ds-pq", DistanceMetric::Euclidean);
    let codebook = Arc::new(
        PqCodebook::train(
            &vectors,
            PqParams {
                num_subspaces: 2,
                codebook_size: 16,
            },
            42,
            20,
        )
        .unwrap(),
    );

    let pipeline = QueryPipeline::builder(Arc::clone(&store), manifest)
        .candidate_stage(Arc::new(PqCandidateStage::new(codebook)))
        .rerank_stage(Arc::new(ExactRerankStage))
        .rerank_oversample(5)
        .build();

    let results = pipeline.search(&records[0].data, 3, 2).unwrap();
    assert_eq!(results[0].id, VectorId(0));
}

#[test]
fn pipeline_rerank_receives_only_merged_candidate_records() {
    struct CountingReranker {
        seen_records: Arc<std::sync::Mutex<usize>>,
    }

    impl RerankStage for CountingReranker {
        fn rerank(
            &self,
            _query: &[f32],
            results: Vec<SearchResult>,
            probed_records: &[VectorRecord],
            _metric: DistanceMetric,
            _k: usize,
        ) -> Vec<SearchResult> {
            *self.seen_records.lock().unwrap() = probed_records.len();
            results
        }
    }

    let records = make_records(20, 4);
    let (store, manifest, _tmp) = build_index(
        records.clone(),
        2,
        4,
        "ds-rerank-input",
        DistanceMetric::Euclidean,
    );
    let seen_records = Arc::new(std::sync::Mutex::new(0));
    let pipeline = QueryPipeline::builder(store, manifest)
        .rerank_stage(Arc::new(CountingReranker {
            seen_records: Arc::clone(&seen_records),
        }))
        .build();

    let results = pipeline.run(&records[0].data, 3, 2).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(*seen_records.lock().unwrap(), 3);
}

#[test]
fn pq_rerank_pipeline_matches_exact_topk_set() {
    let records = make_records(50, 8);
    let query = records[11].data.clone();
    let vectors: Vec<Vec<f32>> = records.iter().map(|record| record.data.clone()).collect();
    let (store, manifest, _tmp) =
        build_index(records, 4, 8, "ds-pq-compare", DistanceMetric::Euclidean);
    let codebook = Arc::new(
        PqCodebook::train(
            &vectors,
            PqParams {
                num_subspaces: 4,
                codebook_size: 16,
            },
            99,
            25,
        )
        .unwrap(),
    );

    let exact_pipeline = QueryPipeline::builder(Arc::clone(&store), manifest.clone()).build();
    let approx_pipeline = QueryPipeline::builder(Arc::clone(&store), manifest)
        .candidate_stage(Arc::new(PqCandidateStage::new(codebook)))
        .rerank_stage(Arc::new(ExactRerankStage))
        .rerank_oversample(8)
        .build();

    let exact_ids: Vec<VectorId> = exact_pipeline
        .run(&query, 5, 2)
        .unwrap()
        .into_iter()
        .map(|result| result.id)
        .collect();
    let approx_ids: Vec<VectorId> = approx_pipeline
        .run(&query, 5, 2)
        .unwrap()
        .into_iter()
        .map(|result| result.id)
        .collect();
    assert_eq!(approx_ids, exact_ids);
}

#[test]
fn pipeline_shard_searches_run_concurrently() {
    struct ConcurrentProbeSearcher {
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    impl CandidateSearchStage for ConcurrentProbeSearcher {
        fn search(
            &self,
            _query: &[f32],
            _shard: &ShardIndex,
            _metric: DistanceMetric,
            _k: usize,
        ) -> shardlake_index::Result<Vec<SearchResult>> {
            let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            let mut cur = self.peak.load(Ordering::SeqCst);
            while now > cur {
                match self
                    .peak
                    .compare_exchange(cur, now, Ordering::SeqCst, Ordering::SeqCst)
                {
                    Ok(_) => break,
                    Err(actual) => cur = actual,
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(vec![])
        }
    }

    let records = make_records(40, 4);
    let (store, manifest, _tmp) =
        build_index(records.clone(), 4, 4, "ds-par", DistanceMetric::Euclidean);
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let pipeline = QueryPipeline::builder(Arc::clone(&store), manifest)
        .with_candidate_search(Box::new(ConcurrentProbeSearcher {
            active: Arc::clone(&active),
            peak: Arc::clone(&peak),
        }))
        .build();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();

    pool.install(|| pipeline.run(&records[0].data, 5, 4))
        .unwrap();

    let observed_peak = peak.load(Ordering::SeqCst);
    assert!(
        observed_peak >= 2,
        "expected at least 2 concurrent shard searches, observed peak = {observed_peak}"
    );
}

#[test]
fn pipeline_shard_load_failure_propagates() {
    struct FailingLoader;

    impl LoadShardStage for FailingLoader {
        fn load(&self, _shard_id: ShardId) -> shardlake_index::Result<Arc<ShardIndex>> {
            Err(IndexError::Other("injected shard load failure".into()))
        }
    }

    let records = make_records(20, 4);
    let (store, manifest, _tmp) =
        build_index(records.clone(), 2, 4, "ds-fail", DistanceMetric::Euclidean);
    let pipeline = QueryPipeline::builder(Arc::clone(&store), manifest)
        .with_loader(Box::new(FailingLoader))
        .build();

    let err = pipeline
        .run(&records[0].data, 5, 2)
        .expect_err("pipeline must propagate shard load error");
    assert!(
        err.to_string().contains("injected shard load failure"),
        "unexpected error message: {err}"
    );
}

#[test]
fn cosine_pipeline_returns_query_vector_as_top_result() {
    // An index built with cosine distance should rank the query's own record
    // (or any record sharing the same direction) at position 0.
    let records = make_records(20, 4);
    let query = records[5].data.clone();
    let (store, manifest, _tmp) =
        build_index(records.clone(), 2, 4, "ds-cosine", DistanceMetric::Cosine);

    let pipeline = QueryPipeline::builder(store, manifest).build();
    let results = pipeline.run(&query, 5, 2).unwrap();
    assert!(!results.is_empty());
    assert_eq!(
        results[0].id,
        VectorId(5),
        "the exact query vector should be closest under cosine distance"
    );
}

#[test]
fn inner_product_pipeline_returns_query_vector_as_top_result() {
    // An index built with inner-product distance should rank the record with
    // the largest dot product with the query first. Because `make_records`
    // generates monotonically increasing values, the last record in the corpus
    // has the largest component magnitudes and therefore the largest dot product
    // with itself — making it its own top-1 under inner-product scoring.
    let n = 20;
    let records = make_records(n, 4);
    // Use the last record as the query: it has the highest values in every
    // dimension, so records[n-1] · records[n-1] > records[i] · records[n-1]
    // for all i < n-1.
    let last_id = VectorId((n - 1) as u64);
    let query = records[n - 1].data.clone();
    let (store, manifest, _tmp) = build_index(
        records.clone(),
        2,
        4,
        "ds-inner-product",
        DistanceMetric::InnerProduct,
    );

    let pipeline = QueryPipeline::builder(store, manifest).build();
    let results = pipeline.run(&query, 5, 2).unwrap();
    assert!(!results.is_empty());
    assert_eq!(
        results[0].id, last_id,
        "the last record has the largest self-dot-product under inner-product metric"
    );
}

#[test]
fn cosine_and_euclidean_pipelines_produce_different_top_results_for_scale_differing_corpus() {
    // Construct a corpus where cosine and euclidean give different top-1 results:
    //   id=0: [1.0, 0.1, 0.0, 0.0]  — nearest in L2, but slightly off-angle
    //   id=1: [100.0, 0.0, 0.0, 0.0] — exact cosine match, but far in L2
    //   id=2: [1.0, 1.0, 0.0, 0.0]  — clearly worse under both metrics
    //
    // This makes the metric choice observable end-to-end: euclidean should prefer
    // the nearby off-angle point, while cosine should prefer the distant point with
    // exactly matching direction.
    let corpus: Vec<VectorRecord> = vec![
        VectorRecord {
            id: VectorId(0),
            data: vec![1.0, 0.1, 0.0, 0.0],
            metadata: None,
        },
        VectorRecord {
            id: VectorId(1),
            data: vec![100.0, 0.0, 0.0, 0.0],
            metadata: None,
        },
        VectorRecord {
            id: VectorId(2),
            data: vec![1.0, 1.0, 0.0, 0.0],
            metadata: None,
        },
    ];

    let query = vec![1.0f32, 0.0, 0.0, 0.0];

    let (store_euc, manifest_euc, _tmp_euc) = build_index(
        corpus.clone(),
        1,
        4,
        "ds-scale-euc",
        DistanceMetric::Euclidean,
    );
    let (store_cos, manifest_cos, _tmp_cos) =
        build_index(corpus, 1, 4, "ds-scale-cos", DistanceMetric::Cosine);

    let euclidean_pipeline = QueryPipeline::builder(store_euc, manifest_euc).build();
    let cosine_pipeline = QueryPipeline::builder(store_cos, manifest_cos).build();

    let euc_top1 = euclidean_pipeline.run(&query, 1, 1).unwrap();
    let cos_top1 = cosine_pipeline.run(&query, 1, 1).unwrap();

    assert_eq!(
        euc_top1[0].id,
        VectorId(0),
        "euclidean should prefer the nearby off-angle record"
    );
    assert_eq!(
        cos_top1[0].id,
        VectorId(1),
        "cosine should prefer the exact directional match even when it is far away"
    );
    assert!(
        cos_top1[0].score < 1e-6,
        "cosine score for a parallel vector must be ~0"
    );
    assert_ne!(
        euc_top1[0].id, cos_top1[0].id,
        "the two metrics should produce different top-1 results for this corpus"
    );
}
