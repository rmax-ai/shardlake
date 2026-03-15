//! Integration tests for the composable ANN query pipeline.

use std::sync::Arc;

use shardlake_core::{
    config::{FanOutPolicy, SystemConfig},
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, SearchResult, ShardId,
        VectorId, VectorRecord,
    },
};
use shardlake_index::{
    pipeline::{
        CandidateSearchStage, ExactRerankStage, MmapShardLoader, PqCandidateStage, QueryPipeline,
        RerankStage, MMAP_MIN_SIZE_BYTES,
    },
    pq::{PqCodebook, PqParams},
    shard::ShardIndex,
    BuildParams, IndexBuilder, IndexSearcher, LoadShardStage,
};
use shardlake_storage::{paths, LocalObjectStore, ObjectStore, StorageError};

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

fn default_config(root: &std::path::Path, num_shards: usize) -> SystemConfig {
    SystemConfig {
        storage_root: root.to_path_buf(),
        num_shards: num_shards as u32,
        kmeans_iters: 10,
        nprobe: 2,
        kmeans_seed: SystemConfig::default_kmeans_seed(),
        ..SystemConfig::default()
    }
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
        let searcher_ids: Vec<VectorId> = searcher_results.iter().map(|result| result.id).collect();
        let pipeline_ids: Vec<VectorId> = pipeline_results.iter().map(|result| result.id).collect();
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

/// `MmapShardLoader` produces the same nearest-neighbour results as the
/// default `CachedShardLoader` when the store is `LocalObjectStore`.
#[test]
fn test_mmap_loader_returns_same_results_as_cached_loader() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = default_config(tmp.path(), 2);
    let records = make_records(20, 4);

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: records.clone(),
            dataset_version: DatasetVersion("ds-mmap-cmp".into()),
            embedding_version: EmbeddingVersion("emb-mmap-cmp".into()),
            index_version: IndexVersion("idx-mmap-cmp".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-mmap-cmp"),
            metadata_key: paths::dataset_metadata_key("ds-mmap-cmp"),
            pq_params: None,
        })
        .unwrap();

    let query = records[0].data.clone();

    // Default (CachedShardLoader) path.
    let cached_pipeline =
        QueryPipeline::builder(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest.clone())
            .build();
    let cached_results = cached_pipeline.run(&query, 5, 2).unwrap();

    // MmapShardLoader path with threshold=0 so every file is memory-mapped.
    let mmap_pipeline =
        QueryPipeline::builder(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest.clone())
            .with_loader(Box::new(MmapShardLoader::with_threshold(
                Arc::clone(&store),
                manifest,
                0,
            )))
            .build();
    let mmap_results = mmap_pipeline.run(&query, 5, 2).unwrap();

    assert!(!mmap_results.is_empty(), "mmap loader must return results");
    assert_eq!(
        cached_results.len(),
        mmap_results.len(),
        "mmap and cached loaders must return the same number of results"
    );
    for (c, m) in cached_results.iter().zip(mmap_results.iter()) {
        assert_eq!(c.id, m.id, "mmap result id must match cached result id");
    }
}

/// When the shard file is smaller than the configured threshold the
/// `MmapShardLoader` falls back to the regular `ObjectStore::get` path
/// and still returns correct results.
#[test]
fn test_mmap_loader_fallback_for_small_files() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = default_config(tmp.path(), 2);
    let records = make_records(20, 4);

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: records.clone(),
            dataset_version: DatasetVersion("ds-mmap-small".into()),
            embedding_version: EmbeddingVersion("emb-mmap-small".into()),
            index_version: IndexVersion("idx-mmap-small".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-mmap-small"),
            metadata_key: paths::dataset_metadata_key("ds-mmap-small"),
            pq_params: None,
        })
        .unwrap();

    // Use a very large threshold so that all shard files fall back to the
    // regular read path (no file will be >= u64::MAX bytes).
    let mmap_pipeline =
        QueryPipeline::builder(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest.clone())
            .with_loader(Box::new(MmapShardLoader::with_threshold(
                Arc::clone(&store),
                manifest,
                u64::MAX,
            )))
            .build();

    let query = records[0].data.clone();
    let results = mmap_pipeline.run(&query, 5, 2).unwrap();

    assert!(
        !results.is_empty(),
        "fallback path must still return results"
    );
    assert_eq!(
        results[0].id,
        VectorId(0),
        "fallback path must find the correct nearest neighbour"
    );
}

/// `MmapShardLoader` caches shards in memory: repeated loads of the same
/// shard ID return the same `Arc` allocation without re-reading from storage.
#[test]
fn test_mmap_loader_caches_shards() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = default_config(tmp.path(), 2);
    let records = make_records(20, 4);

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: records.clone(),
            dataset_version: DatasetVersion("ds-mmap-cache".into()),
            embedding_version: EmbeddingVersion("emb-mmap-cache".into()),
            index_version: IndexVersion("idx-mmap-cache".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-mmap-cache"),
            metadata_key: paths::dataset_metadata_key("ds-mmap-cache"),
            pq_params: None,
        })
        .unwrap();

    // Collect a shard ID from the manifest.
    let shard_id = manifest.shards[0].shard_id;

    let loader = MmapShardLoader::with_threshold(Arc::clone(&store), manifest, 0);

    // Two loads of the same shard ID must return the same Arc allocation,
    // proving the result was served from cache on the second call.
    let first = loader.load(shard_id).unwrap();
    let second = loader.load(shard_id).unwrap();
    assert!(
        Arc::ptr_eq(&first, &second),
        "second load must return the cached Arc, not a new allocation"
    );
}

/// `MMAP_MIN_SIZE_BYTES` has the documented default value of 1 MiB.
#[test]
fn test_mmap_min_size_bytes_constant() {
    assert_eq!(
        MMAP_MIN_SIZE_BYTES,
        1024 * 1024,
        "MMAP_MIN_SIZE_BYTES must equal 1 MiB"
    );
}
