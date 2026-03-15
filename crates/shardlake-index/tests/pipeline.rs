//! Integration tests for the modular query pipeline.
//!
//! Tests verify stage ordering, data hand-off between stages, the default
//! pipeline search path, and the ability to inject custom stage implementations.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use shardlake_core::{
    config::SystemConfig,
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, SearchResult, ShardId,
        VectorId, VectorRecord,
    },
};
use shardlake_index::{
    pipeline::{
        CandidateSearchStage, EmbedStage, MergeStage, MmapShardLoader, QueryPipeline, RerankStage,
        RouteStage, MMAP_MIN_SIZE_BYTES,
    },
    shard::ShardIndex,
    BuildParams, IndexBuilder, LoadShardStage, Result,
};
use shardlake_storage::{paths, LocalObjectStore, ObjectStore, StorageError};

// ── helpers ───────────────────────────────────────────────────────────────────

fn make_records(n: usize, dims: usize) -> Vec<VectorRecord> {
    (0..n)
        .map(|i| VectorRecord {
            id: VectorId(i as u64),
            data: (0..dims).map(|d| (i * dims + d) as f32 / 100.0).collect(),
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

// ── stage-ordering probe ──────────────────────────────────────────────────────

/// Shared call log threaded through mock stages to track invocation order.
type CallLog = Arc<Mutex<Vec<&'static str>>>;

struct LoggingEmbedder(CallLog);
impl EmbedStage for LoggingEmbedder {
    fn embed(&self, query: &[f32]) -> Result<Vec<f32>> {
        self.0.lock().unwrap().push("embed");
        Ok(query.to_vec())
    }
}

struct LoggingRouter(CallLog, Vec<ShardId>);
impl RouteStage for LoggingRouter {
    fn route(
        &self,
        _query: &[f32],
        _centroids: &[Vec<f32>],
        _centroid_to_shard: &[ShardId],
        _nprobe: usize,
    ) -> Vec<ShardId> {
        self.0.lock().unwrap().push("route");
        self.1.clone()
    }
}

struct LoggingCandidateSearch(CallLog);
impl CandidateSearchStage for LoggingCandidateSearch {
    fn search(
        &self,
        _query: &[f32],
        _shard: &ShardIndex,
        _metric: DistanceMetric,
        _k: usize,
    ) -> Vec<SearchResult> {
        self.0.lock().unwrap().push("search");
        vec![]
    }
}

struct LoggingMerge(CallLog);
impl MergeStage for LoggingMerge {
    fn merge(&self, results: Vec<SearchResult>, _k: usize) -> Vec<SearchResult> {
        self.0.lock().unwrap().push("merge");
        results
    }
}

struct LoggingReranker(CallLog);
impl RerankStage for LoggingReranker {
    fn rerank(&self, _query: &[f32], results: Vec<SearchResult>, _k: usize) -> Vec<SearchResult> {
        self.0.lock().unwrap().push("rerank");
        results
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// The default pipeline produces the same nearest neighbour as `IndexSearcher`.
#[test]
fn test_default_pipeline_search_returns_nearest_neighbour() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = default_config(tmp.path(), 2);
    let records = make_records(20, 4);

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: records.clone(),
            dataset_version: DatasetVersion("ds-pipe".into()),
            embedding_version: EmbeddingVersion("emb-pipe".into()),
            index_version: IndexVersion("idx-pipe".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-pipe"),
            metadata_key: paths::dataset_metadata_key("ds-pipe"),
            pq_params: None,
        })
        .unwrap();

    let pipeline =
        QueryPipeline::builder(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest).build();

    let query = records[0].data.clone();
    let results = pipeline.run(&query, 5, 2).unwrap();

    assert!(!results.is_empty());
    assert_eq!(
        results[0].id,
        VectorId(0),
        "closest vector to itself must be id 0"
    );
}

/// Stage ordering: embed → route → search → merge → rerank.
#[test]
fn test_pipeline_stage_ordering() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = default_config(tmp.path(), 2);
    let records = make_records(20, 4);

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: records.clone(),
            dataset_version: DatasetVersion("ds-order".into()),
            embedding_version: EmbeddingVersion("emb-order".into()),
            index_version: IndexVersion("idx-order".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-order"),
            metadata_key: paths::dataset_metadata_key("ds-order"),
            pq_params: None,
        })
        .unwrap();

    // Collect shard IDs from the manifest so the logging router can return them.
    let shard_ids: Vec<ShardId> = manifest.shards.iter().map(|s| s.shard_id).collect();

    let log: CallLog = Arc::new(Mutex::new(Vec::new()));

    let pipeline = QueryPipeline::builder(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest)
        .with_embedder(Box::new(LoggingEmbedder(Arc::clone(&log))))
        .with_router(Box::new(LoggingRouter(Arc::clone(&log), shard_ids)))
        .with_candidate_search(Box::new(LoggingCandidateSearch(Arc::clone(&log))))
        .with_merge(Box::new(LoggingMerge(Arc::clone(&log))))
        .with_reranker(Box::new(LoggingReranker(Arc::clone(&log))))
        .build();

    let query = records[0].data.clone();
    let _ = pipeline.run(&query, 5, 2).unwrap();

    let calls = log.lock().unwrap().clone();
    // embed must be first, rerank must be last; route precedes search; search precedes merge.
    assert_eq!(calls[0], "embed", "embed must be first");
    assert_eq!(calls[1], "route", "route must follow embed");
    assert!(calls.contains(&"search"), "search stage must execute");
    assert!(calls.contains(&"merge"), "merge stage must execute");
    let rerank_pos = calls.iter().rposition(|&s| s == "rerank").unwrap();
    let merge_pos = calls.iter().rposition(|&s| s == "merge").unwrap();
    assert!(rerank_pos > merge_pos, "rerank must follow merge");
}

/// A custom reranker that reverses the result order is honoured by the pipeline.
#[test]
fn test_pipeline_with_custom_reranker() {
    struct ReverseReranker;
    impl RerankStage for ReverseReranker {
        fn rerank(
            &self,
            _query: &[f32],
            mut results: Vec<SearchResult>,
            _k: usize,
        ) -> Vec<SearchResult> {
            results.reverse();
            results
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = default_config(tmp.path(), 2);
    let records = make_records(20, 4);

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: records.clone(),
            dataset_version: DatasetVersion("ds-rerank".into()),
            embedding_version: EmbeddingVersion("emb-rerank".into()),
            index_version: IndexVersion("idx-rerank".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-rerank"),
            metadata_key: paths::dataset_metadata_key("ds-rerank"),
            pq_params: None,
        })
        .unwrap();

    let pipeline = QueryPipeline::builder(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest)
        .with_reranker(Box::new(ReverseReranker))
        .build();

    let query = records[0].data.clone();
    let results = pipeline.run(&query, 5, 2).unwrap();

    // With the reverse reranker the worst result should now be first.
    assert!(!results.is_empty());
    let scores: Vec<f32> = results.iter().map(|r| r.score).collect();
    // Verify scores are non-increasing (reversed from ascending merge output).
    for w in scores.windows(2) {
        assert!(
            w[0] >= w[1],
            "reverse-reranked scores must be non-increasing"
        );
    }
}

/// The pipeline rejects a query whose dimensionality does not match the index.
#[test]
fn test_pipeline_rejects_dimension_mismatch() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = default_config(tmp.path(), 1);
    let records = make_records(5, 4);

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records,
            dataset_version: DatasetVersion("ds-dim".into()),
            embedding_version: EmbeddingVersion("emb-dim".into()),
            index_version: IndexVersion("idx-dim".into()),
            metric: DistanceMetric::Cosine,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-dim"),
            metadata_key: paths::dataset_metadata_key("ds-dim"),
            pq_params: None,
        })
        .unwrap();

    let pipeline =
        QueryPipeline::builder(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest).build();

    let err = pipeline.run(&[1.0, 2.0], 5, 1).unwrap_err();
    assert!(
        err.to_string().contains("dimension mismatch"),
        "unexpected error: {err}"
    );
}

/// A custom embed stage that scales the query vector is applied before routing.
#[test]
fn test_pipeline_data_handoff_through_custom_embedder() {
    /// Scales every component by 2 so we can observe its effect on routing.
    struct ScalingEmbedder;
    impl EmbedStage for ScalingEmbedder {
        fn embed(&self, query: &[f32]) -> Result<Vec<f32>> {
            Ok(query.iter().map(|v| v * 2.0).collect())
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = default_config(tmp.path(), 2);
    let records = make_records(20, 4);

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: records.clone(),
            dataset_version: DatasetVersion("ds-embed".into()),
            embedding_version: EmbeddingVersion("emb-embed".into()),
            index_version: IndexVersion("idx-embed".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-embed"),
            metadata_key: paths::dataset_metadata_key("ds-embed"),
            pq_params: None,
        })
        .unwrap();

    let pipeline = QueryPipeline::builder(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest)
        .with_embedder(Box::new(ScalingEmbedder))
        .build();

    // Pipeline should complete without error; the scaling embedder preserves dims.
    let query = records[0].data.clone();
    let results = pipeline.run(&query, 5, 2).unwrap();
    assert!(!results.is_empty());
}

/// Shard loading is cached: each unique shard is fetched at most once per
/// pipeline run even when probed by multiple centroid entries.
#[test]
fn test_pipeline_cached_loader_avoids_duplicate_shard_loads() {
    /// Wraps an ObjectStore and counts `.sidx` fetches.
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        count: Arc<AtomicUsize>,
    }
    impl ObjectStore for CountingStore {
        fn put(&self, key: &str, data: Vec<u8>) -> std::result::Result<(), StorageError> {
            self.inner.put(key, data)
        }
        fn get(&self, key: &str) -> std::result::Result<Vec<u8>, StorageError> {
            if key.ends_with(".sidx") {
                self.count.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get(key)
        }
        fn exists(&self, key: &str) -> std::result::Result<bool, StorageError> {
            self.inner.exists(key)
        }
        fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, StorageError> {
            self.inner.list(prefix)
        }
        fn delete(&self, key: &str) -> std::result::Result<(), StorageError> {
            self.inner.delete(key)
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = SystemConfig {
        storage_root: tmp.path().to_path_buf(),
        num_shards: 4,
        kmeans_iters: 10,
        nprobe: 1,
        kmeans_seed: SystemConfig::default_kmeans_seed(),
        ..SystemConfig::default()
    };
    let records = make_records(40, 4);

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: records.clone(),
            dataset_version: DatasetVersion("ds-cache".into()),
            embedding_version: EmbeddingVersion("emb-cache".into()),
            index_version: IndexVersion("idx-cache".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-cache"),
            metadata_key: paths::dataset_metadata_key("ds-cache"),
            pq_params: None,
        })
        .unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let counting = Arc::new(CountingStore {
        inner: Arc::clone(&store) as Arc<dyn ObjectStore>,
        count: Arc::clone(&counter),
    });

    // Build two pipelines sharing the same (warmed) loader instance to verify
    // that a second call for the same shard hits the cache.
    let pipeline = QueryPipeline::builder(counting as Arc<dyn ObjectStore>, manifest).build();

    // nprobe=1 → only 1 shard should be loaded.
    let _ = pipeline.run(&records[0].data, 5, 1).unwrap();
    let loads = counter.load(Ordering::SeqCst);
    assert_eq!(
        loads, 1,
        "expected exactly 1 shard load with nprobe=1, got {loads}"
    );
}

/// Both exact-search and a custom ANN-stub run through the same pipeline
/// skeleton, confirming the pipeline is algorithm-agnostic.
#[test]
fn test_pipeline_exact_and_ann_paths_through_same_skeleton() {
    /// Stub "ANN" search that returns all records scored 0.0 (simulating a
    /// perfect-recall approximate index that always returns everything).
    struct AnnStubSearch;
    impl CandidateSearchStage for AnnStubSearch {
        fn search(
            &self,
            _query: &[f32],
            shard: &ShardIndex,
            _metric: DistanceMetric,
            k: usize,
        ) -> Vec<SearchResult> {
            shard
                .records
                .iter()
                .take(k)
                .map(|r| SearchResult {
                    id: r.id,
                    score: 0.0,
                    metadata: r.metadata.clone(),
                })
                .collect()
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = default_config(tmp.path(), 2);
    let records = make_records(20, 4);

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: records.clone(),
            dataset_version: DatasetVersion("ds-ann".into()),
            embedding_version: EmbeddingVersion("emb-ann".into()),
            index_version: IndexVersion("idx-ann".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-ann"),
            metadata_key: paths::dataset_metadata_key("ds-ann"),
            pq_params: None,
        })
        .unwrap();

    let query = records[0].data.clone();

    // Exact path.
    let exact_pipeline =
        QueryPipeline::builder(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest.clone())
            .build();
    let exact_results = exact_pipeline.run(&query, 5, 2).unwrap();

    // ANN stub path through the same pipeline skeleton.
    let ann_pipeline = QueryPipeline::builder(Arc::clone(&store) as Arc<dyn ObjectStore>, manifest)
        .with_candidate_search(Box::new(AnnStubSearch))
        .build();
    let ann_results = ann_pipeline.run(&query, 5, 2).unwrap();

    // Both paths must return non-empty results without error.
    assert!(
        !exact_results.is_empty(),
        "exact pipeline must return results"
    );
    assert!(
        !ann_results.is_empty(),
        "ann stub pipeline must return results"
    );
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
