//! Composable ANN candidate retrieval pipeline.
//!
//! # Overview
//!
//! The [`QueryPipeline`] orchestrates ANN search through four pluggable
//! stages:
//!
//! 1. **Route** — route the query to the `nprobe` nearest IVF cells (shards)
//!    using centroids embedded in the manifest.
//! 2. **Load** — lazily load each probed shard from the object store (with an
//!    in-process cache shared across calls).
//! 3. **Candidate search** — score vectors within each loaded shard and
//!    return per-shard top-k candidates.  The default
//!    [`ExactCandidateStage`] uses brute-force exact distance;
//!    [`PqCandidateStage`] uses PQ approximate scoring for faster throughput.
//! 4. **Rerank** (optional) — re-score merged candidates with exact distances
//!    using the original float vectors.  This lets callers trade throughput
//!    for recall: retrieve a large candidate set cheaply with PQ, then rerank
//!    only the top candidates exactly.
//!
//! # Building a pipeline
//!
//! ```no_run
//! use std::sync::Arc;
//! use shardlake_index::pipeline::QueryPipeline;
//! use shardlake_manifest::Manifest;
//! use shardlake_storage::ObjectStore;
//!
//! # fn example(store: Arc<dyn ObjectStore>, manifest: Manifest) {
//! // Default: exact candidate search, no reranking.
//! let pipeline = QueryPipeline::builder(store, manifest).build();
//! # }
//! ```

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tracing::debug;

use shardlake_core::{
    error::CoreError,
    types::{DistanceMetric, SearchResult, ShardId, VectorRecord},
};
use shardlake_manifest::Manifest;
use shardlake_storage::ObjectStore;

use crate::{
    exact::{distance, exact_search, merge_top_k},
    kmeans::top_n_centroids,
    pq::PqCodebook,
    shard::ShardIndex,
    IndexError, Result,
};

// ── Stage traits ──────────────────────────────────────────────────────────────

/// Stage that searches for candidate nearest-neighbours within a single
/// loaded shard.
///
/// Implementors can use exact distance computation ([`ExactCandidateStage`])
/// or PQ approximate scoring ([`PqCandidateStage`]).
pub trait CandidateSearchStage: Send + Sync {
    /// Return approximate or exact top-`k` candidates from `shard`.
    ///
    /// May return fewer than `k` results when the shard contains fewer
    /// vectors.
    fn search_shard(
        &self,
        query: &[f32],
        shard: &ShardIndex,
        k: usize,
        metric: DistanceMetric,
    ) -> Result<Vec<SearchResult>>;
}

/// Stage that re-ranks approximate candidates with exact distances.
///
/// Receives the merged approximate candidate set and the union of all probed
/// shard records so that the original float vectors are available for exact
/// rescoring.
pub trait RerankStage: Send + Sync {
    /// Re-score `candidates` and return the top-`k` exactly ranked results.
    ///
    /// `probed_records` is the concatenation of all records from every probed
    /// shard, providing the original float vectors needed for rescoring.
    fn rerank(
        &self,
        query: &[f32],
        candidates: Vec<SearchResult>,
        probed_records: &[VectorRecord],
        metric: DistanceMetric,
        k: usize,
    ) -> Vec<SearchResult>;
}

// ── Default stage implementations ────────────────────────────────────────────

/// Candidate stage that uses brute-force exact distance computation.
///
/// Wraps the existing [`exact_search`] function.  Use as the default stage
/// or as the reranking pass after a cheap PQ first stage.
pub struct ExactCandidateStage;

impl CandidateSearchStage for ExactCandidateStage {
    fn search_shard(
        &self,
        query: &[f32],
        shard: &ShardIndex,
        k: usize,
        metric: DistanceMetric,
    ) -> Result<Vec<SearchResult>> {
        Ok(exact_search(query, &shard.records, metric, k))
    }
}

/// Candidate stage that uses Product Quantization (PQ) approximate scoring.
///
/// For each vector in the shard the query vector is scored using Asymmetric
/// Distance Computation (ADC): distance tables are precomputed once per
/// query and each per-vector score is a table-lookup sum over sub-spaces.
/// This is significantly faster than computing exact distances when the
/// vector dimension is high.
///
/// Build with [`PqCandidateStage::new`] and supply a trained [`PqCodebook`].
pub struct PqCandidateStage {
    codebook: Arc<PqCodebook>,
}

impl PqCandidateStage {
    /// Create a PQ candidate stage backed by `codebook`.
    pub fn new(codebook: Arc<PqCodebook>) -> Self {
        Self { codebook }
    }

    /// Return the underlying PQ codebook.
    pub fn codebook(&self) -> &PqCodebook {
        &self.codebook
    }
}

impl CandidateSearchStage for PqCandidateStage {
    fn search_shard(
        &self,
        query: &[f32],
        shard: &ShardIndex,
        k: usize,
        metric: DistanceMetric,
    ) -> Result<Vec<SearchResult>> {
        if metric != DistanceMetric::Euclidean {
            return Err(IndexError::Other(
                "PQ search currently supports only euclidean distance".into(),
            ));
        }

        let tables = self.codebook.compute_distance_table(query)?;
        let mut scored = Vec::with_capacity(shard.records.len());
        for rec in &shard.records {
            let codes = self.codebook.encode(&rec.data)?;
            let score = self.codebook.adc_distance(&codes, &tables);
            scored.push(SearchResult {
                id: rec.id,
                score,
                metadata: rec.metadata.clone(),
            });
        }
        scored.sort_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(k);
        Ok(scored)
    }
}

/// Reranking stage that rescores candidates with exact float distances.
///
/// Candidate scores produced by [`PqCandidateStage`] are approximate; this
/// stage looks up the original float vector for each candidate from the
/// probed shard records and recomputes the exact distance, producing a
/// precisely ordered final result set.
pub struct ExactRerankStage;

impl RerankStage for ExactRerankStage {
    fn rerank(
        &self,
        query: &[f32],
        candidates: Vec<SearchResult>,
        probed_records: &[VectorRecord],
        metric: DistanceMetric,
        k: usize,
    ) -> Vec<SearchResult> {
        let lookup: HashMap<_, &VectorRecord> = probed_records.iter().map(|r| (r.id, r)).collect();

        let rescored: Vec<SearchResult> = candidates
            .into_iter()
            .filter_map(|c| {
                lookup.get(&c.id).map(|rec| SearchResult {
                    id: c.id,
                    score: distance(query, &rec.data, metric),
                    metadata: c.metadata,
                })
            })
            .collect();

        merge_top_k(rescored, k)
    }
}

// ── Pipeline ──────────────────────────────────────────────────────────────────

/// Composable ANN candidate retrieval pipeline.
///
/// Orchestrates query routing through the IVF coarse quantizer, shard
/// loading, per-shard candidate search, and optional exact reranking into a
/// single [`search`][QueryPipeline::search] call.  All stages are
/// hot-swappable via [`QueryPipelineBuilder`].
///
/// # Caching
///
/// Loaded shard indexes are kept in an in-process cache
/// (`Mutex<HashMap>`).  The cache is shared across all `search` calls on the
/// same `QueryPipeline` instance; concurrent calls on different threads are
/// safe.
pub struct QueryPipeline {
    store: Arc<dyn ObjectStore>,
    manifest: Manifest,
    cache: Mutex<HashMap<ShardId, Arc<ShardIndex>>>,
    candidate_stage: Arc<dyn CandidateSearchStage>,
    rerank_stage: Option<Arc<dyn RerankStage>>,
    /// Oversample factor: fetch `k × rerank_oversample` candidates per shard
    /// before passing them to the reranking stage.
    rerank_oversample: usize,
}

impl QueryPipeline {
    /// Create a [`QueryPipelineBuilder`] for the given store and manifest.
    ///
    /// Call `.build()` on the returned builder to obtain a pipeline using
    /// [`ExactCandidateStage`] with no reranking.
    pub fn builder(store: Arc<dyn ObjectStore>, manifest: Manifest) -> QueryPipelineBuilder {
        QueryPipelineBuilder::new(store, manifest)
    }

    /// Return the manifest this pipeline was configured from.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Perform ANN search and return top-`k` candidates.
    ///
    /// # Pipeline steps
    ///
    /// 1. Validates the query dimensionality against the manifest.
    /// 2. Routes the query to the `nprobe` nearest IVF centroids.
    /// 3. Loads each probed shard (returned from cache after the first load).
    /// 4. Runs the configured [`CandidateSearchStage`] on each shard.
    /// 5. Merges results with [`merge_top_k`].
    /// 6. If a [`RerankStage`] is configured, rescores the candidates with
    ///    exact distances and returns the precisely ordered top-`k`.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::Core`] on dimension mismatch and
    /// [`IndexError::Storage`] or [`IndexError::Other`] on I/O failures.
    pub fn search(&self, query: &[f32], k: usize, nprobe: usize) -> Result<Vec<SearchResult>> {
        let expected_dims = self.manifest.dims as usize;
        if query.len() != expected_dims {
            return Err(IndexError::Core(CoreError::DimensionMismatch {
                expected: expected_dims,
                got: query.len(),
            }));
        }

        let metric: DistanceMetric = self.manifest.distance_metric;

        // ── Step 1: collect IVF centroids for routing ─────────────────────
        let mut all_centroids: Vec<Vec<f32>> = Vec::new();
        let mut centroid_to_shard: Vec<ShardId> = Vec::new();

        for shard_def in &self.manifest.shards {
            if !shard_def.centroid.is_empty() {
                if shard_def.centroid.len() != expected_dims {
                    return Err(IndexError::Core(CoreError::DimensionMismatch {
                        expected: expected_dims,
                        got: shard_def.centroid.len(),
                    }));
                }
                all_centroids.push(shard_def.centroid.clone());
                centroid_to_shard.push(shard_def.shard_id);
            } else {
                // Legacy: load shard to extract centroid.
                let shard = self.load_shard(shard_def.shard_id)?;
                for c in &shard.centroids {
                    all_centroids.push(c.clone());
                    centroid_to_shard.push(shard_def.shard_id);
                }
            }
        }

        if all_centroids.is_empty() {
            return Ok(Vec::new());
        }

        // ── Step 2: route query to nprobe nearest centroids ───────────────
        let effective_nprobe = nprobe.min(all_centroids.len());
        let probe_indices = top_n_centroids(query, &all_centroids, effective_nprobe);
        let mut probe_shards: Vec<ShardId> = probe_indices
            .into_iter()
            .filter_map(|i| centroid_to_shard.get(i).copied())
            .collect();
        probe_shards.sort();
        probe_shards.dedup();

        debug!(n_shards = probe_shards.len(), "Probing shards");

        // Expand candidate count when reranking to improve recall.
        let candidates_per_shard = if self.rerank_stage.is_some() {
            k.saturating_mul(self.rerank_oversample).max(k)
        } else {
            k
        };

        // ── Step 3: load shards, search candidates ────────────────────────
        let mut all_results: Vec<SearchResult> = Vec::new();
        let mut probed_records: Vec<VectorRecord> = Vec::new();

        for shard_id in probe_shards {
            let shard = self.load_shard(shard_id)?;
            let results =
                self.candidate_stage
                    .search_shard(query, &shard, candidates_per_shard, metric)?;
            all_results.extend(results);
            if self.rerank_stage.is_some() {
                probed_records.extend(shard.records.iter().cloned());
            }
        }

        // ── Step 4: merge candidates across shards ────────────────────────
        let merged = merge_top_k(all_results, candidates_per_shard);

        // ── Step 5: optional exact reranking ──────────────────────────────
        if let Some(reranker) = &self.rerank_stage {
            Ok(reranker.rerank(query, merged, &probed_records, metric, k))
        } else {
            Ok(merged)
        }
    }

    fn load_shard(&self, shard_id: ShardId) -> Result<Arc<ShardIndex>> {
        {
            let cache = self
                .cache
                .lock()
                .map_err(|_| IndexError::Other("pipeline cache lock poisoned".into()))?;
            if let Some(idx) = cache.get(&shard_id) {
                return Ok(Arc::clone(idx));
            }
        }

        let shard_def = self
            .manifest
            .shards
            .iter()
            .find(|s| s.shard_id == shard_id)
            .ok_or_else(|| IndexError::Other(format!("shard {shard_id} not in manifest")))?;

        let bytes = self.store.get(&shard_def.artifact_key)?;
        let idx = Arc::new(ShardIndex::from_bytes(&bytes)?);

        let mut cache = self
            .cache
            .lock()
            .map_err(|_| IndexError::Other("pipeline cache lock poisoned".into()))?;
        cache.insert(shard_id, Arc::clone(&idx));
        Ok(idx)
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

/// Builder for [`QueryPipeline`].
///
/// Start from [`QueryPipeline::builder`].  All stages default to
/// [`ExactCandidateStage`] with no reranking.
pub struct QueryPipelineBuilder {
    store: Arc<dyn ObjectStore>,
    manifest: Manifest,
    candidate_stage: Arc<dyn CandidateSearchStage>,
    rerank_stage: Option<Arc<dyn RerankStage>>,
    rerank_oversample: usize,
}

impl QueryPipelineBuilder {
    fn new(store: Arc<dyn ObjectStore>, manifest: Manifest) -> Self {
        Self {
            store,
            manifest,
            candidate_stage: Arc::new(ExactCandidateStage),
            rerank_stage: None,
            rerank_oversample: 1,
        }
    }

    /// Override the candidate search stage.
    ///
    /// Use [`PqCandidateStage`] for approximate scoring or provide a custom
    /// implementation.
    #[must_use]
    pub fn candidate_stage(mut self, stage: Arc<dyn CandidateSearchStage>) -> Self {
        self.candidate_stage = stage;
        self
    }

    /// Add an optional reranking stage.
    ///
    /// When set, the pipeline fetches `k × oversample` candidates in the
    /// approximate stage and reranks them to the final top-`k`.
    #[must_use]
    pub fn rerank_stage(mut self, stage: Arc<dyn RerankStage>) -> Self {
        self.rerank_stage = Some(stage);
        self
    }

    /// Set the oversample factor for the approximate candidate stage when
    /// reranking is enabled.
    ///
    /// The pipeline will fetch `k × oversample` candidates per shard before
    /// passing them to the reranking stage.  Defaults to `1` (no expansion).
    #[must_use]
    pub fn rerank_oversample(mut self, oversample: usize) -> Self {
        self.rerank_oversample = oversample.max(1);
        self
    }

    /// Build the pipeline.
    pub fn build(self) -> QueryPipeline {
        QueryPipeline {
            store: self.store,
            manifest: self.manifest,
            cache: Mutex::new(HashMap::new()),
            candidate_stage: self.candidate_stage,
            rerank_stage: self.rerank_stage,
            rerank_oversample: self.rerank_oversample,
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::builder::{BuildParams, IndexBuilder};
    use shardlake_core::{
        config::SystemConfig,
        types::{
            DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
        },
    };
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

    fn build_and_get_pipeline(
        records: Vec<VectorRecord>,
        num_shards: u32,
        dims: usize,
    ) -> (QueryPipeline, Arc<dyn ObjectStore>) {
        let tmp = tempdir().unwrap();
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
                dataset_version: DatasetVersion("ds-pl".into()),
                embedding_version: EmbeddingVersion("emb-pl".into()),
                index_version: IndexVersion("idx-pl".into()),
                metric: DistanceMetric::Euclidean,
                dims,
                vectors_key: "datasets/ds-pl/vectors.jsonl".into(),
                metadata_key: "datasets/ds-pl/metadata.json".into(),
                pq_params: None,
            })
            .unwrap();
        // `tmp` is intentionally leaked so the directory stays alive for the
        // duration of the test (acceptable in unit-test contexts).
        std::mem::forget(tmp);
        let pipeline = QueryPipeline::builder(Arc::clone(&store), manifest).build();
        (pipeline, store)
    }

    #[test]
    fn default_pipeline_returns_correct_top1() {
        let records = make_records(20, 4);
        let query = records[0].data.clone();
        let (pipeline, _store) = build_and_get_pipeline(records, 2, 4);
        let results = pipeline.search(&query, 1, 2).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].id,
            VectorId(0),
            "nearest vector to itself must be itself"
        );
    }

    #[test]
    fn pipeline_rejects_dimension_mismatch() {
        let records = make_records(10, 4);
        let (pipeline, _store) = build_and_get_pipeline(records, 2, 4);
        let err = pipeline.search(&[1.0, 2.0, 3.0], 1, 1).unwrap_err();
        assert!(
            err.to_string().contains("dimension mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn pipeline_top_k_never_exceeds_k() {
        let records = make_records(20, 4);
        let query = records[5].data.clone();
        let (pipeline, _store) = build_and_get_pipeline(records, 2, 4);
        for k in [1, 3, 5, 10] {
            let results = pipeline.search(&query, k, 2).unwrap();
            assert!(
                results.len() <= k,
                "pipeline returned {} results for k={k}",
                results.len()
            );
        }
    }

    #[test]
    fn pipeline_returns_empty_for_no_shards() {
        // Build an index, then replace the manifest shards with an empty list.
        let tmp = tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 1,
            kmeans_iters: 5,
            nprobe: 1,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            kmeans_sample_size: None,
            ..SystemConfig::default()
        };
        let mut manifest = IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records: make_records(5, 2),
                dataset_version: DatasetVersion("ds-empty".into()),
                embedding_version: EmbeddingVersion("emb-empty".into()),
                index_version: IndexVersion("idx-empty".into()),
                metric: DistanceMetric::Euclidean,
                dims: 2,
                vectors_key: "datasets/ds-empty/vectors.jsonl".into(),
                metadata_key: "datasets/ds-empty/metadata.json".into(),
                pq_params: None,
            })
            .unwrap();
        manifest.shards.clear();
        let pipeline = QueryPipeline::builder(store, manifest).build();
        let results = pipeline.search(&[1.0, 2.0], 5, 1).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn pq_candidate_stage_returns_approximate_results() {
        use crate::pq::{PqCodebook, PqParams};

        let records = make_records(40, 4);
        let vecs: Vec<Vec<f32>> = records.iter().map(|r| r.data.clone()).collect();

        let pq = PqCodebook::train(
            &vecs,
            PqParams {
                num_subspaces: 2,
                codebook_size: 16,
            },
            42,
            20,
        )
        .unwrap();
        let pq_stage = PqCandidateStage::new(Arc::new(pq));

        let shard = crate::shard::ShardIndex {
            shard_id: shardlake_core::types::ShardId(0),
            dims: 4,
            centroids: vec![records[0].data.clone()],
            records: records.clone(),
        };

        let query = records[0].data.clone();
        let results = pq_stage
            .search_shard(&query, &shard, 5, DistanceMetric::Euclidean)
            .unwrap();

        assert!(!results.is_empty(), "PQ stage should return results");
        assert!(results.len() <= 5);
        // The nearest vector (id=0) should be in the top results.
        assert!(
            results.iter().any(|r| r.id == VectorId(0)),
            "vector 0 (identical to query) should be in PQ top-5"
        );
    }

    #[test]
    fn exact_rerank_stage_rescores_candidates() {
        let records = make_records(10, 4);
        let reranker = ExactRerankStage;

        // Provide approximate candidates with deliberately wrong scores.
        let approx_candidates: Vec<SearchResult> = records
            .iter()
            .map(|r| SearchResult {
                id: r.id,
                score: 999.0, // wrong scores to verify rescoring
                metadata: None,
            })
            .collect();

        let query = records[0].data.clone();
        let exact_results = reranker.rerank(
            &query,
            approx_candidates,
            &records,
            DistanceMetric::Euclidean,
            3,
        );

        assert_eq!(exact_results.len(), 3);
        // After reranking, the nearest vector (id=0) must be first.
        assert_eq!(
            exact_results[0].id,
            VectorId(0),
            "exact reranking must put id=0 (identical to query) first"
        );
        // Scores should no longer be the placeholder 999.0.
        assert!(
            exact_results[0].score < 999.0,
            "reranked score should not be the placeholder"
        );
    }

    #[test]
    fn pipeline_with_pq_stage_and_reranking_finds_correct_top1() {
        use crate::pq::{PqCodebook, PqParams};

        let records = make_records(20, 4);
        let vecs: Vec<Vec<f32>> = records.iter().map(|r| r.data.clone()).collect();

        let tmp = tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 2,
            kmeans_iters: 10,
            nprobe: 2,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            kmeans_sample_size: None,
            ..SystemConfig::default()
        };
        let manifest = IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records: records.clone(),
                dataset_version: DatasetVersion("ds-pq".into()),
                embedding_version: EmbeddingVersion("emb-pq".into()),
                index_version: IndexVersion("idx-pq".into()),
                metric: DistanceMetric::Euclidean,
                dims: 4,
                vectors_key: "datasets/ds-pq/vectors.jsonl".into(),
                metadata_key: "datasets/ds-pq/metadata.json".into(),
                pq_params: None,
            })
            .unwrap();

        let pq = PqCodebook::train(
            &vecs,
            PqParams {
                num_subspaces: 2,
                codebook_size: 16,
            },
            42,
            20,
        )
        .unwrap();

        let pipeline = QueryPipeline::builder(Arc::clone(&store), manifest)
            .candidate_stage(Arc::new(PqCandidateStage::new(Arc::new(pq))))
            .rerank_stage(Arc::new(ExactRerankStage))
            .rerank_oversample(5)
            .build();

        let query = records[0].data.clone();
        let results = pipeline.search(&query, 3, 2).unwrap();

        assert!(!results.is_empty());
        assert_eq!(
            results[0].id,
            VectorId(0),
            "PQ+rerank pipeline must place id=0 first"
        );
    }

    #[test]
    fn builder_oversample_clamped_to_minimum_one() {
        let tmp = tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 1,
            kmeans_iters: 5,
            nprobe: 1,
            kmeans_seed: SystemConfig::default_kmeans_seed(),
            kmeans_sample_size: None,
            ..SystemConfig::default()
        };
        let manifest = IndexBuilder::new(store.as_ref(), &config)
            .build(BuildParams {
                records: make_records(5, 2),
                dataset_version: DatasetVersion("ds-os".into()),
                embedding_version: EmbeddingVersion("emb-os".into()),
                index_version: IndexVersion("idx-os".into()),
                metric: DistanceMetric::Euclidean,
                dims: 2,
                vectors_key: "datasets/ds-os/vectors.jsonl".into(),
                metadata_key: "datasets/ds-os/metadata.json".into(),
                pq_params: None,
            })
            .unwrap();

        // oversample=0 should be clamped to 1.
        let pipeline = QueryPipeline::builder(store, manifest)
            .rerank_oversample(0)
            .build();
        assert_eq!(pipeline.rerank_oversample, 1);
    }
}
