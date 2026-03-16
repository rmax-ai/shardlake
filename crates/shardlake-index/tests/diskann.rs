//! Integration tests for the DiskANN experiment path.
//!
//! These tests verify that the DiskANN plugin and candidate stage can be
//! wired into the query pipeline through the shared [`AnnPlugin`] interface,
//! that metric validation behaves correctly, and that the bounded-exploration
//! candidate stage returns sensible results for both small and large shards
//! including the case where `top_k > beam_width`.

use std::sync::Arc;

use shardlake_core::{
    config::{FanOutPolicy, QueryConfig, SystemConfig},
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, ShardId, VectorId,
        VectorRecord,
    },
    AnnFamily,
};
use shardlake_index::{
    plugin::{AnnPlugin, AnnRegistry, DiskAnnCandidateStage, DiskAnnPlugin},
    shard::ShardIndex,
    BuildParams, CandidateSearchStage, IndexBuilder, QueryPipeline, DISKANN_DEFAULT_BEAM_WIDTH,
};
use shardlake_storage::{LocalObjectStore, ObjectStore};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn make_records(n: usize, dims: usize) -> Vec<VectorRecord> {
    (0..n)
        .map(|i| VectorRecord {
            id: VectorId(i as u64),
            data: (0..dims).map(|d| (i * dims + d) as f32 / 100.0).collect(),
            metadata: None,
        })
        .collect()
}

/// Build a bare `ShardIndex` without going through the full index builder.
/// Useful for unit-testing `DiskAnnCandidateStage` directly.
fn make_shard(records: Vec<VectorRecord>, dims: usize) -> ShardIndex {
    ShardIndex {
        shard_id: ShardId(0),
        dims,
        centroids: vec![vec![0.0; dims]],
        records,
    }
}

fn build_test_index(
    store: &dyn ObjectStore,
    records: Vec<VectorRecord>,
    dims: usize,
    num_shards: u32,
    metric: DistanceMetric,
    root: &std::path::Path,
) -> shardlake_manifest::Manifest {
    let config = SystemConfig {
        storage_root: root.to_path_buf(),
        num_shards,
        kmeans_iters: 5,
        nprobe: num_shards,
        kmeans_seed: SystemConfig::default_kmeans_seed(),
        kmeans_sample_size: None,
        ..SystemConfig::default()
    };
    IndexBuilder::new(store, &config)
        .build(BuildParams {
            records,
            dataset_version: DatasetVersion("ds".into()),
            embedding_version: EmbeddingVersion("emb".into()),
            index_version: IndexVersion("idx".into()),
            metric,
            dims,
            vectors_key: "ds/vectors.jsonl".into(),
            metadata_key: "ds/metadata.json".into(),
            pq_params: None,
        })
        .unwrap()
}

// ── AnnFamily ─────────────────────────────────────────────────────────────────

#[test]
fn ann_family_diskann_parses_and_round_trips() {
    let family = "diskann".parse::<AnnFamily>().unwrap();
    assert_eq!(family, AnnFamily::DiskAnn);
    assert_eq!(family.as_str(), "diskann");
    assert_eq!(family.to_string(), "diskann");
}

#[test]
fn ann_family_diskann_is_not_default() {
    assert_ne!(AnnFamily::default(), AnnFamily::DiskAnn);
}

// ── DiskAnnPlugin – validation ────────────────────────────────────────────────

#[test]
fn diskann_plugin_family_is_diskann() {
    let plugin: &dyn AnnPlugin = &DiskAnnPlugin::new(32);
    assert_eq!(plugin.family(), "diskann");
}

#[test]
fn diskann_plugin_accepts_euclidean_metric() {
    let plugin = DiskAnnPlugin::new(32);
    assert!(
        plugin.validate(128, DistanceMetric::Euclidean).is_ok(),
        "DiskAnn should accept Euclidean distance"
    );
}

#[test]
fn diskann_plugin_rejects_cosine_metric() {
    let plugin = DiskAnnPlugin::new(32);
    let err = plugin.validate(128, DistanceMetric::Cosine).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("euclidean"),
        "error should explain the euclidean constraint: {msg}"
    );
    assert!(
        msg.contains("diskann"),
        "error should identify the family: {msg}"
    );
}

#[test]
fn diskann_plugin_rejects_inner_product_metric() {
    let plugin = DiskAnnPlugin::new(32);
    let err = plugin
        .validate(128, DistanceMetric::InnerProduct)
        .unwrap_err();
    assert!(
        err.to_string().contains("euclidean"),
        "error should explain the euclidean constraint: {err}"
    );
}

// ── DiskAnnCandidateStage – bounded exploration semantics ─────────────────────

/// When `top_k > beam_width` the stage must still return `top_k` results
/// (not just `beam_width` results).  This was a bug in the original
/// beam-admission implementation.
#[test]
fn diskann_stage_top_k_greater_than_beam_width_returns_k_results() {
    let beam_width = 4;
    let k = 10; // k > beam_width
    let n_records = 50; // shard is larger than beam_width
    let dims = 4;

    let records = make_records(n_records, dims);
    let shard = make_shard(records.clone(), dims);
    let stage = DiskAnnCandidateStage::new(beam_width);

    let query = records[0].data.clone();
    let results = stage
        .search(&query, &shard, DistanceMetric::Euclidean, k)
        .unwrap();

    // Must return exactly min(k, n_records) results.
    assert_eq!(
        results.len(),
        k.min(n_records),
        "should return min(k, shard_size) results when k > beam_width"
    );
}

/// When the shard has fewer records than both k and beam_width, return all
/// available records (i.e. exact scan path).
#[test]
fn diskann_stage_small_shard_returns_all_available() {
    let beam_width = 32;
    let k = 20;
    let n_records = 8; // shard smaller than both beam_width and k
    let dims = 4;

    let records = make_records(n_records, dims);
    let shard = make_shard(records.clone(), dims);
    let stage = DiskAnnCandidateStage::new(beam_width);

    let query = records[0].data.clone();
    let results = stage
        .search(&query, &shard, DistanceMetric::Euclidean, k)
        .unwrap();

    assert_eq!(
        results.len(),
        n_records,
        "small shard: all records should be returned"
    );
}

/// Locks in the bounded-exploration contract: with a shard larger than
/// `max(k, beam_width)`, the stage must NOT score every record.
///
/// We verify this behaviourally: construct a shard of 200 records with the
/// true nearest neighbour sitting exactly in the middle (position 100).
/// With beam_width=5 and k=3, only max(3,5)=5 strided positions are scored.
/// The strided positions cannot include index 100 (stride = 200/5 = 40,
/// so positions are 0, 40, 80, 120, 160), so the true nearest neighbour
/// should NOT be in the result set — confirming that only the probe set was
/// scored and no full scan occurred.
#[test]
fn diskann_stage_does_not_scan_full_shard_when_beam_is_small() {
    let beam_width = 5;
    let k = 3;
    let dims = 2;
    let n_records = 200;

    // All records are identical distant points except index 100, which is the
    // true nearest neighbour of the query.
    let query = vec![0.0f32, 0.0];
    let true_nearest_idx = 100usize;

    let records: Vec<VectorRecord> = (0..n_records)
        .map(|i| VectorRecord {
            id: VectorId(i as u64),
            data: if i == true_nearest_idx {
                vec![0.001, 0.001] // very close to query
            } else {
                vec![10.0 + i as f32, 10.0 + i as f32] // far from query
            },
            metadata: None,
        })
        .collect();

    let shard = make_shard(records, dims);
    let stage = DiskAnnCandidateStage::new(beam_width);

    let results = stage
        .search(&query, &shard, DistanceMetric::Euclidean, k)
        .unwrap();

    // With stride=40, probed indices are 0, 40, 80, 120, 160.
    // Index 100 is NOT probed, so the true nearest neighbour should not
    // appear in the results — confirming bounded (not full) exploration.
    assert_eq!(results.len(), k, "should return k results");
    let ids: Vec<u64> = results.iter().map(|r| r.id.0).collect();
    assert!(
        !ids.contains(&(true_nearest_idx as u64)),
        "true nearest at idx {true_nearest_idx} must NOT appear; \
         its strided position is not probed — if it appears the stage \
         is performing a full scan. Got ids: {ids:?}"
    );
}

/// Verify that the returned candidate count equals `min(k, shard.len())`
/// for the full range of `k` relative to `beam_width`.
#[test]
fn diskann_stage_result_count_is_min_k_shard_len() {
    let beam_width = 8;
    let dims = 2;
    let n_records = 30;
    let records = make_records(n_records, dims);
    let shard = make_shard(records.clone(), dims);
    let stage = DiskAnnCandidateStage::new(beam_width);
    let query = records[0].data.clone();

    for k in [
        1,
        3,
        beam_width - 1,
        beam_width,
        beam_width + 1,
        n_records,
        n_records + 5,
    ] {
        let results = stage
            .search(&query, &shard, DistanceMetric::Euclidean, k)
            .unwrap();
        let expected = k.min(n_records);
        assert_eq!(
            results.len(),
            expected,
            "k={k}: expected {expected} results, got {}",
            results.len()
        );
    }
}

// ── DiskAnnPlugin – pipeline wiring ──────────────────────────────────────────

#[test]
fn diskann_plugin_wires_into_query_pipeline_and_returns_results() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let records = make_records(16, 4);
    let manifest = build_test_index(
        store.as_ref(),
        records.clone(),
        4,
        2,
        DistanceMetric::Euclidean,
        tmp.path(),
    );

    let plugin = DiskAnnPlugin::new(8);
    plugin.validate(4, DistanceMetric::Euclidean).unwrap();

    let stage = plugin.candidate_stage();
    let pipeline = QueryPipeline::builder(Arc::clone(&store), manifest)
        .candidate_stage(stage)
        .build();

    let query = records[0].data.clone();
    let results = pipeline
        .search(
            &query,
            &QueryConfig {
                top_k: 3,
                fan_out: FanOutPolicy {
                    candidate_centroids: 2,
                    ..FanOutPolicy::default()
                },
                ..QueryConfig::default()
            },
        )
        .unwrap();

    assert!(
        !results.is_empty(),
        "DiskANN pipeline should return results"
    );
    assert!(results.len() <= 3, "should return at most top_k results");
}

#[test]
fn diskann_plugin_with_wide_beam_matches_exact_quality() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let records = make_records(20, 4);

    let manifest = build_test_index(
        store.as_ref(),
        records.clone(),
        4,
        2,
        DistanceMetric::Euclidean,
        tmp.path(),
    );

    // With beam_width >= shard size, DiskANN degrades to an exact scan.
    let plugin = DiskAnnPlugin::new(512);
    let stage = plugin.candidate_stage();
    let pipeline = QueryPipeline::builder(Arc::clone(&store), manifest)
        .candidate_stage(stage)
        .build();

    let query = records[0].data.clone();
    let results = pipeline
        .search(
            &query,
            &QueryConfig {
                top_k: 5,
                fan_out: FanOutPolicy {
                    candidate_centroids: 2,
                    ..FanOutPolicy::default()
                },
                ..QueryConfig::default()
            },
        )
        .unwrap();

    // The query vector is records[0] itself, so it should be the top result.
    assert!(!results.is_empty());
    assert_eq!(
        results[0].id,
        VectorId(0),
        "nearest result should be the query vector itself"
    );
}

// ── AnnRegistry – DiskANN entries ────────────────────────────────────────────

#[test]
fn registry_includes_diskann_family() {
    let families = AnnRegistry::families();
    assert!(
        families.contains(&"diskann"),
        "AnnRegistry should list diskann"
    );
}

#[test]
fn registry_exists_returns_true_for_diskann() {
    assert!(AnnRegistry::exists("diskann"));
}

#[test]
fn registry_get_flat_returns_diskann_plugin_with_default_beam_width() {
    let plugin = AnnRegistry::get_flat("diskann").unwrap();
    assert_eq!(plugin.family(), "diskann");
    // Validate that it accepts the default metric for DiskANN.
    assert!(plugin.validate(128, DistanceMetric::Euclidean).is_ok());
}

#[test]
fn diskann_default_beam_width_is_positive() {
    assert!(
        DISKANN_DEFAULT_BEAM_WIDTH > 0,
        "default beam width must be positive"
    );
}

// ── Error handling ────────────────────────────────────────────────────────────

/// Validate that the DiskANN plugin rejects a misconfigured pipeline before
/// any pipeline construction occurs.
#[test]
fn diskann_plugin_validation_prevents_wrong_metric_pipeline() {
    let plugin = DiskAnnPlugin::new(32);
    let err = plugin.validate(4, DistanceMetric::Cosine).unwrap_err();
    assert!(
        err.to_string().contains("euclidean"),
        "validation should surface the metric constraint: {err}"
    );
}

/// Verify that the stage itself returns an error for non-Euclidean metrics.
#[test]
fn diskann_stage_rejects_non_euclidean_metric() {
    let records = make_records(10, 2);
    let shard = make_shard(records.clone(), 2);
    let stage = DiskAnnCandidateStage::new(4);
    let query = records[0].data.clone();

    let err = stage
        .search(&query, &shard, DistanceMetric::Cosine, 3)
        .unwrap_err();
    assert!(
        err.to_string().contains("euclidean"),
        "stage should reject cosine metric: {err}"
    );
}
