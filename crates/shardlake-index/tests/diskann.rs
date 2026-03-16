//! Integration tests for the DiskANN experiment path.
//!
//! These tests verify that the DiskANN plugin and candidate stage can be
//! wired into the query pipeline through the shared [`AnnPlugin`] interface,
//! that metric validation behaves correctly, and that the beam-search
//! candidate stage returns sensible results for both small and large shards.

use std::sync::Arc;

use shardlake_core::{
    config::{FanOutPolicy, QueryConfig, SystemConfig},
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
    },
    AnnFamily,
};
use shardlake_index::{
    plugin::{AnnPlugin, AnnRegistry, DiskAnnPlugin},
    BuildParams, IndexBuilder, QueryPipeline, DISKANN_DEFAULT_BEAM_WIDTH,
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
