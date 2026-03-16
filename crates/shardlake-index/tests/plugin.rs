//! Integration tests for the ANN plugin interface.
//!
//! These tests verify that multiple ANN backends can be registered or selected
//! through the same interface with predictable validation and error behaviour,
//! and that the query pipeline can be wired up via the plugin without
//! algorithm-specific branching at the call site.

use std::sync::Arc;

use shardlake_core::{
    config::{FanOutPolicy, QueryConfig, SystemConfig},
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
    },
    AnnFamily,
};
use shardlake_index::{
    plugin::{AnnPlugin, AnnRegistry, DiskAnnPlugin, IvfFlatPlugin, IvfPqPlugin},
    pq::{PqCodebook, PqParams},
    BuildParams, IndexBuilder, QueryPipeline,
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

fn make_codebook(dims: usize, num_subspaces: usize) -> Arc<PqCodebook> {
    let records: Vec<Vec<f32>> = (0..32_u32)
        .map(|i| (0..dims).map(|d| (i as f32 + d as f32) / 10.0).collect())
        .collect();
    let params = PqParams {
        num_subspaces,
        codebook_size: 4,
    };
    Arc::new(PqCodebook::train(&records, params, 42, 5).unwrap())
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
fn ann_family_parses_all_known_names() {
    assert_eq!("ivf_flat".parse::<AnnFamily>().unwrap(), AnnFamily::IvfFlat);
    assert_eq!("ivf_pq".parse::<AnnFamily>().unwrap(), AnnFamily::IvfPq);
    assert_eq!("diskann".parse::<AnnFamily>().unwrap(), AnnFamily::DiskAnn);
}

#[test]
fn ann_family_display_matches_as_str() {
    assert_eq!(AnnFamily::IvfFlat.to_string(), AnnFamily::IvfFlat.as_str());
    assert_eq!(AnnFamily::IvfPq.to_string(), AnnFamily::IvfPq.as_str());
    assert_eq!(AnnFamily::DiskAnn.to_string(), AnnFamily::DiskAnn.as_str());
}

#[test]
fn ann_family_parse_unknown_returns_error() {
    let err = "hnsw".parse::<AnnFamily>().unwrap_err();
    assert!(err.to_string().contains("unknown ANN family"));
    assert!(err.to_string().contains("hnsw"));
}

#[test]
fn ann_family_default_is_ivf_flat() {
    assert_eq!(AnnFamily::default(), AnnFamily::IvfFlat);
}

// ── IvfFlatPlugin interface behaviour ────────────────────────────────────────

#[test]
fn ivf_flat_plugin_satisfies_ann_plugin_trait() {
    let plugin: &dyn AnnPlugin = &IvfFlatPlugin;
    assert_eq!(plugin.family(), "ivf_flat");
}

#[test]
fn ivf_flat_plugin_accepts_all_distance_metrics() {
    let plugin = IvfFlatPlugin;
    for metric in [
        DistanceMetric::Cosine,
        DistanceMetric::Euclidean,
        DistanceMetric::InnerProduct,
    ] {
        assert!(
            plugin.validate(128, metric).is_ok(),
            "ivf_flat should accept metric {metric}"
        );
    }
}

#[test]
fn ivf_flat_plugin_candidate_stage_searches_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let records = make_records(12, 4);
    let manifest = build_test_index(
        store.as_ref(),
        records.clone(),
        4,
        2,
        DistanceMetric::Cosine,
        tmp.path(),
    );

    let stage = IvfFlatPlugin.candidate_stage();

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
    assert!(!results.is_empty());
    assert!(results.len() <= 3);
}

// ── IvfPqPlugin interface behaviour ──────────────────────────────────────────

#[test]
fn ivf_pq_plugin_satisfies_ann_plugin_trait() {
    let codebook = make_codebook(4, 2);
    let plugin: &dyn AnnPlugin = &IvfPqPlugin::new(codebook);
    assert_eq!(plugin.family(), "ivf_pq");
}

#[test]
fn ivf_pq_plugin_accepts_euclidean_metric() {
    let plugin = IvfPqPlugin::new(make_codebook(4, 2));
    assert!(plugin.validate(4, DistanceMetric::Euclidean).is_ok());
}

#[test]
fn ivf_pq_plugin_rejects_cosine_metric() {
    let plugin = IvfPqPlugin::new(make_codebook(4, 2));
    let err = plugin.validate(4, DistanceMetric::Cosine).unwrap_err();
    assert!(err.to_string().contains("euclidean"));
    assert!(err.to_string().contains("ivf_pq"));
}

#[test]
fn ivf_pq_plugin_rejects_inner_product_metric() {
    let plugin = IvfPqPlugin::new(make_codebook(4, 2));
    let err = plugin
        .validate(4, DistanceMetric::InnerProduct)
        .unwrap_err();
    assert!(err.to_string().contains("euclidean"));
}

// ── AnnRegistry ───────────────────────────────────────────────────────────────

#[test]
fn registry_exposes_all_builtin_families() {
    let families = AnnRegistry::families();
    assert!(
        families.contains(&"ivf_flat"),
        "ivf_flat should be registered"
    );
    assert!(families.contains(&"ivf_pq"), "ivf_pq should be registered");
    assert!(
        families.contains(&"diskann"),
        "diskann should be registered"
    );
}

#[test]
fn registry_exists_for_known_families() {
    assert!(AnnRegistry::exists("ivf_flat"));
    assert!(AnnRegistry::exists("ivf_pq"));
    assert!(AnnRegistry::exists("diskann"));
}

#[test]
fn registry_does_not_exist_for_unknown_family() {
    assert!(!AnnRegistry::exists("hnsw"));
    assert!(!AnnRegistry::exists(""));
    assert!(!AnnRegistry::exists("HNSW"));
}

#[test]
fn registry_get_flat_returns_ivf_flat_plugin() {
    let plugin = AnnRegistry::get_flat("ivf_flat").unwrap();
    assert_eq!(plugin.family(), "ivf_flat");
}

#[test]
fn registry_get_flat_returns_diskann_plugin() {
    let plugin = AnnRegistry::get_flat("diskann").unwrap();
    assert_eq!(plugin.family(), "diskann");
}

#[test]
fn registry_get_flat_rejects_ivf_pq_with_actionable_message() {
    let err = AnnRegistry::get_flat("ivf_pq").err().unwrap();
    let msg = err.to_string();
    assert!(
        msg.contains("codebook"),
        "error should mention codebook: {msg}"
    );
}

#[test]
fn registry_get_flat_rejects_unknown_family() {
    let err = AnnRegistry::get_flat("hnsw").err().unwrap();
    let msg = err.to_string();
    assert!(
        msg.contains("unknown ANN family"),
        "error should mention unknown family: {msg}"
    );
    assert!(
        msg.contains("hnsw"),
        "error should include the bad name: {msg}"
    );
    // The error message should also list the valid choices.
    assert!(
        msg.contains("ivf_flat"),
        "error should list valid families: {msg}"
    );
}

// ── Pipeline integration – no algorithm-specific branching ───────────────────

/// Demonstrate that all three backends can be wired into a QueryPipeline
/// through the same AnnPlugin interface without algorithm-specific branching
/// at the call site.
#[test]
fn all_backends_wire_into_pipeline_via_plugin_interface() {
    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let records = make_records(12, 4);
    // Use Euclidean so IvfFlatPlugin, IvfPqPlugin, and DiskAnnPlugin are all compatible.
    let manifest = build_test_index(
        store.as_ref(),
        records.clone(),
        4,
        2,
        DistanceMetric::Euclidean,
        tmp.path(),
    );

    // All plugins implement the same trait; the call site below is identical
    // for all – no algorithm-specific branching at the wiring edge.
    let plugins: Vec<Box<dyn AnnPlugin>> = vec![
        Box::new(IvfFlatPlugin),
        Box::new(IvfPqPlugin::new(make_codebook(4, 2))),
        Box::new(DiskAnnPlugin::new(8)),
    ];

    let query = records[0].data.clone();
    for plugin in &plugins {
        // validate first (the integration edge)
        plugin.validate(4, DistanceMetric::Euclidean).unwrap();

        let stage = plugin.candidate_stage();
        let pipeline = QueryPipeline::builder(Arc::clone(&store), manifest.clone())
            .candidate_stage(stage)
            .build();

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
            "plugin {} should find results",
            plugin.family()
        );
    }
}

/// Validate that the IVF-PQ plugin correctly rejects a non-Euclidean metric
/// before any pipeline is constructed—this is the validation error path.
#[test]
fn ivf_pq_plugin_validation_prevents_misconfigured_pipeline() {
    let plugin = IvfPqPlugin::new(make_codebook(4, 2));
    // Would be caught at the integration edge before pipeline construction.
    let err = plugin.validate(4, DistanceMetric::Cosine).unwrap_err();
    assert!(
        err.to_string().contains("euclidean"),
        "validation should explain the constraint: {err}"
    );
}
