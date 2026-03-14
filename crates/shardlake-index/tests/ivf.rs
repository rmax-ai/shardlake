//! Integration tests for the IVF build path.
//!
//! Covers IVF coarse-quantizer training, vector assignment, and artifact
//! persistence end-to-end.

use std::sync::Arc;

use rand::SeedableRng;
use shardlake_core::{
    config::SystemConfig,
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
    },
};
use shardlake_index::{ivf::IvfQuantizer, BuildParams, IndexBuilder};
use shardlake_storage::{paths, LocalObjectStore, ObjectStore};

fn make_records(n: usize, dims: usize) -> Vec<VectorRecord> {
    (0..n)
        .map(|i| VectorRecord {
            id: VectorId(i as u64),
            data: (0..dims).map(|d| ((i * dims + d) as f32) / 100.0).collect(),
            metadata: None,
        })
        .collect()
}

fn default_config(tmp: &std::path::Path, num_shards: u32) -> SystemConfig {
    SystemConfig {
        storage_root: tmp.to_path_buf(),
        num_shards,
        kmeans_iters: 10,
        nprobe: 2,
        kmeans_seed: SystemConfig::default_kmeans_seed(),
    }
}

// ---------------------------------------------------------------------------
// IvfQuantizer unit-level integration
// ---------------------------------------------------------------------------

/// Training on a linearly-spaced dataset should converge and produce centroids
/// that cover distinct parts of the space.
#[test]
fn ivf_quantizer_train_and_assign_basic() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(42u64);
    let vecs: Vec<Vec<f32>> = (0..100).map(|i| vec![i as f32, (i * 2) as f32]).collect();
    let q = IvfQuantizer::train(&vecs, 4, 20, &mut rng);

    assert_eq!(q.num_clusters(), 4);
    assert_eq!(q.dims(), 2);

    // Every vector must be assignable to a valid cluster index.
    for v in &vecs {
        let cluster = q.assign(v);
        assert!(cluster < 4, "cluster index {cluster} out of range");
    }
}

/// Two clearly-separated clusters should be assigned to distinct centroids.
#[test]
fn ivf_quantizer_separates_two_clusters() {
    // Use seed 42, which reliably separates this two-cluster dataset.
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let mut vecs: Vec<Vec<f32>> = (0..50).map(|_| vec![0.0f32, 0.0]).collect();
    vecs.extend((0..50).map(|_| vec![1000.0f32, 1000.0]));

    let q = IvfQuantizer::train(&vecs, 2, 30, &mut rng);

    let c_near_origin = q.assign(&[1.0, 1.0]);
    let c_far = q.assign(&[999.0, 999.0]);
    assert_ne!(
        c_near_origin, c_far,
        "near-origin and far vectors should be in different clusters"
    );
}

/// `top_probes` must return results sorted by ascending distance.
#[test]
fn ivf_quantizer_top_probes_ordered() {
    let centroids = vec![
        vec![0.0f32, 0.0],
        vec![10.0f32, 0.0],
        vec![20.0f32, 0.0],
        vec![30.0f32, 0.0],
    ];
    let q = IvfQuantizer::from_centroids(centroids);

    let probes = q.top_probes(&[12.0, 0.0], 3);
    assert_eq!(probes.len(), 3);
    // Nearest to (12, 0) should be index 1 (centroid at (10,0)), then 2, then 0.
    assert_eq!(probes[0], 1);
    assert_eq!(probes[1], 2);
    assert_eq!(probes[2], 0);
}

// ---------------------------------------------------------------------------
// IVF build-path: manifest and artifact persistence
// ---------------------------------------------------------------------------

/// A successful build must emit `algorithm = "ivf-flat"` in the manifest.
#[test]
fn build_emits_ivf_flat_algorithm() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = default_config(tmp.path(), 2);

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: make_records(20, 4),
            dataset_version: DatasetVersion("ds-ivf".into()),
            embedding_version: EmbeddingVersion("emb-ivf".into()),
            index_version: IndexVersion("idx-ivf".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-ivf"),
            metadata_key: paths::dataset_metadata_key("ds-ivf"),
        })
        .unwrap();

    assert_eq!(
        manifest.algorithm.algorithm, "ivf-flat",
        "algorithm field must be 'ivf-flat'"
    );
}

/// The manifest must record `num_clusters` in `algorithm.params`.
#[test]
fn build_records_num_clusters_in_params() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = default_config(tmp.path(), 3);

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: make_records(30, 4),
            dataset_version: DatasetVersion("ds-nc".into()),
            embedding_version: EmbeddingVersion("emb-nc".into()),
            index_version: IndexVersion("idx-nc".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-nc"),
            metadata_key: paths::dataset_metadata_key("ds-nc"),
        })
        .unwrap();

    let nc = manifest
        .algorithm
        .params
        .get("num_clusters")
        .expect("num_clusters must be in algorithm.params");
    assert_eq!(nc.as_u64().unwrap(), 3);
}

/// A built index must have a `coarse_quantizer_key` in the manifest that points
/// to an artifact that actually exists in storage and round-trips correctly.
#[test]
fn build_persists_coarse_quantizer_artifact() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = default_config(tmp.path(), 2);
    let num_clusters = config.num_shards as usize;

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: make_records(20, 4),
            dataset_version: DatasetVersion("ds-cq".into()),
            embedding_version: EmbeddingVersion("emb-cq".into()),
            index_version: IndexVersion("idx-cq".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-cq"),
            metadata_key: paths::dataset_metadata_key("ds-cq"),
        })
        .unwrap();

    // manifest must advertise a coarse_quantizer_key.
    let cq_key = manifest
        .coarse_quantizer_key
        .as_deref()
        .expect("coarse_quantizer_key must be set in manifest");

    // The artifact must exist in storage.
    assert!(
        store.exists(cq_key).unwrap(),
        "coarse quantizer artifact must exist at key '{cq_key}'"
    );

    // The stored bytes must round-trip to a valid IvfQuantizer.
    let bytes = store.get(cq_key).unwrap();
    let recovered = IvfQuantizer::from_bytes(&bytes).unwrap();

    assert_eq!(
        recovered.num_clusters(),
        num_clusters,
        "recovered quantizer cluster count should match build config"
    );
    assert_eq!(
        recovered.dims(),
        4,
        "recovered quantizer dims should match input"
    );
}

/// The key stored in `coarse_quantizer_key` must equal the canonical path
/// produced by `paths::index_coarse_quantizer_key`.
#[test]
fn coarse_quantizer_key_matches_canonical_path() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = default_config(tmp.path(), 2);

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: make_records(20, 4),
            dataset_version: DatasetVersion("ds-path".into()),
            embedding_version: EmbeddingVersion("emb-path".into()),
            index_version: IndexVersion("idx-path".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-path"),
            metadata_key: paths::dataset_metadata_key("ds-path"),
        })
        .unwrap();

    assert_eq!(
        manifest.coarse_quantizer_key.as_deref().unwrap(),
        paths::index_coarse_quantizer_key("idx-path")
    );
}

/// After a build the coarse-quantizer centroids stored in `ShardDef.centroid`
/// must match the centroids recovered from the coarse-quantizer artifact.
#[test]
fn coarse_quantizer_centroids_match_shard_def_centroids() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
    let config = default_config(tmp.path(), 2);

    let manifest = IndexBuilder::new(store.as_ref(), &config)
        .build(BuildParams {
            records: make_records(20, 4),
            dataset_version: DatasetVersion("ds-cen".into()),
            embedding_version: EmbeddingVersion("emb-cen".into()),
            index_version: IndexVersion("idx-cen".into()),
            metric: DistanceMetric::Euclidean,
            dims: 4,
            vectors_key: paths::dataset_vectors_key("ds-cen"),
            metadata_key: paths::dataset_metadata_key("ds-cen"),
        })
        .unwrap();

    let cq_key = manifest.coarse_quantizer_key.as_deref().unwrap();
    let bytes = store.get(cq_key).unwrap();
    let quantizer = IvfQuantizer::from_bytes(&bytes).unwrap();

    // Build a shard-id → centroid map from the manifest shards.
    for shard in &manifest.shards {
        let shard_idx = shard.shard_id.0 as usize;
        let cq_centroid = &quantizer.centroids()[shard_idx];
        assert_eq!(
            &shard.centroid, cq_centroid,
            "shard {} centroid in manifest must match coarse-quantizer centroid",
            shard.shard_id
        );
    }
}
