//! Integration tests for the distributed build merge step.
//!
//! Each test focuses on one aspect of [`merge_worker_outputs`] behaviour:
//! successful merges, validation failures, and determinism guarantees.

use chrono::Utc;
use shardlake_core::{
    config::SystemConfig,
    types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, ShardId, VectorId,
        VectorRecord,
    },
};
use shardlake_index::{
    merge_worker_outputs, plan_workers, MergeParams, WorkerBuilder, WorkerOutput, WorkerPlan,
    WorkerPlanParams, WorkerShardOutput,
};
use shardlake_storage::LocalObjectStore;
use tempfile::tempdir;

// ── helpers ──────────────────────────────────────────────────────────────────

fn record(id: u64, data: Vec<f32>) -> VectorRecord {
    VectorRecord {
        id: VectorId(id),
        data,
        metadata: None,
    }
}

/// Two clearly separated clusters in 2-D (50 records each).
fn two_cluster_records() -> Vec<VectorRecord> {
    let mut recs: Vec<VectorRecord> = (0..50).map(|i| record(i, vec![0.0f32, 0.0])).collect();
    recs.extend((50..100).map(|i| record(i, vec![100.0f32, 100.0])));
    recs
}

fn default_config(tmp: &std::path::Path, num_shards: u32) -> SystemConfig {
    SystemConfig {
        storage_root: tmp.to_path_buf(),
        num_shards,
        kmeans_iters: 20,
        nprobe: 1,
        kmeans_seed: 0xdead_beef,
        kmeans_sample_size: None,
        ..SystemConfig::default()
    }
}

fn plan_params(index_version: &str) -> WorkerPlanParams {
    WorkerPlanParams {
        index_version: IndexVersion(index_version.into()),
        dataset_version: DatasetVersion("ds-merge-test".into()),
        embedding_version: EmbeddingVersion("emb-merge-test".into()),
        metric: DistanceMetric::Euclidean,
        dims: 2,
        vectors_key: shardlake_storage::paths::dataset_vectors_key("ds-merge-test"),
        metadata_key: shardlake_storage::paths::dataset_metadata_key("ds-merge-test"),
        num_workers: 2,
    }
}

fn default_merge_params() -> MergeParams {
    MergeParams {
        alias: "latest".into(),
        built_at: Utc::now(),
        builder_version: "test".into(),
        build_duration_secs: 0.0,
    }
}

/// Plan workers and execute all of them, returning the plan and outputs.
fn plan_and_execute_all(
    store: &LocalObjectStore,
    config: &SystemConfig,
    records: &[VectorRecord],
    index_version: &str,
) -> (WorkerPlan, Vec<WorkerOutput>) {
    let plan = plan_workers(store, config, records, plan_params(index_version)).unwrap();
    let builder = WorkerBuilder::new(store);
    let outputs: Vec<WorkerOutput> = (0..plan.num_workers)
        .map(|w| {
            let assignment = plan.assignment(w).unwrap();
            builder.execute(&plan, assignment, records).unwrap()
        })
        .collect();
    (plan, outputs)
}

// ── successful merge ──────────────────────────────────────────────────────────

#[test]
fn merge_produces_valid_manifest() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let (plan, outputs) = plan_and_execute_all(&store, &config, &records, "idx-merge-ok");

    let manifest = merge_worker_outputs(&plan, outputs, default_merge_params()).unwrap();

    assert_eq!(manifest.index_version.0, "idx-merge-ok");
    assert_eq!(manifest.dataset_version.0, "ds-merge-test");
    assert!(!manifest.shards.is_empty());
    let total: u64 = manifest.shards.iter().map(|s| s.vector_count).sum();
    assert_eq!(total, manifest.total_vector_count);
    // Shard IDs must be in ascending order.
    let ids: Vec<u32> = manifest.shards.iter().map(|s| s.shard_id.0).collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "shards must be sorted by shard_id");
}

#[test]
fn merge_saves_manifest_to_storage() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let (plan, outputs) = plan_and_execute_all(&store, &config, &records, "idx-merge-save");
    let manifest = merge_worker_outputs(&plan, outputs, default_merge_params()).unwrap();
    manifest.save(&store).unwrap();

    // Manifest must be loadable by index version.
    let loaded = shardlake_manifest::Manifest::load(&store, &manifest.index_version).unwrap();
    assert_eq!(loaded.index_version, manifest.index_version);
    assert_eq!(loaded.total_vector_count, manifest.total_vector_count);
}

#[test]
fn merge_preserves_coarse_quantizer_key() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let (plan, outputs) = plan_and_execute_all(&store, &config, &records, "idx-merge-cq");
    let manifest = merge_worker_outputs(&plan, outputs, default_merge_params()).unwrap();

    assert_eq!(
        manifest.coarse_quantizer_key.as_deref(),
        Some(plan.coarse_quantizer_key.as_str())
    );
}

#[test]
fn merge_includes_routing_metadata() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let (plan, outputs) = plan_and_execute_all(&store, &config, &records, "idx-merge-routing");
    let manifest = merge_worker_outputs(&plan, outputs, default_merge_params()).unwrap();

    for shard in &manifest.shards {
        let routing = shard.routing.as_ref().expect("routing must be populated");
        assert!(!routing.centroid_id.is_empty());
        assert_eq!(routing.index_type, "flat");
        assert!(!routing.file_location.is_empty());
    }
}

#[test]
fn merge_respects_alias_param() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let (plan, outputs) = plan_and_execute_all(&store, &config, &records, "idx-merge-alias");
    let mut mp = default_merge_params();
    mp.alias = "staging".into();
    let manifest = merge_worker_outputs(&plan, outputs, mp).unwrap();

    assert_eq!(manifest.alias, "staging");
}

// ── determinism ───────────────────────────────────────────────────────────────

#[test]
fn merge_is_deterministic_regardless_of_output_order() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let (plan, mut outputs) = plan_and_execute_all(&store, &config, &records, "idx-merge-det");

    // Produce manifest from outputs in both orders.
    let manifest_a = merge_worker_outputs(&plan, outputs.clone(), default_merge_params()).unwrap();

    outputs.reverse();
    let manifest_b = merge_worker_outputs(&plan, outputs, default_merge_params()).unwrap();

    let ids_a: Vec<u32> = manifest_a.shards.iter().map(|s| s.shard_id.0).collect();
    let ids_b: Vec<u32> = manifest_b.shards.iter().map(|s| s.shard_id.0).collect();
    assert_eq!(
        ids_a, ids_b,
        "shard order must be identical regardless of output order"
    );

    let fps_a: Vec<&str> = manifest_a
        .shards
        .iter()
        .map(|s| s.fingerprint.as_str())
        .collect();
    let fps_b: Vec<&str> = manifest_b
        .shards
        .iter()
        .map(|s| s.fingerprint.as_str())
        .collect();
    assert_eq!(
        fps_a, fps_b,
        "fingerprints must match regardless of output order"
    );
}

// ── validation failures ───────────────────────────────────────────────────────

#[test]
fn merge_rejects_empty_outputs() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let plan = plan_workers(&store, &config, &records, plan_params("idx-merge-empty")).unwrap();
    let err = merge_worker_outputs(&plan, vec![], default_merge_params()).unwrap_err();
    assert!(
        err.to_string().contains("at least one worker output"),
        "{err}"
    );
}

#[test]
fn merge_rejects_duplicate_worker_id() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let (plan, mut outputs) =
        plan_and_execute_all(&store, &config, &records, "idx-merge-dup-worker");

    // Duplicate worker 0.
    let dup = outputs[0].clone();
    outputs.push(dup);

    let err = merge_worker_outputs(&plan, outputs, default_merge_params()).unwrap_err();
    assert!(err.to_string().contains("duplicate worker output"), "{err}");
}

#[test]
fn merge_rejects_missing_worker() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let (plan, mut outputs) =
        plan_and_execute_all(&store, &config, &records, "idx-merge-missing-worker");

    // This test requires at least 2 workers; skip when K-means collapses to 1 shard.
    if plan.num_workers < 2 {
        return;
    }

    // Remove worker 1.
    outputs.retain(|o| o.worker_id != 1);

    let err = merge_worker_outputs(&plan, outputs, default_merge_params()).unwrap_err();
    assert!(
        err.to_string().contains("missing output for worker_id"),
        "{err}"
    );
}

#[test]
fn merge_rejects_out_of_range_worker_id() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let (plan, mut outputs) =
        plan_and_execute_all(&store, &config, &records, "idx-merge-oor-worker");

    // Forge an out-of-range worker id.
    outputs[0].worker_id = 999;

    let err = merge_worker_outputs(&plan, outputs, default_merge_params()).unwrap_err();
    assert!(err.to_string().contains("out of range"), "{err}");
}

#[test]
fn merge_rejects_incompatible_index_version() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let (plan, mut outputs) =
        plan_and_execute_all(&store, &config, &records, "idx-merge-ver-mismatch");

    // Tamper with the index version of one output.
    outputs[0].index_version = IndexVersion("idx-WRONG".into());

    let err = merge_worker_outputs(&plan, outputs, default_merge_params()).unwrap_err();
    assert!(
        err.to_string()
            .contains("does not match plan index_version"),
        "{err}"
    );
}

#[test]
fn merge_rejects_duplicate_shard_across_workers() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let (plan, mut outputs) =
        plan_and_execute_all(&store, &config, &records, "idx-merge-dup-shard");

    // This test requires at least 2 workers with at least one shard each.
    if plan.num_workers < 2 || outputs[0].shards.is_empty() {
        return;
    }

    // Inject a duplicate shard from worker 0 into worker 1's output.
    let first_shard = outputs[0].shards[0].clone();
    outputs[1].shards.push(first_shard);

    let err = merge_worker_outputs(&plan, outputs, default_merge_params()).unwrap_err();
    assert!(err.to_string().contains("duplicate shard_id"), "{err}");
}

#[test]
fn merge_rejects_missing_shard() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let (plan, mut outputs) =
        plan_and_execute_all(&store, &config, &records, "idx-merge-missing-shard");

    // Remove one shard from worker 0's output.
    if !outputs[0].shards.is_empty() {
        outputs[0].shards.pop();
    }

    let err = merge_worker_outputs(&plan, outputs, default_merge_params()).unwrap_err();
    assert!(
        err.to_string().contains("missing from worker outputs"),
        "{err}"
    );
}

#[test]
fn merge_rejects_out_of_range_shard_id() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let (plan, mut outputs) =
        plan_and_execute_all(&store, &config, &records, "idx-merge-oor-shard");

    // Inject a shard with an id beyond the plan's shard count.
    outputs[0].shards.push(WorkerShardOutput {
        shard_id: ShardId(9999),
        artifact_key: "fake".into(),
        vector_count: 0,
        fingerprint: "0000000000000000".into(),
        centroid: vec![0.0, 0.0],
        worker_id: 0,
    });

    let err = merge_worker_outputs(&plan, outputs, default_merge_params()).unwrap_err();
    assert!(err.to_string().contains("out of range"), "{err}");
}

#[test]
fn merge_rejects_mismatched_shard_worker_id() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let (plan, mut outputs) =
        plan_and_execute_all(&store, &config, &records, "idx-merge-shard-worker-mismatch");

    outputs[0].shards[0].worker_id = outputs[0].worker_id + 1;

    let err = merge_worker_outputs(&plan, outputs, default_merge_params()).unwrap_err();
    assert!(err.to_string().contains("reports worker_id"), "{err}");
}

// ── shard_summary ─────────────────────────────────────────────────────────────

#[test]
fn merge_produces_correct_shard_summary() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = default_config(tmp.path(), 2);
    let records = two_cluster_records();

    let (plan, outputs) = plan_and_execute_all(&store, &config, &records, "idx-merge-summary");
    let manifest = merge_worker_outputs(&plan, outputs, default_merge_params()).unwrap();

    let summary = manifest
        .shard_summary
        .as_ref()
        .expect("shard_summary must be present");
    assert_eq!(summary.num_shards, manifest.shards.len() as u32);
    let actual_min = manifest
        .shards
        .iter()
        .map(|s| s.vector_count)
        .min()
        .unwrap();
    let actual_max = manifest
        .shards
        .iter()
        .map(|s| s.vector_count)
        .max()
        .unwrap();
    assert_eq!(summary.min_shard_vector_count, actual_min);
    assert_eq!(summary.max_shard_vector_count, actual_max);
}

#[test]
fn merge_preserves_plan_build_metadata() {
    let tmp = tempdir().unwrap();
    let store = LocalObjectStore::new(tmp.path()).unwrap();
    let config = SystemConfig {
        storage_root: tmp.path().to_path_buf(),
        num_shards: 2,
        kmeans_iters: 7,
        nprobe: 5,
        kmeans_seed: 1234,
        kmeans_sample_size: Some(10),
        ..SystemConfig::default()
    };
    let records = two_cluster_records();

    let (plan, outputs) = plan_and_execute_all(&store, &config, &records, "idx-merge-meta");
    let manifest = merge_worker_outputs(&plan, outputs, default_merge_params()).unwrap();

    assert_eq!(manifest.build_metadata.num_kmeans_iters, 7);
    assert_eq!(manifest.build_metadata.nprobe_default, 5);
    assert_eq!(
        manifest.algorithm.params.get("kmeans_seed"),
        Some(&serde_json::json!(1234))
    );
    assert_eq!(
        manifest.algorithm.params.get("kmeans_sample_size"),
        Some(&serde_json::json!(10))
    );
}
