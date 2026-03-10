//! Merge multiple partial index manifests into a single combined manifest.
//!
//! In a distributed build each worker produces a partial manifest that covers
//! only the shards it was responsible for.  Once all workers have finished,
//! [`merge_manifests`] combines those partial manifests into a single
//! authoritative manifest and writes it to storage.

use chrono::Utc;

use shardlake_core::types::IndexVersion;
use shardlake_manifest::{BuildMetadata, Manifest};
use shardlake_storage::ObjectStore;

use crate::{IndexError, Result};

/// Merge `partials` into a single manifest stored under `output_version`.
///
/// # Requirements
///
/// * All partial manifests must share the same `dataset_version`,
///   `embedding_version`, `dims`, `distance_metric`, `vectors_key`, and
///   `metadata_key`.
/// * Shard IDs across the partials must be unique; if two partials contain the
///   same shard ID the function returns an error.
///
/// # Errors
///
/// Returns [`IndexError::Other`] when the preconditions above are violated or
/// when `partials` is empty.
pub fn merge_manifests(
    partials: Vec<Manifest>,
    output_version: IndexVersion,
    store: &dyn ObjectStore,
) -> Result<Manifest> {
    if partials.is_empty() {
        return Err(IndexError::Other("no partial manifests to merge".into()));
    }

    // Use the first manifest as the reference for shared fields.
    let reference = &partials[0];

    // Validate that all partials are compatible.
    for (idx, m) in partials.iter().enumerate().skip(1) {
        if m.dataset_version != reference.dataset_version {
            return Err(IndexError::Other(format!(
                "manifest {idx}: dataset_version mismatch ({} vs {})",
                m.dataset_version.0, reference.dataset_version.0
            )));
        }
        if m.embedding_version != reference.embedding_version {
            return Err(IndexError::Other(format!(
                "manifest {idx}: embedding_version mismatch ({} vs {})",
                m.embedding_version.0, reference.embedding_version.0
            )));
        }
        if m.dims != reference.dims {
            return Err(IndexError::Other(format!(
                "manifest {idx}: dims mismatch ({} vs {})",
                m.dims, reference.dims
            )));
        }
        if m.distance_metric != reference.distance_metric {
            return Err(IndexError::Other(format!(
                "manifest {idx}: distance_metric mismatch"
            )));
        }
        if m.vectors_key != reference.vectors_key {
            return Err(IndexError::Other(format!(
                "manifest {idx}: vectors_key mismatch"
            )));
        }
        if m.metadata_key != reference.metadata_key {
            return Err(IndexError::Other(format!(
                "manifest {idx}: metadata_key mismatch"
            )));
        }
    }

    // Collect and deduplicate shard definitions.
    let mut all_shards = Vec::new();
    let mut seen_ids = std::collections::HashSet::new();
    for m in &partials {
        for shard in &m.shards {
            if !seen_ids.insert(shard.shard_id.0) {
                return Err(IndexError::Other(format!(
                    "duplicate shard_id {} found in partial manifests",
                    shard.shard_id.0
                )));
            }
            all_shards.push(shard.clone());
        }
    }

    // Sort deterministically by shard_id.
    all_shards.sort_by_key(|s| s.shard_id.0);

    let total_vector_count: u64 = all_shards.iter().map(|s| s.vector_count).sum();

    // Preserve the nprobe_default from the first partial.
    let nprobe_default = reference.build_metadata.nprobe_default;
    let num_kmeans_iters = reference.build_metadata.num_kmeans_iters;

    let merged = Manifest {
        manifest_version: reference.manifest_version,
        dataset_version: reference.dataset_version.clone(),
        embedding_version: reference.embedding_version.clone(),
        index_version: output_version,
        alias: reference.alias.clone(),
        dims: reference.dims,
        distance_metric: reference.distance_metric,
        vectors_key: reference.vectors_key.clone(),
        metadata_key: reference.metadata_key.clone(),
        total_vector_count,
        shards: all_shards,
        build_metadata: BuildMetadata {
            built_at: Utc::now(),
            builder_version: env!("CARGO_PKG_VERSION").into(),
            num_kmeans_iters,
            nprobe_default,
        },
    };

    merged.save(store).map_err(IndexError::Manifest)?;
    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use shardlake_core::{
        config::SystemConfig,
        types::{
            DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId, VectorRecord,
        },
    };
    use shardlake_manifest::ShardDef;
    use shardlake_storage::LocalObjectStore;

    use crate::{BuildParams, IndexBuilder};

    fn make_records(n: usize, dims: usize) -> Vec<VectorRecord> {
        (0..n)
            .map(|i| VectorRecord {
                id: VectorId(i as u64),
                data: (0..dims).map(|d| (i * dims + d) as f32 / 100.0).collect(),
                metadata: None,
            })
            .collect()
    }

    #[test]
    fn test_merge_two_workers() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 4,
            kmeans_iters: 10,
            nprobe: 2,
        };

        let records = make_records(40, 4);
        let builder = IndexBuilder::new(store.as_ref(), &config);

        // Worker 0 builds shards 0 and 2.
        let partial0 = builder
            .build(BuildParams {
                records: records.clone(),
                dataset_version: DatasetVersion("ds-test".into()),
                embedding_version: EmbeddingVersion("emb-test".into()),
                index_version: IndexVersion("idx-test-w0".into()),
                metric: DistanceMetric::Euclidean,
                dims: 4,
                vectors_key: "datasets/ds-test/vectors.jsonl".into(),
                metadata_key: "datasets/ds-test/metadata.json".into(),
                parallel: false,
                worker_id: Some(0),
                num_workers: Some(2),
            })
            .unwrap();

        // Worker 1 builds shards 1 and 3.
        let partial1 = builder
            .build(BuildParams {
                records: records.clone(),
                dataset_version: DatasetVersion("ds-test".into()),
                embedding_version: EmbeddingVersion("emb-test".into()),
                index_version: IndexVersion("idx-test-w1".into()),
                metric: DistanceMetric::Euclidean,
                dims: 4,
                vectors_key: "datasets/ds-test/vectors.jsonl".into(),
                metadata_key: "datasets/ds-test/metadata.json".into(),
                parallel: false,
                worker_id: Some(1),
                num_workers: Some(2),
            })
            .unwrap();

        // Workers own disjoint shards.
        let ids0: Vec<u32> = partial0.shards.iter().map(|s| s.shard_id.0).collect();
        let ids1: Vec<u32> = partial1.shards.iter().map(|s| s.shard_id.0).collect();
        for id in &ids0 {
            assert!(!ids1.contains(id), "shard {id} appears in both partials");
        }

        // Merge.
        let merged = merge_manifests(
            vec![partial0, partial1],
            IndexVersion("idx-test-merged".into()),
            store.as_ref(),
        )
        .unwrap();

        assert_eq!(merged.shards.len(), ids0.len() + ids1.len());
        assert!(merged.total_vector_count > 0);
        let shard_sum: u64 = merged.shards.iter().map(|s| s.vector_count).sum();
        assert_eq!(shard_sum, merged.total_vector_count);
    }

    #[test]
    fn test_merge_rejects_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let result = merge_manifests(vec![], IndexVersion("out".into()), &store);
        assert!(result.is_err());
    }

    #[test]
    fn test_merge_rejects_duplicate_shard_ids() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalObjectStore::new(tmp.path()).unwrap());
        let config = SystemConfig {
            storage_root: tmp.path().to_path_buf(),
            num_shards: 2,
            kmeans_iters: 5,
            nprobe: 1,
        };
        let records = make_records(10, 2);
        let builder = IndexBuilder::new(store.as_ref(), &config);

        // Build the same index twice – shard IDs will overlap.
        let m1 = builder
            .build(BuildParams {
                records: records.clone(),
                dataset_version: DatasetVersion("ds".into()),
                embedding_version: EmbeddingVersion("emb".into()),
                index_version: IndexVersion("idx-dup-a".into()),
                metric: DistanceMetric::Cosine,
                dims: 2,
                vectors_key: "v.jsonl".into(),
                metadata_key: "m.json".into(),
                parallel: false,
                worker_id: None,
                num_workers: None,
            })
            .unwrap();

        let m2 = builder
            .build(BuildParams {
                records,
                dataset_version: DatasetVersion("ds".into()),
                embedding_version: EmbeddingVersion("emb".into()),
                index_version: IndexVersion("idx-dup-b".into()),
                metric: DistanceMetric::Cosine,
                dims: 2,
                vectors_key: "v.jsonl".into(),
                metadata_key: "m.json".into(),
                parallel: false,
                worker_id: None,
                num_workers: None,
            })
            .unwrap();

        // Both manifests contain all shard IDs → merge must fail.
        let result = merge_manifests(
            vec![m1, m2],
            IndexVersion("idx-dup-merged".into()),
            store.as_ref(),
        );
        assert!(result.is_err());
    }

    /// Construct a minimal [`Manifest`] for testing merge field-validation.
    fn minimal_manifest(
        dataset: &str,
        embedding: &str,
        index: &str,
        dims: u32,
        metric: DistanceMetric,
        shard_id: u32,
    ) -> Manifest {
        Manifest {
            manifest_version: 1,
            dataset_version: DatasetVersion(dataset.into()),
            embedding_version: EmbeddingVersion(embedding.into()),
            index_version: IndexVersion(index.into()),
            alias: "latest".into(),
            dims,
            distance_metric: metric,
            vectors_key: "v.jsonl".into(),
            metadata_key: "m.json".into(),
            total_vector_count: 1,
            shards: vec![ShardDef {
                shard_id: shardlake_core::types::ShardId(shard_id),
                artifact_key: format!("indexes/{index}/shards/shard-{shard_id:04}.sidx"),
                vector_count: 1,
                sha256: "abc".into(),
            }],
            build_metadata: BuildMetadata {
                built_at: Utc::now(),
                builder_version: "0.1.0".into(),
                num_kmeans_iters: 10,
                nprobe_default: 2,
            },
        }
    }

    #[test]
    fn test_merge_rejects_dims_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let m1 = minimal_manifest("ds", "emb", "idx-a", 4, DistanceMetric::Cosine, 0);
        let m2 = minimal_manifest("ds", "emb", "idx-b", 8, DistanceMetric::Cosine, 1);
        let result = merge_manifests(vec![m1, m2], IndexVersion("out".into()), &store);
        assert!(result.is_err());
    }

    #[test]
    fn test_merge_rejects_metric_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::new(tmp.path()).unwrap();
        let m1 = minimal_manifest("ds", "emb", "idx-a", 4, DistanceMetric::Cosine, 0);
        let m2 = minimal_manifest("ds", "emb", "idx-b", 4, DistanceMetric::Euclidean, 1);
        let result = merge_manifests(vec![m1, m2], IndexVersion("out".into()), &store);
        assert!(result.is_err());
    }
}
