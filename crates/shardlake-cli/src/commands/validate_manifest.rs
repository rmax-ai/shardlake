//! `shardlake validate-manifest` – check manifest integrity against stored artifacts.
//!
//! At least one of `--index-version` or `--dataset-version` must be supplied.
//! Both may be supplied simultaneously to validate index and dataset manifests
//! in a single invocation.
//!
//! The command exits with a non-zero status code when any validation failure is
//! detected, making it suitable for use in CI pipelines.

use std::path::PathBuf;

use anyhow::{bail, Result};
use clap::Parser;
use tracing::info;

use shardlake_core::types::{DatasetVersion, IndexVersion};
use shardlake_index::validator::{validate_dataset, validate_index};
use shardlake_manifest::{DatasetManifest, Manifest};
use shardlake_storage::LocalObjectStore;

/// Arguments for the `validate-manifest` subcommand.
#[derive(Parser, Debug)]
pub struct ValidateManifestArgs {
    /// Index version to validate (validates the index manifest and all shard artifacts).
    #[arg(long)]
    pub index_version: Option<String>,

    /// Dataset version to validate (validates the dataset manifest and its artifacts).
    #[arg(long)]
    pub dataset_version: Option<String>,
}

/// Entry-point called by `main`.
///
/// Loads each requested manifest, runs the integrity checks supplied by
/// [`validate_index`] / [`validate_dataset`], prints every failure to stderr,
/// and returns an error (causing a non-zero exit code) if any failure was found.
pub async fn run(storage: PathBuf, args: ValidateManifestArgs) -> Result<()> {
    if args.index_version.is_none() && args.dataset_version.is_none() {
        bail!("at least one of --index-version or --dataset-version must be provided");
    }

    let store = LocalObjectStore::new(&storage)?;
    let mut total_failures = 0usize;

    // ── Index manifest validation ──────────────────────────────────────────────
    if let Some(ref iv) = args.index_version {
        let index_ver = IndexVersion(iv.clone());
        info!(index_version = %iv, "Validating index manifest");

        let manifest = Manifest::load(&store, &index_ver)?;
        let report = validate_index(&manifest, &store);

        if report.is_valid() {
            println!("index manifest '{iv}': OK");
        } else {
            eprintln!(
                "index manifest '{iv}': {} failure(s)",
                report.failures.len()
            );
            for failure in &report.failures {
                eprintln!("  - {failure}");
            }
            total_failures += report.failures.len();
        }
    }

    // ── Dataset manifest validation ────────────────────────────────────────────
    if let Some(ref dv) = args.dataset_version {
        let dataset_ver = DatasetVersion(dv.clone());
        info!(dataset_version = %dv, "Validating dataset manifest");

        let manifest = DatasetManifest::load(&store, &dataset_ver)?;
        let report = validate_dataset(&manifest, &store);

        if report.is_valid() {
            println!("dataset manifest '{dv}': OK");
        } else {
            eprintln!(
                "dataset manifest '{dv}': {} failure(s)",
                report.failures.len()
            );
            for failure in &report.failures {
                eprintln!("  - {failure}");
            }
            total_failures += report.failures.len();
        }
    }

    if total_failures > 0 {
        bail!("validation failed with {total_failures} failure(s)");
    }

    Ok(())
}

// ── tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use shardlake_core::types::{DatasetVersion, EmbeddingVersion, IndexVersion};
    use shardlake_index::{BuildParams, IndexBuilder};
    use shardlake_manifest::{DatasetManifest, DATASET_MANIFEST_VERSION};
    use shardlake_storage::{paths, LocalObjectStore, ObjectStore};
    use tempfile::tempdir;

    use super::*;

    // ── helpers ────────────────────────────────────────────────────────────────

    /// Write a minimal but valid `DatasetManifest` and its artifact stubs.
    fn write_dataset(store: &LocalObjectStore, dataset_ver: &str) -> DatasetManifest {
        let vectors_key = paths::dataset_vectors_key(dataset_ver);
        let metadata_key = paths::dataset_metadata_key(dataset_ver);
        store
            .put(
                &vectors_key,
                br#"{"id":1,"data":[1.0,0.0],"metadata":null}"#.to_vec(),
            )
            .unwrap();
        store.put(&metadata_key, br#"{}"#.to_vec()).unwrap();

        let dv = DatasetVersion(dataset_ver.to_string());
        let manifest = DatasetManifest {
            manifest_version: DATASET_MANIFEST_VERSION,
            dataset_version: dv.clone(),
            embedding_version: EmbeddingVersion("emb-v1".into()),
            dims: 2,
            vector_count: 1,
            vectors_key,
            metadata_key,
            ingest_metadata: None,
        };
        manifest.save(store).unwrap();
        manifest
    }

    // ── unit tests ─────────────────────────────────────────────────────────────

    #[test]
    fn run_errors_when_no_versions_provided() {
        let tmp = tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = rt
            .block_on(run(
                tmp.path().join("storage"),
                ValidateManifestArgs {
                    index_version: None,
                    dataset_version: None,
                },
            ))
            .unwrap_err();
        assert!(err.to_string().contains("at least one of"));
    }

    #[tokio::test]
    async fn validates_dataset_manifest_success() {
        let tmp = tempdir().unwrap();
        let storage = tmp.path().join("storage");
        let store = LocalObjectStore::new(&storage).unwrap();
        write_dataset(&store, "ds-ok");

        run(
            storage,
            ValidateManifestArgs {
                index_version: None,
                dataset_version: Some("ds-ok".into()),
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn validates_dataset_manifest_missing_artifacts() {
        let tmp = tempdir().unwrap();
        let storage = tmp.path().join("storage");
        let store = LocalObjectStore::new(&storage).unwrap();

        // Save manifest with keys that point to non-existent artifacts.
        let dv = DatasetVersion("ds-broken".into());
        let manifest = DatasetManifest {
            manifest_version: DATASET_MANIFEST_VERSION,
            dataset_version: dv.clone(),
            embedding_version: EmbeddingVersion("emb-v1".into()),
            dims: 2,
            // vector_count must be > 0 to pass structural validation; the
            // missing artifact files are what we want the validator to catch.
            vector_count: 1,
            vectors_key: paths::dataset_vectors_key("ds-broken"),
            metadata_key: paths::dataset_metadata_key("ds-broken"),
            ingest_metadata: None,
        };
        manifest.save(&store).unwrap();
        // Deliberately do NOT write the artifact files.

        let err = run(
            storage,
            ValidateManifestArgs {
                index_version: None,
                dataset_version: Some("ds-broken".into()),
            },
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string().contains("validation failed"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn validates_index_manifest_missing_returns_error() {
        let tmp = tempdir().unwrap();
        let storage = tmp.path().join("storage");

        // No manifest written → load fails → should propagate as an anyhow error.
        let err = run(
            storage,
            ValidateManifestArgs {
                index_version: Some("idx-missing".into()),
                dataset_version: None,
            },
        )
        .await
        .unwrap_err();

        // The error should mention a storage / not-found problem, not a validation failure.
        let msg = err.to_string();
        assert!(
            msg.contains("not found") || msg.contains("storage") || msg.contains("No such"),
            "unexpected error: {msg}"
        );
    }

    #[tokio::test]
    async fn validates_both_manifests_together() {
        let tmp = tempdir().unwrap();
        let storage = tmp.path().join("storage");
        let store = LocalObjectStore::new(&storage).unwrap();
        write_dataset(&store, "ds-combo");

        // Build a real index so the shard artifacts actually exist on disk.
        let index_ver = IndexVersion("idx-combo".into());
        let vr = shardlake_core::types::VectorRecord {
            id: shardlake_core::types::VectorId(1),
            data: vec![1.0_f32, 0.0_f32],
            metadata: None,
        };
        let config = shardlake_core::config::SystemConfig {
            storage_root: storage.clone(),
            num_shards: 1,
            kmeans_iters: 2,
            nprobe: 1,
            kmeans_seed: shardlake_core::config::DEFAULT_KMEANS_SEED,
        };
        let builder = IndexBuilder::new(&store, &config);
        builder
            .build(BuildParams {
                records: vec![vr],
                dataset_version: DatasetVersion("ds-combo".into()),
                embedding_version: EmbeddingVersion("emb-v1".into()),
                index_version: index_ver.clone(),
                metric: shardlake_core::types::DistanceMetric::Cosine,
                dims: 2,
                vectors_key: paths::dataset_vectors_key("ds-combo"),
                metadata_key: paths::dataset_metadata_key("ds-combo"),
            })
            .unwrap();

        run(
            storage,
            ValidateManifestArgs {
                index_version: Some("idx-combo".into()),
                dataset_version: Some("ds-combo".into()),
            },
        )
        .await
        .unwrap();
    }
}
