//! Manifest integrity validation engine.
//!
//! Provides a reusable validation layer that checks dataset and index manifests
//! against their stored artifacts.  All checks are pure library logic with no
//! dependency on the CLI so they can be called from tests, tooling, or a future
//! HTTP admin endpoint.
//!
//! # Usage
//!
//! ```rust,ignore
//! use shardlake_index::validator::{validate_index, validate_dataset};
//!
//! let report = validate_index(&manifest, store.as_ref());
//! if !report.is_valid() {
//!     for failure in &report.failures {
//!         eprintln!("validation failure: {failure}");
//!     }
//! }
//! ```

use shardlake_core::types::ShardId;
use shardlake_manifest::{DatasetManifest, Manifest};
use shardlake_storage::{ObjectStore, StorageError};

use crate::{artifact_fingerprint, shard::{PqShard, ShardIndex}, PQ8_CODEC};

/// A single structured validation failure returned by the integrity engine.
///
/// Each variant describes a distinct category of problem so callers can filter,
/// count, or format failures independently.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValidationFailure {
    /// The manifest document itself is structurally or semantically invalid.
    #[error("manifest invalid: {0}")]
    ManifestInvalid(String),

    /// A referenced artifact key is not present in storage.
    #[error("artifact missing: {key}")]
    ArtifactMissing { key: String },

    /// A shard artifact's fingerprint does not match the value recorded in the
    /// manifest.
    #[error("fingerprint mismatch for {key}: expected {expected}, actual {actual}")]
    FingerprintMismatch {
        key: String,
        expected: String,
        actual: String,
    },

    /// A shard artifact's embedded vector dimension is inconsistent with the
    /// dimension declared in the manifest.
    #[error("shard {shard_id} dimension mismatch: expected {expected}, actual {actual}")]
    ShardDimensionMismatch {
        shard_id: ShardId,
        expected: u32,
        actual: u32,
    },

    /// A shard artifact's embedded vector count differs from the count recorded
    /// in the manifest's shard definition.
    #[error("shard {shard_id} vector count mismatch: expected {expected}, actual {actual}")]
    ShardVectorCountMismatch {
        shard_id: ShardId,
        expected: u64,
        actual: u64,
    },

    /// The shard centroid embedded in the manifest does not match the shard
    /// binary centroid used for routing.
    #[error(
        "shard {shard_id} centroid mismatch: expected dims {expected_dims}, actual dims {actual_dims} ({reason})"
    )]
    ShardCentroidMismatch {
        shard_id: ShardId,
        expected_dims: usize,
        actual_dims: usize,
        reason: String,
    },

    /// A storage access error prevented a key from being checked.
    #[error("storage error for {key}: {message}")]
    StorageError { key: String, message: String },

    /// The shard artifact bytes could not be parsed as a valid shard binary.
    #[error("shard binary invalid for {key}: {message}")]
    ShardParseError { key: String, message: String },
}

// ── ValidationReport ──────────────────────────────────────────────────────────

/// Summary of a validation run.
///
/// An empty `failures` list means the manifest and all referenced artifacts are
/// internally consistent.  A non-empty list collects *all* detected problems so
/// callers receive a complete picture rather than stopping at the first failure.
#[derive(Debug, Default)]
pub struct ValidationReport {
    /// All failures detected during the validation run.
    pub failures: Vec<ValidationFailure>,
}

impl ValidationReport {
    /// Returns `true` when no failures were detected.
    pub fn is_valid(&self) -> bool {
        self.failures.is_empty()
    }

    /// Convert into a `Result`.
    ///
    /// Returns `Ok(())` when the report is valid, or `Err(failures)` otherwise.
    pub fn into_result(self) -> Result<(), Vec<ValidationFailure>> {
        if self.failures.is_empty() {
            Ok(())
        } else {
            Err(self.failures)
        }
    }
}

// ── validate_index ────────────────────────────────────────────────────────────

/// Validate an index manifest and all its referenced artifacts in `store`.
///
/// The following checks are performed in order.  All failures are collected
/// rather than halting at the first problem.
///
/// 1. **Structural validation** — calls [`Manifest::validate()`] to verify
///    internal consistency (shard count sums, fingerprint presence, etc.).
/// 2. **Artifact existence** — verifies that `vectors_key` and `metadata_key`
///    exist in `store`.
/// 3. **Per-shard checks** (for each entry in `manifest.shards`):
///    - the shard artifact key exists in `store`;
///    - the artifact's FNV-1a fingerprint matches the `fingerprint` field in the
///      shard definition;
///    - the shard binary's embedded `dims` matches `manifest.dims`;
///    - the shard binary's embedded vector count matches `shard.vector_count`;
///    - when `shard.centroid` is present, it matches the shard binary centroid.
///
/// Returns a [`ValidationReport`] whose [`ValidationReport::is_valid`] method
/// returns `true` only when all checks pass.
pub fn validate_index(manifest: &Manifest, store: &dyn ObjectStore) -> ValidationReport {
    let mut report = ValidationReport::default();

    // 1. Structural validation.
    if let Err(e) = manifest.validate() {
        report
            .failures
            .push(ValidationFailure::ManifestInvalid(e.to_string()));
        // Do not bail early: continue to check artifacts so the caller gets a
        // complete failure list.
    }

    // 2. Verify that the dataset artifacts referenced by the manifest exist.
    for key in [
        manifest.vectors_key.as_str(),
        manifest.metadata_key.as_str(),
    ] {
        check_exists(key, &mut report, store);
    }

    // 2b. For PQ indexes, verify that the codebook artifact exists.
    if manifest.compression.enabled && manifest.compression.codec == PQ8_CODEC {
        if let Some(cb_key) = &manifest.compression.codebook_key {
            check_exists(cb_key, &mut report, store);
        } else {
            report
                .failures
                .push(ValidationFailure::ManifestInvalid(
                    "compression.codec is \"pq8\" but codebook_key is absent".into(),
                ));
        }
    }

    // 3. Per-shard checks.
    let pq_encoded = manifest.compression.enabled && manifest.compression.codec == PQ8_CODEC;

    for shard in &manifest.shards {
        let key = shard.artifact_key.as_str();

        match store.get(key) {
            Err(StorageError::NotFound(_)) => {
                report.failures.push(ValidationFailure::ArtifactMissing {
                    key: key.to_owned(),
                });
                // Can't perform fingerprint or dimension checks without bytes.
            }
            Err(e) => {
                report.failures.push(ValidationFailure::StorageError {
                    key: key.to_owned(),
                    message: e.to_string(),
                });
            }
            Ok(bytes) => {
                // Fingerprint check.
                let actual_fp = artifact_fingerprint(&bytes);
                if actual_fp != shard.fingerprint {
                    report
                        .failures
                        .push(ValidationFailure::FingerprintMismatch {
                            key: key.to_owned(),
                            expected: shard.fingerprint.clone(),
                            actual: actual_fp,
                        });
                }

                if pq_encoded {
                    check_pq_shard(&bytes, shard, manifest, &mut report, key);
                } else {
                    check_raw_shard(&bytes, shard, manifest, &mut report, key);
                }
            }
        }
    }

    report
}

// ── per-shard integrity helpers ───────────────────────────────────────────────

/// Validate a format-version-1 (raw vector) shard artifact.
fn check_raw_shard(
    bytes: &[u8],
    shard: &shardlake_manifest::ShardDef,
    manifest: &Manifest,
    report: &mut ValidationReport,
    key: &str,
) {
    match ShardIndex::from_bytes(bytes) {
        Err(e) => {
            report.failures.push(ValidationFailure::ShardParseError {
                key: key.to_owned(),
                message: e.to_string(),
            });
        }
        Ok(shard_index) => {
            let actual_dims = shard_index.dims as u32;
            if actual_dims != manifest.dims {
                report
                    .failures
                    .push(ValidationFailure::ShardDimensionMismatch {
                        shard_id: shard.shard_id,
                        expected: manifest.dims,
                        actual: actual_dims,
                    });
            }

            let actual_count = shard_index.records.len() as u64;
            if actual_count != shard.vector_count {
                report
                    .failures
                    .push(ValidationFailure::ShardVectorCountMismatch {
                        shard_id: shard.shard_id,
                        expected: shard.vector_count,
                        actual: actual_count,
                    });
            }

            check_centroid_match(shard, &shard_index.centroids, report);
        }
    }
}

/// Validate a format-version-2 (PQ-encoded) shard artifact.
fn check_pq_shard(
    bytes: &[u8],
    shard: &shardlake_manifest::ShardDef,
    manifest: &Manifest,
    report: &mut ValidationReport,
    key: &str,
) {
    match PqShard::from_bytes(bytes) {
        Err(e) => {
            report.failures.push(ValidationFailure::ShardParseError {
                key: key.to_owned(),
                message: e.to_string(),
            });
        }
        Ok(pq_shard) => {
            let actual_dims = pq_shard.dims as u32;
            if actual_dims != manifest.dims {
                report
                    .failures
                    .push(ValidationFailure::ShardDimensionMismatch {
                        shard_id: shard.shard_id,
                        expected: manifest.dims,
                        actual: actual_dims,
                    });
            }

            let actual_count = pq_shard.entries.len() as u64;
            if actual_count != shard.vector_count {
                report
                    .failures
                    .push(ValidationFailure::ShardVectorCountMismatch {
                        shard_id: shard.shard_id,
                        expected: shard.vector_count,
                        actual: actual_count,
                    });
            }

            check_centroid_match(shard, &pq_shard.centroids, report);
        }
    }
}

/// Check that the shard's centroid in the manifest matches the binary artifact.
fn check_centroid_match(
    shard: &shardlake_manifest::ShardDef,
    artifact_centroids: &[Vec<f32>],
    report: &mut ValidationReport,
) {
    if shard.centroid.is_empty() {
        return;
    }
    let actual = artifact_centroids.first();
    let (actual_dims, reason) = match actual {
        None => (0, "shard has no centroids".to_string()),
        Some(centroid) if centroid.len() != shard.centroid.len() => (
            centroid.len(),
            "centroid dimensionality mismatch".to_string(),
        ),
        Some(centroid)
            if !centroid
                .iter()
                .zip(shard.centroid.iter())
                .all(|(a, b)| a.to_bits() == b.to_bits()) =>
        {
            (centroid.len(), "centroid values differ".to_string())
        }
        Some(_) => (shard.centroid.len(), String::new()),
    };
    if !reason.is_empty() {
        report
            .failures
            .push(ValidationFailure::ShardCentroidMismatch {
                shard_id: shard.shard_id,
                expected_dims: shard.centroid.len(),
                actual_dims,
                reason,
            });
    }
}

// ── validate_dataset ──────────────────────────────────────────────────────────

/// Validate a dataset manifest and all its referenced artifacts in `store`.
///
/// The following checks are performed:
///
/// 1. **Structural validation** — calls [`DatasetManifest::validate()`] to
///    check field invariants (dims > 0, non-empty keys, etc.).
/// 2. **Artifact existence** — verifies that `vectors_key` and `metadata_key`
///    exist in `store`.
///
/// Returns a [`ValidationReport`] whose [`ValidationReport::is_valid`] method
/// returns `true` only when all checks pass.
pub fn validate_dataset(manifest: &DatasetManifest, store: &dyn ObjectStore) -> ValidationReport {
    let mut report = ValidationReport::default();

    // 1. Structural validation.
    if let Err(e) = manifest.validate() {
        report
            .failures
            .push(ValidationFailure::ManifestInvalid(e.to_string()));
    }

    // 2. Verify that the artifact keys referenced by the manifest exist.
    for key in [
        manifest.vectors_key.as_str(),
        manifest.metadata_key.as_str(),
    ] {
        check_exists(key, &mut report, store);
    }

    report
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Append an [`ArtifactMissing`] or [`StorageError`] failure if `key` is not
/// present in `store`.
fn check_exists(key: &str, report: &mut ValidationReport, store: &dyn ObjectStore) {
    match store.exists(key) {
        Ok(true) => {}
        Ok(false) => {
            report.failures.push(ValidationFailure::ArtifactMissing {
                key: key.to_owned(),
            });
        }
        Err(e) => {
            report.failures.push(ValidationFailure::StorageError {
                key: key.to_owned(),
                message: e.to_string(),
            });
        }
    }
}
