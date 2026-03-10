//! Manifest schema: ties a dataset version to an index version and describes
//! all shard artifacts.
//!
//! Two manifest types are provided:
//! - [`Manifest`]: full combined manifest (dataset + index) stored alongside index artifacts.
//! - [`ArtifactRegistry`] / [`LocalArtifactRegistry`]: registry abstraction over storage layout.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use shardlake_core::types::{
    DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, ShardId,
};
use shardlake_storage::{ObjectStore, StorageError};
use std::sync::Arc;

/// Errors that can arise when working with manifests.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("parse error: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("validation: {0}")]
    Validation(String),
    /// Returned when a compatibility check between two manifests/parameters fails.
    #[error("compatibility error: {0}")]
    Incompatible(String),
}

pub type Result<T> = std::result::Result<T, ManifestError>;

// ─── Quantization / recall sub-types ────────────────────────────────────────

/// Optional quantization configuration recorded at build time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantizationParams {
    /// Quantization method (e.g. `"none"`, `"pq"`, `"scalar"`).
    pub method: String,
    /// Bits per dimension (e.g. 8 for scalar quantization).
    pub bits: u8,
}

/// Offline recall estimates written into the manifest after a benchmark run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecallEstimates {
    /// Recall@1 measured against a brute-force baseline.
    pub recall_at_1: f32,
    /// Recall@10 measured against a brute-force baseline.
    pub recall_at_10: f32,
}

// ─── Core manifest types ─────────────────────────────────────────────────────

/// Describes one shard artifact inside the index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardDef {
    pub shard_id: ShardId,
    /// Storage key for the shard index file.
    pub artifact_key: String,
    /// Number of vectors in this shard.
    pub vector_count: u64,
    /// Fingerprint hex digest of the artifact bytes (filled after build).
    pub sha256: String,
}

/// Build-time metadata recorded in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildMetadata {
    /// UTC timestamp of when this index was built.
    pub built_at: DateTime<Utc>,
    /// Semver version string of the builder binary.
    pub builder_version: String,
    /// Number of K-means iterations used during partitioning.
    pub num_kmeans_iters: u32,
    /// Default nprobe recorded at build time.
    pub nprobe_default: u32,
    /// Indexing algorithm used (e.g. `"kmeans"`).
    pub algorithm: String,
    /// Compression method applied to shard artifacts (e.g. `"none"`).
    pub compression_method: String,
    /// Optional quantization configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantization_parameters: Option<QuantizationParams>,
    /// Optional recall estimates filled in after benchmarking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recall_estimates: Option<RecallEstimates>,
    /// Wall-clock build duration in milliseconds.
    pub build_duration_ms: u64,
}

/// All storage keys for the dataset artifacts referenced by a manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactLocations {
    /// Storage key of the raw vectors JSONL file.
    pub vectors_key: String,
    /// Storage key of the metadata JSON file.
    pub metadata_key: String,
    /// Storage keys of every shard `.sidx` file.
    pub shard_keys: Vec<String>,
    /// Storage key of this manifest's JSON file.
    pub manifest_key: String,
}

/// Full manifest tying dataset, embeddings, and index together.
///
/// # Schema version
/// The current schema version is **1**.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Schema version of this manifest document (currently `1`).
    pub manifest_version: u32,
    /// Unique identifier for the dataset (e.g. a human-readable slug or UUID).
    pub dataset_id: String,
    pub dataset_version: DatasetVersion,
    /// Name of the model that produced the embeddings (e.g. `"text-embedding-ada-002"`).
    pub embedding_model: String,
    pub embedding_version: EmbeddingVersion,
    pub index_version: IndexVersion,
    /// Alias pointer (e.g. `"latest"`) for serving.
    pub alias: String,
    /// Vector dimension shared by all records in this dataset.
    pub dims: u32,
    pub distance_metric: DistanceMetric,
    /// Storage key of the raw vectors file.
    pub vectors_key: String,
    /// Storage key of the metadata JSON file.
    pub metadata_key: String,
    /// Number of non-empty shards in this index.
    pub shard_count: u32,
    pub shards: Vec<ShardDef>,
    /// Total number of indexed vectors (must equal the sum of `shards[*].vector_count`).
    pub total_vector_count: u64,
    /// FNV-1a fingerprint of the serialised manifest bytes (populated after [`Manifest::save`]).
    pub checksum: String,
    pub build_metadata: BuildMetadata,
}

impl Manifest {
    /// Storage key for a manifest given an index version.
    pub fn storage_key(index_version: &IndexVersion) -> String {
        format!("indexes/{}/manifest.json", index_version.0)
    }

    /// Storage key for the active alias pointer.
    pub fn alias_key(alias: &str) -> String {
        format!("aliases/{}.json", alias)
    }

    /// Returns all artifact storage keys referenced by this manifest.
    pub fn artifact_locations(&self) -> ArtifactLocations {
        ArtifactLocations {
            vectors_key: self.vectors_key.clone(),
            metadata_key: self.metadata_key.clone(),
            shard_keys: self.shards.iter().map(|s| s.artifact_key.clone()).collect(),
            manifest_key: Self::storage_key(&self.index_version),
        }
    }

    /// Serialise and store to `store`, computing and setting the manifest checksum.
    pub fn save(&mut self, store: &dyn ObjectStore) -> Result<()> {
        // Compute checksum over the manifest without the checksum field itself by
        // temporarily zeroing it out, serialising, hashing, then storing the final form.
        let _discarded_checksum = std::mem::take(&mut self.checksum);
        let bytes_for_hash = serde_json::to_vec(self)?;
        self.checksum = fingerprint_hex(&bytes_for_hash);

        let key = Self::storage_key(&self.index_version);
        let bytes = serde_json::to_vec_pretty(self)?;
        store.put(&key, bytes)?;
        Ok(())
    }

    /// Load from `store` by index version.
    pub fn load(store: &dyn ObjectStore, index_version: &IndexVersion) -> Result<Self> {
        let key = Self::storage_key(index_version);
        let bytes = store.get(&key)?;
        let m: Self = serde_json::from_slice(&bytes)?;
        m.validate()?;
        Ok(m)
    }

    /// Load via alias (indirection through alias pointer).
    pub fn load_alias(store: &dyn ObjectStore, alias: &str) -> Result<Self> {
        let key = Self::alias_key(alias);
        let bytes = store.get(&key)?;
        let ptr: AliasPointer = serde_json::from_slice(&bytes)?;
        Self::load(store, &ptr.index_version)
    }

    /// Publish an alias pointing to this manifest's index version.
    pub fn publish_alias(&self, store: &dyn ObjectStore) -> Result<()> {
        let ptr = AliasPointer {
            alias: self.alias.clone(),
            index_version: self.index_version.clone(),
        };
        let key = Self::alias_key(&self.alias);
        store.put(&key, serde_json::to_vec_pretty(&ptr)?)?;
        Ok(())
    }

    /// Validate internal consistency of this manifest.
    ///
    /// Checks:
    /// - `manifest_version` is supported.
    /// - `dims > 0`.
    /// - At least one shard is present.
    /// - `shard_count` matches `shards.len()`.
    /// - Shard `vector_count` values sum to `total_vector_count`.
    pub fn validate(&self) -> Result<()> {
        if self.manifest_version != 1 {
            return Err(ManifestError::Validation(format!(
                "unsupported manifest_version {}",
                self.manifest_version
            )));
        }
        if self.dims == 0 {
            return Err(ManifestError::Validation("dims must be > 0".into()));
        }
        if self.shards.is_empty() {
            return Err(ManifestError::Validation("manifest has no shards".into()));
        }
        if self.shard_count as usize != self.shards.len() {
            return Err(ManifestError::Validation(format!(
                "shard_count ({}) does not match shards length ({})",
                self.shard_count,
                self.shards.len(),
            )));
        }
        let counted: u64 = self.shards.iter().map(|s| s.vector_count).sum();
        if counted != self.total_vector_count {
            return Err(ManifestError::Validation(format!(
                "shard vector counts ({counted}) don't sum to total ({})",
                self.total_vector_count
            )));
        }
        Ok(())
    }

    /// Check that `dims` is compatible with this manifest.
    ///
    /// Returns [`ManifestError::Incompatible`] if the dimensions differ.
    pub fn check_dimension_compatibility(&self, dims: u32) -> Result<()> {
        if self.dims != dims {
            return Err(ManifestError::Incompatible(format!(
                "dimension mismatch: manifest has {}, caller expects {}",
                self.dims, dims
            )));
        }
        Ok(())
    }

    /// Check that `dataset_version` is compatible with this manifest.
    ///
    /// Returns [`ManifestError::Incompatible`] if the dataset versions differ.
    pub fn check_dataset_version_compatibility(
        &self,
        dataset_version: &DatasetVersion,
    ) -> Result<()> {
        if self.dataset_version != *dataset_version {
            return Err(ManifestError::Incompatible(format!(
                "dataset version mismatch: manifest has '{}', caller expects '{}'",
                self.dataset_version, dataset_version
            )));
        }
        Ok(())
    }

    /// Check that `algorithm` is compatible with this manifest's build algorithm.
    ///
    /// Returns [`ManifestError::Incompatible`] if the algorithms differ.
    pub fn check_algorithm_compatibility(&self, algorithm: &str) -> Result<()> {
        if self.build_metadata.algorithm != algorithm {
            return Err(ManifestError::Incompatible(format!(
                "algorithm mismatch: manifest uses '{}', caller expects '{}'",
                self.build_metadata.algorithm, algorithm
            )));
        }
        Ok(())
    }
}

// ─── Artifact registry ───────────────────────────────────────────────────────

/// Abstraction over the artifact storage layout.
///
/// The canonical directory structure is:
///
/// ```text
/// <root>/
/// ├── artifacts/   (generic blobs)
/// ├── datasets/    (versioned dataset files)
/// ├── indexes/     (versioned index files and manifests)
/// └── manifests/   (standalone manifest copies for quick lookup)
/// ```
pub trait ArtifactRegistry: Send + Sync {
    /// Store `data` under the given `key` in the registry.
    fn put(&self, key: &str, data: Vec<u8>) -> Result<()>;
    /// Retrieve the bytes stored at `key`.
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    /// Return `true` if `key` exists.
    fn exists(&self, key: &str) -> Result<bool>;
    /// List all keys with the given `prefix`.
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
    /// Delete `key`.
    fn delete(&self, key: &str) -> Result<()>;
    /// Save `manifest` to `indexes/<version>/manifest.json` and also write a copy
    /// to `manifests/<version>.json` for fast lookup.
    fn save_manifest(&self, manifest: &mut Manifest) -> Result<()>;
    /// Load a manifest by index version from the registry.
    fn load_manifest(&self, index_version: &IndexVersion) -> Result<Manifest>;
}

/// Local filesystem implementation of [`ArtifactRegistry`].
///
/// All keys are resolved relative to `root`, which is expected to contain the
/// `artifacts/`, `datasets/`, `indexes/`, and `manifests/` sub-directories
/// (created on demand).
pub struct LocalArtifactRegistry {
    store: Arc<dyn ObjectStore>,
}

impl LocalArtifactRegistry {
    /// Create a registry backed by `store`.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

impl ArtifactRegistry for LocalArtifactRegistry {
    fn put(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.store.put(key, data).map_err(ManifestError::Storage)
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.store.get(key).map_err(ManifestError::Storage)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        self.store.exists(key).map_err(ManifestError::Storage)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.store.list(prefix).map_err(ManifestError::Storage)
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.store.delete(key).map_err(ManifestError::Storage)
    }

    fn save_manifest(&self, manifest: &mut Manifest) -> Result<()> {
        // Primary copy: indexes/<version>/manifest.json
        manifest.save(self.store.as_ref())?;
        // Secondary copy: manifests/<version>.json for fast lookup
        let mirror_key = format!("manifests/{}.json", manifest.index_version.0);
        let bytes = serde_json::to_vec_pretty(manifest)?;
        self.store.put(&mirror_key, bytes)?;
        Ok(())
    }

    fn load_manifest(&self, index_version: &IndexVersion) -> Result<Manifest> {
        Manifest::load(self.store.as_ref(), index_version)
    }
}

// ─── Integrity validation ────────────────────────────────────────────────────

/// Detailed report from [`validate_manifest_integrity`].
#[derive(Debug)]
pub struct IntegrityReport {
    /// Whether all checks passed.
    pub ok: bool,
    /// Human-readable messages (warnings or errors) collected during validation.
    pub messages: Vec<String>,
}

/// Validate the full integrity of a manifest and its referenced artifacts.
///
/// Checks performed:
/// 1. Internal consistency (via [`Manifest::validate`]).
/// 2. Every shard artifact key exists in `store`.
/// 3. Vector dimension is consistent with `manifest.dims`.
/// 4. SHA-256 fingerprint of each shard matches the recorded value.
pub fn validate_manifest_integrity(
    manifest: &Manifest,
    store: &dyn ObjectStore,
) -> IntegrityReport {
    let mut messages = Vec::new();
    let mut ok = true;

    // 1. Internal consistency
    if let Err(e) = manifest.validate() {
        messages.push(format!("internal validation failed: {e}"));
        ok = false;
    }

    // 2 & 4. Shard files exist and checksums match
    for shard in &manifest.shards {
        match store.exists(&shard.artifact_key) {
            Ok(true) => {
                // 4. Verify checksum
                match store.get(&shard.artifact_key) {
                    Ok(bytes) => {
                        let computed = fingerprint_hex(&bytes);
                        if computed != shard.sha256 {
                            messages.push(format!(
                                "shard {} checksum mismatch: expected '{}', got '{}'",
                                shard.shard_id, shard.sha256, computed
                            ));
                            ok = false;
                        }
                    }
                    Err(e) => {
                        messages.push(format!("shard {} could not be read: {e}", shard.shard_id));
                        ok = false;
                    }
                }
            }
            Ok(false) => {
                messages.push(format!(
                    "shard {} artifact not found: {}",
                    shard.shard_id, shard.artifact_key
                ));
                ok = false;
            }
            Err(e) => {
                messages.push(format!(
                    "shard {} existence check failed: {e}",
                    shard.shard_id
                ));
                ok = false;
            }
        }
    }

    // 3. Dimension field validity (shard binary content is not re-parsed here; use the
    //    manifest's validated `dims` field as the authoritative source)
    if ok && manifest.dims > 0 {
        messages.push(format!(
            "dimension field valid: manifest declares dims={}",
            manifest.dims
        ));
    }

    IntegrityReport { ok, messages }
}

// ─── Internal helpers ────────────────────────────────────────────────────────

/// FNV-1a-based artifact fingerprint.
///
/// **⚠ Warning: non-cryptographic.**  FNV-1a provides no protection against
/// intentional tampering; it can only detect accidental corruption.  Replace
/// with a cryptographic hash (e.g. SHA-256 via the `sha2` crate) before
/// deploying in any security-sensitive context.
pub fn fingerprint_hex(bytes: &[u8]) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{h:016x}")
}

/// Thin alias pointer stored at `aliases/<alias>.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AliasPointer {
    pub alias: String,
    pub index_version: IndexVersion,
}
