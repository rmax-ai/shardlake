//! Manifest schema: ties a dataset version to an index version and describes
//! all shard artifacts.  Also provides the versioned dataset manifest written
//! by `shardlake ingest` and consumed by `shardlake build-index`.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use shardlake_core::types::{
    DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, ShardId,
};
use shardlake_storage::{ObjectStore, StorageError};

/// Errors that can arise when working with manifests.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("parse error: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("validation: {0}")]
    Validation(String),
    /// Returned by the `check_*_compat` helpers when the manifest and the
    /// caller disagree on a key parameter (dimension, dataset version, or
    /// algorithm).
    #[error("compatibility: {0}")]
    Compatibility(String),
}

pub type Result<T> = std::result::Result<T, ManifestError>;

/// Routing metadata for partition-aware query routing (manifest v4+).
///
/// Persisted in each [`ShardDef`] so that the serving path can route queries
/// to the correct shard without loading any shard body.  `None` in manifests
/// produced by older builders (manifest_version ≤ 3); callers should fall
/// back to deriving routing information from [`ShardDef::artifact_key`] and
/// [`ShardDef::centroid`] in that case.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoutingMetadata {
    /// Stable identifier for this shard's centroid, used as a routing key.
    ///
    /// Populated by the builder as `"shard-NNNN"` (zero-padded 4-digit shard
    /// number), matching the shard artifact filename.
    pub centroid_id: String,
    /// ANN index algorithm within this shard (e.g. `"flat"`).
    ///
    /// Consumed by the serving path to select the correct search method when
    /// loading this shard.  Always `"flat"` (linear scan) in the current
    /// prototype.
    pub index_type: String,
    /// Canonical location to load this shard when routing a query.
    ///
    /// Equals [`ShardDef::artifact_key`] for local storage backends.
    /// Stored separately so that multi-storage deployments can record a
    /// resolved URL or filesystem path without changing the opaque storage key.
    pub file_location: String,
}

/// Describes one shard artifact inside the index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardDef {
    pub shard_id: ShardId,
    /// Storage key for the shard index file.
    pub artifact_key: String,
    /// Number of vectors in this shard.
    pub vector_count: u64,
    /// Non-cryptographic fingerprint hex digest of the artifact bytes (filled after build).
    ///
    /// Serialized as `sha256` for manifest v1 wire compatibility, while still
    /// accepting `fingerprint` when reading.
    #[serde(rename = "sha256", alias = "fingerprint")]
    pub fingerprint: String,
    /// Centroid vector used for query routing (manifest v2+).
    ///
    /// Populated at build time so that `IndexSearcher` can select probe shards
    /// without deserializing the full shard body.  Empty in manifests produced by
    /// older builders (manifest_version 1); fall back to loading the shard for
    /// routing in that case.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub centroid: Vec<f32>,
    /// Routing metadata for partition-aware query routing (manifest v4+).
    ///
    /// `None` when loading a legacy manifest (manifest_version ≤ 3) that
    /// predates the routing metadata schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<RoutingMetadata>,
}

/// Identifies the indexing algorithm used to build the index (manifest v3+).
///
/// Defaults to `AlgorithmMetadata::default()` (algorithm `"kmeans-flat"`) when
/// deserializing older manifests that pre-date this field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlgorithmMetadata {
    /// Canonical algorithm family name (e.g. `"kmeans-flat"`).
    pub algorithm: String,
    /// Optional algorithm variant (e.g. `"cosine-normalised"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    /// Free-form algorithm parameters recorded at build time.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, serde_json::Value>,
}

impl Default for AlgorithmMetadata {
    /// Returns an `AlgorithmMetadata` representing the default K-means flat
    /// index algorithm with no variant and no extra parameters.
    fn default() -> Self {
        Self {
            algorithm: "kmeans-flat".into(),
            variant: None,
            params: BTreeMap::new(),
        }
    }
}

/// Summary statistics across all shards (manifest v3+).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSummary {
    /// Total number of non-empty shards in the index.
    pub num_shards: u32,
    /// Vector count of the smallest shard.
    pub min_shard_vector_count: u64,
    /// Vector count of the largest shard.
    pub max_shard_vector_count: u64,
}

/// Compression / quantization configuration for an index (manifest v3+).
///
/// When `enabled` is `false` (the default) and `codec` is `"none"`, shard
/// artifacts use the raw-vector format (version 1).
///
/// When `enabled` is `true` and `codec` is `"pq8"`, shard artifacts use the
/// PQ-encoded format (version 2) and an additional codebook artifact is
/// stored at the key given by `codebook_key`.
///
/// # Schema constraints
///
/// The following invariants are enforced by [`Manifest::validate`]:
///
/// - `codec` must be `"none"` or `"pq8"`.
/// - `enabled` and `codec` must be consistent: `enabled=true` requires
///   `codec="pq8"`; `enabled=false` requires `codec="none"`.
/// - When `codec="pq8"`: `pq_num_subspaces` must be > 0, `pq_codebook_size`
///   must be in the range 1–256, and `codebook_key` must be present and
///   non-empty.
/// - When `codec="none"`: `pq_num_subspaces` and `pq_codebook_size` must be
///   0, and `codebook_key` must be absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// Whether compression / quantization is active.
    #[serde(default)]
    pub enabled: bool,
    /// Codec identifier: `"none"` or `"pq8"`.
    #[serde(default = "CompressionConfig::default_codec")]
    pub codec: String,
    /// Number of PQ sub-spaces (`M`).  `0` when codec is not `"pq8"`.
    #[serde(default, skip_serializing_if = "CompressionConfig::is_zero_u32")]
    pub pq_num_subspaces: u32,
    /// PQ codebook size (`K`): number of centroids per sub-space.
    ///
    /// Must be in the range 1–256 when `codec="pq8"` (`"pq8"` encodes each
    /// sub-space index as a single byte, so at most 256 entries are
    /// representable).  `0` for uncompressed indexes.
    #[serde(default, skip_serializing_if = "CompressionConfig::is_zero_u32")]
    pub pq_codebook_size: u32,
    /// Storage key of the PQ codebook artifact.  `None` for uncompressed
    /// indexes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codebook_key: Option<String>,
}

impl CompressionConfig {
    /// Maximum allowed `pq_codebook_size` for the `"pq8"` codec.
    ///
    /// `"pq8"` encodes each sub-space centroid index as a single unsigned byte,
    /// which can represent at most 256 distinct values (0–255).
    pub const MAX_PQ_CODEBOOK_SIZE: u32 = 256;

    fn default_codec() -> String {
        "none".into()
    }

    fn is_zero_u32(v: &u32) -> bool {
        *v == 0
    }
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            codec: Self::default_codec(),
            pq_num_subspaces: 0,
            pq_codebook_size: 0,
            codebook_key: None,
        }
    }
}

/// Approximate recall estimate recorded at build time (manifest v3+).
///
/// The estimate is produced by running a small sample query against the
/// freshly-built index and comparing approximate nearest-neighbour results
/// with a brute-force ground truth.  `None` when the estimate was not
/// computed (e.g. in fast prototype builds).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecallEstimate {
    /// The *k* used for the estimate (e.g. `10` for recall@10).
    pub k: u32,
    /// Estimated recall@k in the closed interval [0, 1].
    pub recall_at_k: f32,
    /// Number of sample queries used to compute the estimate.
    pub sample_size: u64,
}

/// Configuration for the optional BM25 lexical index artifact (manifest v4+).
///
/// When present in a [`Manifest`], a BM25 inverted-index artifact has been
/// built alongside the vector shards and can be used for text-only or hybrid
/// retrieval.  `None` means no lexical index was built.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LexicalIndexConfig {
    /// Storage key of the serialised [`Bm25Index`] artifact (`.bm25` file).
    ///
    /// [`Bm25Index`]: shardlake_index::bm25::Bm25Index
    pub artifact_key: String,
    /// BM25 term-frequency saturation parameter (k1) used at build time.
    ///
    /// Typical range: 1.2–2.0.  Stored here so that callers can verify that
    /// a loaded index matches the expected configuration.
    pub k1: f32,
    /// BM25 document-length normalisation parameter (b) used at build time.
    ///
    /// Range: 0.0–1.0.
    pub b: f32,
    /// Number of documents (vectors) indexed by this lexical index.
    pub doc_count: u64,
}

/// Build-time metadata recorded in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildMetadata {
    pub built_at: DateTime<Utc>,
    pub builder_version: String,
    pub num_kmeans_iters: u32,
    pub nprobe_default: u32,
    /// Wall-clock duration of the full build in seconds (manifest v3+).
    ///
    /// Defaults to `0.0` when deserializing older manifests that do not
    /// include this field.
    #[serde(default)]
    pub build_duration_secs: f64,
}

/// Full manifest tying dataset, embeddings, and index together.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Schema version of this manifest document.
    pub manifest_version: u32,
    pub dataset_version: DatasetVersion,
    pub embedding_version: EmbeddingVersion,
    pub index_version: IndexVersion,
    /// Alias pointer (e.g. "latest") for serving.
    pub alias: String,
    pub dims: u32,
    pub distance_metric: DistanceMetric,
    /// Storage key of the raw vectors file.
    pub vectors_key: String,
    /// Storage key of the metadata JSON file.
    pub metadata_key: String,
    pub shards: Vec<ShardDef>,
    pub total_vector_count: u64,
    pub build_metadata: BuildMetadata,
    /// Algorithm used to build this index (manifest v3+).
    ///
    /// Defaults to `AlgorithmMetadata::default()` (`"kmeans-flat"`) when
    /// deserializing manifest v1 or v2 documents.
    #[serde(default)]
    pub algorithm: AlgorithmMetadata,
    /// Shard count and per-shard vector-count statistics (manifest v3+).
    ///
    /// `None` when deserializing manifest v1 or v2 documents that do not
    /// include this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_summary: Option<ShardSummary>,
    /// Compression / quantization configuration (manifest v3+).
    ///
    /// Defaults to `CompressionConfig::default()` (disabled, codec `"none"`)
    /// when deserializing manifest v1 or v2 documents.
    #[serde(default)]
    pub compression: CompressionConfig,
    /// Approximate recall estimate recorded at build time (manifest v3+).
    ///
    /// `None` when not computed (e.g. prototype builds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recall_estimate: Option<RecallEstimate>,
    /// Storage key of the IVF coarse-quantizer artifact (`coarse_quantizer.cq`).
    ///
    /// Present for indexes built with algorithm `"ivf-flat"`.  `None` for
    /// older `"kmeans-flat"` indexes that pre-date IVF coarse-quantizer
    /// persistence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coarse_quantizer_key: Option<String>,
    /// Configuration for the optional BM25 lexical index (manifest v4+).
    ///
    /// `Some` when a BM25 inverted-index artifact was built alongside the
    /// vector shards for this index version.  `None` when no lexical index
    /// was built (the default for pure vector-only indexes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lexical: Option<LexicalIndexConfig>,
}

impl Manifest {
    /// Storage key for a manifest given an index version.
    pub fn storage_key(index_version: &IndexVersion) -> String {
        shardlake_storage::paths::index_manifest_key(&index_version.0)
    }

    /// Storage key for the active alias pointer.
    pub fn alias_key(alias: &str) -> String {
        shardlake_storage::paths::alias_key(alias)
    }

    /// Serialise and store to `store`.
    pub fn save(&self, store: &dyn ObjectStore) -> Result<()> {
        let key = Self::storage_key(&self.index_version);
        let bytes = serde_json::to_vec_pretty(&self.normalised_for_save())?;
        store.put(&key, bytes)?;
        Ok(())
    }

    /// Return the manifest document that should be emitted to storage.
    ///
    /// Legacy manifest versions are upgraded to the current schema on write so
    /// the stored wire format stays internally consistent.
    fn normalised_for_save(&self) -> Self {
        let mut manifest = self.clone();
        if manifest.manifest_version < 4 {
            manifest.manifest_version = 4;
        }
        manifest
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

    /// Validate internal consistency.
    pub fn validate(&self) -> Result<()> {
        if self.manifest_version != 1
            && self.manifest_version != 2
            && self.manifest_version != 3
            && self.manifest_version != 4
        {
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
        if let Some(shard) = self
            .shards
            .iter()
            .find(|shard| shard.fingerprint.is_empty())
        {
            return Err(ManifestError::Validation(format!(
                "shard {} fingerprint must not be empty",
                shard.shard_id
            )));
        }
        let counted: u64 = self.shards.iter().map(|s| s.vector_count).sum();
        if counted != self.total_vector_count {
            return Err(ManifestError::Validation(format!(
                "shard vector counts ({counted}) don't sum to total ({})",
                self.total_vector_count
            )));
        }
        self.validate_lifecycle_metadata()?;
        Ok(())
    }

    fn validate_lifecycle_metadata(&self) -> Result<()> {
        if self.algorithm.algorithm.trim().is_empty() {
            return Err(ManifestError::Validation(
                "algorithm.algorithm must not be empty".into(),
            ));
        }
        if !self.build_metadata.build_duration_secs.is_finite()
            || self.build_metadata.build_duration_secs < 0.0
        {
            return Err(ManifestError::Validation(
                "build_metadata.build_duration_secs must be finite and >= 0".into(),
            ));
        }
        if self.compression.codec.trim().is_empty() {
            return Err(ManifestError::Validation(
                "compression.codec must not be empty".into(),
            ));
        }
        let known_codecs = ["none", "pq8"];
        if !known_codecs.contains(&self.compression.codec.as_str()) {
            return Err(ManifestError::Validation(format!(
                "compression.codec '{}' is not a recognised codec; expected one of: {}",
                self.compression.codec,
                known_codecs.join(", ")
            )));
        }
        if self.compression.enabled && self.compression.codec == "none" {
            return Err(ManifestError::Validation(
                "compression.enabled is true but codec is \"none\"; set codec to \"pq8\" or disable compression".into(),
            ));
        }
        if !self.compression.enabled && self.compression.codec != "none" {
            return Err(ManifestError::Validation(format!(
                "compression.codec is \"{}\" but enabled is false; set enabled=true or use codec \"none\"",
                self.compression.codec
            )));
        }
        if self.compression.codec == "pq8" {
            if self.compression.pq_num_subspaces == 0 {
                return Err(ManifestError::Validation(
                    "compression.pq_num_subspaces must be > 0 when codec is \"pq8\"".into(),
                ));
            }
            if self.compression.pq_codebook_size == 0
                || self.compression.pq_codebook_size > CompressionConfig::MAX_PQ_CODEBOOK_SIZE
            {
                return Err(ManifestError::Validation(format!(
                    "compression.pq_codebook_size must be in 1..={} when codec is \"pq8\", found {}",
                    CompressionConfig::MAX_PQ_CODEBOOK_SIZE,
                    self.compression.pq_codebook_size
                )));
            }
            match &self.compression.codebook_key {
                None => {
                    return Err(ManifestError::Validation(
                        "compression.codebook_key must be present when codec is \"pq8\"".into(),
                    ));
                }
                Some(key) if key.trim().is_empty() => {
                    return Err(ManifestError::Validation(
                        "compression.codebook_key must not be empty when codec is \"pq8\"".into(),
                    ));
                }
                Some(_) => {}
            }
        }
        if self.compression.codec == "none" {
            if self.compression.pq_num_subspaces != 0 {
                return Err(ManifestError::Validation(format!(
                    "compression.pq_num_subspaces must be 0 when codec is \"none\", found {}",
                    self.compression.pq_num_subspaces
                )));
            }
            if self.compression.pq_codebook_size != 0 {
                return Err(ManifestError::Validation(format!(
                    "compression.pq_codebook_size must be 0 when codec is \"none\", found {}",
                    self.compression.pq_codebook_size
                )));
            }
            if self.compression.codebook_key.is_some() {
                return Err(ManifestError::Validation(
                    "compression.codebook_key must be absent when codec is \"none\"".into(),
                ));
            }
        }
        if let Some(key) = &self.coarse_quantizer_key {
            if key.trim().is_empty() {
                return Err(ManifestError::Validation(
                    "coarse_quantizer_key must not be empty when present".into(),
                ));
            }
        }
        if self.algorithm.algorithm == "ivf-flat" && self.coarse_quantizer_key.is_none() {
            return Err(ManifestError::Validation(
                "algorithm 'ivf-flat' requires coarse_quantizer_key".into(),
            ));
        }
        if self.algorithm.algorithm == "hnsw" && self.coarse_quantizer_key.is_none() {
            return Err(ManifestError::Validation(
                "algorithm 'hnsw' requires coarse_quantizer_key".into(),
            ));
        }

        if let Some(summary) = &self.shard_summary {
            let actual_num_shards = self.shards.len() as u32;
            let actual_min = self
                .shards
                .iter()
                .map(|shard| shard.vector_count)
                .min()
                .unwrap_or(0);
            let actual_max = self
                .shards
                .iter()
                .map(|shard| shard.vector_count)
                .max()
                .unwrap_or(0);

            if summary.num_shards != actual_num_shards {
                return Err(ManifestError::Validation(format!(
                    "shard_summary.num_shards mismatch: expected {}, found {}",
                    actual_num_shards, summary.num_shards
                )));
            }
            if summary.min_shard_vector_count > summary.max_shard_vector_count {
                return Err(ManifestError::Validation(
                    "shard_summary.min_shard_vector_count must be <= max_shard_vector_count".into(),
                ));
            }
            if summary.min_shard_vector_count != actual_min {
                return Err(ManifestError::Validation(format!(
                    "shard_summary.min_shard_vector_count mismatch: expected {}, found {}",
                    actual_min, summary.min_shard_vector_count
                )));
            }
            if summary.max_shard_vector_count != actual_max {
                return Err(ManifestError::Validation(format!(
                    "shard_summary.max_shard_vector_count mismatch: expected {}, found {}",
                    actual_max, summary.max_shard_vector_count
                )));
            }
        }

        if let Some(recall_estimate) = &self.recall_estimate {
            if recall_estimate.k == 0 {
                return Err(ManifestError::Validation(
                    "recall_estimate.k must be > 0".into(),
                ));
            }
            if recall_estimate.sample_size == 0 {
                return Err(ManifestError::Validation(
                    "recall_estimate.sample_size must be > 0".into(),
                ));
            }
            if !recall_estimate.recall_at_k.is_finite()
                || !(0.0..=1.0).contains(&recall_estimate.recall_at_k)
            {
                return Err(ManifestError::Validation(
                    "recall_estimate.recall_at_k must be finite and within [0, 1]".into(),
                ));
            }
        }

        self.validate_routing_metadata()?;

        if let Some(lexical) = &self.lexical {
            if self.manifest_version < 4 {
                return Err(ManifestError::Validation(
                    "lexical index config requires manifest_version >= 4".into(),
                ));
            }
            if lexical.artifact_key.trim().is_empty() {
                return Err(ManifestError::Validation(
                    "lexical.artifact_key must not be empty when present".into(),
                ));
            }
            if !lexical.k1.is_finite() || lexical.k1 <= 0.0 {
                return Err(ManifestError::Validation(
                    "lexical.k1 must be finite and > 0".into(),
                ));
            }
            if !lexical.b.is_finite() || !(0.0..=1.0).contains(&lexical.b) {
                return Err(ManifestError::Validation(
                    "lexical.b must be finite and within [0, 1]".into(),
                ));
            }
            if lexical.doc_count != self.total_vector_count {
                return Err(ManifestError::Validation(format!(
                    "lexical.doc_count mismatch: expected {}, found {}",
                    self.total_vector_count, lexical.doc_count
                )));
            }
        }

        Ok(())
    }

    /// Validate that every shard's optional routing metadata is internally
    /// consistent (non-empty `centroid_id`, `index_type`, and
    /// `file_location`).
    fn validate_routing_metadata(&self) -> Result<()> {
        for shard in &self.shards {
            if let Some(routing) = &shard.routing {
                if routing.centroid_id.trim().is_empty() {
                    return Err(ManifestError::Validation(format!(
                        "shard {} routing.centroid_id must not be empty",
                        shard.shard_id
                    )));
                }
                if routing.index_type.trim().is_empty() {
                    return Err(ManifestError::Validation(format!(
                        "shard {} routing.index_type must not be empty",
                        shard.shard_id
                    )));
                }
                if routing.file_location.trim().is_empty() {
                    return Err(ManifestError::Validation(format!(
                        "shard {} routing.file_location must not be empty",
                        shard.shard_id
                    )));
                }
            }
        }
        Ok(())
    }

    /// Check that this manifest is compatible with the requested vector
    /// dimension.
    ///
    /// Returns [`ManifestError::Compatibility`] when `dims` does not match the
    /// dimension stored in the manifest.
    pub fn check_dimension_compat(&self, dims: u32) -> Result<()> {
        if self.dims != dims {
            return Err(ManifestError::Compatibility(format!(
                "dimension mismatch: manifest has {}, requested {}",
                self.dims, dims
            )));
        }
        Ok(())
    }

    /// Check that this manifest was built from the given dataset version.
    ///
    /// Returns [`ManifestError::Compatibility`] when the stored
    /// `dataset_version` does not match `dataset_version`.
    pub fn check_dataset_version_compat(&self, dataset_version: &DatasetVersion) -> Result<()> {
        if &self.dataset_version != dataset_version {
            return Err(ManifestError::Compatibility(format!(
                "dataset version mismatch: manifest has {}, requested {}",
                self.dataset_version, dataset_version
            )));
        }
        Ok(())
    }

    /// Check that this manifest was built with a compatible indexing algorithm.
    ///
    /// Returns [`ManifestError::Compatibility`] when the stored
    /// [`AlgorithmMetadata::algorithm`] name does not match `algorithm`.
    pub fn check_algorithm_compat(&self, algorithm: &str) -> Result<()> {
        if self.algorithm.algorithm != algorithm {
            return Err(ManifestError::Compatibility(format!(
                "algorithm mismatch: manifest has {}, requested {}",
                self.algorithm.algorithm, algorithm
            )));
        }
        Ok(())
    }

    /// Check that this manifest is compatible with the requested algorithm
    /// family, variant, and a subset of critical algorithm parameters.
    ///
    /// # Compatibility semantics
    ///
    /// * **algorithm** — must match [`AlgorithmMetadata::algorithm`] exactly.
    /// * **variant** — when `Some(v)`, [`AlgorithmMetadata::variant`] must
    ///   equal `Some(v)` exactly; pass `None` to skip the variant check
    ///   entirely (any manifest variant, including `None`, is accepted).
    /// * **required_params** — each `(key, value)` pair must appear in
    ///   [`AlgorithmMetadata::params`] with an identical value.  Parameters
    ///   present in the manifest but absent from `required_params` are
    ///   silently ignored, enabling forward-compatibility when new
    ///   informational parameters are added in future builder versions.  A
    ///   parameter listed in `required_params` but absent from the manifest
    ///   is treated as a mismatch.
    ///
    /// Returns [`ManifestError::Compatibility`] on the first mismatch
    /// encountered.
    pub fn check_algorithm_full_compat(
        &self,
        algorithm: &str,
        variant: Option<&str>,
        required_params: &[(&str, &serde_json::Value)],
    ) -> Result<()> {
        if self.algorithm.algorithm != algorithm {
            return Err(ManifestError::Compatibility(format!(
                "algorithm mismatch: manifest has {}, requested {}",
                self.algorithm.algorithm, algorithm
            )));
        }
        if let Some(required_variant) = variant {
            if self.algorithm.variant.as_deref() != Some(required_variant) {
                return Err(ManifestError::Compatibility(format!(
                    "algorithm variant mismatch: manifest has {:?}, requested {:?}",
                    self.algorithm.variant.as_deref(),
                    required_variant
                )));
            }
        }
        for (key, expected) in required_params {
            match self.algorithm.params.get(*key) {
                None => {
                    return Err(ManifestError::Compatibility(format!(
                        "algorithm param {:?} missing from manifest",
                        key
                    )));
                }
                Some(actual) if actual != *expected => {
                    return Err(ManifestError::Compatibility(format!(
                        "algorithm param {:?} mismatch: manifest has {}, requested {}",
                        key, actual, expected
                    )));
                }
                Some(_) => {}
            }
        }
        Ok(())
    }
}

/// Thin alias pointer stored at `aliases/<alias>.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AliasPointer {
    pub alias: String,
    pub index_version: IndexVersion,
}

// ── Dataset manifest ──────────────────────────────────────────────────────────

/// Current schema version of the dataset manifest.
///
/// Written by `shardlake ingest` into `datasets/<version>/info.json`.
pub const DATASET_MANIFEST_VERSION: u32 = 1;

/// Ingest-time lifecycle metadata recorded in the dataset manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestMetadata {
    /// When the dataset was ingested (UTC).
    pub ingested_at: DateTime<Utc>,
    /// Version of the `shardlake` binary that produced this dataset.
    pub ingester_version: String,
}

/// Versioned dataset manifest stored at `datasets/<version>/info.json`.
///
/// Written by `shardlake ingest`, loaded by `shardlake build-index`.
///
/// # Backwards compatibility
///
/// Pre-versioning `info.json` files (schema version `0`) are still accepted:
/// - `manifest_version` defaults to `0` when the field is absent.
/// - `vector_count` accepts the legacy `count` field name via a serde alias.
/// - `ingest_metadata` is `None` when the field is absent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetManifest {
    /// Schema version of this manifest document.
    ///
    /// `0` = pre-versioning legacy file; [`DATASET_MANIFEST_VERSION`] = current.
    #[serde(default)]
    pub manifest_version: u32,
    pub dataset_version: DatasetVersion,
    pub embedding_version: EmbeddingVersion,
    /// Vector dimension.
    pub dims: u32,
    /// Total number of vectors in this dataset.
    ///
    /// Serialised as `vector_count`; also accepts the legacy `count` field name
    /// for backward compatibility with pre-versioning `info.json` files.
    #[serde(alias = "count")]
    pub vector_count: u64,
    /// Storage key of the raw vectors JSONL file.
    pub vectors_key: String,
    /// Storage key of the metadata JSON file.
    pub metadata_key: String,
    /// Lifecycle metadata captured at ingest time.
    ///
    /// `None` when loading a legacy `info.json` that predates the versioned schema.
    #[serde(default)]
    pub ingest_metadata: Option<IngestMetadata>,
}

impl DatasetManifest {
    /// Storage key for a dataset manifest given a dataset version.
    pub fn storage_key(dataset_version: &DatasetVersion) -> String {
        shardlake_storage::paths::dataset_info_key(&dataset_version.0)
    }

    /// Serialise and store to `store`.
    pub fn save(&self, store: &dyn ObjectStore) -> Result<()> {
        let key = Self::storage_key(&self.dataset_version);
        let bytes = serde_json::to_vec_pretty(self)?;
        store.put(&key, bytes)?;
        Ok(())
    }

    /// Load from `store` by dataset version.
    pub fn load(store: &dyn ObjectStore, dataset_version: &DatasetVersion) -> Result<Self> {
        let key = Self::storage_key(dataset_version);
        let bytes = store.get(&key)?;
        let m: Self = serde_json::from_slice(&bytes)?;
        m.validate()?;
        if m.dataset_version != *dataset_version {
            return Err(ManifestError::Validation(format!(
                "dataset manifest: dataset_version mismatch (expected {}, found {})",
                dataset_version.0, m.dataset_version.0
            )));
        }
        Ok(m)
    }

    /// Validate internal consistency of this dataset manifest.
    pub fn validate(&self) -> Result<()> {
        if self.manifest_version > DATASET_MANIFEST_VERSION {
            return Err(ManifestError::Validation(format!(
                "unsupported dataset manifest_version {}",
                self.manifest_version
            )));
        }
        if self.dims == 0 {
            return Err(ManifestError::Validation(
                "dataset manifest: dims must be > 0".into(),
            ));
        }
        if self.vector_count == 0 {
            return Err(ManifestError::Validation(
                "dataset manifest: vector_count must be > 0".into(),
            ));
        }
        if self.vectors_key.is_empty() {
            return Err(ManifestError::Validation(
                "dataset manifest: vectors_key must not be empty".into(),
            ));
        }
        if self.metadata_key.is_empty() {
            return Err(ManifestError::Validation(
                "dataset manifest: metadata_key must not be empty".into(),
            ));
        }
        Ok(())
    }
}
