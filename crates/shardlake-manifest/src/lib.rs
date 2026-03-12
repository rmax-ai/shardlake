//! Manifest schema: ties a dataset version to an index version and describes
//! all shard artifacts.

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
}

pub type Result<T> = std::result::Result<T, ManifestError>;

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
    /// Populated at build time so that [`IndexSearcher`] can select probe shards
    /// without deserializing the full shard body.  Empty in manifests produced by
    /// older builders (manifest_version 1); fall back to loading the shard for
    /// routing in that case.
    ///
    /// [`IndexSearcher`]: shardlake_index::IndexSearcher
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub centroid: Vec<f32>,
}

/// Build-time metadata recorded in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildMetadata {
    pub built_at: DateTime<Utc>,
    pub builder_version: String,
    pub num_kmeans_iters: u32,
    pub nprobe_default: u32,
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

    /// Validate internal consistency.
    pub fn validate(&self) -> Result<()> {
        if self.manifest_version != 1 && self.manifest_version != 2 {
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
        Ok(())
    }
}

/// Thin alias pointer stored at `aliases/<alias>.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AliasPointer {
    pub alias: String,
    pub index_version: IndexVersion,
}
