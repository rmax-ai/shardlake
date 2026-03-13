use serde::{Deserialize, Serialize};

/// Default K-means RNG seed used for reproducible shard partitioning.
///
/// This constant is the default value of [`SystemConfig::kmeans_seed`] and is
/// recorded verbatim in every index manifest so that a build can be reproduced
/// by supplying the same seed alongside the same dataset and configuration.
pub const DEFAULT_KMEANS_SEED: u64 = 0xdead_beef;

/// Top-level system configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemConfig {
    /// Root path for all artifact storage.
    pub storage_root: std::path::PathBuf,
    /// Number of shards to partition into.
    pub num_shards: u32,
    /// Number of K-means iterations for centroid computation.
    pub kmeans_iters: u32,
    /// Number of shards to probe at query time (nprobe).
    pub nprobe: u32,
    /// RNG seed for K-means centroid initialisation.
    ///
    /// All other build inputs being equal, two builds with the same seed
    /// produce identical shard assignments and artifact fingerprints.
    /// Defaults to [`DEFAULT_KMEANS_SEED`].  Deserialises to the default
    /// when the field is absent (backwards-compatible with older config files).
    #[serde(default = "SystemConfig::default_kmeans_seed")]
    pub kmeans_seed: u64,
}

impl SystemConfig {
    /// Returns the default K-means RNG seed.
    ///
    /// Exposed as a function so it can be used as a `serde` field-level
    /// default via `#[serde(default = "SystemConfig::default_kmeans_seed")]`.
    pub fn default_kmeans_seed() -> u64 {
        DEFAULT_KMEANS_SEED
    }
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            storage_root: std::path::PathBuf::from("./data"),
            num_shards: 4,
            kmeans_iters: 20,
            nprobe: 2,
            kmeans_seed: DEFAULT_KMEANS_SEED,
        }
    }
}
