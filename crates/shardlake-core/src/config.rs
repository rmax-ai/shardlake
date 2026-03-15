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
    /// Enable product quantisation (PQ) compression for shard artifacts.
    ///
    /// When `true`, the builder trains a PQ codebook on the full dataset,
    /// encodes all vectors as PQ codes, and stores them in format-version-2
    /// `.sidx` artifacts.  The codebook is persisted as a separate artifact
    /// alongside the shards.  Defaults to `false`.
    #[serde(default)]
    pub pq_enabled: bool,
    /// Number of PQ sub-spaces (`M`).  Ignored when `pq_enabled` is `false`.
    ///
    /// Must be ≥ 1 and must divide `dims` evenly.  Defaults to `8`.
    #[serde(default = "SystemConfig::default_pq_num_subspaces")]
    pub pq_num_subspaces: u32,
    /// PQ codebook size (`K`) per sub-space.  Ignored when `pq_enabled` is
    /// `false`.  Must be in the range `[1, 256]`.  Defaults to `256`.
    #[serde(default = "SystemConfig::default_pq_codebook_size")]
    pub pq_codebook_size: u32,
    /// Maximum number of vectors to use for K-means centroid training.
    ///
    /// When `None` (default), every vector in the dataset is used to train
    /// centroids.  When `Some(n)`, a random sample of up to `n` vectors is
    /// drawn (without replacement) using the seeded RNG before K-means runs.
    /// All vectors—including those not in the sample—are still assigned to the
    /// nearest centroid after training, so no data is lost.
    ///
    /// Sampling speeds up centroid training on large datasets while preserving
    /// shard assignment correctness.  Two builds with the same seed and the
    /// same `kmeans_sample_size` produce identical centroids and fingerprints.
    #[serde(default)]
    pub kmeans_sample_size: Option<u32>,
}

impl SystemConfig {
    /// Returns the default K-means RNG seed.
    ///
    /// Exposed as a function so it can be used as a `serde` field-level
    /// default via `#[serde(default = "SystemConfig::default_kmeans_seed")]`.
    pub fn default_kmeans_seed() -> u64 {
        DEFAULT_KMEANS_SEED
    }

    /// Returns the default number of PQ sub-spaces (8).
    pub fn default_pq_num_subspaces() -> u32 {
        8
    }

    /// Returns the default PQ codebook size (256).
    pub fn default_pq_codebook_size() -> u32 {
        256
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
            kmeans_sample_size: None,
            pq_enabled: false,
            pq_num_subspaces: Self::default_pq_num_subspaces(),
            pq_codebook_size: Self::default_pq_codebook_size(),
        }
    }
}
