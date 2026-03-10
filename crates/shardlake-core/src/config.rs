use serde::{Deserialize, Serialize};

/// Top-level system configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemConfig {
    /// Root path for all artifact storage.
    pub storage_root: std::path::PathBuf,
    /// Number of shards to partition into (K in K-means).
    pub num_shards: u32,
    /// Number of K-means iterations for centroid computation.
    pub kmeans_iters: u32,
    /// Default number of shards to probe at query time.
    pub nprobe: u32,
    /// Number of candidate centroids to evaluate during query routing.
    ///
    /// This controls the fan-out: the top-`candidate_centroids` nearest
    /// centroids are found, then deduplicated to at most `candidate_shards`
    /// unique shards for probing.  Set to `0` to use `nprobe` as the
    /// effective value (backward-compatible default).
    #[serde(default)]
    pub candidate_centroids: u32,
    /// Maximum number of unique shards to probe after centroid routing.
    ///
    /// Caps the query fan-out regardless of how many candidate centroids were
    /// selected.  Set to `0` to use `nprobe` as the effective value.
    #[serde(default)]
    pub candidate_shards: u32,
    /// Maximum number of vectors allowed per shard.
    ///
    /// Overflow vectors (those furthest from the centroid) are re-assigned to
    /// their next-nearest centroid.  Set to `0` for unlimited shard size.
    #[serde(default)]
    pub max_vectors_per_shard: u32,
    /// If non-zero, K-means centroids are trained on a random sample of this
    /// many vectors rather than the full corpus.  Useful for large datasets
    /// where full-corpus K-means training is expensive.  Set to `0` to train
    /// on all vectors.
    #[serde(default)]
    pub kmeans_sample_size: u32,
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            storage_root: std::path::PathBuf::from("./data"),
            num_shards: 4,
            kmeans_iters: 20,
            nprobe: 2,
            candidate_centroids: 0,
            candidate_shards: 0,
            max_vectors_per_shard: 0,
            kmeans_sample_size: 0,
        }
    }
}

impl SystemConfig {
    /// Resolved number of candidate centroids: falls back to `nprobe` when
    /// `candidate_centroids` is `0`.
    pub fn effective_candidate_centroids(&self) -> u32 {
        if self.candidate_centroids == 0 {
            self.nprobe
        } else {
            self.candidate_centroids
        }
    }

    /// Resolved maximum shards to probe: falls back to `nprobe` when
    /// `candidate_shards` is `0`.
    pub fn effective_candidate_shards(&self) -> u32 {
        if self.candidate_shards == 0 {
            self.nprobe
        } else {
            self.candidate_shards
        }
    }
}
