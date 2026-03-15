use serde::{Deserialize, Serialize};

use crate::error::CoreError;

/// Default K-means RNG seed used for reproducible shard partitioning.
///
/// This constant is the default value of [`SystemConfig::kmeans_seed`] and is
/// recorded verbatim in every index manifest so that a build can be reproduced
/// by supplying the same seed alongside the same dataset and configuration.
pub const DEFAULT_KMEANS_SEED: u64 = 0xdead_beef;

/// Query fan-out policy controlling how many centroids, shards, and per-shard
/// vectors are evaluated during a single ANN search.
///
/// These knobs let you trade recall for latency:
///
/// - **`candidate_centroids`** – How many nearest IVF centroids to select
///   before mapping them to shards.  Increasing this value improves recall at
///   the cost of routing more shards.  Must be ≥ 1.
/// - **`candidate_shards`** – Hard cap on the number of shards probed after
///   deduplication.  `0` means *no cap* (all shards selected by
///   `candidate_centroids` are probed).
/// - **`max_vectors_per_shard`** – Maximum number of vectors evaluated inside
///   each probed shard.  `0` means *no limit* (all vectors in the shard are
///   scored).
///
/// # Validation
///
/// Call [`FanOutPolicy::validate`] before using a policy obtained from
/// untrusted input (e.g. an HTTP query request).  The method returns
/// [`CoreError::InvalidFanOutPolicy`] when any invariant is violated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanOutPolicy {
    /// Number of nearest centroids to select for shard routing.  Must be ≥ 1.
    pub candidate_centroids: u32,
    /// Maximum number of shards to probe after centroid-to-shard deduplication.
    /// `0` means no cap.
    #[serde(default)]
    pub candidate_shards: u32,
    /// Maximum number of vectors to evaluate per probed shard.
    /// `0` means no limit.
    #[serde(default)]
    pub max_vectors_per_shard: u32,
}

impl FanOutPolicy {
    /// Validate the policy.
    ///
    /// Returns [`CoreError::InvalidFanOutPolicy`] when `candidate_centroids`
    /// is `0`, which would cause every query to return no results.
    pub fn validate(&self) -> crate::error::Result<()> {
        if self.candidate_centroids == 0 {
            return Err(CoreError::InvalidFanOutPolicy(
                "candidate_centroids must be ≥ 1".into(),
            ));
        }
        Ok(())
    }

    fn default_candidate_centroids() -> u32 {
        2
    }
}

impl Default for FanOutPolicy {
    fn default() -> Self {
        Self {
            candidate_centroids: Self::default_candidate_centroids(),
            candidate_shards: 0,
            max_vectors_per_shard: 0,
        }
    }
}

/// Policy controlling optional shard prefetch warming based on query frequency.
///
/// When enabled, shards that have been probed at least `min_query_count` times
/// are considered "hot" and will be loaded into the cache proactively on the
/// next cache-miss load event, reducing I/O latency for frequently accessed
/// shards.
///
/// Prefetching is **disabled** by default, which preserves the existing
/// lazy-load-on-probe semantics for all shards.
///
/// # Validation
///
/// Call [`PrefetchPolicy::validate`] before using a policy from untrusted
/// input (e.g. a config file).  The method returns
/// [`CoreError::InvalidPrefetchPolicy`] when `min_query_count` is `0` while
/// `enabled` is `true`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PrefetchPolicy {
    /// Whether shard prefetch warming is enabled.
    ///
    /// When `false` (default) the cache behaves lazily: a shard is only
    /// loaded when a query explicitly probes it.
    #[serde(default)]
    pub enabled: bool,
    /// Minimum number of times a shard must be probed before it is treated as
    /// "hot" and eligible for prefetch warming.
    ///
    /// Must be ≥ 1 when `enabled` is `true`.  Defaults to `3`.
    #[serde(default = "PrefetchPolicy::default_min_query_count")]
    pub min_query_count: u32,
}

impl PrefetchPolicy {
    /// Validate the policy.
    ///
    /// Returns [`CoreError::InvalidPrefetchPolicy`] when `enabled` is `true`
    /// and `min_query_count` is `0`, which would cause every shard to be
    /// eagerly warmed on its very first access.
    pub fn validate(&self) -> crate::error::Result<()> {
        if self.enabled && self.min_query_count == 0 {
            return Err(crate::error::CoreError::InvalidPrefetchPolicy(
                "min_query_count must be ≥ 1 when prefetch is enabled".into(),
            ));
        }
        Ok(())
    }

    fn default_min_query_count() -> u32 {
        3
    }
}

impl Default for PrefetchPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            min_query_count: Self::default_min_query_count(),
        }
    }
}

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
    ///
    /// This field maps to [`FanOutPolicy::candidate_centroids`] and is kept for
    /// backward compatibility with existing configuration files.  When building
    /// a [`FanOutPolicy`] at runtime, prefer calling [`SystemConfig::fan_out_policy`]
    /// so that `candidate_shards` and `max_vectors_per_shard` are also applied.
    pub nprobe: u32,
    /// RNG seed for K-means centroid initialisation.
    ///
    /// All other build inputs being equal, two builds with the same seed
    /// produce identical shard assignments and artifact fingerprints.
    /// Defaults to [`DEFAULT_KMEANS_SEED`].  Deserialises to the default
    /// when the field is absent (backwards-compatible with older config files).
    #[serde(default = "SystemConfig::default_kmeans_seed")]
    pub kmeans_seed: u64,
    /// Maximum number of shards to probe after centroid-to-shard deduplication.
    /// `0` means no cap (all shards selected by `nprobe` centroids are probed).
    ///
    /// See [`FanOutPolicy::candidate_shards`] for details.
    #[serde(default)]
    pub candidate_shards: u32,
    /// Maximum number of vectors to evaluate per probed shard.
    /// `0` means no limit (all vectors in the shard are scored).
    ///
    /// See [`FanOutPolicy::max_vectors_per_shard`] for details.
    #[serde(default)]
    pub max_vectors_per_shard: u32,
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
    /// Maximum number of shards to hold in the in-memory shard cache at any
    /// one time.
    ///
    /// `0` means no limit (all loaded shards are retained for the process
    /// lifetime, which is the historical default).  When the limit is exceeded,
    /// the least-frequently-accessed shard is evicted to make room for the new
    /// entry (LFU eviction).
    ///
    /// Has no effect on the cold-path behaviour: shards that are not probed are
    /// never loaded regardless of this setting.
    #[serde(default)]
    pub cache_capacity: u32,
    /// Prefetch policy for warming hot shards into the cache proactively.
    ///
    /// Disabled by default (`enabled = false`), which preserves the existing
    /// lazy-load-on-probe behaviour.  When enabled, shards whose probe count
    /// reaches [`PrefetchPolicy::min_query_count`] are loaded into the cache
    /// proactively on the next cache-miss event.
    #[serde(default)]
    pub prefetch: PrefetchPolicy,
}

impl SystemConfig {
    /// Returns the default K-means RNG seed.
    ///
    /// Exposed as a function so it can be used as a `serde` field-level
    /// default via `#[serde(default = "SystemConfig::default_kmeans_seed")]`.
    pub fn default_kmeans_seed() -> u64 {
        DEFAULT_KMEANS_SEED
    }

    /// Derive a [`FanOutPolicy`] from this configuration.
    ///
    /// `nprobe` maps to `candidate_centroids`; `candidate_shards` and
    /// `max_vectors_per_shard` are forwarded directly.
    pub fn fan_out_policy(&self) -> FanOutPolicy {
        FanOutPolicy {
            candidate_centroids: self.nprobe,
            candidate_shards: self.candidate_shards,
            max_vectors_per_shard: self.max_vectors_per_shard,
        }
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
            candidate_shards: 0,
            max_vectors_per_shard: 0,
            kmeans_sample_size: None,
            pq_enabled: false,
            pq_num_subspaces: Self::default_pq_num_subspaces(),
            pq_codebook_size: Self::default_pq_codebook_size(),
            cache_capacity: 0,
            prefetch: PrefetchPolicy::default(),
        }
    }
}
