use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    error::CoreError,
    types::{DistanceMetric, QueryMode},
};

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
/// are considered "hot" and may be loaded into the shard cache proactively on
/// a later cache miss.
///
/// # Validation
///
/// Call [`PrefetchPolicy::validate`] before using a policy obtained from
/// untrusted input. The method returns
/// [`CoreError::InvalidPrefetchPolicy`] when `min_query_count` is `0` while
/// `enabled` is `true`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PrefetchPolicy {
    /// Whether shard prefetch warming is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Minimum number of probe events required before a shard becomes eligible
    /// for prefetch warming.
    #[serde(default = "PrefetchPolicy::default_min_query_count")]
    pub min_query_count: u32,
}

impl PrefetchPolicy {
    /// Validate the policy.
    pub fn validate(&self) -> crate::error::Result<()> {
        if self.enabled && self.min_query_count == 0 {
            return Err(CoreError::InvalidPrefetchPolicy(
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

/// Per-query execution configuration.
///
/// Bundles all the knobs that tune a single query: retrieval mode, how many
/// results to return, how wide to fan out across shards, an optional cap on
/// the number of rerank candidates, and an optional per-query distance metric
/// override.
///
/// # Defaults
///
/// Use [`QueryConfig::default`] to get a sensible starting point:
/// - `query_mode = QueryMode::Vector`
/// - `top_k = 10`
/// - `fan_out = FanOutPolicy::default()` (2 centroids, no caps)
/// - `rerank_limit = None` (no limit; use the pipeline's `rerank_oversample`)
/// - `distance_metric = None` (use the metric stored in the manifest)
///
/// # Validation
///
/// Call [`QueryConfig::validate`] before using a config obtained from
/// untrusted input (e.g. an HTTP request body).  Returns
/// [`CoreError::InvalidQueryConfig`] when any invariant is violated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Retrieval mode for this query.
    ///
    /// Controls which search backend(s) are engaged:
    /// - [`QueryMode::Vector`] (default) – ANN vector search only.
    /// - [`QueryMode::Lexical`] – BM25 full-text search only.
    /// - [`QueryMode::Hybrid`] – blend of vector and lexical scores.
    #[serde(default)]
    pub query_mode: QueryMode,
    /// Number of results to return.  Must be ≥ 1.
    pub top_k: usize,
    /// Fan-out policy controlling centroid and shard selection.
    pub fan_out: FanOutPolicy,
    /// Maximum number of merged candidates passed to the reranker.
    ///
    /// When `None` (default) the pipeline falls back to multiplying `top_k`
    /// by its configured `rerank_oversample` factor.  When `Some(n)`, exactly
    /// `n` merged candidates are handed to the reranker before it returns the
    /// final top-`top_k` results.  Must be ≥ 1 when set.
    #[serde(default)]
    pub rerank_limit: Option<usize>,
    /// Distance metric for this query.
    ///
    /// When `None` (default), the metric recorded in the index manifest is
    /// used.  When `Some(metric)`, the specified metric overrides the manifest
    /// value for this query only.
    #[serde(default)]
    pub distance_metric: Option<DistanceMetric>,
}

impl QueryConfig {
    /// Validate the configuration.
    ///
    /// Returns [`CoreError::InvalidQueryConfig`] when:
    /// - `top_k` is `0`,
    /// - the embedded [`FanOutPolicy`] is invalid (delegates to
    ///   [`FanOutPolicy::validate`]), or
    /// - `rerank_limit` is `Some(0)`.
    pub fn validate(&self) -> crate::error::Result<()> {
        if self.top_k == 0 {
            return Err(CoreError::InvalidQueryConfig("top_k must be ≥ 1".into()));
        }
        self.fan_out.validate()?;
        if self.rerank_limit == Some(0) {
            return Err(CoreError::InvalidQueryConfig(
                "rerank_limit must be ≥ 1 when set".into(),
            ));
        }
        Ok(())
    }
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            query_mode: QueryMode::Vector,
            top_k: 10,
            fan_out: FanOutPolicy::default(),
            rerank_limit: None,
            distance_metric: None,
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
    /// Maximum number of shard indexes to retain in the in-memory LRU cache.
    ///
    /// The shard cache bounds memory usage at query time by evicting the
    /// least-recently-used shard when the limit is reached.  Set this to at
    /// least `nprobe` (or `candidate_shards` when it is non-zero) so that the
    /// shards probed in a single query all fit in cache simultaneously.
    ///
    /// Defaults to `128`.  Must be ≥ 1; passing `0` when constructing an
    /// `IndexSearcher` or `CachedShardLoader` will panic at construction time.
    #[serde(
        default = "SystemConfig::default_shard_cache_capacity",
        deserialize_with = "deserialize_nonzero_shard_cache_capacity"
    )]
    pub shard_cache_capacity: usize,
    /// Prefetch policy for warming frequently probed shards into the cache.
    ///
    /// Disabled by default so the historical lazy-load behaviour is preserved
    /// unless callers explicitly opt in.
    #[serde(default)]
    pub prefetch: PrefetchPolicy,
    /// Number of sample queries used for build-time recall@k estimation.
    ///
    /// When `None` (default), recall estimation is skipped and
    /// `manifest.recall_estimate` is left `None`.  When `Some(n)`, a
    /// reproducible random sample of up to `n` vectors is drawn from the build
    /// corpus (using `kmeans_seed`), queried against the freshly-built index,
    /// and compared against a brute-force ground truth over the full corpus.
    /// The resulting mean recall@`recall_k` is persisted in the manifest.
    ///
    /// Enabling this option loads all shard artifacts back into memory after the
    /// build completes, so the peak memory usage during the estimation step is
    /// proportional to the corpus size.  Disable for very large builds where
    /// this cost is unacceptable.
    #[serde(default)]
    pub recall_sample_size: Option<u32>,
    /// The *k* used for build-time recall@k estimation.
    ///
    /// Defaults to `10` (recall@10).  Ignored when [`recall_sample_size`] is
    /// `None`.  When the corpus contains fewer than `recall_k` vectors, `k` is
    /// automatically clamped to the corpus size.
    ///
    /// [`recall_sample_size`]: SystemConfig::recall_sample_size
    #[serde(default = "SystemConfig::default_recall_k")]
    pub recall_k: u32,
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

    /// Returns the default shard cache capacity (128).
    pub fn default_shard_cache_capacity() -> usize {
        128
    }

    /// Returns the default k for build-time recall estimation (10).
    pub fn default_recall_k() -> u32 {
        10
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
            shard_cache_capacity: Self::default_shard_cache_capacity(),
            prefetch: PrefetchPolicy::default(),
            recall_sample_size: None,
            recall_k: Self::default_recall_k(),
        }
    }
}

fn deserialize_nonzero_shard_cache_capacity<'de, D>(
    deserializer: D,
) -> std::result::Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let capacity = usize::deserialize(deserializer)?;
    if capacity == 0 {
        return Err(serde::de::Error::custom(
            "shard_cache_capacity must be >= 1",
        ));
    }
    Ok(capacity)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{FanOutPolicy, PrefetchPolicy, QueryConfig, SystemConfig};

    #[test]
    fn system_config_rejects_zero_shard_cache_capacity() {
        let err = serde_json::from_value::<SystemConfig>(json!({
            "storage_root": "./data",
            "num_shards": 4,
            "kmeans_iters": 20,
            "nprobe": 2,
            "shard_cache_capacity": 0
        }))
        .expect_err("zero shard_cache_capacity must be rejected during deserialisation");

        assert!(err
            .to_string()
            .contains("shard_cache_capacity must be >= 1"));
    }

    #[test]
    fn prefetch_policy_rejects_zero_threshold_when_enabled() {
        let err = PrefetchPolicy {
            enabled: true,
            min_query_count: 0,
        }
        .validate()
        .expect_err("enabled prefetch policy must reject a zero threshold");

        assert_eq!(
            err.to_string(),
            "invalid prefetch policy: min_query_count must be ≥ 1 when prefetch is enabled"
        );
    }

    #[test]
    fn query_config_rejects_zero_top_k() {
        let err = QueryConfig {
            top_k: 0,
            ..QueryConfig::default()
        }
        .validate()
        .expect_err("top_k = 0 must be rejected");

        assert!(
            err.to_string().contains("top_k must be ≥ 1"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn query_config_rejects_zero_rerank_limit() {
        let err = QueryConfig {
            rerank_limit: Some(0),
            ..QueryConfig::default()
        }
        .validate()
        .expect_err("rerank_limit = 0 must be rejected");

        assert!(
            err.to_string().contains("rerank_limit must be ≥ 1"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn query_config_rejects_invalid_fan_out() {
        let err = QueryConfig {
            fan_out: FanOutPolicy {
                candidate_centroids: 0,
                ..Default::default()
            },
            ..QueryConfig::default()
        }
        .validate()
        .expect_err("candidate_centroids = 0 must be rejected");

        assert!(
            err.to_string().contains("candidate_centroids"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn query_config_valid_with_all_fields() {
        use crate::types::{DistanceMetric, QueryMode};
        let config = QueryConfig {
            query_mode: QueryMode::Vector,
            top_k: 5,
            fan_out: FanOutPolicy {
                candidate_centroids: 3,
                candidate_shards: 2,
                max_vectors_per_shard: 100,
            },
            rerank_limit: Some(20),
            distance_metric: Some(DistanceMetric::Cosine),
        };
        config
            .validate()
            .expect("fully specified config must be valid");
    }
}
