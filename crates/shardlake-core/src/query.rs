//! Per-query configuration parameters.

use serde::{Deserialize, Serialize};

use crate::types::DistanceMetric;

/// Configuration for a single query execution.
///
/// All fields have sensible defaults so callers only need to override
/// the ones they care about.
///
/// # Examples
/// ```
/// use shardlake_core::query::QueryConfig;
/// use shardlake_core::types::DistanceMetric;
///
/// let cfg = QueryConfig {
///     top_k: 10,
///     candidate_shards: 4,
///     rerank_limit: Some(50),
///     distance_metric: Some(DistanceMetric::Cosine),
/// };
/// assert_eq!(cfg.top_k, 10);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Number of results to return (top-k).
    pub top_k: usize,
    /// Number of shards to probe during centroid routing (nprobe).
    pub candidate_shards: usize,
    /// When set, the pipeline gathers up to `rerank_limit` candidates
    /// from all probed shards, re-sorts them by score, and then returns
    /// the final `top_k`.  Use a value larger than `top_k` to improve
    /// recall without increasing the final result set size.
    pub rerank_limit: Option<usize>,
    /// Override the distance metric recorded in the index manifest.
    /// When `None` the metric stored at build time is used.
    pub distance_metric: Option<DistanceMetric>,
}

impl QueryConfig {
    /// Return the effective candidate count: `rerank_limit` if set,
    /// otherwise `top_k`.
    #[must_use]
    pub fn effective_candidates(&self) -> usize {
        self.rerank_limit.unwrap_or(self.top_k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_candidates_without_rerank() {
        let cfg = QueryConfig {
            top_k: 5,
            candidate_shards: 2,
            rerank_limit: None,
            distance_metric: None,
        };
        assert_eq!(cfg.effective_candidates(), 5);
    }

    #[test]
    fn effective_candidates_with_rerank() {
        let cfg = QueryConfig {
            top_k: 5,
            candidate_shards: 2,
            rerank_limit: Some(20),
            distance_metric: None,
        };
        assert_eq!(cfg.effective_candidates(), 20);
    }
}
