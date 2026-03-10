//! Query plan types for debugging and introspection.

use serde::Serialize;
use shardlake_core::types::SearchResult;

/// A detailed execution plan produced by [`crate::IndexSearcher::search_with_plan`].
///
/// Use this to inspect which centroids were selected, which shards were probed,
/// which candidates were gathered, and what the final ranked results are.
#[derive(Debug, Clone, Serialize)]
pub struct QueryPlan {
    /// The centroid vectors selected for probing (one per selected centroid index).
    pub selected_centroids: Vec<Vec<f32>>,
    /// The shard IDs that were searched.
    pub searched_shards: Vec<u32>,
    /// All candidate results gathered across all probed shards before the final merge.
    pub candidate_vectors: Vec<SearchResult>,
    /// Final top-k results after merging and reranking.
    pub results: Vec<SearchResult>,
}
