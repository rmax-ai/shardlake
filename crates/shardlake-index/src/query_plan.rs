//! Query-plan capture model for debugging search execution.

use serde::{Deserialize, Serialize};

use shardlake_core::types::{SearchResult, ShardId};

/// Diagnostic snapshot captured during a single query execution.
///
/// Returned by [`crate::IndexSearcher::search_with_plan`] and exposed through
/// the `/debug/query-plan` HTTP endpoint so offline tooling can inspect exactly
/// which centroids were selected, which shards were probed, and which candidate
/// vectors were returned without reranking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    /// Centroid vectors selected during the IVF routing step (one entry per
    /// selected centroid, in selection order).
    pub selected_centroids: Vec<Vec<f32>>,
    /// Shard IDs probed after centroid-to-shard mapping and deduplication, in
    /// probe order.
    pub searched_shards: Vec<ShardId>,
    /// Candidate vectors returned by the fan-out search before any reranking.
    /// These are the same results that [`crate::IndexSearcher::search`] would
    /// return for the same query and policy.
    pub candidate_vectors: Vec<SearchResult>,
}
