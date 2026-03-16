//! Global top-*k* merge stage for combining shard-local candidate sets.
//!
//! After each probed shard returns its local candidate list, all lists are
//! fed to [`GlobalMerge`] to produce a single, globally-ordered top-*k*
//! result that is used as input to the optional rerank stage.
//!
//! # Ordering rules
//!
//! Results are ranked by **score ascending** (lower score = closer neighbour
//! for distance-based metrics; inner-product scores are already negated so
//! that lower still means better).
//!
//! When two distinct candidates share an identical score the **lower
//! [`VectorId`] wins**, giving a fully deterministic order that is stable
//! across runs regardless of the order in which shards return their results.
//!
//! # Deduplication
//!
//! The same [`VectorId`] may appear in multiple shard-local lists when a
//! vector was replicated during index construction.  [`GlobalMerge`] keeps
//! only the copy with the **best (lowest) score** for that ID and discards
//! the rest before applying the top-*k* cut.

use std::collections::{hash_map::Entry, HashMap};

use shardlake_core::types::{SearchResult, VectorId};

use crate::pipeline::MergeStage;

/// Deterministic global top-*k* merge over shard-local candidate lists.
///
/// See the [module documentation](self) for ordering and tie-breaking rules.
pub struct GlobalMerge;

impl MergeStage for GlobalMerge {
    /// Merge `results` from all probed shards and return the global top `k`.
    fn merge(&self, results: Vec<SearchResult>, k: usize) -> Vec<SearchResult> {
        merge_global_top_k(results, k)
    }
}

/// Merge `results` from multiple shards into the global top-`k`.
///
/// **Deduplication**: when the same [`VectorId`] appears more than once only
/// the entry with the lowest score is kept.
///
/// **Ordering**: results are sorted by score ascending; ties are broken by
/// [`VectorId`] ascending so that the output is identical regardless of shard
/// enumeration order.
///
/// Returns an empty [`Vec`] when `k == 0` or `results` is empty.
pub fn merge_global_top_k(results: Vec<SearchResult>, k: usize) -> Vec<SearchResult> {
    if k == 0 || results.is_empty() {
        return Vec::new();
    }

    // Deduplicate: keep the entry with the lowest (best) score per VectorId.
    let mut best: HashMap<VectorId, SearchResult> = HashMap::with_capacity(results.len());
    for result in results {
        match best.entry(result.id) {
            Entry::Occupied(mut e) => {
                if result.score < e.get().score {
                    e.insert(result);
                }
            }
            Entry::Vacant(e) => {
                e.insert(result);
            }
        }
    }

    // Sort: primary key = score ascending, secondary = VectorId ascending.
    //
    // `partial_cmp` returns `None` only when a score is NaN, which should not
    // occur under normal operation.  We fall back to `Equal` to match the
    // convention used throughout the rest of the codebase; NaN-scored entries
    // will cluster unpredictably but this avoids a panic on corrupt data.
    let mut deduped: Vec<SearchResult> = best.into_values().collect();
    deduped.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.id.cmp(&b.id))
    });

    deduped.truncate(k);
    deduped
}
