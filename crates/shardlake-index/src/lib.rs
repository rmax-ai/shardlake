//! Index building and approximate nearest-neighbour search.

pub mod bm25;
pub mod builder;
pub mod cache;
pub mod exact;
pub mod ivf;
pub mod kmeans;
pub mod merge;
pub mod metrics;
pub mod pipeline;
pub mod pq;
pub mod searcher;
pub mod shard;
pub mod validator;
pub mod worker;

pub use bm25::{tokenize, BM25Params, Bm25Index, BM25_MAGIC};
pub use builder::{BuildParams, IndexBuilder};
pub use cache::{ShardCache, DEFAULT_SHARD_CACHE_CAPACITY};
pub use exact::ExactSearcher;
pub use ivf::IvfQuantizer;
pub use merge::GlobalMerge;
pub use metrics::{CacheMetrics, CacheMetricsSnapshot};
pub use pipeline::{
    CachedShardLoader, CandidateSearchStage, CentroidRouter, EmbedStage, ExactCandidateSearch,
    ExactCandidateStage, ExactRerankStage, IdentityEmbedder, LoadShardStage, MergeStage,
    MmapShardLoader, NoopReranker, PqCandidateStage, QueryPipeline, QueryPipelineBuilder,
    RerankStage, RouteStage, TopKMerge, MMAP_MIN_SIZE_BYTES,
};
pub use pq::{PqCodebook, PqParams};
pub use searcher::IndexSearcher;
pub use shard::{PqShard, ShardIndex, SHARD_MAGIC};
pub use validator::{ValidationFailure, ValidationReport};
pub use worker::{
    merge_worker_outputs, plan_workers, MergeParams, WorkerAssignment, WorkerBuilder, WorkerOutput,
    WorkerPlan, WorkerPlanParams, WorkerShardOutput,
};

/// Codec identifier for 8-bit product quantisation.
///
/// Used in [`shardlake_manifest::CompressionConfig::codec`] to identify
/// PQ-compressed indexes and referenced by the builder, searcher, and
/// validator to distinguish PQ shards from raw-vector shards.
pub const PQ8_CODEC: &str = "pq8";

/// Errors that can arise in index operations.
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("core error: {0}")]
    Core(#[from] shardlake_core::error::CoreError),
    #[error("storage error: {0}")]
    Storage(#[from] shardlake_storage::StorageError),
    #[error("manifest error: {0}")]
    Manifest(#[from] shardlake_manifest::ManifestError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, IndexError>;

/// Compute the FNV-1a fingerprint of `bytes` and return it as a lowercase
/// 16-digit hex string.
///
/// This is a fast, non-cryptographic hash used to detect accidental artifact
/// corruption and to enable deduplication during prototyping.  It is shared by
/// the builder (which records fingerprints in the manifest) and the validator
/// (which recomputes them from stored artifact bytes for comparison).
pub fn artifact_fingerprint(bytes: &[u8]) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{h:016x}")
}
