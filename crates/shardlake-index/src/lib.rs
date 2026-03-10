//! Index building and approximate nearest-neighbour search.

pub mod builder;
pub mod cache;
pub mod exact;
pub mod kmeans;
pub mod searcher;
pub mod shard;

pub use builder::{BuildParams, IndexBuilder};
pub use cache::{CacheConfig, CacheMetrics, ShardCache};
pub use exact::ExactSearcher;
pub use searcher::IndexSearcher;
pub use shard::{ShardIndex, SHARD_MAGIC};

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
