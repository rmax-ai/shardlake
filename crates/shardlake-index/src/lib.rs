//! Index building and approximate nearest-neighbour search.
//!
//! This crate provides two complementary search paths:
//!
//! * **IVF + exact** ([`IndexSearcher`]): K-means sharding with brute-force search
//!   within each probed shard. Build with [`IndexBuilder`].
//! * **IVF + PQ** ([`IvfPqIndex`]): coarse IVF quantizer whose posting lists store
//!   PQ-compressed vectors. Supports optional exact reranking. Build with
//!   [`IvfPqIndex::build`] and persist with [`IvfPqIndex::to_bytes`].

pub mod builder;
pub mod exact;
pub mod ivf_pq;
pub mod kmeans;
pub mod pq;
pub mod searcher;
pub mod shard;

pub use builder::{BuildParams, IndexBuilder};
pub use exact::ExactSearcher;
pub use ivf_pq::IvfPqIndex;
pub use pq::PqCodebook;
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
