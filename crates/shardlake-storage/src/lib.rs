//! Object-store abstraction for artifact persistence.
//!
//! Two backends are shipped in this crate:
//!
//! - [`LocalObjectStore`] — production-ready local-filesystem backend.
//! - [`s3::S3CompatibleBackend`] — initial stub for S3-compatible services
//!   (AWS S3, MinIO, GCS, etc.).  Compiles against the abstraction but does
//!   not yet perform real network I/O.  See [`s3`] for non-goals and roadmap.
//!
//! Additional backends only need to implement [`ObjectStore`].

pub mod local;
pub mod paths;
pub mod s3;

pub use local::LocalObjectStore;
pub use s3::{S3CompatibleBackend, S3Config};

/// Errors surfaced by storage operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("invalid key {key:?}: {reason}")]
    InvalidKey { key: String, reason: String },
    #[error("key not found: {0}")]
    NotFound(String),
    #[error("I/O error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Minimal object-store interface.
pub trait ObjectStore: Send + Sync {
    /// Store `data` at `key`, creating intermediate directories as needed.
    fn put(&self, key: &str, data: Vec<u8>) -> Result<()>;
    /// Retrieve the bytes stored at `key`.
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    /// Return true if `key` exists.
    fn exists(&self, key: &str) -> Result<bool>;
    /// List all keys that start with `prefix`.
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
    /// Delete `key`.
    fn delete(&self, key: &str) -> Result<()>;
}
