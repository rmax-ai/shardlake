//! Object-store abstraction for artifact persistence.
//!
//! Currently only a local filesystem backend is provided.
//! A future backend (S3, MinIO) only needs to implement [`ObjectStore`].

pub mod local;

pub use local::LocalObjectStore;

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
