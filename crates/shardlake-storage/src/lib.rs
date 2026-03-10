//! Object-store abstraction for artifact persistence.
//!
//! The crate exposes two traits:
//!
//! * [`ObjectStore`] – minimal key/value interface used throughout the system.
//! * [`StorageBackend`] – extends `ObjectStore` with optional filesystem-path
//!   access, enabling memory-mapped I/O for large shard files.
//!
//! Two implementations are provided:
//! * [`LocalObjectStore`] / [`LocalFilesystemBackend`] – local filesystem.
//! * [`S3CompatibleBackend`] – stub for AWS S3 / MinIO (not yet implemented).

pub mod local;
pub mod s3;

pub use local::LocalObjectStore;
/// Alias for [`LocalObjectStore`] using the canonical backend naming convention.
pub type LocalFilesystemBackend = LocalObjectStore;
pub use s3::S3CompatibleBackend;

/// Errors surfaced by storage operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
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

/// Extended storage backend that optionally exposes a local filesystem path.
///
/// Backends that store objects on the local filesystem should override
/// [`path_for_key`] to return the absolute path of the file backing `key`.
/// This enables callers (e.g. the shard cache) to memory-map large files
/// instead of reading them into a heap-allocated `Vec<u8>`.
///
/// Backends that do **not** have a local filesystem representation (e.g.
/// [`S3CompatibleBackend`]) should leave the default implementation, which
/// returns `None`.
pub trait StorageBackend: ObjectStore {
    /// Return the local filesystem path that backs `key`, or `None`.
    ///
    /// The default implementation always returns `None`.
    fn path_for_key(&self, key: &str) -> Option<std::path::PathBuf> {
        let _ = key;
        None
    }
}
