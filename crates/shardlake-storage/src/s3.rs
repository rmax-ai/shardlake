//! S3-compatible object-storage backend (stub).
//!
//! This module provides a placeholder that satisfies [`ObjectStore`] and
//! [`StorageBackend`] so that the rest of the system can be compiled and
//! tested end-to-end without a live object-storage service.  Every method
//! returns [`StorageError::Other`] with an "not yet implemented" message.
//!
//! A real implementation would carry AWS credentials, region, endpoint
//! overrides, and an async HTTP client.  The stub intentionally omits all of
//! that to keep the surface minimal until the feature is needed.

use crate::{ObjectStore, Result, StorageBackend, StorageError};

/// S3-compatible storage backend stub.
///
/// # Examples
///
/// ```rust
/// use shardlake_storage::{S3CompatibleBackend, ObjectStore};
///
/// let backend = S3CompatibleBackend::new("my-bucket", None, "shardlake/");
/// assert_eq!(backend.bucket, "my-bucket");
/// // All operations return an "is not yet implemented" error.
/// assert!(backend.exists("some/key").is_err());
/// ```
pub struct S3CompatibleBackend {
    /// S3 bucket name.
    pub bucket: String,
    /// Optional endpoint URL for S3-compatible services (e.g. MinIO, Ceph).
    pub endpoint: Option<String>,
    /// Key prefix prepended to every object path.
    pub prefix: String,
}

impl S3CompatibleBackend {
    /// Create a new stub backend targeting `bucket`.
    ///
    /// * `bucket`   – bucket name.
    /// * `endpoint` – optional custom endpoint URL (e.g. `"http://minio:9000"`).
    /// * `prefix`   – key prefix, e.g. `"shardlake/"`.
    pub fn new(
        bucket: impl Into<String>,
        endpoint: Option<String>,
        prefix: impl Into<String>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            endpoint,
            prefix: prefix.into(),
        }
    }

    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{key}", self.prefix.trim_end_matches('/'))
        }
    }
}

impl ObjectStore for S3CompatibleBackend {
    fn put(&self, key: &str, _data: Vec<u8>) -> Result<()> {
        Err(StorageError::Other(format!(
            "S3 backend is not yet implemented: put({})",
            self.full_key(key)
        )))
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        Err(StorageError::Other(format!(
            "S3 backend is not yet implemented: get({})",
            self.full_key(key)
        )))
    }

    fn exists(&self, key: &str) -> Result<bool> {
        Err(StorageError::Other(format!(
            "S3 backend is not yet implemented: exists({})",
            self.full_key(key)
        )))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Err(StorageError::Other(format!(
            "S3 backend is not yet implemented: list({})",
            self.full_key(prefix)
        )))
    }

    fn delete(&self, key: &str) -> Result<()> {
        Err(StorageError::Other(format!(
            "S3 backend is not yet implemented: delete({})",
            self.full_key(key)
        )))
    }
}

/// `S3CompatibleBackend` does not expose a local filesystem path; the default
/// implementation of `path_for_key` (returning `None`) is therefore correct.
impl StorageBackend for S3CompatibleBackend {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_returns_error() {
        let b = S3CompatibleBackend::new("bucket", None, "pfx/");
        assert!(b.put("k", vec![]).is_err());
        assert!(b.get("k").is_err());
        assert!(b.exists("k").is_err());
        assert!(b.list("k").is_err());
        assert!(b.delete("k").is_err());
    }

    #[test]
    fn path_for_key_is_none() {
        let b = S3CompatibleBackend::new("bucket", None, "");
        assert!(b.path_for_key("some/key").is_none());
    }

    #[test]
    fn full_key_with_prefix() {
        let b = S3CompatibleBackend::new("bucket", None, "pfx/");
        assert_eq!(b.full_key("a/b"), "pfx/a/b");
    }

    #[test]
    fn full_key_without_prefix() {
        let b = S3CompatibleBackend::new("bucket", None, "");
        assert_eq!(b.full_key("a/b"), "a/b");
    }
}
