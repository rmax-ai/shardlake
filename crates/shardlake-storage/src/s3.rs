//! S3-compatible object-store backend stub.
//!
//! This module provides an initial [`S3CompatibleBackend`] that compiles and
//! satisfies the [`ObjectStore`] interface, but does **not** yet perform any
//! real network I/O.  Every operation returns
//! [`StorageError::Other`] with a clear "not yet implemented" message.
//!
//! # Non-goals (current stub)
//!
//! The following capabilities are intentionally **out of scope** for this
//! stub and will be addressed in follow-up work:
//!
//! - Actual HTTP requests to any S3-compatible service (AWS S3, MinIO, GCS, etc.)
//! - Authentication / credential refresh
//! - Multipart upload for large objects
//! - Presigned URL generation
//! - Streaming / range-request `get`
//! - List pagination beyond a single response
//! - Server-side encryption (SSE)
//! - Object versioning
//! - Retry / back-off logic
//!
//! # Usage
//!
//! Downstream code that needs to compile against the storage abstraction can
//! construct an [`S3CompatibleBackend`] and pass it as `dyn ObjectStore`,
//! but must not call any of its methods in production until a real
//! implementation replaces this stub.
//!
//! ```rust
//! use shardlake_storage::s3::{S3CompatibleBackend, S3Config};
//! use shardlake_storage::ObjectStore;
//!
//! let cfg = S3Config {
//!     endpoint: "https://s3.amazonaws.com".into(),
//!     bucket: "my-bucket".into(),
//!     region: "us-east-1".into(),
//!     access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
//!     secret_access_key: "test-secret-DO-NOT-USE".into(),
//! };
//! let _backend = S3CompatibleBackend::new(cfg);
//! ```

use tracing::warn;

use crate::{ObjectStore, Result, StorageError};

// ── Configuration ─────────────────────────────────────────────────────────────

/// Configuration for an S3-compatible object-store backend.
///
/// All fields are plain strings so that the type is easy to construct from
/// environment variables, config files, or CLI flags without pulling in extra
/// dependencies. Sensitive fields (access key, secret key) are stored in
/// memory only; callers are responsible for not logging these values.
///
/// A custom [`Debug`] implementation is provided that redacts both credential
/// fields to prevent accidental leaks in logs.
#[derive(Clone)]
pub struct S3Config {
    /// HTTP(S) endpoint URL, e.g. `https://s3.amazonaws.com` or a MinIO base
    /// URL such as `http://localhost:9000`.
    pub endpoint: String,
    /// Target bucket name.
    pub bucket: String,
    /// AWS-style region identifier (e.g. `us-east-1`).
    ///
    /// For MinIO or other S3-compatible services that do not use AWS regions,
    /// this may be set to any non-empty string.
    pub region: String,
    ///
    /// **Keep this value out of logs and traces.**
    /// AWS access key ID (or equivalent credential for S3-compatible services).
    pub access_key_id: String,
    /// AWS secret access key (or equivalent credential).
    ///
    /// **Keep this value out of logs and traces.**
    pub secret_access_key: String,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .finish()
    }
}

// ── Backend ────────────────────────────────────────────────────────────────────

/// S3-compatible object-store backend stub.
///
/// Implements [`ObjectStore`] so that it satisfies type-level requirements and
/// downstream code compiles, but every operation currently returns
/// [`StorageError::Other`] with a descriptive error message.
///
/// See the [module-level documentation][self] for the full list of non-goals.
pub struct S3CompatibleBackend {
    config: S3Config,
}

impl S3CompatibleBackend {
    /// Create a new stub backend from the given [`S3Config`].
    ///
    /// Construction always succeeds; no network connection is made.
    #[must_use]
    pub fn new(config: S3Config) -> Self {
        warn!(
            endpoint = %config.endpoint,
            bucket = %config.bucket,
            "S3CompatibleBackend is a stub — no real network I/O will be performed",
        );
        Self { config }
    }

    /// Returns a reference to the configuration used to create this backend.
    #[must_use]
    pub fn config(&self) -> &S3Config {
        &self.config
    }
}

impl ObjectStore for S3CompatibleBackend {
    fn put(&self, key: &str, _data: Vec<u8>) -> Result<()> {
        Err(StorageError::Other(format!(
            "S3CompatibleBackend::put({key:?}) is not yet implemented"
        )))
    }

    fn get(&self, key: &str) -> Result<Vec<u8>> {
        Err(StorageError::Other(format!(
            "S3CompatibleBackend::get({key:?}) is not yet implemented"
        )))
    }

    fn exists(&self, key: &str) -> Result<bool> {
        Err(StorageError::Other(format!(
            "S3CompatibleBackend::exists({key:?}) is not yet implemented"
        )))
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Err(StorageError::Other(format!(
            "S3CompatibleBackend::list({prefix:?}) is not yet implemented"
        )))
    }

    fn delete(&self, key: &str) -> Result<()> {
        Err(StorageError::Other(format!(
            "S3CompatibleBackend::delete({key:?}) is not yet implemented"
        )))
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_backend() -> S3CompatibleBackend {
        S3CompatibleBackend::new(S3Config {
            endpoint: "https://s3.example.com".into(),
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            access_key_id: "AKIATEST".into(),
            secret_access_key: "test-secret-DO-NOT-USE".into(),
        })
    }

    #[test]
    fn construction_succeeds_and_config_is_accessible() {
        let backend = make_backend();
        assert_eq!(backend.config().endpoint, "https://s3.example.com");
        assert_eq!(backend.config().bucket, "test-bucket");
        assert_eq!(backend.config().region, "us-east-1");
    }

    #[test]
    fn all_operations_return_storage_error_other() {
        let backend = make_backend();

        assert!(matches!(
            backend.put("some/key", b"data".to_vec()),
            Err(StorageError::Other(_))
        ));
        assert!(matches!(
            backend.get("some/key"),
            Err(StorageError::Other(_))
        ));
        assert!(matches!(
            backend.exists("some/key"),
            Err(StorageError::Other(_))
        ));
        assert!(matches!(backend.list("some/"), Err(StorageError::Other(_))));
        assert!(matches!(
            backend.delete("some/key"),
            Err(StorageError::Other(_))
        ));
    }

    #[test]
    fn error_messages_include_the_key() {
        let backend = make_backend();

        let err = backend.get("my/artifact/key").unwrap_err();
        assert!(err.to_string().contains("my/artifact/key"));
    }

    #[test]
    fn debug_output_redacts_credentials() {
        let cfg = S3Config {
            endpoint: "https://s3.example.com".into(),
            bucket: "test-bucket".into(),
            region: "us-east-1".into(),
            access_key_id: "AKIATEST".into(),
            secret_access_key: "super-secret".into(),
        };
        let debug_str = format!("{cfg:?}");
        assert!(!debug_str.contains("AKIATEST"));
        assert!(!debug_str.contains("super-secret"));
        assert_eq!(debug_str.matches("<redacted>").count(), 2);
    }
}
