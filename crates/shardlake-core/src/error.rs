use thiserror::Error;

pub type Result<T> = std::result::Result<T, CoreError>;

/// Core domain errors.
#[derive(Debug, Error)]
pub enum CoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialisation error: {0}")]
    Serialisation(#[from] serde_json::Error),

    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },

    #[error("invalid magic bytes in artifact")]
    InvalidMagic,

    #[error("unsupported format version {0}")]
    UnsupportedVersion(u32),

    #[error("empty dataset")]
    EmptyDataset,

    #[error("shard {0} not found")]
    ShardNotFound(u32),

    #[error("manifest not found at {0}")]
    ManifestNotFound(String),

    #[error("invalid fan-out policy: {0}")]
    InvalidFanOutPolicy(String),

    #[error("invalid prefetch policy: {0}")]
    InvalidPrefetchPolicy(String),

    #[error("{0}")]
    Other(String),
}
