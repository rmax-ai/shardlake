use serde::{Deserialize, Serialize};

/// Unique identifier for a stored vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VectorId(pub u64);

impl std::fmt::Display for VectorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Identifies a shard partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardId(pub u32);

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shard-{:04}", self.0)
    }
}

/// Opaque version string for a dataset artifact.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DatasetVersion(pub String);

impl std::fmt::Display for DatasetVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Opaque version string for an embedding artifact.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EmbeddingVersion(pub String);

impl std::fmt::Display for EmbeddingVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Opaque version string for an index artifact.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexVersion(pub String);

impl std::fmt::Display for IndexVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Identifies the ANN algorithm family used to build and query an index.
///
/// This type is the key used by `AnnPlugin` implementations and `AnnRegistry`
/// to select the right candidate-search backend without hard-coding algorithm
/// checks at call sites.
///
/// # Examples
///
/// ```rust
/// use shardlake_core::AnnFamily;
///
/// let family = "ivf_flat".parse::<AnnFamily>().unwrap();
/// assert_eq!(family.as_str(), "ivf_flat");
/// assert_eq!(family.to_string(), "ivf_flat");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnnFamily {
    /// IVF with exact (flat) distance scoring within each shard.
    #[default]
    IvfFlat,
    /// IVF with product-quantised distance scoring within each shard.
    IvfPq,
    /// Experimental ANN backend loosely inspired by DiskANN.
    ///
    /// Uses a bounded strided probe over each shard's flat vector list rather
    /// than a navigable graph search. Supports Euclidean distance only. The
    /// beam width acts as a probe budget that trades query latency against
    /// recall quality.
    DiskAnn,
}

impl AnnFamily {
    /// Returns the canonical string identifier for this family.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IvfFlat => "ivf_flat",
            Self::IvfPq => "ivf_pq",
            Self::DiskAnn => "diskann",
        }
    }
}

impl std::fmt::Display for AnnFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for AnnFamily {
    type Err = crate::error::CoreError;

    /// Parse a family name produced by [`AnnFamily::as_str`].
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::CoreError::Other`] when `s` does not match any
    /// known family.
    fn from_str(s: &str) -> crate::error::Result<Self> {
        match s {
            "ivf_flat" => Ok(Self::IvfFlat),
            "ivf_pq" => Ok(Self::IvfPq),
            "diskann" => Ok(Self::DiskAnn),
            other => Err(crate::error::CoreError::Other(format!(
                "unknown ANN family: \"{other}\"; valid values are: ivf_flat, ivf_pq, diskann"
            ))),
        }
    }
}

/// Supported distance metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum DistanceMetric {
    #[default]
    Cosine,
    Euclidean,
    InnerProduct,
}

/// Query retrieval mode.
///
/// Controls which search backend(s) are engaged for a query:
///
/// - **`Vector`** (default) – approximate nearest-neighbour search against the
///   IVF vector index.  Requires a query `vector`.
/// - **`Lexical`** – BM25 full-text search against the lexical index.  Requires
///   `query_text`.  The vector field is ignored.  The index must have been built
///   with a lexical artifact.
/// - **`Hybrid`** – runs both vector and lexical search, then blends the scores
///   using [`shardlake_index::ranking::rank_hybrid`].  Requires both `vector`
///   and `query_text`.
///
/// Invalid or unsupported modes are rejected at the query surface (HTTP handler
/// or CLI) before any search work is performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum QueryMode {
    /// Vector-only approximate nearest-neighbour search (default).
    #[default]
    Vector,
    /// Lexical-only BM25 full-text search.
    Lexical,
    /// Hybrid: blend vector-distance and BM25 scores.
    Hybrid,
}

impl std::fmt::Display for QueryMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryMode::Vector => write!(f, "vector"),
            QueryMode::Lexical => write!(f, "lexical"),
            QueryMode::Hybrid => write!(f, "hybrid"),
        }
    }
}

impl std::fmt::Display for DistanceMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistanceMetric::Cosine => write!(f, "cosine"),
            DistanceMetric::Euclidean => write!(f, "euclidean"),
            DistanceMetric::InnerProduct => write!(f, "inner_product"),
        }
    }
}

/// A raw vector with its id and optional metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorRecord {
    pub id: VectorId,
    pub data: Vec<f32>,
    pub metadata: Option<serde_json::Value>,
}

/// A single search result returned to callers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub id: VectorId,
    /// Lower is better for distance metrics, higher for inner-product.
    pub score: f32,
    pub metadata: Option<serde_json::Value>,
}
