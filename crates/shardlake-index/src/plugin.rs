//! ANN plugin interface.
//!
//! Every ANN backend must implement [`AnnPlugin`], which is the single entry
//! point for validating algorithm parameters and producing a
//! [`CandidateSearchStage`] for the query pipeline.  This design eliminates
//! algorithm-specific branching at the integration edges: callers simply select
//! a plugin by name via [`AnnRegistry`] and hand it to
//! [`QueryPipelineBuilder`](crate::QueryPipelineBuilder).
//!
//! # Built-in backends
//!
//! | Family | Struct | Notes |
//! |--------|--------|-------|
//! | `"ivf_flat"` | [`IvfFlatPlugin`] | Exact (flat) distance scoring, all metrics supported |
//! | `"ivf_pq"` | [`IvfPqPlugin`] | Product-quantised scoring, Euclidean only |
//!
//! # Example – selecting a backend via the registry
//!
//! ```rust,ignore
//! use shardlake_index::plugin::{AnnRegistry, IvfFlatPlugin};
//! use shardlake_core::DistanceMetric;
//!
//! // Validate that ivf_flat is compatible with the chosen metric.
//! let plugin = AnnRegistry::get_flat("ivf_flat").unwrap();
//! plugin.validate(128, DistanceMetric::Cosine).unwrap();
//!
//! // Wire the candidate stage into the query pipeline.
//! let pipeline = QueryPipeline::builder(store, manifest)
//!     .candidate_stage(plugin.candidate_stage())
//!     .build();
//! ```

use std::sync::Arc;

use shardlake_core::{AnnFamily, DistanceMetric};

use crate::{
    pipeline::{CandidateSearchStage, ExactCandidateSearch, PqCandidateStage},
    pq::PqCodebook,
    IndexError, Result,
};

// ── AnnPlugin trait ───────────────────────────────────────────────────────────

/// Shared interface every ANN backend must implement.
///
/// An `AnnPlugin` can validate its own configuration against a vector
/// dimension and distance metric, and produce a [`CandidateSearchStage`] for
/// the query pipeline.  New backends implement this trait and are discovered
/// through [`AnnRegistry`].
///
/// # Implementing a custom backend
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use shardlake_core::DistanceMetric;
/// use shardlake_index::{
///     plugin::AnnPlugin,
///     pipeline::CandidateSearchStage,
///     Result,
/// };
///
/// struct MyPlugin;
///
/// impl AnnPlugin for MyPlugin {
///     fn family(&self) -> &str { "my_backend" }
///
///     fn validate(&self, _dims: usize, _metric: DistanceMetric) -> Result<()> {
///         Ok(())
///     }
///
///     fn candidate_stage(&self) -> Arc<dyn CandidateSearchStage> {
///         // return your implementation
///         todo!()
///     }
/// }
/// ```
pub trait AnnPlugin: Send + Sync {
    /// Human-readable family identifier, e.g. `"ivf_flat"` or `"ivf_pq"`.
    ///
    /// The value must match the string produced by
    /// [`AnnFamily::as_str`] for the built-in backends, and must be stable
    /// across process restarts for externally-authored plugins.
    fn family(&self) -> &str;

    /// Validate that this backend is compatible with the given vector
    /// dimension and distance metric before building or querying.
    ///
    /// # Errors
    ///
    /// Returns an [`IndexError`] describing the incompatibility when
    /// validation fails.
    fn validate(&self, dims: usize, metric: DistanceMetric) -> Result<()>;

    /// Create a [`CandidateSearchStage`] that will be used by the query
    /// pipeline when this backend is selected.
    fn candidate_stage(&self) -> Arc<dyn CandidateSearchStage>;
}

// ── IvfFlatPlugin ─────────────────────────────────────────────────────────────

/// ANN plugin for the IVF-flat backend.
///
/// Performs exact (brute-force) distance scoring within each probed shard.
/// Supports all [`DistanceMetric`] variants and imposes no constraints on the
/// vector dimension.
pub struct IvfFlatPlugin;

impl AnnPlugin for IvfFlatPlugin {
    fn family(&self) -> &str {
        AnnFamily::IvfFlat.as_str()
    }

    /// Always succeeds: IVF-flat is compatible with every metric and dimension.
    fn validate(&self, _dims: usize, _metric: DistanceMetric) -> Result<()> {
        Ok(())
    }

    fn candidate_stage(&self) -> Arc<dyn CandidateSearchStage> {
        Arc::new(ExactCandidateSearch)
    }
}

// ── IvfPqPlugin ───────────────────────────────────────────────────────────────

/// ANN plugin for the IVF-PQ backend.
///
/// Performs product-quantised asymmetric distance computation within each
/// probed shard.  Requires a trained [`PqCodebook`] and only supports
/// [`DistanceMetric::Euclidean`].
pub struct IvfPqPlugin {
    codebook: Arc<PqCodebook>,
}

impl IvfPqPlugin {
    /// Create a new plugin from a trained `codebook`.
    pub fn new(codebook: Arc<PqCodebook>) -> Self {
        Self { codebook }
    }

    /// Return a reference to the underlying codebook.
    pub fn codebook(&self) -> &PqCodebook {
        &self.codebook
    }
}

impl AnnPlugin for IvfPqPlugin {
    fn family(&self) -> &str {
        AnnFamily::IvfPq.as_str()
    }

    /// Validates that `metric` is [`DistanceMetric::Euclidean`].
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::Other`] when a non-Euclidean metric is supplied.
    fn validate(&self, _dims: usize, metric: DistanceMetric) -> Result<()> {
        if metric != DistanceMetric::Euclidean {
            return Err(IndexError::Other(format!(
                "ANN family \"{}\" requires euclidean distance, got {metric}",
                self.family()
            )));
        }
        Ok(())
    }

    fn candidate_stage(&self) -> Arc<dyn CandidateSearchStage> {
        Arc::new(PqCandidateStage::new(Arc::clone(&self.codebook)))
    }
}

// ── AnnRegistry ───────────────────────────────────────────────────────────────

/// Registry of known ANN family names.
///
/// [`AnnRegistry`] is intentionally **stateless**: it validates that a
/// requested family name is known and returns a ready-to-use [`IvfFlatPlugin`]
/// for families that need no runtime artifacts.  Families that require
/// artifacts loaded at query time (e.g. `"ivf_pq"`, which needs a codebook)
/// must be constructed directly from their plugin struct with the artifact.
///
/// # Examples
///
/// ```rust,ignore
/// use shardlake_index::plugin::AnnRegistry;
///
/// // Enumerate all built-in families.
/// for name in AnnRegistry::families() {
///     println!("{name}");
/// }
///
/// // Validate a family name supplied by a user.
/// AnnRegistry::exists("ivf_flat"); // true
/// AnnRegistry::exists("hnsw");    // false
/// ```
pub struct AnnRegistry;

impl AnnRegistry {
    /// Returns the names of all built-in ANN families.
    pub fn families() -> &'static [&'static str] {
        &["ivf_flat", "ivf_pq"]
    }

    /// Returns `true` if `family` is a known built-in ANN family name.
    pub fn exists(family: &str) -> bool {
        Self::families().contains(&family)
    }

    /// Return an [`IvfFlatPlugin`] for `"ivf_flat"`, or an error for any
    /// other family name.
    ///
    /// For families that need runtime artifacts (like `"ivf_pq"`) construct
    /// the plugin directly, e.g.
    /// [`IvfPqPlugin::new(codebook)`](IvfPqPlugin::new).
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::Other`] when `family` is not `"ivf_flat"` or
    /// is an unknown name.
    pub fn get_flat(family: &str) -> Result<Box<dyn AnnPlugin>> {
        match family {
            "ivf_flat" => Ok(Box::new(IvfFlatPlugin)),
            "ivf_pq" => Err(IndexError::Other(
                "family \"ivf_pq\" requires a PQ codebook; \
                 construct IvfPqPlugin::new(codebook) directly"
                    .into(),
            )),
            other => Err(IndexError::Other(format!(
                "unknown ANN family: \"{other}\"; valid values are: {}",
                Self::families().join(", ")
            ))),
        }
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use shardlake_core::DistanceMetric;

    // ── AnnFamily round-trip ──────────────────────────────────────────────────

    #[test]
    fn ann_family_as_str_round_trips() {
        for name in ["ivf_flat", "ivf_pq"] {
            let family: shardlake_core::AnnFamily = name.parse().unwrap();
            assert_eq!(family.as_str(), name);
            assert_eq!(family.to_string(), name);
        }
    }

    #[test]
    fn ann_family_unknown_name_returns_error() {
        let err = "hnsw".parse::<shardlake_core::AnnFamily>().unwrap_err();
        assert!(err.to_string().contains("unknown ANN family"));
        assert!(err.to_string().contains("hnsw"));
    }

    // ── IvfFlatPlugin ─────────────────────────────────────────────────────────

    #[test]
    fn ivf_flat_plugin_family_name() {
        assert_eq!(IvfFlatPlugin.family(), "ivf_flat");
    }

    #[test]
    fn ivf_flat_plugin_validate_accepts_all_metrics() {
        let plugin = IvfFlatPlugin;
        for metric in [
            DistanceMetric::Cosine,
            DistanceMetric::Euclidean,
            DistanceMetric::InnerProduct,
        ] {
            assert!(
                plugin.validate(128, metric).is_ok(),
                "should accept {metric}"
            );
        }
    }

    #[test]
    fn ivf_flat_plugin_candidate_stage_is_sendable() {
        let stage = IvfFlatPlugin.candidate_stage();
        // Verify the stage is Arc<dyn CandidateSearchStage + Send + Sync>.
        let _: Arc<dyn CandidateSearchStage> = stage;
    }

    // ── IvfPqPlugin ───────────────────────────────────────────────────────────

    fn make_codebook() -> Arc<PqCodebook> {
        use crate::pq::PqParams;
        let params = PqParams {
            num_subspaces: 2,
            codebook_size: 4,
        };
        let records: Vec<Vec<f32>> = (0..16_u32)
            .map(|i| vec![i as f32, i as f32 + 1.0, i as f32 + 2.0, i as f32 + 3.0])
            .collect();
        Arc::new(PqCodebook::train(&records, params, 42, 5).unwrap())
    }

    #[test]
    fn ivf_pq_plugin_family_name() {
        let plugin = IvfPqPlugin::new(make_codebook());
        assert_eq!(plugin.family(), "ivf_pq");
    }

    #[test]
    fn ivf_pq_plugin_validate_accepts_euclidean() {
        let plugin = IvfPqPlugin::new(make_codebook());
        assert!(plugin.validate(4, DistanceMetric::Euclidean).is_ok());
    }

    #[test]
    fn ivf_pq_plugin_validate_rejects_cosine() {
        let plugin = IvfPqPlugin::new(make_codebook());
        let err = plugin.validate(4, DistanceMetric::Cosine).unwrap_err();
        assert!(err.to_string().contains("euclidean"));
    }

    #[test]
    fn ivf_pq_plugin_validate_rejects_inner_product() {
        let plugin = IvfPqPlugin::new(make_codebook());
        let err = plugin
            .validate(4, DistanceMetric::InnerProduct)
            .unwrap_err();
        assert!(err.to_string().contains("euclidean"));
    }

    #[test]
    fn ivf_pq_plugin_candidate_stage_is_sendable() {
        let stage = IvfPqPlugin::new(make_codebook()).candidate_stage();
        let _: Arc<dyn CandidateSearchStage> = stage;
    }

    // ── AnnRegistry ───────────────────────────────────────────────────────────

    #[test]
    fn registry_families_contains_both_builtins() {
        let families = AnnRegistry::families();
        assert!(families.contains(&"ivf_flat"));
        assert!(families.contains(&"ivf_pq"));
    }

    #[test]
    fn registry_exists_returns_true_for_known_names() {
        assert!(AnnRegistry::exists("ivf_flat"));
        assert!(AnnRegistry::exists("ivf_pq"));
    }

    #[test]
    fn registry_exists_returns_false_for_unknown_name() {
        assert!(!AnnRegistry::exists("hnsw"));
        assert!(!AnnRegistry::exists(""));
    }

    #[test]
    fn registry_get_flat_returns_ivf_flat_plugin() {
        let plugin = AnnRegistry::get_flat("ivf_flat").unwrap();
        assert_eq!(plugin.family(), "ivf_flat");
    }

    #[test]
    fn registry_get_flat_rejects_ivf_pq_with_helpful_message() {
        let err = AnnRegistry::get_flat("ivf_pq").err().unwrap();
        assert!(err.to_string().contains("codebook"));
    }

    #[test]
    fn registry_get_flat_rejects_unknown_family() {
        let err = AnnRegistry::get_flat("hnsw").err().unwrap();
        assert!(err.to_string().contains("unknown ANN family"));
        assert!(err.to_string().contains("hnsw"));
    }

    // ── Plugin selection replaces branching ───────────────────────────────────

    #[test]
    fn plugin_dispatch_based_on_family_name() {
        let families: &[(&str, DistanceMetric, bool)] = &[
            ("ivf_flat", DistanceMetric::Cosine, true),
            ("ivf_flat", DistanceMetric::Euclidean, true),
            ("ivf_flat", DistanceMetric::InnerProduct, true),
        ];
        for &(name, metric, should_pass) in families {
            let plugin = AnnRegistry::get_flat(name).unwrap();
            let result = plugin.validate(128, metric);
            assert_eq!(
                result.is_ok(),
                should_pass,
                "expected validate to be ok={should_pass} for family={name}, metric={metric}"
            );
        }
    }
}
