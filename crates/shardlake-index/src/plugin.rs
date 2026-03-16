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
//! | `"hnsw"` | [`HnswPlugin`] | Graph-based HNSW candidate search, all metrics supported |
//! | `"diskann"` | [`DiskAnnPlugin`] | Strided-probe experiment, Euclidean only |
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

use serde::{Deserialize, Serialize};

use shardlake_core::{config::SystemConfig, AnnFamily, DistanceMetric};

use crate::{
    exact::{distance, exact_search},
    pipeline::{CandidateSearchStage, ExactCandidateSearch, HnswCandidateSearch, PqCandidateStage},
    pq::{PqCodebook, PqParams},
    shard::ShardIndex,
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

// ── HnswPlugin ────────────────────────────────────────────────────────────────

/// Construction and search parameters for the HNSW backend.
///
/// These values mirror the standard HNSW hyperparameters exposed by most
/// HNSW libraries.  `m` controls the graph density and memory footprint;
/// `ef_construction` trades build speed for recall.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HnswConfig {
    /// Number of bi-directional links created for each inserted node (`M`).
    ///
    /// Higher values increase memory use and build time but improve recall.
    /// Must be ≥ 1.  Typical values: 8–32, default: 16.
    pub m: usize,
    /// Size of the dynamic candidate list used during graph construction
    /// (`efConstruction`).
    ///
    /// Must be ≥ `m`.  Higher values produce higher recall at the cost of
    /// slower index build.  Default: 200.
    pub ef_construction: usize,
    /// Search-time beam width (`ef`).
    ///
    /// Must be ≥ `top_k` at query time.  Higher values improve recall at the
    /// cost of slower queries.  Default: 50.
    pub ef_search: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            ef_construction: 200,
            ef_search: 50,
        }
    }
}

impl HnswConfig {
    /// Validate that the configuration values are self-consistent.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::Other`] when:
    /// - `m` is 0
    /// - `ef_construction` is less than `m`
    /// - `ef_search` is 0
    pub fn validate(&self) -> Result<()> {
        if self.m == 0 {
            return Err(IndexError::Other("HNSW config: m must be ≥ 1".into()));
        }
        if self.ef_construction < self.m {
            return Err(IndexError::Other(format!(
                "HNSW config: ef_construction ({}) must be ≥ m ({})",
                self.ef_construction, self.m
            )));
        }
        if self.ef_search == 0 {
            return Err(IndexError::Other(
                "HNSW config: ef_search must be ≥ 1".into(),
            ));
        }
        Ok(())
    }
}

/// ANN plugin for the HNSW backend.
///
/// Builds and queries a Hierarchical Navigable Small World graph index.
/// Supports all [`DistanceMetric`] variants.  The graph construction
/// parameters are carried in [`HnswConfig`] and validated before any build
/// or query operation.
pub struct HnswPlugin {
    config: HnswConfig,
}

impl HnswPlugin {
    /// Create a new plugin with the given HNSW configuration.
    pub fn new(config: HnswConfig) -> Self {
        Self { config }
    }

    /// Return a reference to the HNSW configuration.
    pub fn config(&self) -> &HnswConfig {
        &self.config
    }
}

impl Default for HnswPlugin {
    fn default() -> Self {
        Self::new(HnswConfig::default())
    }
}

impl AnnPlugin for HnswPlugin {
    fn family(&self) -> &str {
        AnnFamily::Hnsw.as_str()
    }

    /// Validates HNSW configuration and metric compatibility.
    ///
    /// All distance metrics are supported.  Returns an error if the
    /// [`HnswConfig`] values are invalid (e.g. `m == 0` or
    /// `ef_construction < m`).
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::Other`] when the configuration is invalid.
    fn validate(&self, _dims: usize, _metric: DistanceMetric) -> Result<()> {
        self.config.validate()
    }

    fn candidate_stage(&self) -> Arc<dyn CandidateSearchStage> {
        Arc::new(HnswCandidateSearch::new(
            self.config.m,
            self.config.ef_search,
        ))
    }
}

// ── DiskAnnPlugin ─────────────────────────────────────────────────────────────

/// Default beam width used when constructing [`DiskAnnPlugin`] via
/// [`AnnRegistry::get_flat`].
pub const DISKANN_DEFAULT_BEAM_WIDTH: usize = 64;

/// ANN plugin for the experimental DiskANN-style backend.
///
/// Limits per-shard distance computations to a bounded probe set
/// (`max(k, beam_width)` records per shard, spread evenly by stride),
/// delivering approximate / lower-latency search without requiring a
/// persisted navigating-graph artifact.  This is a strided-probe experiment,
/// not a graph-based beam search. Only supports
/// [`DistanceMetric::Euclidean`].
///
/// The `beam_width` parameter controls the trade-off between query latency
/// and recall quality:
/// - A larger beam width probes more candidate vectors → higher recall,
///   higher per-shard latency.
/// - A smaller beam width limits exploration → lower latency, lower recall.
/// - When `max(k, beam_width) ≥ shard_size` the search degrades to an exact
///   scan, matching `"ivf_flat"` behaviour.
pub struct DiskAnnPlugin {
    beam_width: usize,
}

impl DiskAnnPlugin {
    /// Create a new plugin with the given `beam_width`.
    ///
    /// # Panics
    ///
    /// Panics if `beam_width` is zero.
    pub fn new(beam_width: usize) -> Self {
        assert!(beam_width > 0, "beam_width must be greater than zero");
        Self { beam_width }
    }

    /// Return the configured beam width.
    pub fn beam_width(&self) -> usize {
        self.beam_width
    }
}

impl AnnPlugin for DiskAnnPlugin {
    fn family(&self) -> &str {
        AnnFamily::DiskAnn.as_str()
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
        Arc::new(DiskAnnCandidateStage::new(self.beam_width))
    }
}

/// Strided-probe candidate stage for the DiskANN experiment path.
///
/// Limits per-shard exploration to a bounded probe set rather than scoring
/// every record in the shard, giving the approximate / lower-latency property
/// claimed by this DiskANN-inspired experiment without requiring a persisted
/// navigating graph. It does not implement DiskANN's graph traversal or greedy
/// best-first search. Only Euclidean distance is supported; any other metric
/// results in an [`IndexError`].
///
/// # Algorithm
///
/// 1. Compute `probe_count = max(k, beam_width).min(shard_size)`.
///    - This bounds the total number of distance computations to
///      `probe_count` per shard (O(`probe_count`), not O(shard_size)).
///    - It also guarantees the stage can return up to `k` candidates when
///      the shard has at least `k` records.
/// 2. Select `probe_count` record indices spread evenly across the shard
///    with an endpoint-inclusive sampler so the probe set spans the full
///    record range.
/// 3. Score only those records and return the top-`k` by distance.
///
/// When `probe_count == shard_size` (small shards or large `k`/`beam_width`)
/// the stage falls back to an exact flat scan.
pub struct DiskAnnCandidateStage {
    beam_width: usize,
}

impl DiskAnnCandidateStage {
    /// Create a new stage with the given `beam_width`.
    pub fn new(beam_width: usize) -> Self {
        assert!(beam_width > 0, "beam_width must be greater than zero");
        Self { beam_width }
    }

    fn probe_index(record_count: usize, probe_count: usize, probe_idx: usize) -> usize {
        if probe_count <= 1 {
            return 0;
        }

        probe_idx * (record_count - 1) / (probe_count - 1)
    }
}

impl CandidateSearchStage for DiskAnnCandidateStage {
    fn search(
        &self,
        query: &[f32],
        shard: &ShardIndex,
        metric: DistanceMetric,
        k: usize,
    ) -> Result<Vec<shardlake_core::types::SearchResult>> {
        if metric != DistanceMetric::Euclidean {
            return Err(IndexError::Other(format!(
                "DiskANN candidate search supports only euclidean distance, got {metric}"
            )));
        }

        let records = &shard.records;

        if k == 0 || records.is_empty() {
            return Ok(vec![]);
        }

        // probe_count: total distance computations for this shard.
        //
        // - At least k so the caller's top_k request can always be satisfied.
        // - At most beam_width (when k ≤ beam_width) so per-shard work is
        //   O(beam_width) rather than O(shard_size), delivering the
        //   approximate / bounded-exploration behaviour.
        // - Capped at records.len() so we never request more than available.
        let probe_count = k.max(self.beam_width).min(records.len());

        // Fast path: probing every record → exact scan.
        if probe_count == records.len() {
            return Ok(exact_search(query, records, metric, k));
        }

        // Score an endpoint-inclusive probe set so bounded exploration spans
        // the full shard range instead of skipping the tail.
        // Track (score, shard_index) to defer
        // metadata cloning until the final top-k result construction.
        let mut scored: Vec<(f32, usize)> = (0..probe_count)
            .map(|i| {
                let idx = Self::probe_index(records.len(), probe_count, i);
                (distance(query, &records[idx].data, metric), idx)
            })
            .collect();

        scored.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);

        Ok(scored
            .into_iter()
            .map(|(score, idx)| {
                let r = &records[idx];
                shardlake_core::types::SearchResult {
                    id: r.id,
                    score,
                    metadata: r.metadata.clone(),
                }
            })
            .collect())
    }
}

// ── AnnRegistry ───────────────────────────────────────────────────────────────

/// Registry of known ANN family names.
///
/// [`AnnRegistry`] is intentionally **stateless**: it validates that a
/// requested family name is known and returns a ready-to-use plugin for
/// families that need no runtime artifacts.  Families that require
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
/// AnnRegistry::exists("hnsw");    // true
/// AnnRegistry::exists("unknown"); // false
/// ```
pub struct AnnRegistry;

impl AnnRegistry {
    /// Returns the names of all built-in ANN families.
    pub fn families() -> &'static [&'static str] {
        &["ivf_flat", "ivf_pq", "hnsw", "diskann"]
    }

    /// Returns `true` if `family` is a known built-in ANN family name.
    pub fn exists(family: &str) -> bool {
        Self::families().contains(&family)
    }

    /// Resolve the build-time PQ parameters from an explicit override and the
    /// system configuration, centralising algorithm selection so callers do not
    /// need to branch on `pq_enabled` directly.
    ///
    /// Returns `explicit` if supplied, otherwise derives [`PqParams`] from the
    /// config when `config.pq_enabled` is `true`, and `None` otherwise.
    pub fn resolve_build_params(
        explicit: Option<PqParams>,
        config: &SystemConfig,
    ) -> Option<PqParams> {
        explicit.or_else(|| {
            config.pq_enabled.then_some(PqParams {
                num_subspaces: config.pq_num_subspaces as usize,
                codebook_size: config.pq_codebook_size as usize,
            })
        })
    }

    /// Return a plugin for families that need no runtime artifacts.
    ///
    /// Returns a boxed [`AnnPlugin`] for `"ivf_flat"` and `"hnsw"`.
    /// For families that need runtime artifacts (like `"ivf_pq"`) construct
    /// the plugin directly, e.g.
    /// [`IvfPqPlugin::new(codebook)`](IvfPqPlugin::new).
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::Other`] when `family` is `"ivf_pq"` (requires a
    /// codebook) or is an unknown name.
    pub fn get_flat(family: &str) -> Result<Box<dyn AnnPlugin>> {
        match family {
            "ivf_flat" => Ok(Box::new(IvfFlatPlugin)),
            "hnsw" => Ok(Box::new(HnswPlugin::default())),
            "diskann" => Ok(Box::new(DiskAnnPlugin::new(DISKANN_DEFAULT_BEAM_WIDTH))),
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
        for name in ["ivf_flat", "ivf_pq", "hnsw", "diskann"] {
            let family: shardlake_core::AnnFamily = name.parse().unwrap();
            assert_eq!(family.as_str(), name);
            assert_eq!(family.to_string(), name);
        }
    }

    #[test]
    fn ann_family_unknown_name_returns_error() {
        let err = "unknown_algo"
            .parse::<shardlake_core::AnnFamily>()
            .unwrap_err();
        assert!(err.to_string().contains("unknown ANN family"));
        assert!(err.to_string().contains("unknown_algo"));
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

    // ── HnswPlugin ────────────────────────────────────────────────────────────

    #[test]
    fn hnsw_plugin_family_name() {
        assert_eq!(HnswPlugin::default().family(), "hnsw");
    }

    #[test]
    fn hnsw_plugin_validate_accepts_all_metrics() {
        let plugin = HnswPlugin::default();
        for metric in [
            DistanceMetric::Cosine,
            DistanceMetric::Euclidean,
            DistanceMetric::InnerProduct,
        ] {
            assert!(
                plugin.validate(128, metric).is_ok(),
                "hnsw should accept {metric}"
            );
        }
    }

    #[test]
    fn hnsw_plugin_validate_rejects_zero_m() {
        let plugin = HnswPlugin::new(HnswConfig {
            m: 0,
            ef_construction: 200,
            ef_search: 50,
        });
        let err = plugin.validate(128, DistanceMetric::Cosine).unwrap_err();
        assert!(err.to_string().contains("m must be"));
    }

    #[test]
    fn hnsw_plugin_validate_rejects_ef_construction_less_than_m() {
        let plugin = HnswPlugin::new(HnswConfig {
            m: 16,
            ef_construction: 4,
            ef_search: 50,
        });
        let err = plugin.validate(128, DistanceMetric::Cosine).unwrap_err();
        assert!(err.to_string().contains("ef_construction"));
        assert!(err.to_string().contains("m"));
    }

    #[test]
    fn hnsw_plugin_validate_rejects_zero_ef_search() {
        let plugin = HnswPlugin::new(HnswConfig {
            m: 16,
            ef_construction: 200,
            ef_search: 0,
        });
        let err = plugin.validate(128, DistanceMetric::Cosine).unwrap_err();
        assert!(err.to_string().contains("ef_search"));
    }

    #[test]
    fn hnsw_plugin_candidate_stage_is_sendable() {
        let stage = HnswPlugin::default().candidate_stage();
        let _: Arc<dyn CandidateSearchStage> = stage;
    }

    #[test]
    fn hnsw_config_default_values_are_valid() {
        HnswConfig::default().validate().unwrap();
    }

    // ── AnnRegistry ───────────────────────────────────────────────────────────

    #[test]
    fn registry_families_contains_all_builtins() {
        let families = AnnRegistry::families();
        assert!(families.contains(&"ivf_flat"));
        assert!(families.contains(&"ivf_pq"));
        assert!(families.contains(&"hnsw"));
        assert!(families.contains(&"diskann"));
    }

    #[test]
    fn registry_exists_returns_true_for_known_names() {
        assert!(AnnRegistry::exists("ivf_flat"));
        assert!(AnnRegistry::exists("ivf_pq"));
        assert!(AnnRegistry::exists("hnsw"));
        assert!(AnnRegistry::exists("diskann"));
    }

    #[test]
    fn registry_exists_returns_false_for_unknown_name() {
        assert!(!AnnRegistry::exists("unknown_algo"));
        assert!(!AnnRegistry::exists(""));
    }

    #[test]
    fn registry_get_flat_returns_ivf_flat_plugin() {
        let plugin = AnnRegistry::get_flat("ivf_flat").unwrap();
        assert_eq!(plugin.family(), "ivf_flat");
    }

    #[test]
    fn registry_get_flat_returns_hnsw_plugin() {
        let plugin = AnnRegistry::get_flat("hnsw").unwrap();
        assert_eq!(plugin.family(), "hnsw");
    }

    #[test]
    fn registry_get_flat_returns_diskann_plugin() {
        let plugin = AnnRegistry::get_flat("diskann").unwrap();
        assert_eq!(plugin.family(), "diskann");
    }

    #[test]
    fn registry_get_flat_rejects_ivf_pq_with_helpful_message() {
        let err = AnnRegistry::get_flat("ivf_pq").err().unwrap();
        assert!(err.to_string().contains("codebook"));
    }

    #[test]
    fn registry_get_flat_rejects_unknown_family() {
        let err = AnnRegistry::get_flat("unknown_algo").err().unwrap();
        assert!(err.to_string().contains("unknown ANN family"));
        assert!(err.to_string().contains("unknown_algo"));
    }

    // ── DiskAnnPlugin ─────────────────────────────────────────────────────────

    #[test]
    fn diskann_plugin_family_name() {
        let plugin = DiskAnnPlugin::new(32);
        assert_eq!(plugin.family(), "diskann");
    }

    #[test]
    fn diskann_plugin_beam_width_accessor() {
        let plugin = DiskAnnPlugin::new(48);
        assert_eq!(plugin.beam_width(), 48);
    }

    #[test]
    fn diskann_plugin_validate_accepts_euclidean() {
        let plugin = DiskAnnPlugin::new(32);
        assert!(plugin.validate(128, DistanceMetric::Euclidean).is_ok());
    }

    #[test]
    fn diskann_plugin_validate_rejects_cosine() {
        let plugin = DiskAnnPlugin::new(32);
        let err = plugin.validate(128, DistanceMetric::Cosine).unwrap_err();
        assert!(err.to_string().contains("euclidean"));
        assert!(err.to_string().contains("diskann"));
    }

    #[test]
    fn diskann_plugin_validate_rejects_inner_product() {
        let plugin = DiskAnnPlugin::new(32);
        let err = plugin
            .validate(128, DistanceMetric::InnerProduct)
            .unwrap_err();
        assert!(err.to_string().contains("euclidean"));
    }

    #[test]
    fn diskann_plugin_candidate_stage_is_sendable() {
        let stage = DiskAnnPlugin::new(32).candidate_stage();
        let _: Arc<dyn CandidateSearchStage> = stage;
    }

    // ── Plugin selection replaces branching ───────────────────────────────────

    #[test]
    fn plugin_dispatch_based_on_family_name() {
        let families: &[(&str, DistanceMetric, bool)] = &[
            ("ivf_flat", DistanceMetric::Cosine, true),
            ("ivf_flat", DistanceMetric::Euclidean, true),
            ("ivf_flat", DistanceMetric::InnerProduct, true),
            ("hnsw", DistanceMetric::Cosine, true),
            ("hnsw", DistanceMetric::Euclidean, true),
            ("hnsw", DistanceMetric::InnerProduct, true),
            ("diskann", DistanceMetric::Euclidean, true),
            ("diskann", DistanceMetric::Cosine, false),
            ("diskann", DistanceMetric::InnerProduct, false),
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
