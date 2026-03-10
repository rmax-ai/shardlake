//! Synthetic benchmark dataset generator.
//!
//! Generates reproducible vector datasets with configurable dimensionality,
//! cluster structure, and size. Useful for reproducible benchmarks and tests.
//!
//! # Examples
//!
//! ```rust
//! use shardlake_bench::generator::{DatasetConfig, generate_dataset};
//!
//! let config = DatasetConfig {
//!     dims: 32,
//!     num_vectors: 1_000,
//!     num_clusters: 8,
//!     seed: 42,
//!     cluster_spread: 0.1,
//! };
//! let records = generate_dataset(&config);
//! assert_eq!(records.len(), 1_000);
//! assert_eq!(records[0].data.len(), 32);
//! ```

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use shardlake_core::types::{VectorId, VectorRecord};

/// Configuration for synthetic dataset generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetConfig {
    /// Number of dimensions per vector.
    pub dims: usize,
    /// Total number of vectors to generate.
    pub num_vectors: usize,
    /// Number of clusters in the generated dataset.
    pub num_clusters: usize,
    /// Random seed for reproducibility.
    pub seed: u64,
    /// Standard deviation of each vector component around its cluster centroid.
    pub cluster_spread: f32,
}

impl Default for DatasetConfig {
    fn default() -> Self {
        Self {
            dims: 128,
            num_vectors: 10_000,
            num_clusters: 10,
            seed: 42,
            cluster_spread: 0.1,
        }
    }
}

/// Generate a synthetic vector dataset according to `config`.
///
/// Vectors are drawn from Gaussian distributions centred on randomly placed
/// cluster centroids. Each centroid is uniformly sampled from `[-1, 1]^dims`.
/// Vectors are assigned to clusters in round-robin order so that cluster sizes
/// are balanced.
///
/// The returned `VectorRecord` ids start at `1` and are contiguous.
/// Each record carries `{"cluster": <index>}` metadata for ground-truth labelling.
pub fn generate_dataset(config: &DatasetConfig) -> Vec<VectorRecord> {
    assert!(config.dims > 0, "dims must be > 0");
    assert!(config.num_vectors > 0, "num_vectors must be > 0");
    assert!(config.num_clusters > 0, "num_clusters must be > 0");

    let mut rng = StdRng::seed_from_u64(config.seed);

    // Generate cluster centroids uniformly in [-1, 1]^dims.
    let centroids: Vec<Vec<f32>> = (0..config.num_clusters)
        .map(|_| {
            (0..config.dims)
                .map(|_| rng.gen_range(-1.0_f32..=1.0_f32))
                .collect()
        })
        .collect();

    // Generate vectors: round-robin assignment to clusters.
    let mut records = Vec::with_capacity(config.num_vectors);
    for i in 0..config.num_vectors {
        let cluster_idx = i % config.num_clusters;
        let centroid = &centroids[cluster_idx];
        let data: Vec<f32> = centroid
            .iter()
            .map(|&c| c + gaussian_sample(&mut rng) * config.cluster_spread)
            .collect();
        records.push(VectorRecord {
            id: VectorId(i as u64 + 1),
            data,
            metadata: Some(serde_json::json!({ "cluster": cluster_idx })),
        });
    }

    records
}

/// Sample from the standard normal distribution using the Box-Muller transform.
fn gaussian_sample(rng: &mut impl Rng) -> f32 {
    // u1 must be strictly positive to avoid ln(0). Using 1e-10 as the lower
    // bound avoids both log-of-zero and the excessive magnitudes that arise
    // from values as small as f32::EPSILON (~1.2e-7, which gives ln ≈ -16).
    let u1: f32 = rng.gen_range(1e-10_f32..=1.0_f32);
    let u2: f32 = rng.gen_range(0.0_f32..=1.0_f32);
    (-2.0 * u1.ln()).sqrt() * (2.0 * std::f32::consts::PI * u2).cos()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> DatasetConfig {
        DatasetConfig {
            dims: 8,
            num_vectors: 100,
            num_clusters: 5,
            seed: 0,
            cluster_spread: 0.05,
        }
    }

    #[test]
    fn test_output_count_and_dims() {
        let cfg = default_config();
        let records = generate_dataset(&cfg);
        assert_eq!(records.len(), cfg.num_vectors);
        for r in &records {
            assert_eq!(r.data.len(), cfg.dims);
        }
    }

    #[test]
    fn test_ids_are_contiguous_from_one() {
        let records = generate_dataset(&default_config());
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.id.0, (i + 1) as u64);
        }
    }

    #[test]
    fn test_cluster_metadata_present() {
        let records = generate_dataset(&default_config());
        for r in &records {
            let meta = r.metadata.as_ref().expect("metadata should be present");
            assert!(meta["cluster"].is_number());
        }
    }

    #[test]
    fn test_reproducibility() {
        let cfg = default_config();
        let a = generate_dataset(&cfg);
        let b = generate_dataset(&cfg);
        for (ra, rb) in a.iter().zip(b.iter()) {
            assert_eq!(ra.data, rb.data, "same seed must produce identical output");
        }
    }

    #[test]
    fn test_different_seeds_differ() {
        let cfg_a = DatasetConfig {
            seed: 1,
            ..default_config()
        };
        let cfg_b = DatasetConfig {
            seed: 2,
            ..default_config()
        };
        let a = generate_dataset(&cfg_a);
        let b = generate_dataset(&cfg_b);
        // With overwhelming probability the first vector differs across seeds.
        assert_ne!(a[0].data, b[0].data, "different seeds should differ");
    }
}
