//! Reproducible synthetic benchmark dataset generator.
//!
//! Generates clustered vector corpora with controlled dimensionality, cluster
//! structure, and dataset size from a deterministic seed.  Identical
//! [`GenerateConfig`] values always produce identical [`VectorRecord`]
//! sequences, which enables repeatable benchmarking evaluations.
//!
//! # Example
//!
//! ```
//! use shardlake_bench::generate::{GenerateConfig, generate_dataset};
//!
//! let config = GenerateConfig {
//!     num_vectors: 100,
//!     dims: 4,
//!     num_clusters: 3,
//!     seed: 42,
//!     cluster_spread: 0.1,
//! };
//! let records = generate_dataset(&config);
//! assert_eq!(records.len(), 100);
//! assert!(records.iter().all(|r| r.data.len() == 4));
//! ```

use rand::{Rng, SeedableRng};

use shardlake_core::types::{VectorId, VectorRecord};

/// Configuration for synthetic dataset generation.
#[derive(Debug, Clone)]
pub struct GenerateConfig {
    /// Total number of vectors to generate.
    pub num_vectors: usize,
    /// Dimensionality of each generated vector.
    pub dims: usize,
    /// Number of clusters used to structure the synthetic corpus.
    ///
    /// More clusters produce a richer distribution; a single cluster is
    /// equivalent to pure uniform noise around a single centroid.
    pub num_clusters: usize,
    /// RNG seed for fully deterministic generation.
    ///
    /// Identical seeds with identical configs always produce the same
    /// [`VectorRecord`] sequence.
    pub seed: u64,
    /// Half-range of the uniform noise added to each dimension of a cluster
    /// centroid when sampling a vector.
    ///
    /// Smaller values create tighter, more separable clusters.  Larger values
    /// produce a more spread-out, harder-to-distinguish distribution.
    /// Defaults to `0.1`.
    pub cluster_spread: f32,
}

impl Default for GenerateConfig {
    fn default() -> Self {
        Self {
            num_vectors: 1_000,
            dims: 128,
            num_clusters: 10,
            seed: 0xdead_beef,
            cluster_spread: 0.1,
        }
    }
}

/// Generate a synthetic vector dataset from the given configuration.
///
/// Cluster centroids are drawn uniformly from `[-1.0, 1.0]^dims`.  Each
/// vector is then placed at a randomly chosen centroid plus per-dimension
/// uniform noise in `[-cluster_spread, cluster_spread]`.  Vector ids start at
/// `1` and increment by `1`.
///
/// Identical `config` values always produce the same `Vec<VectorRecord>`.
///
/// # Panics
///
/// Panics if any of `num_vectors`, `dims`, or `num_clusters` is zero, or if
/// `cluster_spread` is negative or non-finite.
pub fn generate_dataset(config: &GenerateConfig) -> Vec<VectorRecord> {
    assert!(config.num_vectors > 0, "num_vectors must be > 0");
    assert!(config.dims > 0, "dims must be > 0");
    assert!(config.num_clusters > 0, "num_clusters must be > 0");
    assert!(
        config.cluster_spread.is_finite(),
        "cluster_spread must be finite"
    );
    assert!(config.cluster_spread >= 0.0, "cluster_spread must be >= 0");

    let mut rng = rand::rngs::StdRng::seed_from_u64(config.seed);

    // Generate cluster centroids uniformly in [-1.0, 1.0]^dims.
    let centroids: Vec<Vec<f32>> = (0..config.num_clusters)
        .map(|_| {
            (0..config.dims)
                .map(|_| rng.gen_range(-1.0_f32..=1.0_f32))
                .collect()
        })
        .collect();

    // Generate each vector as a noisy perturbation of a randomly chosen centroid.
    (0..config.num_vectors)
        .map(|i| {
            let cluster = rng.gen_range(0..config.num_clusters);
            let centroid = &centroids[cluster];
            let spread = config.cluster_spread;
            let data: Vec<f32> = centroid
                .iter()
                .map(|&c| c + rng.gen_range(-spread..=spread))
                .collect();
            VectorRecord {
                id: VectorId(i as u64 + 1),
                data,
                metadata: None,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> GenerateConfig {
        GenerateConfig {
            num_vectors: 100,
            dims: 8,
            num_clusters: 4,
            seed: 42,
            cluster_spread: 0.1,
        }
    }

    #[test]
    fn generates_correct_shape() {
        let cfg = default_config();
        let records = generate_dataset(&cfg);
        assert_eq!(records.len(), cfg.num_vectors);
        for r in &records {
            assert_eq!(r.data.len(), cfg.dims);
        }
    }

    #[test]
    fn ids_are_sequential_from_one() {
        let cfg = default_config();
        let records = generate_dataset(&cfg);
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.id.0, i as u64 + 1);
        }
    }

    #[test]
    fn reproducible_with_same_seed() {
        let cfg = default_config();
        let a = generate_dataset(&cfg);
        let b = generate_dataset(&cfg);
        assert_eq!(a.len(), b.len());
        for (ra, rb) in a.iter().zip(b.iter()) {
            assert_eq!(ra.id, rb.id);
            assert_eq!(ra.data, rb.data);
        }
    }

    #[test]
    fn different_seeds_produce_different_data() {
        let cfg_a = GenerateConfig {
            seed: 1,
            ..default_config()
        };
        let cfg_b = GenerateConfig {
            seed: 2,
            ..default_config()
        };
        let a = generate_dataset(&cfg_a);
        let b = generate_dataset(&cfg_b);
        assert_ne!(a[0].data, b[0].data);
    }

    #[test]
    fn metadata_is_none() {
        let cfg = default_config();
        let records = generate_dataset(&cfg);
        for r in &records {
            assert!(r.metadata.is_none());
        }
    }

    #[test]
    #[should_panic(expected = "num_vectors must be > 0")]
    fn rejects_zero_num_vectors() {
        generate_dataset(&GenerateConfig {
            num_vectors: 0,
            ..default_config()
        });
    }

    #[test]
    #[should_panic(expected = "dims must be > 0")]
    fn rejects_zero_dims() {
        generate_dataset(&GenerateConfig {
            dims: 0,
            ..default_config()
        });
    }

    #[test]
    #[should_panic(expected = "num_clusters must be > 0")]
    fn rejects_zero_num_clusters() {
        generate_dataset(&GenerateConfig {
            num_clusters: 0,
            ..default_config()
        });
    }

    #[test]
    #[should_panic(expected = "cluster_spread must be >= 0")]
    fn rejects_negative_cluster_spread() {
        generate_dataset(&GenerateConfig {
            cluster_spread: -0.1,
            ..default_config()
        });
    }

    #[test]
    #[should_panic(expected = "cluster_spread must be finite")]
    fn rejects_non_finite_cluster_spread() {
        generate_dataset(&GenerateConfig {
            cluster_spread: f32::NAN,
            ..default_config()
        });
    }

    /// Verify that vectors stay close to their cluster centroids.
    #[test]
    fn vectors_are_close_to_centroids() {
        let cfg = GenerateConfig {
            num_vectors: 200,
            dims: 4,
            num_clusters: 2,
            seed: 99,
            cluster_spread: 0.05,
        };
        let records = generate_dataset(&cfg);
        // With spread=0.05 in 4 dims, max per-dim deviation is 0.05, so max
        // squared L2 distance to the nearest centroid is 4 * 0.05^2 = 0.01.
        for r in &records {
            let min_dist: f32 = {
                // Reconstruct centroids with the same seed to get ground truth.
                let mut rng2 = rand::rngs::StdRng::seed_from_u64(cfg.seed);
                let centroids: Vec<Vec<f32>> = (0..cfg.num_clusters)
                    .map(|_| {
                        (0..cfg.dims)
                            .map(|_| rng2.gen_range(-1.0_f32..=1.0_f32))
                            .collect()
                    })
                    .collect();
                centroids
                    .iter()
                    .map(|c| {
                        c.iter()
                            .zip(r.data.iter())
                            .map(|(a, b)| (a - b) * (a - b))
                            .sum::<f32>()
                    })
                    .fold(f32::INFINITY, f32::min)
            };
            assert!(
                // 4 dims × 0.05² spread = 0.01 max sq-distance + small ε for f32 rounding.
                min_dist <= 0.011,
                "vector {:?} too far from nearest centroid: sq_dist={min_dist}",
                r.id
            );
        }
    }
}
