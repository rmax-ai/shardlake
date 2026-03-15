//! Product Quantisation (PQ) codebook training, encoding, and approximate
//! distance computation.
//!
//! # Overview
//!
//! Product Quantisation splits each `dims`-dimensional vector into `M`
//! equal-length sub-vectors and trains a separate K-means codebook (with `K`
//! centroids) for each sub-space.  Every vector is then represented as `M`
//! one-byte codes (assuming `K ≤ 256`), yielding a significant reduction in
//! storage compared to the original `dims × 4` bytes.
//!
//! At query time, Asymmetric Distance Computation (ADC) is used: a distance
//! table of size `M × K` is pre-computed once per query, and the approximate
//! distance to any encoded vector is obtained as a sum of `M` table look-ups.
//!
//! # Example
//!
//! ```rust,ignore
//! use shardlake_index::pq::{PqCodebook, PqParams};
//!
//! let params = PqParams { num_subspaces: 4, codebook_size: 256 };
//! let codebook = PqCodebook::train(&vectors, params, seed, kmeans_iters)?;
//!
//! let codes: Vec<u8> = codebook.encode(&vector);
//! let table  = codebook.compute_distance_table(&query);
//! let score  = codebook.adc_distance(&codes, &table);
//! ```

use std::io::{Cursor, Read};

use rand::SeedableRng;

use crate::{
    kmeans::{kmeans, nearest_centroid},
    IndexError, Result,
};
use shardlake_core::error::CoreError;

// ── Magic / format version ────────────────────────────────────────────────────

/// Magic bytes that identify a PQ codebook artifact.
pub const PQ_CODEBOOK_MAGIC: &[u8; 8] = b"SLKPQCB\0";
const PQ_CODEBOOK_FORMAT_VERSION: u32 = 1;

// ── PqParams ──────────────────────────────────────────────────────────────────

/// Configuration for a product-quantisation codebook.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PqParams {
    /// Number of sub-spaces `M`.  Must be ≥ 1 and must divide `dims` evenly.
    pub num_subspaces: usize,
    /// Number of centroids per sub-space codebook `K`.  Must satisfy
    /// `1 ≤ K ≤ 256` so that each code fits in one byte.
    pub codebook_size: usize,
}

impl Default for PqParams {
    /// Returns reasonable defaults: 8 sub-spaces, 256-entry codebooks.
    fn default() -> Self {
        Self {
            num_subspaces: 8,
            codebook_size: 256,
        }
    }
}

// ── PqCodebook ────────────────────────────────────────────────────────────────

/// Trained product-quantisation codebook.
///
/// Created by [`PqCodebook::train`] and persisted as a binary artifact using
/// [`PqCodebook::to_bytes`] / [`PqCodebook::from_bytes`].
#[derive(Debug, Clone)]
pub struct PqCodebook {
    /// Parameters this codebook was trained with.
    pub params: PqParams,
    /// Full vector dimension.
    pub dims: usize,
    /// Dimension of each sub-vector (`dims / num_subspaces`).
    pub sub_dims: usize,
    /// `codebooks[m][k]` is the k-th centroid vector of sub-space `m`.
    ///
    /// Shape: `[M][K][sub_dims]`.
    pub codebooks: Vec<Vec<Vec<f32>>>,
}

impl PqCodebook {
    // ── Construction ──────────────────────────────────────────────────────────

    /// Train a PQ codebook from `vectors` using one K-means pass per sub-space.
    ///
    /// `kmeans_iters` controls the maximum number of K-means update steps.  A
    /// higher value yields better codebook quality at the cost of training time.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::Other`] when:
    /// - `vectors` is empty,
    /// - `params.num_subspaces` is 0,
    /// - `params.codebook_size` is 0 or > 256,
    /// - `dims` is 0 or is not evenly divisible by `num_subspaces`.
    pub fn train(
        vectors: &[Vec<f32>],
        params: PqParams,
        seed: u64,
        kmeans_iters: u32,
    ) -> Result<Self> {
        if vectors.is_empty() {
            return Err(IndexError::Other("PQ training: empty vector set".into()));
        }
        let dims = vectors[0].len();
        Self::validate_params(&params, dims)?;
        for (idx, vector) in vectors.iter().enumerate() {
            if vector.len() != dims {
                return Err(IndexError::Core(CoreError::DimensionMismatch {
                    expected: dims,
                    got: vector.len(),
                }))
                .map_err(|err| match err {
                    IndexError::Core(core) => IndexError::Other(format!(
                        "PQ training vector {idx} has invalid dimension: {core}"
                    )),
                    other => other,
                });
            }
        }

        let sub_dims = dims / params.num_subspaces;
        let k = params.codebook_size;
        let mut codebooks = Vec::with_capacity(params.num_subspaces);
        let mut effective_codebook_size = k;

        for m in 0..params.num_subspaces {
            let start = m * sub_dims;
            let end = start + sub_dims;

            // Extract sub-vectors for sub-space m.
            let sub_vecs: Vec<Vec<f32>> = vectors.iter().map(|v| v[start..end].to_vec()).collect();

            // Each sub-space gets its own seeded RNG derived from the global
            // seed to ensure full reproducibility while avoiding identical
            // initialisation across sub-spaces.
            let sub_seed = seed.wrapping_add(m as u64);
            let mut rng = rand::rngs::StdRng::seed_from_u64(sub_seed);
            let centroids = kmeans(&sub_vecs, k, kmeans_iters, &mut rng);
            if m == 0 {
                effective_codebook_size = centroids.len();
            } else if centroids.len() != effective_codebook_size {
                return Err(IndexError::Other(
                    "PQ training produced inconsistent centroid counts across subspaces".into(),
                ));
            }
            codebooks.push(centroids);
        }

        Ok(Self {
            params: PqParams {
                num_subspaces: params.num_subspaces,
                codebook_size: effective_codebook_size,
            },
            dims,
            sub_dims,
            codebooks,
        })
    }

    // ── Encoding ──────────────────────────────────────────────────────────────

    /// Encode a single `dims`-dimensional vector as `M` one-byte codes.
    ///
    /// Each byte is the index of the nearest centroid in the corresponding
    /// sub-space codebook.
    pub fn encode(&self, vector: &[f32]) -> Result<Vec<u8>> {
        if vector.len() != self.dims {
            return Err(IndexError::Core(CoreError::DimensionMismatch {
                expected: self.dims,
                got: vector.len(),
            }));
        }

        let mut codes = Vec::with_capacity(self.params.num_subspaces);
        for m in 0..self.params.num_subspaces {
            let start = m * self.sub_dims;
            let end = start + self.sub_dims;
            let sub_vec = &vector[start..end];
            let code = nearest_centroid(sub_vec, &self.codebooks[m]);
            codes.push(code as u8);
        }
        Ok(codes)
    }

    /// Encode a slice of vectors, returning one code vector per input vector.
    pub fn encode_batch(&self, vectors: &[Vec<f32>]) -> Result<Vec<Vec<u8>>> {
        vectors.iter().map(|v| self.encode(v)).collect()
    }

    // ── Approximate Distance Computation (ADC) ────────────────────────────────

    /// Compute a `M × K` ADC distance table for `query`.
    ///
    /// `table[m][k]` is the squared-L2 distance between the `m`-th sub-vector
    /// of `query` and the `k`-th centroid of sub-space `m`.  Pre-computing
    /// this table once per query allows the approximate distance to any encoded
    /// vector to be computed with only `M` table look-ups.
    pub fn compute_distance_table(&self, query: &[f32]) -> Result<Vec<Vec<f32>>> {
        if query.len() != self.dims {
            return Err(IndexError::Core(CoreError::DimensionMismatch {
                expected: self.dims,
                got: query.len(),
            }));
        }

        let m = self.params.num_subspaces;
        let k = self.params.codebook_size;
        let mut table = vec![vec![0.0f32; k]; m];

        for (sub, row) in table.iter_mut().enumerate().take(m) {
            let start = sub * self.sub_dims;
            let end = start + self.sub_dims;
            let q_sub = &query[start..end];
            for (ki, centroid) in self.codebooks[sub].iter().enumerate() {
                let dist: f32 = q_sub
                    .iter()
                    .zip(centroid.iter())
                    .map(|(a, b)| (a - b) * (a - b))
                    .sum();
                row[ki] = dist;
            }
        }

        Ok(table)
    }

    /// Compute the approximate squared-L2 distance between `query` (represented
    /// by `table`) and an encoded vector (represented by `codes`).
    ///
    /// `codes` must have length `M`; `table` must have shape `[M][K]`.
    pub fn adc_distance(&self, codes: &[u8], table: &[Vec<f32>]) -> f32 {
        codes
            .iter()
            .zip(table.iter())
            .map(|(&code, row)| row[code as usize])
            .sum()
    }

    // ── Serialisation ─────────────────────────────────────────────────────────

    /// Serialise the codebook to bytes.
    ///
    /// Binary layout (all integers little-endian):
    ///
    /// ```text
    /// magic    : [u8; 8] = b"SLKPQCB\0"
    /// version  : u32     = 1
    /// dims     : u32
    /// pq_m     : u32
    /// pq_k     : u32
    /// sub_dims : u32
    /// --- codebooks (pq_m × pq_k entries) ---
    /// per centroid: sub_dims × f32 (little-endian)
    /// ```
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(PQ_CODEBOOK_MAGIC);
        write_u32(&mut buf, PQ_CODEBOOK_FORMAT_VERSION);
        write_u32(&mut buf, self.dims as u32);
        write_u32(&mut buf, self.params.num_subspaces as u32);
        write_u32(&mut buf, self.params.codebook_size as u32);
        write_u32(&mut buf, self.sub_dims as u32);

        for sub_codebook in &self.codebooks {
            for centroid in sub_codebook {
                for &v in centroid {
                    buf.extend_from_slice(&v.to_le_bytes());
                }
            }
        }

        buf
    }

    /// Deserialise a codebook from bytes produced by [`PqCodebook::to_bytes`].
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::Other`] when the magic bytes are wrong, the format
    /// version is unsupported, or the byte stream is truncated.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cur = Cursor::new(bytes);

        let mut magic = [0u8; 8];
        cur.read_exact(&mut magic)
            .map_err(|_| IndexError::Other("PQ codebook: truncated header".into()))?;
        if &magic != PQ_CODEBOOK_MAGIC {
            return Err(IndexError::Other("PQ codebook: invalid magic bytes".into()));
        }

        let version = read_u32(&mut cur)?;
        if version != PQ_CODEBOOK_FORMAT_VERSION {
            return Err(IndexError::Other(format!(
                "PQ codebook: unsupported format version {version}"
            )));
        }

        let dims = read_u32(&mut cur)? as usize;
        let pq_m = read_u32(&mut cur)? as usize;
        let pq_k = read_u32(&mut cur)? as usize;
        let sub_dims = read_u32(&mut cur)? as usize;

        let params = PqParams {
            num_subspaces: pq_m,
            codebook_size: pq_k,
        };
        Self::validate_params(&params, dims)?;
        if sub_dims != dims / pq_m {
            return Err(IndexError::Other(format!(
                "PQ codebook: sub_dims ({sub_dims}) inconsistent with dims ({dims}) / pq_m ({pq_m})"
            )));
        }

        let mut codebooks = Vec::with_capacity(pq_m);
        for _ in 0..pq_m {
            let mut sub_codebook = Vec::with_capacity(pq_k);
            for _ in 0..pq_k {
                let centroid = read_f32_vec(&mut cur, sub_dims)?;
                sub_codebook.push(centroid);
            }
            codebooks.push(sub_codebook);
        }

        Ok(Self {
            params,
            dims,
            sub_dims,
            codebooks,
        })
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    fn validate_params(params: &PqParams, dims: usize) -> Result<()> {
        if params.num_subspaces == 0 {
            return Err(IndexError::Other(
                "PQ: num_subspaces must be at least 1".into(),
            ));
        }
        if params.codebook_size == 0 || params.codebook_size > 256 {
            return Err(IndexError::Other(
                "PQ: codebook_size must be in range [1, 256]".into(),
            ));
        }
        if dims == 0 {
            return Err(IndexError::Other(
                "PQ: vector dimension must be at least 1".into(),
            ));
        }
        if !dims.is_multiple_of(params.num_subspaces) {
            return Err(IndexError::Other(format!(
                "PQ: dims ({dims}) must be evenly divisible by num_subspaces ({})",
                params.num_subspaces
            )));
        }
        Ok(())
    }
}

// ── I/O helpers ───────────────────────────────────────────────────────────────

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn read_u32(cur: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut buf = [0u8; 4];
    cur.read_exact(&mut buf)
        .map_err(|_| IndexError::Other("PQ codebook: unexpected end of bytes".into()))?;
    Ok(u32::from_le_bytes(buf))
}

fn read_f32_vec(cur: &mut Cursor<&[u8]>, len: usize) -> Result<Vec<f32>> {
    let mut v = vec![0.0f32; len];
    for x in v.iter_mut() {
        let mut buf = [0u8; 4];
        cur.read_exact(&mut buf)
            .map_err(|_| IndexError::Other("PQ codebook: unexpected end of bytes".into()))?;
        *x = f32::from_le_bytes(buf);
    }
    Ok(v)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_vectors(n: usize, dims: usize, seed: u64) -> Vec<Vec<f32>> {
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        (0..n)
            .map(|_| (0..dims).map(|_| rng.gen::<f32>()).collect())
            .collect()
    }

    #[test]
    fn train_produces_correct_shape() {
        let vectors = make_vectors(100, 8, 42);
        let params = PqParams {
            num_subspaces: 4,
            codebook_size: 8,
        };
        let cb = PqCodebook::train(&vectors, params.clone(), 0, 10).unwrap();

        assert_eq!(cb.dims, 8);
        assert_eq!(cb.sub_dims, 2);
        assert_eq!(cb.codebooks.len(), params.num_subspaces);
        for sub in &cb.codebooks {
            assert_eq!(sub.len(), params.codebook_size);
            for centroid in sub {
                assert_eq!(centroid.len(), 2);
            }
        }
    }

    #[test]
    fn train_rejects_invalid_params() {
        let vectors = make_vectors(10, 8, 0);

        // num_subspaces does not divide dims
        let err = PqCodebook::train(
            &vectors,
            PqParams {
                num_subspaces: 3,
                codebook_size: 8,
            },
            0,
            10,
        )
        .unwrap_err();
        assert!(err.to_string().contains("evenly divisible"));

        // codebook_size > 256
        let err = PqCodebook::train(
            &vectors,
            PqParams {
                num_subspaces: 2,
                codebook_size: 300,
            },
            0,
            10,
        )
        .unwrap_err();
        assert!(err.to_string().contains("codebook_size"));
    }

    #[test]
    fn encode_returns_valid_codes() {
        let vectors = make_vectors(50, 8, 1);
        let params = PqParams {
            num_subspaces: 4,
            codebook_size: 16,
        };
        let cb = PqCodebook::train(&vectors, params.clone(), 0, 10).unwrap();

        let codes = cb.encode(&vectors[0]).unwrap();
        assert_eq!(codes.len(), params.num_subspaces);
        for &c in &codes {
            assert!((c as usize) < params.codebook_size, "code {c} out of range");
        }
    }

    #[test]
    fn encoding_is_deterministic() {
        let vectors = make_vectors(30, 8, 7);
        let params = PqParams {
            num_subspaces: 2,
            codebook_size: 4,
        };
        let cb = PqCodebook::train(&vectors, params, 42, 10).unwrap();

        let c1 = cb.encode(&vectors[0]).unwrap();
        let c2 = cb.encode(&vectors[0]).unwrap();
        assert_eq!(c1, c2);
    }

    #[test]
    fn adc_distance_is_non_negative() {
        let vectors = make_vectors(40, 8, 3);
        let params = PqParams {
            num_subspaces: 4,
            codebook_size: 8,
        };
        let cb = PqCodebook::train(&vectors, params, 0, 10).unwrap();

        let query = &vectors[0];
        let table = cb.compute_distance_table(query).unwrap();
        let codes = cb.encode(query).unwrap();
        let dist = cb.adc_distance(&codes, &table);
        assert!(dist >= 0.0, "ADC distance must be non-negative, got {dist}");
    }

    #[test]
    fn adc_self_distance_is_near_zero() {
        // When a vector is encoded and the same vector is the query, the
        // ADC distance should be small (but not necessarily exactly zero
        // because of quantisation error).
        let vectors = make_vectors(200, 8, 9);
        let params = PqParams {
            num_subspaces: 4,
            codebook_size: 64,
        };
        let cb = PqCodebook::train(&vectors, params, 0, 10).unwrap();

        // Use the centroid of sub-space 0 as a test vector — its encode is
        // exact so its ADC self-distance is 0.
        let centroid = cb.codebooks[0][0]
            .iter()
            .cloned()
            .chain(cb.codebooks[1][0].iter().cloned())
            .chain(cb.codebooks[2][0].iter().cloned())
            .chain(cb.codebooks[3][0].iter().cloned())
            .collect::<Vec<f32>>();

        let table = cb.compute_distance_table(&centroid).unwrap();
        let codes = cb.encode(&centroid).unwrap();
        let dist = cb.adc_distance(&codes, &table);
        assert!(
            dist < 1e-4,
            "ADC self-distance for exact centroid should be ~0, got {dist}"
        );
    }

    #[test]
    fn codebook_roundtrip() {
        let vectors = make_vectors(50, 8, 5);
        let params = PqParams {
            num_subspaces: 4,
            codebook_size: 8,
        };
        let cb = PqCodebook::train(&vectors, params.clone(), 0, 10).unwrap();

        let bytes = cb.to_bytes();
        let cb2 = PqCodebook::from_bytes(&bytes).unwrap();

        assert_eq!(cb2.dims, cb.dims);
        assert_eq!(cb2.sub_dims, cb.sub_dims);
        assert_eq!(cb2.params, cb.params);
        assert_eq!(cb2.codebooks.len(), cb.codebooks.len());

        // Centroid values must be bit-for-bit identical after roundtrip.
        for (sub, sub2) in cb.codebooks.iter().zip(cb2.codebooks.iter()) {
            for (c, c2) in sub.iter().zip(sub2.iter()) {
                for (&a, &b) in c.iter().zip(c2.iter()) {
                    assert_eq!(a.to_bits(), b.to_bits());
                }
            }
        }
    }

    #[test]
    fn train_rejects_inconsistent_vector_dimensions() {
        let vectors = vec![vec![0.0; 8], vec![1.0; 7]];
        let err = PqCodebook::train(
            &vectors,
            PqParams {
                num_subspaces: 4,
                codebook_size: 8,
            },
            0,
            10,
        )
        .unwrap_err();
        assert!(err.to_string().contains("invalid dimension"));
    }

    #[test]
    fn train_clamps_codebook_size_to_available_vectors() {
        let vectors = make_vectors(2, 8, 11);
        let cb = PqCodebook::train(
            &vectors,
            PqParams {
                num_subspaces: 4,
                codebook_size: 8,
            },
            0,
            10,
        )
        .unwrap();
        assert_eq!(cb.params.codebook_size, 2);
        assert!(cb.codebooks.iter().all(|sub| sub.len() == 2));
    }

    #[test]
    fn encode_rejects_dimension_mismatch() {
        let vectors = make_vectors(10, 8, 13);
        let cb = PqCodebook::train(
            &vectors,
            PqParams {
                num_subspaces: 4,
                codebook_size: 8,
            },
            0,
            10,
        )
        .unwrap();
        let err = cb.encode(&vectors[0][..7]).unwrap_err();
        assert!(err.to_string().contains("dimension mismatch"));
    }

    #[test]
    fn distance_table_rejects_dimension_mismatch() {
        let vectors = make_vectors(10, 8, 17);
        let cb = PqCodebook::train(
            &vectors,
            PqParams {
                num_subspaces: 4,
                codebook_size: 8,
            },
            0,
            10,
        )
        .unwrap();
        let err = cb.compute_distance_table(&vectors[0][..7]).unwrap_err();
        assert!(err.to_string().contains("dimension mismatch"));
    }

    #[test]
    fn from_bytes_rejects_wrong_magic() {
        let mut bytes = PQ_CODEBOOK_MAGIC.to_vec();
        bytes[0] = b'X';
        bytes.extend_from_slice(&1u32.to_le_bytes()); // version
        let err = PqCodebook::from_bytes(&bytes).unwrap_err();
        assert!(err.to_string().contains("invalid magic"));
    }

    #[test]
    fn from_bytes_rejects_invalid_params() {
        let mut bytes = PQ_CODEBOOK_MAGIC.to_vec();
        bytes.extend_from_slice(&PQ_CODEBOOK_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&8u32.to_le_bytes()); // dims
        bytes.extend_from_slice(&0u32.to_le_bytes()); // pq_m
        bytes.extend_from_slice(&8u32.to_le_bytes()); // pq_k
        bytes.extend_from_slice(&2u32.to_le_bytes()); // sub_dims

        let err = PqCodebook::from_bytes(&bytes).unwrap_err();
        assert!(err.to_string().contains("num_subspaces"));
    }

    #[test]
    fn from_bytes_rejects_inconsistent_sub_dims() {
        let mut bytes = PQ_CODEBOOK_MAGIC.to_vec();
        bytes.extend_from_slice(&PQ_CODEBOOK_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&8u32.to_le_bytes()); // dims
        bytes.extend_from_slice(&4u32.to_le_bytes()); // pq_m
        bytes.extend_from_slice(&8u32.to_le_bytes()); // pq_k
        bytes.extend_from_slice(&3u32.to_le_bytes()); // sub_dims (should be 2)

        let err = PqCodebook::from_bytes(&bytes).unwrap_err();
        assert!(err.to_string().contains("sub_dims"));
    }
}
