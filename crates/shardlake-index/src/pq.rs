//! Product Quantizer (PQ) for approximate distance computation.
//!
//! # Overview
//!
//! A product quantizer decomposes each vector into `M` disjoint sub-vectors
//! and independently quantizes each sub-space into `K` centroids (codebook
//! entries).  This allows a vector to be represented as `M` small integer
//! codes, typically one byte per sub-space (so `K ≤ 256`).
//!
//! At query time, **Asymmetric Distance Computation (ADC)** is used: the
//! squared-Euclidean distance from each query sub-vector to all `K` codebook
//! entries is precomputed once, and the approximate distance from the query
//! to any encoded vector is the sum of the looked-up per-sub-space distances.
//! This achieves O(M·K) precomputation and O(M) per-vector scoring.
//!
//! # Binary format (`.pq` files)
//!
//! ```text
//! Offset   Size           Field
//! ------   ----           -----
//! 0        8              Magic bytes: b"SLKPQ\0\0\0"
//! 8        4              Format version (u32) — currently 1
//! 12       4              dims (u32)
//! 16       4              num_subspaces (u32)
//! 20       4              num_centroids (u32)
//!
//! --- Codebooks (num_subspaces × num_centroids entries) ---
//! per sub-space (outer loop, num_subspaces iterations):
//!   per centroid (inner loop, num_centroids iterations):
//!     sub_dims × 4       Centroid coordinates (f32 × sub_dims, little-endian)
//!     where sub_dims = dims / num_subspaces
//! ```

use std::io::{Cursor, Read, Write};

use rand::Rng;

use crate::{
    kmeans::{kmeans, nearest_centroid, sq_l2},
    IndexError, Result,
};

/// Magic bytes identifying a `.pq` product-quantizer artifact.
pub const PQ_MAGIC: &[u8; 8] = b"SLKPQ\0\0\0";
const PQ_FORMAT_VERSION: u32 = 1;
const PQ_HEADER_LEN: usize = 24; // 8 (magic) + 4 (version) + 4 (dims) + 4 (subspaces) + 4 (centroids)

/// Trained product quantizer.
///
/// Partitions the vector space into `num_subspaces` independent sub-spaces,
/// each quantized with `num_centroids` centroids.  Vectors are encoded as
/// `num_subspaces` code bytes (one per sub-space, supporting up to 256
/// centroids per sub-space).
///
/// # Examples
///
/// ```
/// use rand::SeedableRng;
/// use shardlake_index::pq::ProductQuantizer;
///
/// let mut rng = rand::rngs::StdRng::seed_from_u64(42);
/// let vectors: Vec<Vec<f32>> = (0..100)
///     .map(|i| vec![i as f32, (i * 2) as f32, (i * 3) as f32, (i * 4) as f32])
///     .collect();
///
/// let pq = ProductQuantizer::train(&vectors, 2, 16, 20, &mut rng).unwrap();
/// assert_eq!(pq.num_subspaces(), 2);
/// assert_eq!(pq.num_centroids(), 16);
///
/// let codes = pq.encode(&[50.0, 100.0, 150.0, 200.0]);
/// assert_eq!(codes.len(), 2);
/// ```
#[derive(Debug, Clone)]
pub struct ProductQuantizer {
    dims: usize,
    num_subspaces: usize,
    num_centroids: usize,
    /// Codebooks: `codebooks[m][k]` is the k-th centroid in sub-space `m`.
    codebooks: Vec<Vec<Vec<f32>>>,
}

impl ProductQuantizer {
    /// Train a product quantizer by running K-means independently on each sub-space.
    ///
    /// # Parameters
    ///
    /// - `vectors`: training vectors; all must have the same dimensionality.
    /// - `num_subspaces`: M — number of independent sub-spaces.  Must divide
    ///   `dims`.
    /// - `num_centroids`: K — number of centroids per sub-space; must be in
    ///   `[1, 256]`.
    /// - `iters`: number of K-means iterations per sub-space.
    /// - `rng`: seeded random number generator for reproducibility.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::Other`] if:
    /// - `vectors` is empty,
    /// - `!dims.is_multiple_of(num_subspaces)`,
    /// - `num_subspaces` is 0, or
    /// - `num_centroids` is 0 or greater than 256.
    pub fn train(
        vectors: &[Vec<f32>],
        num_subspaces: usize,
        num_centroids: usize,
        iters: u32,
        rng: &mut impl Rng,
    ) -> Result<Self> {
        if vectors.is_empty() {
            return Err(IndexError::Other(
                "ProductQuantizer::train: empty input".into(),
            ));
        }
        let dims = vectors[0].len();
        if dims == 0 {
            return Err(IndexError::Other(
                "ProductQuantizer::train: dims must be > 0".into(),
            ));
        }
        if num_subspaces == 0 {
            return Err(IndexError::Other(
                "ProductQuantizer::train: num_subspaces must be > 0".into(),
            ));
        }
        if !dims.is_multiple_of(num_subspaces) {
            return Err(IndexError::Other(format!(
                "ProductQuantizer::train: dims ({dims}) must be divisible by num_subspaces ({num_subspaces})"
            )));
        }
        if num_centroids == 0 || num_centroids > 256 {
            return Err(IndexError::Other(format!(
                "ProductQuantizer::train: num_centroids must be in [1, 256], got {num_centroids}"
            )));
        }

        let sub_dims = dims / num_subspaces;
        let mut codebooks = Vec::with_capacity(num_subspaces);

        for m in 0..num_subspaces {
            let sub_start = m * sub_dims;
            let sub_end = sub_start + sub_dims;
            let sub_vecs: Vec<Vec<f32>> = vectors
                .iter()
                .map(|v| v[sub_start..sub_end].to_vec())
                .collect();
            let centroids = kmeans(&sub_vecs, num_centroids, iters, rng);
            codebooks.push(centroids);
        }

        Ok(Self {
            dims,
            num_subspaces,
            num_centroids,
            codebooks,
        })
    }

    /// Construct a product quantizer directly from pre-computed codebooks.
    ///
    /// Useful when loading from a binary artifact or building test fixtures.
    ///
    /// # Panics
    ///
    /// Panics if `!dims.is_multiple_of(num_subspaces)`, or if codebook shapes are
    /// inconsistent with `num_subspaces`, `num_centroids`, and `sub_dims`.
    pub fn from_codebooks(
        dims: usize,
        num_subspaces: usize,
        num_centroids: usize,
        codebooks: Vec<Vec<Vec<f32>>>,
    ) -> Self {
        assert!(num_subspaces > 0, "num_subspaces must be > 0");
        assert!(
            dims.is_multiple_of(num_subspaces),
            "dims must be divisible by num_subspaces"
        );
        assert_eq!(
            codebooks.len(),
            num_subspaces,
            "codebooks length must match num_subspaces"
        );
        let sub_dims = dims / num_subspaces;
        for (m, book) in codebooks.iter().enumerate() {
            assert_eq!(
                book.len(),
                num_centroids,
                "codebooks[{m}] must have {num_centroids} entries"
            );
            for (k, c) in book.iter().enumerate() {
                assert_eq!(
                    c.len(),
                    sub_dims,
                    "codebooks[{m}][{k}] must have sub_dims={sub_dims} entries"
                );
            }
        }
        Self {
            dims,
            num_subspaces,
            num_centroids,
            codebooks,
        }
    }

    /// Total vector dimensionality.
    pub fn dims(&self) -> usize {
        self.dims
    }

    /// Number of sub-spaces M.
    pub fn num_subspaces(&self) -> usize {
        self.num_subspaces
    }

    /// Number of centroids K per sub-space.
    pub fn num_centroids(&self) -> usize {
        self.num_centroids
    }

    /// Sub-space dimension (`dims / num_subspaces`).
    pub fn sub_dims(&self) -> usize {
        self.dims / self.num_subspaces
    }

    /// Encode `vector` as a compact byte code: one code per sub-space.
    ///
    /// Each byte is the index of the nearest centroid in that sub-space.
    /// Returns a `Vec<u8>` of length `num_subspaces`.
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let sub_dims = self.sub_dims();
        let mut codes = Vec::with_capacity(self.num_subspaces);
        for m in 0..self.num_subspaces {
            let sub_start = m * sub_dims;
            let sub_vec = &vector[sub_start..sub_start + sub_dims];
            let nearest = nearest_centroid(sub_vec, &self.codebooks[m]);
            codes.push(nearest as u8);
        }
        codes
    }

    /// Precompute ADC distance tables for a query vector.
    ///
    /// Returns a 2-D table where `tables[m][k]` is the squared-Euclidean
    /// distance from the m-th query sub-vector to the k-th centroid in
    /// sub-space m.  Pass the returned tables to [`Self::score_with_tables`] to
    /// score many database vectors cheaply.
    #[must_use]
    pub fn precompute_distance_tables(&self, query: &[f32]) -> Vec<Vec<f32>> {
        let sub_dims = self.sub_dims();
        let mut tables = Vec::with_capacity(self.num_subspaces);
        for m in 0..self.num_subspaces {
            let sub_start = m * sub_dims;
            let q_sub = &query[sub_start..sub_start + sub_dims];
            let dists: Vec<f32> = self.codebooks[m]
                .iter()
                .map(|centroid| sq_l2(q_sub, centroid))
                .collect();
            tables.push(dists);
        }
        tables
    }

    /// Score an encoded vector against precomputed ADC distance tables.
    ///
    /// Returns the approximate squared-Euclidean distance between the query
    /// (encoded in `tables`) and the database vector encoded as `codes`.
    ///
    /// `tables` must have been produced by [`Self::precompute_distance_tables`] for
    /// the same quantizer.  `codes` must have length `num_subspaces`.
    #[must_use]
    pub fn score_with_tables(&self, tables: &[Vec<f32>], codes: &[u8]) -> f32 {
        tables
            .iter()
            .zip(codes.iter())
            .map(|(table, &code)| table[code as usize])
            .sum()
    }

    /// Decode PQ codes back to an approximate reconstructed vector.
    ///
    /// Replaces each code with the corresponding codebook centroid.  Useful
    /// for visualisation and sanity checks; reconstruction error depends on
    /// quantizer quality.
    #[must_use]
    pub fn decode(&self, codes: &[u8]) -> Vec<f32> {
        let sub_dims = self.sub_dims();
        let mut out = vec![0.0f32; self.dims];
        for (m, &code) in codes.iter().enumerate() {
            let centroid = &self.codebooks[m][code as usize];
            let out_start = m * sub_dims;
            out[out_start..out_start + sub_dims].copy_from_slice(centroid);
        }
        out
    }

    /// Return the codebooks slice (`[num_subspaces][num_centroids][sub_dims]`).
    pub fn codebooks(&self) -> &[Vec<Vec<f32>>] {
        &self.codebooks
    }

    /// Serialise the product quantizer state to bytes using the `.pq` binary
    /// format.
    ///
    /// The resulting bytes can be stored as a product-quantizer artifact and
    /// later recovered with [`ProductQuantizer::from_bytes`].
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.write_all(PQ_MAGIC)?;
        write_u32(&mut buf, PQ_FORMAT_VERSION)?;
        write_u32(&mut buf, self.dims as u32)?;
        write_u32(&mut buf, self.num_subspaces as u32)?;
        write_u32(&mut buf, self.num_centroids as u32)?;
        for book in &self.codebooks {
            for centroid in book {
                for &v in centroid {
                    buf.write_all(&v.to_le_bytes())?;
                }
            }
        }
        Ok(buf)
    }

    /// Deserialise a [`ProductQuantizer`] from bytes produced by
    /// [`ProductQuantizer::to_bytes`].
    ///
    /// Returns an error if the magic bytes, format version, or data layout are
    /// not recognised.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < PQ_HEADER_LEN {
            return Err(IndexError::Other(
                "product-quantizer payload is shorter than the fixed header".into(),
            ));
        }

        let mut cur = Cursor::new(bytes);

        let mut magic = [0u8; 8];
        cur.read_exact(&mut magic)?;
        if &magic != PQ_MAGIC {
            return Err(IndexError::Other(
                "invalid product-quantizer magic bytes".into(),
            ));
        }

        let fmt_ver = read_u32(&mut cur)?;
        if fmt_ver != PQ_FORMAT_VERSION {
            return Err(IndexError::Other(format!(
                "unsupported product-quantizer format version {fmt_ver}"
            )));
        }

        let dims = read_u32(&mut cur)? as usize;
        let num_subspaces = read_u32(&mut cur)? as usize;
        let num_centroids = read_u32(&mut cur)? as usize;

        if dims == 0 {
            return Err(IndexError::Other(
                "product-quantizer dims must be > 0".into(),
            ));
        }
        if num_subspaces == 0 {
            return Err(IndexError::Other(
                "product-quantizer num_subspaces must be > 0".into(),
            ));
        }
        if num_centroids == 0 {
            return Err(IndexError::Other(
                "product-quantizer num_centroids must be > 0".into(),
            ));
        }
        if !dims.is_multiple_of(num_subspaces) {
            return Err(IndexError::Other(format!(
                "product-quantizer dims ({dims}) not divisible by num_subspaces ({num_subspaces})"
            )));
        }

        let sub_dims = dims / num_subspaces;
        let payload_len = num_subspaces
            .checked_mul(num_centroids)
            .and_then(|v| v.checked_mul(sub_dims))
            .and_then(|v| v.checked_mul(std::mem::size_of::<f32>()))
            .ok_or_else(|| IndexError::Other("product-quantizer payload size overflow".into()))?;
        let expected_len = PQ_HEADER_LEN
            .checked_add(payload_len)
            .ok_or_else(|| IndexError::Other("product-quantizer payload size overflow".into()))?;
        if bytes.len() != expected_len {
            return Err(IndexError::Other(format!(
                "product-quantizer payload length mismatch: expected {expected_len} bytes, got {}",
                bytes.len()
            )));
        }

        let mut codebooks = Vec::with_capacity(num_subspaces);
        for _ in 0..num_subspaces {
            let mut book = Vec::with_capacity(num_centroids);
            for _ in 0..num_centroids {
                book.push(read_f32_vec(&mut cur, sub_dims)?);
            }
            codebooks.push(book);
        }

        Ok(Self {
            dims,
            num_subspaces,
            num_centroids,
            codebooks,
        })
    }
}

// ── I/O helpers ──────────────────────────────────────────────────────────────

fn write_u32(buf: &mut Vec<u8>, v: u32) -> Result<()> {
    buf.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn read_u32(cur: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut buf = [0u8; 4];
    cur.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_f32_vec(cur: &mut Cursor<&[u8]>, dims: usize) -> Result<Vec<f32>> {
    let mut v = vec![0.0f32; dims];
    for x in v.iter_mut() {
        let mut buf = [0u8; 4];
        cur.read_exact(&mut buf)?;
        *x = f32::from_le_bytes(buf);
    }
    Ok(v)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    fn seeded_rng() -> rand::rngs::StdRng {
        rand::rngs::StdRng::seed_from_u64(0xdead_beef)
    }

    /// 100 4-D vectors evenly spaced along the diagonal.
    fn diagonal_vectors() -> Vec<Vec<f32>> {
        (0..100)
            .map(|i| vec![i as f32, (i * 2) as f32, (i * 3) as f32, (i * 4) as f32])
            .collect()
    }

    #[test]
    fn train_produces_correct_shape() {
        let mut rng = seeded_rng();
        let vecs = diagonal_vectors();
        let pq = ProductQuantizer::train(&vecs, 2, 8, 20, &mut rng).unwrap();

        assert_eq!(pq.dims(), 4);
        assert_eq!(pq.num_subspaces(), 2);
        assert_eq!(pq.num_centroids(), 8);
        assert_eq!(pq.sub_dims(), 2);
        assert_eq!(pq.codebooks().len(), 2);
        assert!(pq.codebooks().iter().all(|book| book.len() == 8));
        assert!(pq
            .codebooks()
            .iter()
            .all(|book| book.iter().all(|c| c.len() == 2)));
    }

    #[test]
    fn encode_returns_one_code_per_subspace() {
        let mut rng = seeded_rng();
        let vecs = diagonal_vectors();
        let pq = ProductQuantizer::train(&vecs, 2, 8, 20, &mut rng).unwrap();

        let codes = pq.encode(&[50.0, 100.0, 150.0, 200.0]);
        assert_eq!(codes.len(), 2);
        assert!(codes.iter().all(|&c| (c as usize) < 8));
    }

    #[test]
    fn decode_reconstructs_approximate_vector() {
        let mut rng = seeded_rng();
        let vecs = diagonal_vectors();
        let pq = ProductQuantizer::train(&vecs, 2, 16, 30, &mut rng).unwrap();

        let original = vec![50.0f32, 100.0, 150.0, 200.0];
        let codes = pq.encode(&original);
        let reconstructed = pq.decode(&codes);

        assert_eq!(reconstructed.len(), 4);
        // Reconstruction should be close to the original (within quantization error).
        let mse: f32 = original
            .iter()
            .zip(&reconstructed)
            .map(|(a, b)| (a - b) * (a - b))
            .sum::<f32>()
            / original.len() as f32;
        // With 16 centroids per sub-space over 100 well-spaced vectors the
        // mean-squared error should be well below 100.
        assert!(mse < 100.0, "reconstruction MSE too high: {mse}");
    }

    #[test]
    fn adc_score_approximates_squared_euclidean() {
        let mut rng = seeded_rng();
        let vecs = diagonal_vectors();
        let pq = ProductQuantizer::train(&vecs, 2, 32, 30, &mut rng).unwrap();

        let query = vec![30.0f32, 60.0, 90.0, 120.0];
        let target = vec![31.0f32, 62.0, 92.0, 123.0];

        let tables = pq.precompute_distance_tables(&query);
        let codes = pq.encode(&target);
        let approx_score = pq.score_with_tables(&tables, &codes);

        // Exact squared-L2 distance.
        let exact_sq_l2: f32 = query
            .iter()
            .zip(&target)
            .map(|(a, b)| (a - b) * (a - b))
            .sum();

        // The approximate score should be in the same order of magnitude as the
        // exact distance; allow a large relative tolerance since we only have 32
        // centroids per sub-space.
        let ratio = approx_score / (exact_sq_l2 + 1e-6);
        assert!(
            ratio < 100.0,
            "approx ADC score ({approx_score}) is more than 100× the exact sq-L2 ({exact_sq_l2})"
        );
    }

    #[test]
    fn adc_ranks_nearer_vector_first() {
        let mut rng = seeded_rng();
        let vecs = diagonal_vectors();
        let pq = ProductQuantizer::train(&vecs, 2, 16, 30, &mut rng).unwrap();

        let query = vec![50.0f32, 100.0, 150.0, 200.0];
        let near = vec![51.0f32, 102.0, 151.0, 202.0]; // close to query
        let far = vec![0.0f32, 0.0, 0.0, 0.0]; // far from query

        let tables = pq.precompute_distance_tables(&query);
        let near_codes = pq.encode(&near);
        let far_codes = pq.encode(&far);

        let near_score = pq.score_with_tables(&tables, &near_codes);
        let far_score = pq.score_with_tables(&tables, &far_codes);

        assert!(
            near_score < far_score,
            "PQ should score the nearer vector lower: near={near_score}, far={far_score}"
        );
    }

    #[test]
    fn train_fails_on_empty_input() {
        let mut rng = seeded_rng();
        let err = ProductQuantizer::train(&[], 2, 8, 10, &mut rng).unwrap_err();
        assert!(err.to_string().contains("empty input"));
    }

    #[test]
    fn train_fails_when_dims_not_divisible() {
        let mut rng = seeded_rng();
        let vecs: Vec<Vec<f32>> = (0..10)
            .map(|i| vec![i as f32, i as f32, i as f32])
            .collect();
        // 3 dims, 2 subspaces → 3 % 2 != 0
        let err = ProductQuantizer::train(&vecs, 2, 4, 10, &mut rng).unwrap_err();
        assert!(
            err.to_string().contains("divisible by num_subspaces"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn train_fails_on_zero_num_centroids() {
        let mut rng = seeded_rng();
        let vecs = diagonal_vectors();
        let err = ProductQuantizer::train(&vecs, 2, 0, 10, &mut rng).unwrap_err();
        assert!(err
            .to_string()
            .contains("num_centroids must be in [1, 256]"));
    }

    #[test]
    fn train_fails_on_centroids_exceeding_256() {
        let mut rng = seeded_rng();
        let vecs = diagonal_vectors();
        let err = ProductQuantizer::train(&vecs, 2, 257, 10, &mut rng).unwrap_err();
        assert!(err
            .to_string()
            .contains("num_centroids must be in [1, 256]"));
    }

    #[test]
    fn roundtrip_serialisation() {
        let mut rng = seeded_rng();
        let vecs = diagonal_vectors();
        let original = ProductQuantizer::train(&vecs, 2, 8, 20, &mut rng).unwrap();

        let bytes = original.to_bytes().unwrap();
        assert_eq!(&bytes[..8], PQ_MAGIC);

        let recovered = ProductQuantizer::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.dims(), original.dims());
        assert_eq!(recovered.num_subspaces(), original.num_subspaces());
        assert_eq!(recovered.num_centroids(), original.num_centroids());
        for (m, (ob, rb)) in original
            .codebooks()
            .iter()
            .zip(recovered.codebooks().iter())
            .enumerate()
        {
            for (k, (oc, rc)) in ob.iter().zip(rb.iter()).enumerate() {
                assert_eq!(oc, rc, "codebooks[{m}][{k}] mismatch after round-trip");
            }
        }
    }

    #[test]
    fn from_bytes_rejects_wrong_magic() {
        let mut bad = b"BADMAGIC".to_vec();
        bad.extend_from_slice(&PQ_FORMAT_VERSION.to_le_bytes());
        bad.extend_from_slice(&4u32.to_le_bytes()); // dims
        bad.extend_from_slice(&2u32.to_le_bytes()); // subspaces
        bad.extend_from_slice(&4u32.to_le_bytes()); // centroids
                                                    // Codebook data: 2 subspaces × 4 centroids × 2 sub_dims f32s
        bad.extend_from_slice(&vec![0u8; 2 * 4 * 2 * 4]);
        let err = ProductQuantizer::from_bytes(&bad).unwrap_err();
        assert!(err.to_string().contains("magic"), "unexpected error: {err}");
    }

    #[test]
    fn from_bytes_rejects_zero_dims() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(PQ_MAGIC);
        bytes.extend_from_slice(&PQ_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes()); // dims = 0
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&4u32.to_le_bytes());
        let err = ProductQuantizer::from_bytes(&bytes).unwrap_err();
        assert!(err.to_string().contains("dims must be > 0"), "{err}");
    }

    #[test]
    fn from_bytes_rejects_payload_length_mismatch() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(PQ_MAGIC);
        bytes.extend_from_slice(&PQ_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&4u32.to_le_bytes()); // dims
        bytes.extend_from_slice(&2u32.to_le_bytes()); // subspaces
        bytes.extend_from_slice(&4u32.to_le_bytes()); // centroids
                                                      // Only 1 f32 instead of 2*4*2*4 = 64 bytes
        bytes.extend_from_slice(&1.0f32.to_le_bytes());
        let err = ProductQuantizer::from_bytes(&bytes).unwrap_err();
        assert!(
            err.to_string().contains("payload length mismatch"),
            "unexpected error: {err}"
        );
    }
}
