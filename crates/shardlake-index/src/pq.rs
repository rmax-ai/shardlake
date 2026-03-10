//! Product Quantization (PQ) encoder and asymmetric distance computation.
//!
//! Splits each D-dimensional vector into `m` sub-vectors of `D/m` dimensions.
//! Each sub-space has a separate codebook of `k_sub` codewords trained with K-means.
//! Encoded vectors are stored as `m` byte codes (valid for `k_sub ≤ 256`).
//!
//! At query time, an asymmetric distance computation (ADC) table is built once per
//! query and individual approximate distances are computed in O(m) lookups.

use std::io::{Cursor, Read, Write};

use rand::Rng;

use crate::{kmeans::kmeans, IndexError, Result};

/// Magic bytes for PQ codebook binary files.
pub const PQ_MAGIC: &[u8; 8] = b"SLKPQ\0\0\0";
const PQ_FORMAT_VERSION: u32 = 1;

/// Product Quantization codebook.
///
/// Splits each `dims`-dimensional vector into `m` sub-vectors of `sub_dim = dims / m`
/// dimensions. Each sub-space has an independent codebook of `k_sub` codewords trained
/// with K-means. Vectors are encoded as `m` byte indices (for `k_sub ≤ 256`).
#[derive(Debug, Clone)]
pub struct PqCodebook {
    /// Number of sub-spaces.
    pub m: usize,
    /// Number of codewords per sub-space (must be ≤ 256 for `u8` codes).
    pub k_sub: usize,
    /// Total vector dimensionality.
    pub dims: usize,
    /// Dimensionality of each sub-vector (`dims / m`).
    pub sub_dim: usize,
    /// Codebooks indexed as `codebooks[sub_space][codeword][dimension]`.
    pub codebooks: Vec<Vec<Vec<f32>>>,
}

impl PqCodebook {
    /// Train PQ codebooks by running K-means independently on each sub-space.
    ///
    /// # Errors
    ///
    /// Returns an error if `vectors` is empty, `m == 0`, `k_sub` is outside
    /// `[1, 256]`, or `dims % m != 0`.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let codebook = PqCodebook::train(&vecs, 4, 256, 20, &mut rng)?;
    /// let codes = codebook.encode(&vecs[0]);
    /// ```
    pub fn train(
        vectors: &[Vec<f32>],
        m: usize,
        k_sub: usize,
        kmeans_iters: u32,
        rng: &mut impl Rng,
    ) -> Result<Self> {
        if vectors.is_empty() {
            return Err(IndexError::Other("PQ train: empty vectors".into()));
        }
        if m == 0 {
            return Err(IndexError::Other("PQ train: m must be > 0".into()));
        }
        if k_sub == 0 || k_sub > 256 {
            return Err(IndexError::Other(
                "PQ train: k_sub must be in [1, 256]".into(),
            ));
        }
        let dims = vectors[0].len();
        if !dims.is_multiple_of(m) {
            return Err(IndexError::Other(format!(
                "PQ train: dims ({dims}) must be divisible by m ({m})"
            )));
        }
        let sub_dim = dims / m;

        let mut codebooks = Vec::with_capacity(m);
        for sub_idx in 0..m {
            let start = sub_idx * sub_dim;
            let end = start + sub_dim;
            let sub_vecs: Vec<Vec<f32>> = vectors.iter().map(|v| v[start..end].to_vec()).collect();
            let codebook = kmeans(&sub_vecs, k_sub, kmeans_iters, rng);
            codebooks.push(codebook);
        }

        Ok(Self {
            m,
            k_sub,
            dims,
            sub_dim,
            codebooks,
        })
    }

    /// Encode a vector into `m` byte codes by finding the nearest codeword in each sub-space.
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        (0..self.m)
            .map(|sub_idx| {
                let start = sub_idx * self.sub_dim;
                let end = start + self.sub_dim;
                let sub_vec = &vector[start..end];
                self.codebooks[sub_idx]
                    .iter()
                    .enumerate()
                    .map(|(i, cw)| (i, sq_l2_slice(sub_vec, cw)))
                    .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
                    .map(|(i, _)| i as u8)
                    .unwrap_or(0)
            })
            .collect()
    }

    /// Reconstruct (decode) a vector from its `m` byte codes.
    ///
    /// The reconstruction is lossy: it concatenates the nearest codeword from each sub-space.
    pub fn decode(&self, codes: &[u8]) -> Vec<f32> {
        let mut result = Vec::with_capacity(self.dims);
        for (sub_idx, &code) in codes.iter().enumerate() {
            result.extend_from_slice(&self.codebooks[sub_idx][code as usize]);
        }
        result
    }

    /// Pre-compute an asymmetric distance computation (ADC) table for `query`.
    ///
    /// Returns a table `table[m][k_sub]` where `table[i][j]` is the squared-L2
    /// distance from the `i`-th query sub-vector to codeword `j` in sub-space `i`.
    /// Use [`approx_distance`] to evaluate an encoded vector against this table in O(m).
    pub fn adc_table(&self, query: &[f32]) -> Vec<Vec<f32>> {
        (0..self.m)
            .map(|sub_idx| {
                let start = sub_idx * self.sub_dim;
                let end = start + self.sub_dim;
                let q_sub = &query[start..end];
                self.codebooks[sub_idx]
                    .iter()
                    .map(|cw| sq_l2_slice(q_sub, cw))
                    .collect()
            })
            .collect()
    }

    /// Compute an approximate distance between a query and a PQ-encoded vector.
    ///
    /// Sums up lookup values from a pre-computed ADC table (see [`adc_table`]) in O(m).
    pub fn approx_distance(&self, codes: &[u8], table: &[Vec<f32>]) -> f32 {
        codes
            .iter()
            .enumerate()
            .map(|(m_idx, &code)| table[m_idx][code as usize])
            .sum()
    }

    /// Serialise to a little-endian binary representation.
    ///
    /// # Errors
    ///
    /// Propagates any I/O errors encountered during serialisation.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.write_all(PQ_MAGIC)?;
        write_u32(&mut buf, PQ_FORMAT_VERSION)?;
        write_u32(&mut buf, self.dims as u32)?;
        write_u32(&mut buf, self.m as u32)?;
        write_u32(&mut buf, self.k_sub as u32)?;
        for sub_book in &self.codebooks {
            for codeword in sub_book {
                for &v in codeword {
                    buf.write_all(&v.to_le_bytes())?;
                }
            }
        }
        Ok(buf)
    }

    /// Deserialise from a little-endian binary representation.
    ///
    /// # Errors
    ///
    /// Returns an error on invalid magic bytes, unsupported format version, or
    /// truncated input.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cur = Cursor::new(bytes);

        let mut magic = [0u8; 8];
        cur.read_exact(&mut magic)?;
        if &magic != PQ_MAGIC {
            return Err(IndexError::Other("invalid PQ codebook magic".into()));
        }

        let fmt_ver = read_u32(&mut cur)?;
        if fmt_ver != PQ_FORMAT_VERSION {
            return Err(IndexError::Other(format!(
                "unsupported PQ format version {fmt_ver}"
            )));
        }

        let dims = read_u32(&mut cur)? as usize;
        let m = read_u32(&mut cur)? as usize;
        let k_sub = read_u32(&mut cur)? as usize;
        let sub_dim = if m > 0 { dims / m } else { 0 };

        let mut codebooks = Vec::with_capacity(m);
        for _ in 0..m {
            let mut sub_book = Vec::with_capacity(k_sub);
            for _ in 0..k_sub {
                sub_book.push(read_f32_vec(&mut cur, sub_dim)?);
            }
            codebooks.push(sub_book);
        }

        Ok(Self {
            m,
            k_sub,
            dims,
            sub_dim,
            codebooks,
        })
    }
}

/// Squared L2 distance between two slices.
fn sq_l2_slice(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
}

// ── Binary I/O helpers ──────────────────────────────────────────────────────

fn write_u32(buf: &mut Vec<u8>, v: u32) -> Result<()> {
    buf.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn read_u32(cur: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut buf = [0u8; 4];
    cur.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_f32_vec(cur: &mut Cursor<&[u8]>, n: usize) -> Result<Vec<f32>> {
    let mut v = vec![0.0f32; n];
    for x in v.iter_mut() {
        let mut buf = [0u8; 4];
        cur.read_exact(&mut buf)?;
        *x = f32::from_le_bytes(buf);
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    fn make_vecs(n: usize, dims: usize) -> Vec<Vec<f32>> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        (0..n)
            .map(|_| (0..dims).map(|_| rng.gen::<f32>()).collect())
            .collect()
    }

    #[test]
    fn test_train_encode_decode() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(0);
        let vecs = make_vecs(100, 8);
        let pq = PqCodebook::train(&vecs, 4, 16, 10, &mut rng).unwrap();
        assert_eq!(pq.m, 4);
        assert_eq!(pq.k_sub, 16);
        assert_eq!(pq.sub_dim, 2);

        let codes = pq.encode(&vecs[0]);
        assert_eq!(codes.len(), 4);
        let decoded = pq.decode(&codes);
        assert_eq!(decoded.len(), 8);
    }

    #[test]
    fn test_adc_and_approx_distance() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(1);
        let vecs = make_vecs(50, 4);
        let pq = PqCodebook::train(&vecs, 2, 8, 10, &mut rng).unwrap();

        let query = vecs[0].clone();
        let table = pq.adc_table(&query);
        assert_eq!(table.len(), 2);
        assert_eq!(table[0].len(), 8);

        // Self-distance via decoded codes should be ≥ 0
        let codes = pq.encode(&query);
        let dist = pq.approx_distance(&codes, &table);
        assert!(dist >= 0.0);
    }

    #[test]
    fn test_roundtrip_serialisation() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(2);
        let vecs = make_vecs(50, 4);
        let pq = PqCodebook::train(&vecs, 2, 4, 5, &mut rng).unwrap();
        let bytes = pq.to_bytes().unwrap();
        let pq2 = PqCodebook::from_bytes(&bytes).unwrap();
        assert_eq!(pq2.m, pq.m);
        assert_eq!(pq2.k_sub, pq.k_sub);
        assert_eq!(pq2.dims, pq.dims);
        assert_eq!(pq2.sub_dim, pq.sub_dim);
        assert_eq!(pq2.codebooks.len(), pq.codebooks.len());
    }

    #[test]
    fn test_train_errors() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(3);
        // Empty vectors
        assert!(PqCodebook::train(&[], 2, 4, 5, &mut rng).is_err());
        // m=0
        let vecs = make_vecs(10, 4);
        assert!(PqCodebook::train(&vecs, 0, 4, 5, &mut rng).is_err());
        // dims not divisible by m
        assert!(PqCodebook::train(&vecs, 3, 4, 5, &mut rng).is_err());
        // k_sub=0
        assert!(PqCodebook::train(&vecs, 2, 0, 5, &mut rng).is_err());
        // k_sub>256
        assert!(PqCodebook::train(&vecs, 2, 300, 5, &mut rng).is_err());
    }
}
