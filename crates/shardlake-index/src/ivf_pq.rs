//! IVF-PQ index: coarse IVF quantizer with PQ-compressed posting lists.
//!
//! The search pipeline is:
//! 1. Route the query to the `nprobe` nearest coarse centroids (IVF candidate selection).
//! 2. For each selected cluster, compute approximate distances using an ADC table (PQ).
//! 3. Collect and rank the top `rerank_factor × k` candidates by approximate distance.
//! 4. Optionally rerank those candidates with exact distances computed from raw vectors.
//! 5. Return the final top-k results.
//!
//! # Binary format (`ivfpq.bin`)
//!
//! ```text
//! magic            : [u8; 8]  = b"SLKIVPQ\0"
//! format_ver       : u32      = 1
//! dims             : u32
//! metric           : u32      (0=Cosine, 1=Euclidean, 2=InnerProduct)
//! pq_len           : u64      (byte length of embedded PQ codebook blob)
//! pq_blob          : [u8; pq_len]
//! centroid_count   : u32
//! centroids        : centroid_count × dims × f32
//! posting_lists    : centroid_count entries
//!   count          : u64
//!   vectors        : count entries
//!     id           : u64
//!     codes        : pq.m × u8
//!     raw          : dims × f32
//! ```

use std::{
    collections::HashSet,
    io::{Cursor, Read, Write},
};

use rand::Rng;
use tracing::{debug, info};

use shardlake_core::types::{DistanceMetric, SearchResult, VectorId, VectorRecord};

use crate::{
    exact::distance,
    kmeans::{kmeans, nearest_centroid, top_n_centroids},
    pq::PqCodebook,
    IndexError, Result,
};

/// Magic bytes for IVF-PQ binary index files.
pub const IVFPQ_MAGIC: &[u8; 8] = b"SLKIVPQ\0";
const IVFPQ_FORMAT_VERSION: u32 = 1;

/// Multiplier applied to `k` to determine the candidate pool size before reranking.
const RERANK_FACTOR: usize = 4;

/// Per-cluster posting list storing PQ-encoded and raw vectors.
#[derive(Debug, Clone)]
pub struct PostingList {
    /// Vector IDs assigned to this cluster.
    pub ids: Vec<VectorId>,
    /// PQ byte codes, one entry per vector (`codes[i]` corresponds to `ids[i]`).
    pub codes: Vec<Vec<u8>>,
    /// Raw vectors retained for exact reranking (`raw[i]` corresponds to `ids[i]`).
    pub raw: Vec<Vec<f32>>,
}

impl PostingList {
    fn new() -> Self {
        Self {
            ids: Vec::new(),
            codes: Vec::new(),
            raw: Vec::new(),
        }
    }
}

/// IVF-PQ index: a coarse IVF quantizer whose posting lists hold PQ-encoded vectors.
///
/// Construct with [`IvfPqIndex::build`], query with [`IvfPqIndex::search`], and
/// persist with [`IvfPqIndex::to_bytes`] / [`IvfPqIndex::from_bytes`].
#[derive(Debug, Clone)]
pub struct IvfPqIndex {
    /// Distance metric used by this index.
    pub metric: DistanceMetric,
    /// Total vector dimensionality.
    pub dims: usize,
    /// Coarse quantizer centroids (one per cluster).
    pub centroids: Vec<Vec<f32>>,
    /// PQ codebook shared across all clusters.
    pub pq: PqCodebook,
    /// Posting lists, one per coarse centroid.
    pub posting_lists: Vec<PostingList>,
}

impl IvfPqIndex {
    /// Build an IVF-PQ index from a flat list of vector records.
    ///
    /// # Parameters
    ///
    /// - `records` – input vectors (non-empty).
    /// - `num_centroids` – number of IVF clusters.
    /// - `pq_m` – number of PQ sub-spaces (`dims` must be divisible by `pq_m`).
    /// - `pq_k_sub` – codewords per PQ sub-space (1–256).
    /// - `kmeans_iters` – K-means iterations for both the coarse quantizer and PQ.
    /// - `metric` – distance metric used when computing exact distances during reranking.
    /// - `rng` – random number generator (seed for reproducibility).
    ///
    /// # Errors
    ///
    /// Returns an error if `records` is empty or if the PQ parameters are invalid.
    pub fn build(
        records: &[VectorRecord],
        num_centroids: usize,
        pq_m: usize,
        pq_k_sub: usize,
        kmeans_iters: u32,
        metric: DistanceMetric,
        rng: &mut impl Rng,
    ) -> Result<Self> {
        if records.is_empty() {
            return Err(IndexError::Other(
                "IvfPqIndex::build: no records provided".into(),
            ));
        }

        let dims = records[0].data.len();
        let vecs: Vec<Vec<f32>> = records.iter().map(|r| r.data.clone()).collect();
        let k = num_centroids.min(records.len());

        info!(
            n = records.len(),
            k, pq_m, pq_k_sub, "Training IVF-PQ coarse quantizer"
        );

        // Train coarse quantizer.
        let centroids = kmeans(&vecs, k, kmeans_iters, rng);

        // Assign each vector to its nearest centroid.
        let assignments: Vec<usize> = vecs
            .iter()
            .map(|v| nearest_centroid(v, &centroids))
            .collect();

        info!("Training PQ codebook on {} vectors", vecs.len());

        // Train a global PQ codebook on all vectors.
        let pq = PqCodebook::train(&vecs, pq_m, pq_k_sub, kmeans_iters, rng)?;

        // Build posting lists.
        let mut posting_lists: Vec<PostingList> = (0..k).map(|_| PostingList::new()).collect();
        for (rec, &cluster) in records.iter().zip(assignments.iter()) {
            let codes = pq.encode(&rec.data);
            posting_lists[cluster].ids.push(rec.id);
            posting_lists[cluster].codes.push(codes);
            posting_lists[cluster].raw.push(rec.data.clone());
        }

        for (i, pl) in posting_lists.iter().enumerate() {
            debug!(cluster = i, count = pl.ids.len(), "Posting list built");
        }

        Ok(Self {
            metric,
            dims,
            centroids,
            pq,
            posting_lists,
        })
    }

    /// Search for the `k` approximate nearest neighbours of `query`.
    ///
    /// # Parameters
    ///
    /// - `nprobe` – number of clusters to probe (capped at `centroids.len()`).
    /// - `rerank` – when `true`, the top `RERANK_FACTOR × k` PQ candidates are
    ///   reranked using exact distances computed from the stored raw vectors.
    ///
    /// Returns at most `k` results ordered by score (lower is better for L2/cosine).
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        nprobe: usize,
        rerank: bool,
    ) -> Vec<SearchResult> {
        if self.centroids.is_empty() {
            return Vec::new();
        }

        let nprobe = nprobe.min(self.centroids.len());
        let probe_indices = top_n_centroids(query, &self.centroids, nprobe);

        // Pre-compute asymmetric distance computation table.
        let adc = self.pq.adc_table(query);

        let candidate_k = if rerank {
            (k * RERANK_FACTOR).max(k)
        } else {
            k
        };

        // Collect candidates with PQ approximate distances, deduplicating by id.
        let mut seen: HashSet<VectorId> = HashSet::new();
        let mut candidates: Vec<(VectorId, f32)> = Vec::new();

        for cluster_idx in probe_indices {
            let pl = &self.posting_lists[cluster_idx];
            for (&id, codes) in pl.ids.iter().zip(pl.codes.iter()) {
                if seen.insert(id) {
                    let approx_dist = self.pq.approx_distance(codes, &adc);
                    candidates.push((id, approx_dist));
                }
            }
        }

        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(candidate_k);

        if rerank {
            // Rerank candidates using exact distances from raw vectors.
            let mut reranked: Vec<SearchResult> = candidates
                .iter()
                .filter_map(|(id, _)| {
                    self.posting_lists.iter().find_map(|pl| {
                        pl.ids
                            .iter()
                            .position(|&pid| pid == *id)
                            .map(|pos| SearchResult {
                                id: *id,
                                score: distance(query, &pl.raw[pos], self.metric),
                                metadata: None,
                            })
                    })
                })
                .collect();
            reranked.sort_by(|a, b| {
                a.score
                    .partial_cmp(&b.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            reranked.truncate(k);
            reranked
        } else {
            candidates.truncate(k);
            candidates
                .into_iter()
                .map(|(id, score)| SearchResult {
                    id,
                    score,
                    metadata: None,
                })
                .collect()
        }
    }

    /// Serialise the index to a little-endian binary representation.
    ///
    /// # Errors
    ///
    /// Propagates any I/O errors encountered during serialisation.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.write_all(IVFPQ_MAGIC)?;
        write_u32(&mut buf, IVFPQ_FORMAT_VERSION)?;
        write_u32(&mut buf, self.dims as u32)?;

        let metric_code: u32 = match self.metric {
            DistanceMetric::Cosine => 0,
            DistanceMetric::Euclidean => 1,
            DistanceMetric::InnerProduct => 2,
        };
        write_u32(&mut buf, metric_code)?;

        // Embed PQ codebook blob.
        let pq_bytes = self.pq.to_bytes()?;
        write_u64(&mut buf, pq_bytes.len() as u64)?;
        buf.write_all(&pq_bytes)?;

        // Write coarse centroids.
        write_u32(&mut buf, self.centroids.len() as u32)?;
        for c in &self.centroids {
            for &v in c {
                buf.write_all(&v.to_le_bytes())?;
            }
        }

        // Write posting lists.
        for pl in &self.posting_lists {
            write_u64(&mut buf, pl.ids.len() as u64)?;
            for ((&id, codes), raw) in pl.ids.iter().zip(pl.codes.iter()).zip(pl.raw.iter()) {
                write_u64(&mut buf, id.0)?;
                buf.write_all(codes)?;
                for &v in raw {
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
    /// truncated / corrupt input.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cur = Cursor::new(bytes);

        let mut magic = [0u8; 8];
        cur.read_exact(&mut magic)?;
        if &magic != IVFPQ_MAGIC {
            return Err(IndexError::Other("invalid IVF-PQ magic bytes".into()));
        }

        let fmt_ver = read_u32(&mut cur)?;
        if fmt_ver != IVFPQ_FORMAT_VERSION {
            return Err(IndexError::Other(format!(
                "unsupported IVF-PQ format version {fmt_ver}"
            )));
        }

        let dims = read_u32(&mut cur)? as usize;
        let metric_code = read_u32(&mut cur)?;
        let metric = match metric_code {
            0 => DistanceMetric::Cosine,
            1 => DistanceMetric::Euclidean,
            2 => DistanceMetric::InnerProduct,
            _ => {
                return Err(IndexError::Other(format!(
                    "unknown metric code {metric_code}"
                )))
            }
        };

        // Read embedded PQ codebook blob.
        let pq_len = read_u64(&mut cur)? as usize;
        let mut pq_bytes = vec![0u8; pq_len];
        cur.read_exact(&mut pq_bytes)?;
        let pq = PqCodebook::from_bytes(&pq_bytes)?;

        // Read coarse centroids.
        let num_centroids = read_u32(&mut cur)? as usize;
        let mut centroids = Vec::with_capacity(num_centroids);
        for _ in 0..num_centroids {
            centroids.push(read_f32_vec(&mut cur, dims)?);
        }

        // Read posting lists.
        let mut posting_lists = Vec::with_capacity(num_centroids);
        for _ in 0..num_centroids {
            let count = read_u64(&mut cur)? as usize;
            let mut pl = PostingList::new();
            pl.ids.reserve(count);
            pl.codes.reserve(count);
            pl.raw.reserve(count);
            for _ in 0..count {
                pl.ids.push(VectorId(read_u64(&mut cur)?));
                let mut code_buf = vec![0u8; pq.m];
                cur.read_exact(&mut code_buf)?;
                pl.codes.push(code_buf);
                pl.raw.push(read_f32_vec(&mut cur, dims)?);
            }
            posting_lists.push(pl);
        }

        Ok(Self {
            metric,
            dims,
            centroids,
            pq,
            posting_lists,
        })
    }
}

// ── Binary I/O helpers ──────────────────────────────────────────────────────

fn write_u32(buf: &mut Vec<u8>, v: u32) -> Result<()> {
    buf.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn write_u64(buf: &mut Vec<u8>, v: u64) -> Result<()> {
    buf.write_all(&v.to_le_bytes())?;
    Ok(())
}

fn read_u32(cur: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut buf = [0u8; 4];
    cur.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64(cur: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut buf = [0u8; 8];
    cur.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
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
    use shardlake_core::types::VectorId;

    fn make_records(n: usize, dims: usize) -> Vec<VectorRecord> {
        (0..n)
            .map(|i| VectorRecord {
                id: VectorId(i as u64),
                data: (0..dims).map(|d| (i * dims + d) as f32 / 100.0).collect(),
                metadata: None,
            })
            .collect()
    }

    #[test]
    fn test_build_and_search_no_rerank() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let records = make_records(40, 4);
        let idx =
            IvfPqIndex::build(&records, 4, 2, 4, 10, DistanceMetric::Euclidean, &mut rng).unwrap();
        assert_eq!(idx.centroids.len(), 4);
        assert_eq!(idx.posting_lists.len(), 4);

        let query = records[0].data.clone();
        let results = idx.search(&query, 5, 2, false);
        assert!(!results.is_empty());
        assert!(results.len() <= 5);
    }

    #[test]
    fn test_build_and_search_with_rerank() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        let records = make_records(40, 4);
        let idx =
            IvfPqIndex::build(&records, 4, 2, 4, 10, DistanceMetric::Euclidean, &mut rng).unwrap();

        let query = records[0].data.clone();
        let results = idx.search(&query, 5, 4, true);
        assert!(!results.is_empty());
        // With full nprobe and reranking the nearest neighbour should be itself.
        assert_eq!(results[0].id, VectorId(0));
    }

    #[test]
    fn test_roundtrip_serialisation() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(99);
        let records = make_records(20, 4);
        let idx =
            IvfPqIndex::build(&records, 2, 2, 4, 5, DistanceMetric::Cosine, &mut rng).unwrap();
        let bytes = idx.to_bytes().unwrap();
        let idx2 = IvfPqIndex::from_bytes(&bytes).unwrap();
        assert_eq!(idx2.dims, idx.dims);
        assert_eq!(idx2.metric, idx.metric);
        assert_eq!(idx2.centroids.len(), idx.centroids.len());
        assert_eq!(idx2.posting_lists.len(), idx.posting_lists.len());
        let total: usize = idx2.posting_lists.iter().map(|pl| pl.ids.len()).sum();
        assert_eq!(total, records.len());
    }

    #[test]
    fn test_empty_records_error() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(0);
        let result = IvfPqIndex::build(&[], 2, 2, 4, 5, DistanceMetric::Euclidean, &mut rng);
        assert!(result.is_err());
    }
}
