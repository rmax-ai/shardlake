//! IVF (Inverted File) coarse quantizer: training, vector assignment, and
//! binary serialisation of the quantizer state.
//!
//! # Overview
//!
//! An IVF coarse quantizer partitions the vector space into `num_clusters`
//! Voronoi cells.  Each cell is represented by one centroid, and vectors are
//! assigned to the nearest centroid to form per-cluster **posting lists**.
//! At query time only the `nprobe` nearest cluster posting lists are scanned,
//! trading recall for lower latency.
//!
//! # Binary format (`.cq` files)
//!
//! ```text
//! Offset   Size           Field
//! ------   ----           -----
//! 0        8              Magic bytes: b"SLKIVF\0\0"
//! 8        4              Format version (u32) — currently 1
//! 12       4              dims (u32)
//! 16       4              num_clusters (u32)
//!
//! --- Centroids (num_clusters entries) ---
//! per centroid:
//!   dims × 4             Centroid coordinates (f32 × dims, little-endian)
//! ```

use std::io::{Cursor, Read, Write};

use rand::Rng;

use crate::{
    kmeans::{kmeans, nearest_centroid, top_n_centroids},
    IndexError, Result,
};

/// Magic bytes identifying a `.cq` coarse-quantizer artifact.
pub const CQ_MAGIC: &[u8; 8] = b"SLKIVF\0\0";
const CQ_FORMAT_VERSION: u32 = 1;
const CQ_HEADER_LEN: usize = 20;

/// Trained IVF coarse quantizer.
///
/// Encapsulates the `num_clusters` centroids produced by K-means training.
/// The quantizer is the central component of an IVF index build: it drives
/// both the **assignment** of vectors into posting-list shards and the
/// **routing** of query vectors to the nearest cluster shards at search time.
///
/// # Examples
///
/// ```
/// use rand::SeedableRng;
/// use shardlake_index::ivf::IvfQuantizer;
///
/// let mut rng = rand::rngs::StdRng::seed_from_u64(42);
/// let vectors: Vec<Vec<f32>> = (0..100)
///     .map(|i| vec![i as f32, (i * 2) as f32])
///     .collect();
///
/// let quantizer = IvfQuantizer::train(&vectors, 4, 20, &mut rng);
/// assert_eq!(quantizer.num_clusters(), 4);
///
/// // Assign a vector to its nearest cluster.
/// let cluster = quantizer.assign(&[50.0, 100.0]);
/// assert!(cluster < 4);
/// ```
#[derive(Debug, Clone)]
pub struct IvfQuantizer {
    dims: usize,
    centroids: Vec<Vec<f32>>,
}

impl IvfQuantizer {
    /// Construct an [`IvfQuantizer`] directly from pre-computed centroids.
    ///
    /// Useful when loading a quantizer from a source other than the binary
    /// artifact format, or when building unit tests with known centroids.
    ///
    /// # Panics
    ///
    /// Panics if `centroids` is empty or if the centroids do not all share the
    /// same length.
    pub fn from_centroids(centroids: Vec<Vec<f32>>) -> Self {
        assert!(
            !centroids.is_empty(),
            "IvfQuantizer::from_centroids: empty centroids"
        );
        let dims = centroids[0].len();
        assert!(
            centroids.iter().all(|c| c.len() == dims),
            "IvfQuantizer::from_centroids: centroids have inconsistent lengths"
        );
        Self { dims, centroids }
    }

    /// Train an IVF coarse quantizer by running K-means on `vectors`.
    ///
    /// At most `num_clusters` clusters are produced; if `vectors.len() <
    /// num_clusters` the number of clusters is clamped to the vector count.
    ///
    /// # Panics
    ///
    /// Panics if `vectors` is empty.
    pub fn train(
        vectors: &[Vec<f32>],
        num_clusters: usize,
        iters: u32,
        rng: &mut impl Rng,
    ) -> Self {
        assert!(!vectors.is_empty(), "IvfQuantizer::train: empty input");
        let centroids = kmeans(vectors, num_clusters, iters, rng);
        let dims = centroids[0].len();
        Self { dims, centroids }
    }

    /// Number of clusters (centroids) in this quantizer.
    pub fn num_clusters(&self) -> usize {
        self.centroids.len()
    }

    /// Vector dimension this quantizer was trained on.
    pub fn dims(&self) -> usize {
        self.dims
    }

    /// Return the centroids slice.
    pub fn centroids(&self) -> &[Vec<f32>] {
        &self.centroids
    }

    /// Assign `vector` to its nearest cluster, returning the cluster index.
    ///
    /// Uses squared-Euclidean distance for the assignment decision, consistent
    /// with the K-means training objective.
    pub fn assign(&self, vector: &[f32]) -> usize {
        nearest_centroid(vector, &self.centroids)
    }

    /// Return the indices of the `nprobe` nearest clusters to `vector`, sorted
    /// by ascending distance.
    ///
    /// Used at query time to select which posting-list shards to probe.
    pub fn top_probes(&self, vector: &[f32], nprobe: usize) -> Vec<usize> {
        top_n_centroids(vector, &self.centroids, nprobe)
    }

    /// Serialise the quantizer state to bytes using the `.cq` binary format.
    ///
    /// The resulting bytes can be stored as a coarse-quantizer artifact and
    /// later recovered with [`IvfQuantizer::from_bytes`].
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.write_all(CQ_MAGIC)?;
        write_u32(&mut buf, CQ_FORMAT_VERSION)?;
        write_u32(&mut buf, self.dims as u32)?;
        write_u32(&mut buf, self.centroids.len() as u32)?;
        for centroid in &self.centroids {
            for &v in centroid {
                buf.write_all(&v.to_le_bytes())?;
            }
        }
        Ok(buf)
    }

    /// Deserialise an [`IvfQuantizer`] from bytes previously produced by
    /// [`IvfQuantizer::to_bytes`].
    ///
    /// Returns an error if the magic bytes, format version, or data layout are
    /// not recognised.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < CQ_HEADER_LEN {
            return Err(IndexError::Other(
                "coarse-quantizer payload is shorter than the fixed header".into(),
            ));
        }

        let mut cur = Cursor::new(bytes);

        let mut magic = [0u8; 8];
        cur.read_exact(&mut magic)?;
        if &magic != CQ_MAGIC {
            return Err(IndexError::Other(
                "invalid coarse-quantizer magic bytes".into(),
            ));
        }

        let fmt_ver = read_u32(&mut cur)?;
        if fmt_ver != CQ_FORMAT_VERSION {
            return Err(IndexError::Other(format!(
                "unsupported coarse-quantizer format version {fmt_ver}"
            )));
        }

        let dims = read_u32(&mut cur)? as usize;
        let num_clusters = read_u32(&mut cur)? as usize;
        if dims == 0 {
            return Err(IndexError::Other(
                "coarse-quantizer dims must be greater than 0".into(),
            ));
        }
        if num_clusters == 0 {
            return Err(IndexError::Other(
                "coarse-quantizer num_clusters must be greater than 0".into(),
            ));
        }
        let payload_len = dims
            .checked_mul(num_clusters)
            .and_then(|value| value.checked_mul(std::mem::size_of::<f32>()))
            .ok_or_else(|| IndexError::Other("coarse-quantizer payload size overflow".into()))?;
        let expected_len = CQ_HEADER_LEN
            .checked_add(payload_len)
            .ok_or_else(|| IndexError::Other("coarse-quantizer payload size overflow".into()))?;
        if bytes.len() != expected_len {
            return Err(IndexError::Other(format!(
                "coarse-quantizer payload length mismatch: expected {expected_len} bytes, got {}",
                bytes.len()
            )));
        }

        let mut centroids = Vec::with_capacity(num_clusters);
        for _ in 0..num_clusters {
            centroids.push(read_f32_vec(&mut cur, dims)?);
        }

        Ok(Self { dims, centroids })
    }
}

// --- I/O helpers ------------------------------------------------------------

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

// --- Tests ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    fn seeded_rng() -> rand::rngs::StdRng {
        rand::rngs::StdRng::seed_from_u64(0xdead_beef)
    }

    /// Build a dataset of two clearly separated clusters in 2-D.
    fn two_cluster_vecs() -> Vec<Vec<f32>> {
        let mut vecs: Vec<Vec<f32>> = (0..50).map(|_| vec![0.0f32, 0.0]).collect();
        vecs.extend((0..50).map(|_| vec![100.0f32, 100.0]));
        vecs
    }

    #[test]
    fn train_produces_correct_num_clusters() {
        let mut rng = seeded_rng();
        let vecs = two_cluster_vecs();
        let q = IvfQuantizer::train(&vecs, 2, 20, &mut rng);
        assert_eq!(q.num_clusters(), 2);
        assert_eq!(q.dims(), 2);
    }

    #[test]
    fn train_clamps_clusters_to_vector_count() {
        let mut rng = seeded_rng();
        // Only 3 vectors but requesting 10 clusters.
        let vecs = vec![vec![0.0f32], vec![1.0], vec![2.0]];
        let q = IvfQuantizer::train(&vecs, 10, 5, &mut rng);
        assert!(q.num_clusters() <= 3);
    }

    #[test]
    fn assign_routes_to_correct_cluster() {
        // Use a seed that reliably separates the two well-separated clusters.
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let vecs = two_cluster_vecs();
        let q = IvfQuantizer::train(&vecs, 2, 20, &mut rng);

        // A vector near (0,0) must land in a different cluster than one near (100,100).
        let c0 = q.assign(&[1.0, 1.0]);
        let c1 = q.assign(&[99.0, 99.0]);
        assert_ne!(
            c0, c1,
            "near-(0,0) and near-(100,100) vectors should be in different clusters"
        );
    }

    #[test]
    fn top_probes_returns_nprobe_clusters_sorted_by_distance() {
        let centroids = vec![vec![0.0f32, 0.0], vec![5.0, 5.0], vec![10.0, 10.0]];
        let q = IvfQuantizer::from_centroids(centroids);

        // Query near centroid 1 → top probe is cluster 1, then cluster 0 or 2.
        let probes = q.top_probes(&[5.0, 5.0], 2);
        assert_eq!(probes.len(), 2);
        assert_eq!(probes[0], 1, "nearest centroid to (5,5) should be index 1");
    }

    #[test]
    fn top_probes_clamped_by_num_clusters() {
        let mut rng = seeded_rng();
        let vecs = two_cluster_vecs();
        let q = IvfQuantizer::train(&vecs, 2, 10, &mut rng);
        // Requesting more probes than clusters returns all clusters.
        let probes = q.top_probes(&[0.0, 0.0], 10);
        assert_eq!(probes.len(), q.num_clusters());
    }

    #[test]
    fn roundtrip_serialisation() {
        let mut rng = seeded_rng();
        let vecs = two_cluster_vecs();
        let original = IvfQuantizer::train(&vecs, 2, 20, &mut rng);

        let bytes = original.to_bytes().unwrap();

        // Magic bytes must be present at the start.
        assert_eq!(&bytes[..8], CQ_MAGIC);

        let recovered = IvfQuantizer::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.num_clusters(), original.num_clusters());
        assert_eq!(recovered.dims(), original.dims());
        for (orig, rec) in original
            .centroids()
            .iter()
            .zip(recovered.centroids().iter())
        {
            assert_eq!(orig, rec, "centroid mismatch after round-trip");
        }
    }

    #[test]
    fn from_bytes_rejects_wrong_magic() {
        let mut bad = b"BADMAGIC".to_vec();
        bad.extend_from_slice(&CQ_FORMAT_VERSION.to_le_bytes());
        bad.extend_from_slice(&1u32.to_le_bytes());
        bad.extend_from_slice(&1u32.to_le_bytes());
        bad.extend_from_slice(&0.0f32.to_le_bytes());
        let err = IvfQuantizer::from_bytes(&bad).unwrap_err();
        assert!(
            err.to_string().contains("magic"),
            "expected magic-bytes error, got: {err}"
        );
    }

    #[test]
    fn from_bytes_rejects_zero_dims() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(CQ_MAGIC);
        bytes.extend_from_slice(&CQ_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());

        let err = IvfQuantizer::from_bytes(&bytes).unwrap_err();
        assert!(err.to_string().contains("dims must be greater than 0"));
    }

    #[test]
    fn from_bytes_rejects_zero_clusters() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(CQ_MAGIC);
        bytes.extend_from_slice(&CQ_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());

        let err = IvfQuantizer::from_bytes(&bytes).unwrap_err();
        assert!(err
            .to_string()
            .contains("num_clusters must be greater than 0"));
    }

    #[test]
    fn from_bytes_rejects_payload_length_mismatch() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(CQ_MAGIC);
        bytes.extend_from_slice(&CQ_FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&1.0f32.to_le_bytes());

        let err = IvfQuantizer::from_bytes(&bytes).unwrap_err();
        assert!(err.to_string().contains("payload length mismatch"));
    }
}
