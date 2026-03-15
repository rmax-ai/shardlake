//! Shard index binary format: serialisation and deserialisation.
//!
//! Two format versions are supported:
//!
//! ## Format version 1 — raw vectors
//!
//! ```text
//! magic         : [u8; 8]  = b"SLKIDX\0\0"
//! format_ver    : u32      = 1
//! shard_id      : u32
//! dims          : u32
//! centroid_count: u32
//! vector_count  : u64
//! --- centroids ---
//! per centroid  : dims * f32
//! --- vectors  ---
//! per vector    : id: u64, data: dims * f32
//! ```
//!
//! ## Format version 2 — PQ-encoded vectors
//!
//! ```text
//! magic         : [u8; 8]  = b"SLKIDX\0\0"
//! format_ver    : u32      = 2
//! shard_id      : u32
//! dims          : u32
//! centroid_count: u32
//! vector_count  : u64
//! pq_m          : u32      (number of PQ sub-spaces)
//! pq_k          : u32      (PQ codebook size)
//! --- centroids ---
//! per centroid  : dims * f32
//! --- PQ-encoded vectors ---
//! per vector    : id: u64, codes: pq_m * u8
//! ```
//!
//! The PQ codebook (used to decode codes back to approximate vectors or to
//! compute ADC distances) is stored as a separate artifact at the key given by
//! [`shardlake_storage::paths::index_pq_codebook_key`].

use std::io::{Cursor, Read, Write};

use shardlake_core::types::{ShardId, VectorId, VectorRecord};

use crate::{IndexError, Result};

pub const SHARD_MAGIC: &[u8; 8] = b"SLKIDX\0\0";
const FORMAT_VERSION_RAW: u32 = 1;
const FORMAT_VERSION_PQ: u32 = 2;

// ── ShardIndex (format version 1 — raw vectors) ───────────────────────────────

/// In-memory shard index: a set of centroids and the raw vectors assigned to it.
///
/// Serialised as format version 1.
#[derive(Debug, Clone)]
pub struct ShardIndex {
    pub shard_id: ShardId,
    pub dims: usize,
    pub centroids: Vec<Vec<f32>>,
    pub records: Vec<VectorRecord>,
}

impl ShardIndex {
    /// Serialise to bytes (format version 1).
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.write_all(SHARD_MAGIC)?;
        write_u32(&mut buf, FORMAT_VERSION_RAW)?;
        write_u32(&mut buf, self.shard_id.0)?;
        write_u32(&mut buf, self.dims as u32)?;
        write_u32(&mut buf, self.centroids.len() as u32)?;
        write_u64(&mut buf, self.records.len() as u64)?;

        for centroid in &self.centroids {
            for &v in centroid {
                buf.write_all(&v.to_le_bytes())?;
            }
        }

        for rec in &self.records {
            write_u64(&mut buf, rec.id.0)?;
            for &v in &rec.data {
                buf.write_all(&v.to_le_bytes())?;
            }
        }
        Ok(buf)
    }

    /// Deserialise a format-version-1 shard from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cur = Cursor::new(bytes);

        let mut magic = [0u8; 8];
        cur.read_exact(&mut magic)?;
        if &magic != SHARD_MAGIC {
            return Err(IndexError::Other("invalid shard magic".into()));
        }

        let fmt_ver = read_u32(&mut cur)?;
        if fmt_ver != FORMAT_VERSION_RAW {
            return Err(IndexError::Other(format!(
                "unsupported format version {fmt_ver}"
            )));
        }

        let shard_id = ShardId(read_u32(&mut cur)?);
        let dims = read_u32(&mut cur)? as usize;
        let centroid_count = read_u32(&mut cur)? as usize;
        let vector_count = read_u64(&mut cur)? as usize;

        let mut centroids = Vec::with_capacity(centroid_count);
        for _ in 0..centroid_count {
            centroids.push(read_f32_vec(&mut cur, dims)?);
        }

        let mut records = Vec::with_capacity(vector_count);
        for _ in 0..vector_count {
            let id = VectorId(read_u64(&mut cur)?);
            let data = read_f32_vec(&mut cur, dims)?;
            records.push(VectorRecord {
                id,
                data,
                metadata: None,
            });
        }

        Ok(Self {
            shard_id,
            dims,
            centroids,
            records,
        })
    }
}

// ── PqShard (format version 2 — PQ-encoded vectors) ──────────────────────────

/// In-memory PQ-encoded shard: centroids for routing and one byte-code per
/// sub-space per vector.
///
/// The full PQ codebook is **not** stored here; it is loaded separately from
/// [`shardlake_storage::paths::index_pq_codebook_key`].
///
/// Serialised as format version 2.
#[derive(Debug, Clone)]
pub struct PqShard {
    pub shard_id: ShardId,
    /// Full vector dimension (same as the index `dims` field).
    pub dims: usize,
    /// Number of PQ sub-spaces.
    pub pq_m: usize,
    /// PQ codebook size (number of centroids per sub-space).
    pub pq_k: usize,
    /// K-means centroids for shard routing (same role as in [`ShardIndex`]).
    pub centroids: Vec<Vec<f32>>,
    /// PQ-encoded vectors: each entry is `(VectorId, codes)` where
    /// `codes.len() == pq_m` and each byte is in `[0, pq_k)`.
    pub entries: Vec<(VectorId, Vec<u8>)>,
}

impl PqShard {
    /// Serialise to bytes (format version 2).
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.write_all(SHARD_MAGIC)?;
        write_u32(&mut buf, FORMAT_VERSION_PQ)?;
        write_u32(&mut buf, self.shard_id.0)?;
        write_u32(&mut buf, self.dims as u32)?;
        write_u32(&mut buf, self.centroids.len() as u32)?;
        write_u64(&mut buf, self.entries.len() as u64)?;
        write_u32(&mut buf, self.pq_m as u32)?;
        write_u32(&mut buf, self.pq_k as u32)?;

        for centroid in &self.centroids {
            for &v in centroid {
                buf.write_all(&v.to_le_bytes())?;
            }
        }

        for (id, codes) in &self.entries {
            write_u64(&mut buf, id.0)?;
            buf.write_all(codes)?;
        }

        Ok(buf)
    }

    /// Deserialise a format-version-2 shard from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cur = Cursor::new(bytes);

        let mut magic = [0u8; 8];
        cur.read_exact(&mut magic)?;
        if &magic != SHARD_MAGIC {
            return Err(IndexError::Other("invalid shard magic".into()));
        }

        let fmt_ver = read_u32(&mut cur)?;
        if fmt_ver != FORMAT_VERSION_PQ {
            return Err(IndexError::Other(format!(
                "PqShard: unsupported format version {fmt_ver}"
            )));
        }

        let shard_id = ShardId(read_u32(&mut cur)?);
        let dims = read_u32(&mut cur)? as usize;
        let centroid_count = read_u32(&mut cur)? as usize;
        let vector_count = read_u64(&mut cur)? as usize;
        let pq_m = read_u32(&mut cur)? as usize;
        let pq_k = read_u32(&mut cur)? as usize;
        if pq_m == 0 {
            return Err(IndexError::Other("PqShard: pq_m must be at least 1".into()));
        }
        if pq_k == 0 || pq_k > 256 {
            return Err(IndexError::Other(
                "PqShard: pq_k must be in range [1, 256]".into(),
            ));
        }

        let mut centroids = Vec::with_capacity(centroid_count);
        for _ in 0..centroid_count {
            centroids.push(read_f32_vec(&mut cur, dims)?);
        }

        let mut entries = Vec::with_capacity(vector_count);
        for _ in 0..vector_count {
            let id = VectorId(read_u64(&mut cur)?);
            let mut codes = vec![0u8; pq_m];
            cur.read_exact(&mut codes)?;
            if let Some(&code) = codes.iter().find(|&&code| code as usize >= pq_k) {
                return Err(IndexError::Other(format!(
                    "PqShard: code {code} out of range for pq_k {pq_k}"
                )));
            }
            entries.push((id, codes));
        }

        Ok(Self {
            shard_id,
            dims,
            pq_m,
            pq_k,
            centroids,
            entries,
        })
    }
}

// ── Shared I/O helpers ────────────────────────────────────────────────────────

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

fn read_f32_vec(cur: &mut Cursor<&[u8]>, dims: usize) -> Result<Vec<f32>> {
    let mut v = vec![0.0f32; dims];
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

    #[test]
    fn roundtrip() {
        let idx = ShardIndex {
            shard_id: ShardId(0),
            dims: 3,
            centroids: vec![vec![1.0, 2.0, 3.0]],
            records: vec![
                VectorRecord {
                    id: VectorId(1),
                    data: vec![1.0, 0.0, 0.0],
                    metadata: None,
                },
                VectorRecord {
                    id: VectorId(2),
                    data: vec![0.0, 1.0, 0.0],
                    metadata: None,
                },
            ],
        };
        let bytes = idx.to_bytes().unwrap();
        let decoded = ShardIndex::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.shard_id, idx.shard_id);
        assert_eq!(decoded.dims, idx.dims);
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].id, VectorId(1));
    }

    #[test]
    fn pq_shard_roundtrip() {
        let shard = PqShard {
            shard_id: ShardId(1),
            dims: 4,
            pq_m: 2,
            pq_k: 8,
            centroids: vec![vec![0.1, 0.2, 0.3, 0.4]],
            entries: vec![(VectorId(10), vec![3, 7]), (VectorId(11), vec![0, 1])],
        };
        let bytes = shard.to_bytes().unwrap();
        let decoded = PqShard::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.shard_id, shard.shard_id);
        assert_eq!(decoded.dims, shard.dims);
        assert_eq!(decoded.pq_m, shard.pq_m);
        assert_eq!(decoded.pq_k, shard.pq_k);
        assert_eq!(decoded.centroids.len(), 1);
        for (&a, &b) in decoded.centroids[0].iter().zip(shard.centroids[0].iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
        assert_eq!(decoded.entries.len(), 2);
        assert_eq!(decoded.entries[0].0, VectorId(10));
        assert_eq!(decoded.entries[0].1, vec![3, 7]);
        assert_eq!(decoded.entries[1].0, VectorId(11));
        assert_eq!(decoded.entries[1].1, vec![0, 1]);
    }

    #[test]
    fn pq_shard_rejects_version_1_bytes() {
        let raw_shard = ShardIndex {
            shard_id: ShardId(0),
            dims: 2,
            centroids: vec![vec![0.0, 0.0]],
            records: vec![VectorRecord {
                id: VectorId(1),
                data: vec![1.0, 0.0],
                metadata: None,
            }],
        };
        let bytes = raw_shard.to_bytes().unwrap();
        let err = PqShard::from_bytes(&bytes).unwrap_err();
        assert!(err.to_string().contains("unsupported format version"));
    }

    #[test]
    fn pq_shard_rejects_code_out_of_range() {
        let shard = PqShard {
            shard_id: ShardId(1),
            dims: 4,
            pq_m: 2,
            pq_k: 4,
            centroids: vec![vec![0.1, 0.2, 0.3, 0.4]],
            entries: vec![(VectorId(10), vec![1, 4])],
        };
        let bytes = shard.to_bytes().unwrap();

        let err = PqShard::from_bytes(&bytes).unwrap_err();
        assert!(err.to_string().contains("out of range"));
    }
}
