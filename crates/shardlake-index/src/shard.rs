//! Shard index binary format: serialisation and deserialisation.
//!
//! Format (little-endian):
//! ```text
//! magic         : [u8; 8]  = b"SLKIDX\0\0"
//! format_ver    : u32
//! shard_id      : u32
//! dims          : u32
//! centroid_count: u32
//! vector_count  : u64
//! --- centroids ---
//! per centroid  : dims * f32
//! --- vectors  ---
//! per vector    : id: u64, data: dims * f32
//! ```

use std::io::{Cursor, Read, Write};

use shardlake_core::types::{ShardId, VectorId, VectorRecord};

use crate::{IndexError, Result};

pub const SHARD_MAGIC: &[u8; 8] = b"SLKIDX\0\0";
const FORMAT_VERSION: u32 = 1;

/// In-memory shard index: a set of centroids and the vectors assigned to it.
#[derive(Debug, Clone)]
pub struct ShardIndex {
    pub shard_id: ShardId,
    pub dims: usize,
    pub centroids: Vec<Vec<f32>>,
    pub records: Vec<VectorRecord>,
}

impl ShardIndex {
    /// Estimated heap memory occupied by this shard index, in bytes.
    ///
    /// Accounts for centroid and vector data arrays but not metadata payloads
    /// (which are not serialised in the `.sidx` binary format).
    pub fn memory_bytes(&self) -> usize {
        let centroid_bytes = self.centroids.len() * self.dims * std::mem::size_of::<f32>();
        let record_bytes = self.records.len()
            * (std::mem::size_of::<u64>() + self.dims * std::mem::size_of::<f32>());
        centroid_bytes + record_bytes
    }

    /// Serialise to bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.write_all(SHARD_MAGIC)?;
        write_u32(&mut buf, FORMAT_VERSION)?;
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

    /// Deserialise from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cur = Cursor::new(bytes);

        let mut magic = [0u8; 8];
        cur.read_exact(&mut magic)?;
        if &magic != SHARD_MAGIC {
            return Err(IndexError::Other("invalid shard magic".into()));
        }

        let fmt_ver = read_u32(&mut cur)?;
        if fmt_ver != FORMAT_VERSION {
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
}
