//! `shardlake ingest` – read a JSONL file and write versioned vector artifacts.
//!
//! Input JSONL format (one JSON object per line):
//! ```json
//! {"id": 1, "vector": [0.1, 0.2, ...], "metadata": {"label": "cat"}}
//! ```

use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
};

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use tracing::info;

use shardlake_core::types::{VectorId, VectorRecord};
use shardlake_storage::{LocalObjectStore, ObjectStore};

#[derive(Parser, Debug)]
pub struct IngestArgs {
    /// Path to the input JSONL file.
    #[arg(long)]
    pub input: PathBuf,
    /// Dataset version tag (defaults to a timestamp).
    #[arg(long)]
    pub dataset_version: Option<String>,
    /// Embedding version tag (defaults to same as dataset version).
    #[arg(long)]
    pub embedding_version: Option<String>,
}

pub async fn run(storage: PathBuf, args: IngestArgs) -> Result<()> {
    let store = LocalObjectStore::new(&storage)?;
    let dataset_ver = args
        .dataset_version
        .unwrap_or_else(|| format!("ds-{}", Utc::now().format("%Y%m%dT%H%M%S")));
    let embedding_ver = args
        .embedding_version
        .unwrap_or_else(|| dataset_ver.clone());

    info!(dataset_version = %dataset_ver, embedding_version = %embedding_ver, "Ingesting");

    let file = std::fs::File::open(&args.input)
        .with_context(|| format!("Cannot open {}", args.input.display()))?;
    let reader = BufReader::new(file);

    let records = parse_records(reader)?;

    let dims = records[0].data.len();
    info!(records = records.len(), dims, "Parsed vectors");

    let vectors_key = format!("datasets/{dataset_ver}/vectors.jsonl");
    let metadata_key = format!("datasets/{dataset_ver}/metadata.json");

    let jsonl = serialise_records_jsonl(&records)?;
    store.put(&vectors_key, jsonl)?;

    let meta_map: serde_json::Map<String, serde_json::Value> = records
        .iter()
        .filter_map(|r| r.metadata.clone().map(|m| (r.id.to_string(), m)))
        .collect();
    store.put(&metadata_key, serde_json::to_vec_pretty(&meta_map)?)?;

    let pointer = serde_json::json!({
        "dataset_version": dataset_ver,
        "embedding_version": embedding_ver,
        "dims": dims,
        "count": records.len(),
        "vectors_key": vectors_key,
        "metadata_key": metadata_key,
    });
    store.put(
        &format!("datasets/{dataset_ver}/info.json"),
        serde_json::to_vec_pretty(&pointer)?,
    )?;

    info!(vectors_key, metadata_key, "Ingest complete");
    println!(
        "Ingested {} vectors (dims={}) → dataset_version={}",
        records.len(),
        dims,
        dataset_ver
    );
    Ok(())
}

fn parse_records(reader: impl BufRead) -> Result<Vec<VectorRecord>> {
    let mut records: Vec<VectorRecord> = Vec::new();
    let mut expected_dims: Option<usize> = None;

    for (lineno, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value =
            serde_json::from_str(&line).with_context(|| format!("line {}", lineno + 1))?;
        let id = VectorId(
            v["id"]
                .as_u64()
                .with_context(|| format!("line {}: missing numeric id", lineno + 1))?,
        );
        let raw_vec = v["vector"]
            .as_array()
            .with_context(|| format!("line {}: missing vector array", lineno + 1))?;
        let mut vector: Vec<f32> = Vec::with_capacity(raw_vec.len());
        for (idx, elem) in raw_vec.iter().enumerate() {
            let val = elem.as_f64().with_context(|| {
                format!(
                    "line {}, element {idx}: vector value is not a number",
                    lineno + 1
                )
            })?;
            vector.push(val as f32);
        }

        let dims = vector.len();
        if let Some(expected) = expected_dims {
            anyhow::ensure!(
                dims == expected,
                "line {}: vector dimension mismatch for id {}: expected {}, got {}",
                lineno + 1,
                id,
                expected,
                dims
            );
        } else {
            expected_dims = Some(dims);
        }

        let metadata = v.get("metadata").cloned();
        records.push(VectorRecord {
            id,
            data: vector,
            metadata,
        });
    }

    if records.is_empty() {
        anyhow::bail!("No records found in input file");
    }

    Ok(records)
}

fn serialise_records_jsonl(records: &[VectorRecord]) -> Result<Vec<u8>> {
    let mut jsonl = Vec::new();
    for record in records {
        let line = serde_json::to_string(record)
            .with_context(|| format!("failed to serialise vector record {}", record.id))?;
        jsonl.extend_from_slice(line.as_bytes());
        jsonl.push(b'\n');
    }
    Ok(jsonl)
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Cursor};

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn parse_records_rejects_dimension_mismatch() {
        let input = Cursor::new(b"{\"id\":1,\"vector\":[1.0,2.0]}\n{\"id\":2,\"vector\":[3.0]}\n");

        let err = parse_records(input).unwrap_err();
        assert!(err
            .to_string()
            .contains("line 2: vector dimension mismatch for id 2: expected 2, got 1"));
    }

    #[tokio::test]
    async fn run_rejects_dimension_mismatch() {
        let tmp = tempdir().unwrap();
        let input = tmp.path().join("vectors.jsonl");
        fs::write(
            &input,
            b"{\"id\":1,\"vector\":[1.0,2.0]}\n{\"id\":2,\"vector\":[3.0]}\n",
        )
        .unwrap();

        let err = run(
            tmp.path().join("storage"),
            IngestArgs {
                input,
                dataset_version: Some("ds-test".into()),
                embedding_version: None,
            },
        )
        .await
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("line 2: vector dimension mismatch for id 2: expected 2, got 1"));
    }
}
