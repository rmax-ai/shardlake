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

    let mut records: Vec<VectorRecord> = Vec::new();
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
    let dims = records[0].data.len();
    info!(records = records.len(), dims, "Parsed vectors");

    let vectors_key = format!("datasets/{dataset_ver}/vectors.jsonl");
    let metadata_key = format!("datasets/{dataset_ver}/metadata.json");

    let jsonl: Vec<u8> = records
        .iter()
        .map(|r| serde_json::to_string(r).unwrap() + "\n")
        .collect::<String>()
        .into_bytes();
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
