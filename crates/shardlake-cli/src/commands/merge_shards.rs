//! `shardlake merge-shards` – merge partial distributed build manifests.
//!
//! After running `build-index` with `--worker-id` / `--num-workers` on each
//! worker, use this command to combine the resulting partial manifests into a
//! single authoritative index manifest.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::info;

use shardlake_core::types::IndexVersion;
use shardlake_index::merge_manifests;
use shardlake_manifest::Manifest;
use shardlake_storage::LocalObjectStore;

#[derive(Parser, Debug)]
pub struct MergeShardsArgs {
    /// Comma-separated list of partial index versions to merge (e.g. `idx-v1-w0,idx-v1-w1`).
    ///
    /// Each version must correspond to a manifest produced by a prior
    /// `build-index --worker-id … --num-workers …` invocation.
    #[arg(long, value_delimiter = ',', required = true)]
    pub index_versions: Vec<String>,
    /// Output index version for the merged manifest.
    #[arg(long)]
    pub output_version: String,
}

pub async fn run(storage: PathBuf, args: MergeShardsArgs) -> Result<()> {
    let store = LocalObjectStore::new(&storage)?;

    let mut partials: Vec<Manifest> = Vec::with_capacity(args.index_versions.len());
    for ver_str in &args.index_versions {
        let ver = IndexVersion(ver_str.clone());
        info!(index_version = %ver_str, "Loading partial manifest");
        let manifest = Manifest::load(&store, &ver)
            .with_context(|| format!("Failed to load manifest for index version '{ver_str}'"))?;
        partials.push(manifest);
    }

    info!(
        partials = partials.len(),
        output_version = %args.output_version,
        "Merging partial manifests"
    );

    let output_ver = IndexVersion(args.output_version.clone());
    let merged = merge_manifests(partials, output_ver, &store)?;

    println!(
        "Merged index → index_version={} ({} shards, {} vectors)",
        merged.index_version,
        merged.shards.len(),
        merged.total_vector_count,
    );
    Ok(())
}
