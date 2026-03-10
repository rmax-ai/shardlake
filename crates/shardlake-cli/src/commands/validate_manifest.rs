//! `shardlake validate-manifest` – verify a manifest and its referenced shard artifacts.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tracing::info;

use shardlake_core::types::IndexVersion;
use shardlake_manifest::{validate_manifest_integrity, Manifest};
use shardlake_storage::LocalObjectStore;

#[derive(Parser, Debug)]
pub struct ValidateManifestArgs {
    /// Index version whose manifest to validate. Mutually exclusive with `--alias`.
    #[arg(long, conflicts_with = "alias")]
    pub index_version: Option<String>,
    /// Alias to resolve before validating. Mutually exclusive with `--index-version`.
    #[arg(long, conflicts_with = "index_version", default_value = "latest")]
    pub alias: String,
}

pub async fn run(storage: PathBuf, args: ValidateManifestArgs) -> Result<()> {
    let store = LocalObjectStore::new(&storage)?;

    let manifest = if let Some(ref ver) = args.index_version {
        let index_ver = IndexVersion(ver.clone());
        info!(index_version = %ver, "Loading manifest by index version");
        Manifest::load(&store, &index_ver)?
    } else {
        info!(alias = %args.alias, "Loading manifest via alias");
        Manifest::load_alias(&store, &args.alias)?
    };

    info!(
        index_version = %manifest.index_version,
        dataset_version = %manifest.dataset_version,
        shards = manifest.shards.len(),
        "Validating manifest integrity"
    );

    let report = validate_manifest_integrity(&manifest, &store);

    println!("=== Manifest Validation Report ===");
    println!("  Index version:   {}", manifest.index_version);
    println!("  Dataset version: {}", manifest.dataset_version);
    println!("  Dataset ID:      {}", manifest.dataset_id);
    println!("  Embedding model: {}", manifest.embedding_model);
    println!("  Algorithm:       {}", manifest.build_metadata.algorithm);
    println!("  Dims:            {}", manifest.dims);
    println!("  Shards:          {}", manifest.shard_count);
    println!("  Total vectors:   {}", manifest.total_vector_count);
    println!();
    println!(
        "  Note: checksums use FNV-1a (non-cryptographic). \
         They detect accidental corruption only, not intentional tampering."
    );
    println!();

    if report.messages.is_empty() {
        println!("  (no issues found)");
    } else {
        for msg in &report.messages {
            let prefix = if report.ok { "  ✓" } else { "  ✗" };
            println!("{prefix} {msg}");
        }
    }

    println!();
    if report.ok {
        println!("Result: OK");
    } else {
        anyhow::bail!("Manifest validation failed — see messages above");
    }

    Ok(())
}
