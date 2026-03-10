//! `shardlake publish` – update or create an alias pointer.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tracing::info;

use shardlake_core::types::IndexVersion;
use shardlake_manifest::Manifest;
use shardlake_storage::LocalObjectStore;

#[derive(Parser, Debug)]
pub struct PublishArgs {
    /// Index version to publish.
    #[arg(long)]
    pub index_version: String,
    /// Alias name (defaults to "latest").
    #[arg(long, default_value = "latest")]
    pub alias: String,
}

pub async fn run(storage: PathBuf, args: PublishArgs) -> Result<()> {
    let store = LocalObjectStore::new(&storage)?;
    let index_ver = IndexVersion(args.index_version.clone());

    let mut manifest = Manifest::load(&store, &index_ver)?;
    manifest.alias = args.alias.clone();
    manifest.publish_alias(&store)?;

    info!(
        alias = %args.alias,
        index_version = %args.index_version,
        "Alias published"
    );
    println!(
        "Published alias '{}' → index_version={}",
        args.alias, args.index_version
    );
    Ok(())
}
