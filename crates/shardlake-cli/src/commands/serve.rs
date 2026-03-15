//! `shardlake serve` – start HTTP query server.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tracing::info;

use shardlake_core::config::FanOutPolicy;
use shardlake_index::IndexSearcher;
use shardlake_manifest::Manifest;
use shardlake_serve::{build_router, AppState};
use shardlake_storage::{LocalObjectStore, ObjectStore};

#[derive(Parser, Debug)]
pub struct ServeArgs {
    /// Alias to serve (default: "latest").
    #[arg(long, default_value = "latest")]
    pub alias: String,
    /// Bind address.
    #[arg(long, default_value = "0.0.0.0:8080")]
    pub bind: String,
    /// Number of nearest centroids to select per query (candidate_centroids).
    /// This is the primary routing knob: higher values improve recall at the
    /// cost of probing more shards.
    #[arg(long, default_value_t = 2)]
    pub nprobe: u32,
    /// Maximum number of shards to probe after centroid-to-shard deduplication.
    /// `0` means no cap (all shards selected by `--nprobe` are probed).
    #[arg(long, default_value_t = 0)]
    pub candidate_shards: u32,
    /// Maximum number of vectors to evaluate per probed shard.
    /// `0` means no limit (all vectors in the shard are scored).
    #[arg(long, default_value_t = 0)]
    pub max_vectors_per_shard: u32,
}

pub async fn run(storage: PathBuf, args: ServeArgs) -> Result<()> {
    let fan_out = FanOutPolicy {
        candidate_centroids: args.nprobe,
        candidate_shards: args.candidate_shards,
        max_vectors_per_shard: args.max_vectors_per_shard,
    };
    fan_out.validate().map_err(|e| anyhow::anyhow!("{}", e))?;

    let store: std::sync::Arc<dyn ObjectStore> =
        std::sync::Arc::new(LocalObjectStore::new(&storage)?);
    let manifest = Manifest::load_alias(&*store, &args.alias)?;
    info!(
        alias = %args.alias,
        index_version = %manifest.index_version,
        "Serving manifest"
    );
    let searcher = std::sync::Arc::new(IndexSearcher::new(std::sync::Arc::clone(&store), manifest));
    let state = AppState { searcher, fan_out };
    let router = build_router(state);

    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    info!(bind = %args.bind, "Listening");
    println!("Serving on http://{}", args.bind);
    axum::serve(listener, router).await?;
    Ok(())
}
