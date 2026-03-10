//! `shardlake serve` – start HTTP query server.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tracing::info;

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
    /// Number of shards to probe per query.
    #[arg(long, default_value_t = 2)]
    pub nprobe: usize,
}

pub async fn run(storage: PathBuf, args: ServeArgs) -> Result<()> {
    let store: std::sync::Arc<dyn ObjectStore> =
        std::sync::Arc::new(LocalObjectStore::new(&storage)?);
    let manifest = Manifest::load_alias(&*store, &args.alias)?;
    info!(
        alias = %args.alias,
        index_version = %manifest.index_version,
        "Serving manifest"
    );
    let searcher = std::sync::Arc::new(IndexSearcher::new(std::sync::Arc::clone(&store), manifest));
    let state = AppState {
        searcher,
        nprobe: args.nprobe,
    };
    let router = build_router(state);

    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    info!(bind = %args.bind, "Listening");
    println!("Serving on http://{}", args.bind);
    axum::serve(listener, router).await?;
    Ok(())
}
