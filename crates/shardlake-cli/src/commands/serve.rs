//! `shardlake serve` – start HTTP query server.

use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Result;
use clap::Parser;
use tracing::info;

use shardlake_core::types::VectorRecord;
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
    /// Load the full corpus at startup to enable lexical and hybrid search.
    /// When set, the server builds a BM25 index from the corpus metadata.
    #[arg(long, default_value_t = false)]
    pub enable_bm25: bool,
}

pub async fn run(storage: PathBuf, args: ServeArgs) -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(&storage)?);
    let manifest = Manifest::load_alias(&*store, &args.alias)?;
    info!(
        alias = %args.alias,
        index_version = %manifest.index_version,
        "Serving manifest"
    );

    let searcher = if args.enable_bm25 {
        info!("Loading corpus for BM25 index");
        let vecs_bytes = store.get(&manifest.vectors_key)?;
        let reader = BufReader::new(vecs_bytes.as_slice());
        let mut corpus: Vec<VectorRecord> = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            corpus.push(serde_json::from_str(&line)?);
        }
        info!(docs = corpus.len(), "BM25 corpus loaded");
        IndexSearcher::with_corpus(Arc::clone(&store), manifest, &corpus)
    } else {
        IndexSearcher::new(Arc::clone(&store), manifest)
    };

    let searcher = Arc::new(searcher);
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
