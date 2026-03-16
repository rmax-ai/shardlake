//! `shardlake serve` – start HTTP query server.

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use clap::Parser;
use tracing::info;

use shardlake_core::config::FanOutPolicy;
use shardlake_index::{bm25::Bm25Index, IndexSearcher, DEFAULT_SHARD_CACHE_CAPACITY};
use shardlake_manifest::Manifest;
use shardlake_serve::{build_router, AppState, PrometheusMetrics};
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
    /// Maximum number of shard indexes to keep in the in-memory LRU cache.
    /// Larger values improve repeat-query latency at the cost of higher memory
    /// usage.  Should be at least as large as `--nprobe`, or `--candidate-shards`
    /// when that flag is non-zero and smaller than `--nprobe`.
    #[arg(
        long,
        default_value_t = DEFAULT_SHARD_CACHE_CAPACITY,
        value_parser = parse_positive_shard_cache_capacity
    )]
    pub shard_cache_capacity: usize,
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
        shard_cache_capacity = args.shard_cache_capacity,
        "Serving manifest"
    );
    let bm25_index = load_bm25_index(&*store, &manifest)?;
    let searcher = Arc::new(IndexSearcher::with_cache_capacity(
        Arc::clone(&store),
        manifest,
        args.shard_cache_capacity,
    ));
    let metrics = Arc::new(PrometheusMetrics::new(searcher.cache_metrics()));
    let state = AppState {
        searcher,
        fan_out,
        metrics,
        bm25_index,
    };
    let router = build_router(state);

    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    info!(bind = %args.bind, "Listening");
    println!("Serving on http://{}", args.bind);
    axum::serve(listener, router).await?;
    Ok(())
}

fn load_bm25_index(store: &dyn ObjectStore, manifest: &Manifest) -> Result<Option<Arc<Bm25Index>>> {
    manifest
        .lexical
        .as_ref()
        .map(|lexical| {
            Bm25Index::load(store, &lexical.artifact_key)
                .with_context(|| {
                    format!(
                        "failed to load lexical index artifact `{}` for index version `{}`",
                        lexical.artifact_key, manifest.index_version
                    )
                })
                .map(Arc::new)
        })
        .transpose()
}

fn parse_positive_shard_cache_capacity(raw: &str) -> std::result::Result<usize, String> {
    let value = raw
        .parse::<usize>()
        .map_err(|err| format!("invalid integer value `{raw}`: {err}"))?;
    if value == 0 {
        return Err("value must be greater than 0".into());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use clap::Parser;
    use shardlake_core::types::{
        DatasetVersion, DistanceMetric, EmbeddingVersion, IndexVersion, VectorId,
    };
    use shardlake_index::{bm25::BM25Params, Bm25Index};
    use shardlake_manifest::{BuildMetadata, CompressionConfig, LexicalIndexConfig, Manifest};
    use shardlake_storage::{paths, LocalObjectStore};

    use super::{load_bm25_index, ServeArgs};

    #[test]
    fn serve_args_reject_zero_shard_cache_capacity() {
        let err = ServeArgs::try_parse_from(["shardlake", "--shard-cache-capacity", "0"])
            .expect_err("zero shard cache capacity must be rejected");

        let message = err.to_string();
        assert!(message.contains("--shard-cache-capacity"));
        assert!(message.contains("value must be greater than 0"));
    }

    #[test]
    fn load_bm25_index_returns_index_when_manifest_includes_lexical_artifact() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = LocalObjectStore::new(tmp.path()).expect("store");
        let lexical_key = paths::index_lexical_key("idx-test");
        let bm25 = Bm25Index::build(&[(VectorId(1), "quick brown fox")], BM25Params::default());
        bm25.save(&store, &lexical_key).expect("save lexical index");

        let manifest = Manifest {
            manifest_version: 4,
            dataset_version: DatasetVersion("ds-test".into()),
            embedding_version: EmbeddingVersion("emb-test".into()),
            index_version: IndexVersion("idx-test".into()),
            alias: "latest".into(),
            dims: 3,
            distance_metric: DistanceMetric::Cosine,
            vectors_key: "datasets/ds-test/vectors.jsonl".into(),
            metadata_key: "datasets/ds-test/metadata.json".into(),
            shards: Vec::new(),
            total_vector_count: 1,
            build_metadata: BuildMetadata {
                built_at: Utc::now(),
                builder_version: "test".into(),
                num_kmeans_iters: 1,
                nprobe_default: 1,
                build_duration_secs: 0.0,
            },
            algorithm: Default::default(),
            shard_summary: None,
            compression: CompressionConfig::default(),
            recall_estimate: None,
            coarse_quantizer_key: None,
            lexical: Some(LexicalIndexConfig {
                artifact_key: lexical_key,
                k1: 1.5,
                b: 0.75,
                doc_count: 1,
            }),
        };

        let loaded = load_bm25_index(&store, &manifest).expect("load lexical index");

        let index = loaded.expect("bm25 index should be loaded");
        let results = index.search("quick", 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id.0, 1);
    }
}
