use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt, EnvFilter};

mod commands;

#[derive(Parser, Debug)]
#[command(
    name = "shardlake",
    about = "Decoupled vector search prototype",
    version
)]
struct Cli {
    /// Path to the artifact storage root directory.
    #[arg(long, global = true, default_value = "./data")]
    storage: std::path::PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Ingest vectors from a JSONL file.
    Ingest(commands::ingest::IngestArgs),
    /// Build shard-based ANN index from ingested vectors.
    BuildIndex(commands::build_index::BuildIndexArgs),
    /// Distributed build worker: plan or execute one worker's shard assignments.
    BuildIndexWorker(commands::build_index_worker::BuildIndexWorkerArgs),
    /// Generate a reproducible synthetic benchmark dataset.
    Generate(commands::generate::GenerateArgs),
    /// Publish (or re-publish) a manifest alias.
    Publish(commands::publish::PublishArgs),
    /// Start the HTTP query server.
    Serve(commands::serve::ServeArgs),
    /// Run recall/latency benchmark.
    Benchmark(commands::benchmark::BenchmarkArgs),
    /// Evaluate ANN quality: recall@k, precision@k, and latency.
    EvalAnn(commands::eval_ann::EvalAnnArgs),
    /// Compare multiple ANN families: IVF-PQ, HNSW, DiskANN, and others.
    CompareAnn(commands::compare_ann::CompareAnnArgs),
    /// Evaluate hybrid retrieval quality: compare vector-only, BM25-only, and hybrid modes.
    EvalHybrid(commands::eval_hybrid::EvalHybridArgs),
    /// Validate dataset and/or index manifests against stored artifacts.
    ValidateManifest(commands::validate_manifest::ValidateManifestArgs),
    /// Evaluate partition quality: shard size distribution, routing accuracy,
    /// recall impact, and shard hotness.
    EvaluatePartitioning(commands::evaluate_partitioning::EvaluatePartitioningArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("shardlake=info".parse()?))
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Ingest(args) => commands::ingest::run(cli.storage, args).await,
        Commands::BuildIndex(args) => commands::build_index::run(cli.storage, args).await,
        Commands::BuildIndexWorker(args) => {
            commands::build_index_worker::run(cli.storage, args).await
        }
        Commands::Generate(args) => commands::generate::run(cli.storage, args).await,
        Commands::Publish(args) => commands::publish::run(cli.storage, args).await,
        Commands::Serve(args) => commands::serve::run(cli.storage, args).await,
        Commands::Benchmark(args) => commands::benchmark::run(cli.storage, args).await,
        Commands::EvalAnn(args) => commands::eval_ann::run(cli.storage, args).await,
        Commands::CompareAnn(args) => commands::compare_ann::run(cli.storage, args).await,
        Commands::EvalHybrid(args) => commands::eval_hybrid::run(cli.storage, args).await,
        Commands::ValidateManifest(args) => {
            commands::validate_manifest::run(cli.storage, args).await
        }
        Commands::EvaluatePartitioning(args) => {
            commands::evaluate_partitioning::run(cli.storage, args).await
        }
    }
}
