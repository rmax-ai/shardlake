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
    /// Generate a reproducible synthetic benchmark dataset.
    Generate(commands::generate::GenerateArgs),
    /// Publish (or re-publish) a manifest alias.
    Publish(commands::publish::PublishArgs),
    /// Start the HTTP query server.
    Serve(commands::serve::ServeArgs),
    /// Run recall/latency benchmark.
    Benchmark(commands::benchmark::BenchmarkArgs),
    /// Validate dataset and/or index manifests against stored artifacts.
    ValidateManifest(commands::validate_manifest::ValidateManifestArgs),
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
        Commands::Generate(args) => commands::generate::run(cli.storage, args).await,
        Commands::Publish(args) => commands::publish::run(cli.storage, args).await,
        Commands::Serve(args) => commands::serve::run(cli.storage, args).await,
        Commands::Benchmark(args) => commands::benchmark::run(cli.storage, args).await,
        Commands::ValidateManifest(args) => {
            commands::validate_manifest::run(cli.storage, args).await
        }
    }
}
