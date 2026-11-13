//! Binary entry point for `binance-data-downloader`.
//!
//! Initializes tracing, parses command-line arguments, then runs the
//! full download pipeline with the provided TOML configuration.

use anyhow::Context;
use clap::Parser;
use tracing_subscriber::EnvFilter;

mod cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing with `RUST_LOG` (falls back to `info`).
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Parse command-line arguments (`--config`/`-c` is required).
    let cli = cli::Cli::parse();

    // Run the download pipeline, annotating any error with pipeline context.
    let options = bdd::RunOptions::new().with_verify(cli.verify);
    bdd::run_with_options(&cli.config, &options)
        .await
        .context("binance-data-downloader run failed")?;
    Ok(())
}
