use anyhow::Context;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = bdd::cli::parse();
    bdd::app::run(&cli.config)
        .await
        .context("binance-data-downloader run failed")?;
    Ok(())
}
