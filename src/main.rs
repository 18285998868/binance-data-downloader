use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config_path = std::env::args().find_map(|a| {
        a.strip_prefix("--config=").map(PathBuf::from)
    });

    binance_data_downloader::run(config_path.as_deref()).await?;
    Ok(())
}
