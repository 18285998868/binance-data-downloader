use anyhow::Context;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let config_path = std::env::var("DOWNLOADER_CONFIG")
        .unwrap_or_else(|_| manifest.to_string());

    let config = data_downloader::config::Config::load(&config_path)
        .context("failed to load config")?;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(&config.log_level)),
        )
        .init();

    let symbols_count = match &config.symbols {
        Some(s) => s.len().to_string(),
        None => "auto".into(),
    };
    let tf_count = config.timeframes.as_ref().map(|t| t.len()).unwrap_or(0);
    tracing::info!(
        symbols = symbols_count,
        timeframes = tf_count,
        market_types = ?config.market_type,
        data_dir = %config.data_dir.display(),
        "config loaded",
    );
    tracing::info!("starting binance data downloader");
    data_downloader::downloader(&config).await?;
    tracing::info!("all done");

    Ok(())
}
