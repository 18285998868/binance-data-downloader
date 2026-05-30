use anyhow::Context;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "clean.toml".to_string());

    let config = data_clean::Config::load(std::path::Path::new(&config_path))
        .context("failed to load config")?;

    data_clean::run_clean(&config)?;
    Ok(())
}
