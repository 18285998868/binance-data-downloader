use anyhow::Context;

fn main() -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "clean.toml".to_string());

    let config = data_clean::Config::load(std::path::Path::new(&config_path))
        .context("failed to load config")?;

    data_clean::run_clean(&config)?;
    Ok(())
}
