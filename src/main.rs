#[tokio::main]
async fn main() -> Result<(), data_downloader::AppError> {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let config = data_downloader::config::Config::load(manifest).unwrap_or_else(|e| {
        eprintln!("config error: {e}");
        std::process::exit(1);
    });

    let log_level = config
        .log_level
        .parse::<log::LevelFilter>()
        .unwrap_or(log::LevelFilter::Debug);
    let _ = env_logger::Builder::new()
        .filter_level(log_level)
        .try_init();

    let symbols_count = match &config.symbols {
        Some(s) => format!("{}", s.len()),
        None => "auto".into(),
    };
    let tf_count = config.timeframes.as_ref().map(|t| t.len()).unwrap_or(0);
    log::info!(
        "config loaded: symbols={symbols_count} timeframes={tf_count} market_types={:?} data_dir={}",
        config.market_type,
        config.data_dir.display(),
    );
    log::info!("starting binance data downloader");
    data_downloader::downloader(&config).await?;
    log::info!("all done");

    Ok(())
}
