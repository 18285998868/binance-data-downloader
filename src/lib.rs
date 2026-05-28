pub mod clean;
pub mod config;
pub mod downloader;
pub mod error;
pub mod task;

use config::Config;
pub use error::AppError;

pub async fn downloader(config: &Config) -> Result<(), AppError> {
    let client = build_client(config)?;
    clean::cleanup_tmp(&config.data_dir).await;

    let symbols = task::symbols::resolve_symbols(
        &config.symbols,
        &client,
        &config.market_type,
        &config.data_dir,
    )
    .await?;

    let timeframes: &[String] = config.timeframes.as_deref().unwrap_or(&[]);

    let start_dates = task::start_date::resolve_start_dates(
        &symbols,
        &client,
        &config.market_type,
        &config.data_dir,
    )
    .await?;

    let end_date = config
        .end_date
        .as_ref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
        .unwrap_or_else(|| chrono::Local::now().date_naive() - chrono::Duration::days(1));

    let local = task::local_file::scan_local_files(&config.data_dir).await?;

    // Process symbols in batches to avoid building millions of URL tasks in memory at once.
    let batch_size = 20;
    for sym_chunk in symbols.chunks(batch_size) {
        let tasks = task::build_tasks(
            sym_chunk,
            timeframes,
            config,
            &start_dates,
            end_date,
            &local,
            &config.data_dir,
        );

        if !tasks.is_empty() {
            log::info!(
                "downloading {} files (batch of {} symbols)",
                tasks.len(),
                sym_chunk.len()
            );
            downloader::run_downloads(&client, tasks, config.max_concurrent).await?;
        }
    }

    if !timeframes.is_empty() {
        let total = (symbols.len() * config.market_type.len() * timeframes.len()) as f64;
        log::info!("all done — checked up to {total:.0} symbol-type-tf combinations");
    }

    clean::cleanup_tmp(&config.data_dir).await;
    Ok(())
}

fn build_client(config: &Config) -> Result<reqwest::Client, AppError> {
    let mut b = reqwest::Client::builder()
        .user_agent("binance-data-downloader/0.1")
        .timeout(std::time::Duration::from_secs(30))
        .pool_max_idle_per_host(config.max_concurrent)
        .pool_idle_timeout(std::time::Duration::from_secs(30));
    if let Some(ref p) = config.proxy_url
        && let Ok(proxy) = reqwest::Proxy::all(p)
    {
        b = b.proxy(proxy);
    }
    b.build().map_err(AppError::Http)
}
