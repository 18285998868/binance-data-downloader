pub mod clean;
pub mod config;
pub mod downloader;
pub mod error;
pub mod task;

use config::Config;
use config::get_proxy;
pub use error::AppError;

const SYMBOL_BATCH_SIZE: usize = 20;
const DATE_CHUNK_DAYS: i64 = 30;

/// Orchestrates the full download pipeline:
/// 1. Clean up stale .tmp files
/// 2. Resolve symbols and their start dates
/// 3. Download and convert data in bounded batches
pub async fn run(config: &Config) -> Result<(), AppError> {
    let client = build_client(config)?;
    clean::cleanup_tmp(&config.data_dir).await;

    let symbols = config.resolve_symbols().await?;

    let timeframes: &[String] = config.timeframes.as_deref().unwrap_or(&[]);
    if timeframes.is_empty() {
        tracing::warn!("no timeframes configured, nothing to download");
        return Ok(());
    }

    let start_dates =
        config::start_date::resolve_start_dates(&symbols, &client, &config.market_type).await?;

    let overall_end = config
        .end_date
        .as_ref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
        .unwrap_or_else(|| chrono::Local::now().date_naive() - chrono::Duration::days(1));

    let local = task::local_file::scan_local_files(&config.data_dir).await?;

    // Process symbols in batches to avoid building millions of URL tasks in memory at once.
    for sym_chunk in symbols.chunks(SYMBOL_BATCH_SIZE) {
        process_symbol_batch(
            sym_chunk,
            timeframes,
            config,
            &start_dates,
            overall_end,
            &local,
            &client,
        )
        .await?;
    }

    let total = (symbols.len() * config.market_type.len() * timeframes.len()) as f64;
    tracing::info!("all done — checked up to {total:.0} symbol-type-tf combinations");

    clean::cleanup_tmp(&config.data_dir).await;
    Ok(())
}

/// Processes a batch of symbols, further chunked by date range
/// to bound per-batch memory from URL generation.
async fn process_symbol_batch(
    symbols: &[String],
    timeframes: &[String],
    config: &Config,
    start_dates: &config::start_date::StartDateMap,
    overall_end: chrono::NaiveDate,
    local: &std::collections::HashSet<String>,
    client: &reqwest::Client,
) -> Result<(), AppError> {
    let overall_start = determine_overall_start(config, start_dates);

    let mut chunk_start = overall_start;
    while chunk_start <= overall_end {
        let chunk_end =
            (chunk_start + chrono::Duration::days(DATE_CHUNK_DAYS - 1)).min(overall_end);

        let tasks = task::build_tasks(
            symbols,
            timeframes,
            config,
            start_dates,
            chunk_start,
            chunk_end,
            local,
        );

        if !tasks.is_empty() {
            tracing::info!(
                files = tasks.len(),
                symbols = symbols.len(),
                date_range = %format!("{}..{}", chunk_start, chunk_end),
                "downloading batch",
            );
            downloader::run_downloads(client, tasks, config.max_concurrent).await?;
        }

        chunk_start = chunk_end + chrono::Duration::days(1);
    }
    Ok(())
}

/// Determines the earliest date to start downloading from.
/// Prefers config.start_date, then the earliest symbol start date,
/// then falls back to 1 year ago.
fn determine_overall_start(
    config: &Config,
    start_dates: &config::start_date::StartDateMap,
) -> chrono::NaiveDate {
    if let Some(ref sd) = config.start_date
        && let Ok(d) = chrono::NaiveDate::parse_from_str(sd, "%Y-%m-%d")
    {
        return d;
    }
    start_dates
        .values()
        .copied()
        .min()
        .unwrap_or_else(|| chrono::Local::now().date_naive() - chrono::Duration::days(365))
}

fn build_client(config: &Config) -> Result<reqwest::Client, AppError> {
    let mut b = reqwest::Client::builder()
        .user_agent("binance-data-downloader/0.1")
        .timeout(std::time::Duration::from_secs(30))
        .pool_max_idle_per_host(config.max_concurrent)
        .pool_idle_timeout(std::time::Duration::from_secs(30));
    if let Some(ref p) = get_proxy::proxy_url()
        && let Ok(proxy) = reqwest::Proxy::all(p)
    {
        b = b.proxy(proxy);
    }
    b.build().map_err(AppError::Http)
}
