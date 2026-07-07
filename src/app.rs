use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use crate::AppError;
use crate::builder;
use crate::config::AppConfig;
use crate::download;
use crate::get_local_files::scan_local_files;
use crate::get_start_date;
use crate::get_symbols;

/// Run the full batch download pipeline.
///
/// 1. Load config
/// 2. Resolve symbols and timeframes
/// 3. Scan existing local files (once)
/// 4. Iterate symbol (serial) → timeframe (serial), downloading missing files
pub async fn run(config_path: Option<&std::path::Path>) -> Result<(), AppError> {
    let cfg = AppConfig::load(config_path)?;

    let client = reqwest::Client::builder()
        .user_agent("binance-data-downloader/0.1")
        .timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(256)
        .pool_idle_timeout(Duration::from_secs(30))
        .build()
        .expect("build reqwest client");

    let data_dir = PathBuf::from(&cfg.dir);
    let local = scan_local_files(&data_dir).await?;

    let symbols =
        get_symbols::resolve_symbols(&client, cfg.symbol.as_deref().unwrap_or(&[])).await?;
    let timeframes = cfg.timeframe.clone().unwrap_or_default();

    if symbols.is_empty() {
        tracing::warn!("no symbols resolved, nothing to download");
        return Ok(());
    }
    if timeframes.is_empty() {
        tracing::warn!("no timeframes configured, nothing to download");
        return Ok(());
    }

    tracing::info!(
        symbols = symbols.len(),
        timeframes = timeframes.len(),
        total = symbols.len() * timeframes.len(),
        "starting batch download",
    );

    let mut errors = Vec::new();
    for symbol in &symbols {
        for timeframe in &timeframes {
            tracing::info!(symbol = %symbol, timeframe = %timeframe, "running pair");
            if let Err(e) = run_one(&client, symbol, timeframe, &cfg, &local).await {
                tracing::error!(symbol = %symbol, timeframe = %timeframe, "failed: {e}");
                errors.push(format!("{symbol}/{timeframe}: {e}"));
            }
        }
    }

    if errors.is_empty() {
        tracing::info!("batch download complete");
        Ok(())
    } else {
        tracing::error!(count = errors.len(), "batch download completed with errors");
        Err(AppError::Data(format!(
            "{} pair(s) failed: {}",
            errors.len(),
            errors.join("; ")
        )))
    }
}

/// Download and convert one symbol/timeframe pair.
async fn run_one(
    client: &reqwest::Client,
    symbol: &str,
    timeframe: &str,
    cfg: &AppConfig,
    local: &HashSet<String>,
) -> Result<(), AppError> {
    let data_dir = PathBuf::from(&cfg.dir);
    let end = cfg
        .end
        .as_deref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
        .unwrap_or_else(|| chrono::Local::now().date_naive() - chrono::Duration::days(1));

    let start = resolve_start(client, symbol, timeframe, cfg.start.as_deref()).await?;

    if start > end {
        tracing::warn!(
            symbol = %symbol,
            timeframe = %timeframe,
            start = %start,
            end = %end,
            "start date after end date, skipping",
        );
        return Ok(());
    }

    tracing::info!(
        symbol = %symbol,
        timeframe = %timeframe,
        start = %start,
        end = %end,
        "processing symbol-timeframe pair",
    );

    let tasks = builder::build_download_tasks(symbol, timeframe, start, end, local, &data_dir);

    if !tasks.is_empty() {
        tracing::debug!(
            files = tasks.len(),
            start = %start,
            end = %end,
            "downloading all",
        );
        download::run_downloads(client, tasks).await?;
    } else {
        tracing::debug!(
            symbol = %symbol,
            timeframe = %timeframe,
            start = %start,
            end = %end,
            "no files to download, all cached or out of range",
        );
    }

    Ok(())
}

async fn resolve_start(
    client: &reqwest::Client,
    symbol: &str,
    timeframe: &str,
    start: Option<&str>,
) -> Result<chrono::NaiveDate, AppError> {
    let sym_start = get_start_date::get_first_kline_date(client, symbol, timeframe).await?;
    let cfg_start = start.and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
    Ok(cfg_start.map(|cs| cs.max(sym_start)).unwrap_or(sym_start))
}

#[cfg(test)]
mod tests {
    #[test]
    fn resolve_start_date_prefers_cfg() {
        let sym = chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let cfg = Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        assert_eq!(cfg.map(|cs| cs.max(sym)).unwrap_or(sym), cfg.unwrap());
    }

    #[test]
    fn resolve_start_date_prefers_sym_when_later() {
        let sym = chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let cfg = Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        assert_eq!(cfg.map(|cs| cs.max(sym)).unwrap_or(sym), sym);
    }

    #[test]
    fn resolve_start_date_no_cfg() {
        let sym = chrono::NaiveDate::from_ymd_opt(2023, 6, 15).unwrap();
        let cfg: Option<chrono::NaiveDate> = None;
        assert_eq!(cfg.map(|cs| cs.max(sym)).unwrap_or(sym), sym);
    }
}
