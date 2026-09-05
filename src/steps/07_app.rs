use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use crate::AppError;
use crate::build_download_tasks;
use crate::config::AppConfig;
use crate::get_start_date;
use crate::run_downloads;
use crate::scan_local_files::scan_local_files;

/// Run the full batch download pipeline.
///
/// 1. Load config
/// 2. Scan existing local files (once)
/// 3. Iterate symbol (serial) → timeframe (serial), downloading missing files
pub async fn run(config_path: &Path) -> Result<(), AppError> {
    let cfg = AppConfig::load(config_path)?;

    let client = reqwest::Client::builder()
        .user_agent("binance-data-downloader/0.1")
        .timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(256)
        .pool_idle_timeout(Duration::from_secs(30))
        .build()?;

    let data_dir = cfg.output_dir.clone();
    let local = scan_local_files(&data_dir).await?;

    let symbols = cfg.symbol.clone();
    let timeframes = cfg.timeframe.clone();
    let concurrency = cfg.concurrency;
    let end = cfg
        .end
        .unwrap_or_else(|| chrono::Local::now().date_naive() - chrono::Duration::days(1));

    tracing::info!(
        symbols = symbols.len(),
        timeframes = timeframes.len(),
        total = symbols.len() * timeframes.len(),
        concurrency,
        "starting batch download",
    );

    let mut failed_pairs = 0;
    let mut errors = Vec::new();
    for symbol in &symbols {
        for timeframe in &timeframes {
            tracing::info!(symbol = %symbol, timeframe = %timeframe, "running pair");
            if let Err(e) = run_one(&client, symbol, timeframe, cfg.start, end, &cfg, &local).await
            {
                failed_pairs += 1;
                tracing::error!(symbol = %symbol, timeframe = %timeframe, "failed: {e}");
                if errors.len() < MAX_BATCH_ERRORS {
                    errors.push(format!("{symbol}/{timeframe}: {e}"));
                }
            }
        }
    }

    if failed_pairs == 0 {
        tracing::info!("batch download complete");
        Ok(())
    } else {
        tracing::error!(count = failed_pairs, "batch download completed with errors");
        let truncated = if errors.len() < failed_pairs {
            "\n(additional failures not listed)"
        } else {
            ""
        };
        Err(AppError::Data(format!(
            "{} pair(s) failed: {}{}",
            failed_pairs,
            errors.join("; "),
            truncated,
        )))
    }
}

/// Maximum number of pair-level error details retained in the batch summary.
const MAX_BATCH_ERRORS: usize = 20;

/// Download one symbol/timeframe pair.
async fn run_one(
    client: &reqwest::Client,
    symbol: &str,
    timeframe: &str,
    start: Option<chrono::NaiveDate>,
    end: chrono::NaiveDate,
    cfg: &AppConfig,
    local: &HashSet<String>,
) -> Result<(), AppError> {
    let start = resolve_start(client, symbol, timeframe, start).await?;

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

    let tasks = build_download_tasks::build_download_tasks(
        symbol,
        timeframe,
        start,
        end,
        local,
        &cfg.output_dir,
    );

    if !tasks.is_empty() {
        tracing::debug!(
            files = tasks.len(),
            start = %start,
            end = %end,
            "downloading all",
        );
        let stats = run_downloads::run_downloads(client, tasks, cfg.concurrency).await?;
        tracing::info!(
            symbol = %symbol,
            timeframe = %timeframe,
            ok = stats.ok,
            not_found = stats.not_found,
            failed = stats.failed,
            total = stats.total,
            "pair download complete",
        );
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
    start: Option<chrono::NaiveDate>,
) -> Result<chrono::NaiveDate, AppError> {
    let sym_start = get_start_date::get_first_kline_date(client, symbol, timeframe).await?;
    Ok(start.map(|cs| cs.max(sym_start)).unwrap_or(sym_start))
}

#[cfg(test)]
mod tests {
    #[test]
    fn resolve_start_date_prefers_cfg() {
        let sym = chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let cfg = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(Some(cfg).map(|cs| cs.max(sym)).unwrap_or(sym), cfg);
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
