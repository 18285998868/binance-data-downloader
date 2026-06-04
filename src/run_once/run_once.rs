use chrono::Duration;
use std::path::PathBuf;

use crate::AppError;
use crate::run_once::clean;
use crate::run_once::downloader;
use crate::run_once::task::{DownloadTask, local_file, urls};

use super::config::RunOnceConfig;
use super::get_start;

const DATE_CHUNK_DAYS: i64 = 30;

/// Runs a single symbol/timeframe download.
///
/// Chunks the date range into 30-day windows, skips files already on disk,
/// and downloads + processes each chunk with bounded concurrency.
pub async fn run_once(
    client: &reqwest::Client,
    config: &RunOnceConfig,
    max_concurrent: usize,
) -> Result<(), AppError> {
    let data_dir = resolve_dir(&config.dir);
    let end = resolve_end(&config.end);

    clean::cleanup_tmp(&data_dir).await;
    let local = local_file::scan_local_files(&data_dir).await?;
    let start = resolve_start(client, config).await?;

    if start > end {
        tracing::warn!(
            symbol = %config.symbol,
            timeframe = %config.timeframe,
            start = %start,
            end = %end,
            "start date after end date, skipping",
        );
        return Ok(());
    }

    tracing::info!(
        symbol = %config.symbol,
        timeframe = %config.timeframe,
        start = %start,
        end = %end,
        "processing symbol-timeframe pair",
    );

    let mut chunk_start = start;
    while chunk_start <= end {
        let chunk_end = (chunk_start + Duration::days(DATE_CHUNK_DAYS - 1)).min(end);

        let tasks: Vec<DownloadTask> =
            urls::build_urls(&config.symbol, &config.timeframe, chunk_start, chunk_end)
                .into_iter()
                .filter(|dl| !local.contains(&dl.rel_path))
                .map(|dl| DownloadTask {
                    url: dl.url,
                    dest: data_dir.join(&dl.rel_path),
                    pair: dl.pair,
                    timeframe: dl.timeframe,
                })
                .collect();

        if !tasks.is_empty() {
            tracing::debug!(
                files = tasks.len(),
                date_range = %format!("{}..{}", chunk_start, chunk_end),
                "downloading chunk",
            );
            downloader::run_downloads(client, tasks, max_concurrent).await?;
        }

        chunk_start = chunk_end + Duration::days(1);
    }

    Ok(())
}

fn resolve_dir(dir: &Option<String>) -> PathBuf {
    dir.as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data"))
}

fn resolve_end(end_opt: &Option<String>) -> chrono::NaiveDate {
    end_opt
        .as_ref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
        .unwrap_or_else(|| chrono::Local::now().date_naive() - Duration::days(1))
}

async fn resolve_start(
    client: &reqwest::Client,
    config: &RunOnceConfig,
) -> Result<chrono::NaiveDate, AppError> {
    let sym_start =
        get_start::get_first_kline_date(client, &config.symbol, &config.timeframe).await?;
    let cfg_start = config
        .start
        .as_ref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
    Ok(cfg_start.map(|cs| cs.max(sym_start)).unwrap_or(sym_start))
}
