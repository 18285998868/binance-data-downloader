pub mod builder;
pub mod download;
pub mod get_local_files;
pub mod urls;

use std::path::{Path, PathBuf};

use crate::config::BatchConfig;
use crate::config::get_start;
use crate::AppError;

#[derive(Debug, Clone)]
/// A single file to download and its destination metadata.
pub struct DownloadTask {
    pub url: String,
    pub dest: PathBuf,
    pub pair: String,
    pub timeframe: String,
}

/// Download and process all daily klines for one symbol/timeframe pair.
pub async fn run_one_pair(
    client: &reqwest::Client,
    symbol: &str,
    timeframe: &str,
    cfg: &BatchConfig,
) -> Result<(), AppError> {
    let data_dir = PathBuf::from(&cfg.dir);
    let end = cfg.end.as_deref().and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
        .unwrap_or_else(|| chrono::Local::now().date_naive() - chrono::Duration::days(1));

    cleanup_tmp(&data_dir).await;
    let local = get_local_files::scan_local_files(&data_dir).await?;
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

    let tasks = builder::build_download_tasks(symbol, timeframe, start, end, &local, &data_dir);

    if !tasks.is_empty() {
        tracing::debug!(
            files = tasks.len(),
            date_range = %format!("{start}..{end}"),
            "downloading all",
        );
        download::run_downloads(client, tasks, cfg.max_concurrent.min(256)).await?;
    }

    Ok(())
}

async fn resolve_start(
    client: &reqwest::Client,
    symbol: &str,
    timeframe: &str,
    start: Option<&str>,
) -> Result<chrono::NaiveDate, AppError> {
    let sym_start = get_start::get_first_kline_date(client, symbol, timeframe).await?;
    let cfg_start = start.and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
    Ok(cfg_start.map(|cs| cs.max(sym_start)).unwrap_or(sym_start))
}

async fn cleanup_tmp(data_dir: &Path) {
    let d = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        for entry in walkdir::WalkDir::new(&d).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file()
                && entry.path().extension().is_some_and(|x| x == "tmp")
                && let Err(err) = std::fs::remove_file(entry.path())
            {
                tracing::warn!(
                    path = %entry.path().display(),
                    "failed to remove tmp file: {err}",
                );
            }
        }
    })
    .await
    .ok();
}
