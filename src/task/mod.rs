/// Scans `data_dir` for existing `.zip` and `.parquet` files
/// to avoid re-downloading already-fetched data.
pub mod local_file;
/// Builds download URLs from symbol×type×timeframe×date combinations.
pub mod urls;

use std::collections::HashSet;
use std::path::PathBuf;

use crate::config::Config;
use crate::config::start_date::StartDateMap;

/// A single file to download and convert.
#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub url: String,
    pub dest: PathBuf,
    pub pair: String,
    pub timeframe: String,
}

/// Builds download tasks for a batch of symbols within a date range.
///
/// Filters out files already present on disk (via `local` set, which uses
/// `.zip`-normalized paths for direct matching).
/// The date range `[chunk_start, chunk_end]` limits the number of
/// URLs generated per batch to avoid excessive memory usage.
#[allow(clippy::too_many_arguments)]
pub fn build_tasks(
    symbols: &[String],
    timeframes: &[String],
    config: &Config,
    start_dates: &StartDateMap,
    chunk_start: chrono::NaiveDate,
    chunk_end: chrono::NaiveDate,
    local: &HashSet<String>,
) -> Vec<DownloadTask> {
    let config_sd = config
        .start_date
        .as_ref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
    let config_ed = config
        .end_date
        .as_ref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
    let one_year_ago = chrono::Local::now().date_naive() - chrono::Duration::days(365);

    urls::build_urls(
        symbols,
        timeframes,
        config,
        start_dates,
        chunk_start,
        chunk_end,
        config_sd,
        config_ed,
        one_year_ago,
    )
    .into_iter()
    .filter(|dl| !local.contains(&dl.rel_path))
    .map(|u| DownloadTask {
        url: u.url,
        dest: config.data_dir.join(&u.rel_path),
        pair: u.pair,
        timeframe: u.timeframe,
    })
    .collect()
}
