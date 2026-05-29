/// Resolves symbol names for spot/futures markets.
///
/// If `symbols` is `Some` and non-empty, returns it directly.
/// Otherwise fetches all TRADING symbols from Binance exchangeInfo
/// via API with backoff retry, cached in sled with a 7-day TTL.
pub mod symbols;
/// Resolves the earliest available kline date for each symbol.
///
/// For each (market_type, symbol) pair not found in cache,
/// calls Binance klines API with startTime=0 to get the first
/// available date. Results cached in sled with a 7-day TTL.
pub mod start_date;
/// Scans `data_dir` for existing `.zip` and `.parquet` files
/// to avoid re-downloading already-fetched data.
pub mod local_file;
/// Persist cache using sled, with TTL via stored expiration timestamps.
pub mod store;
/// Builds download URLs from symbol×type×timeframe×date combinations.
pub mod urls;

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::config::Config;
use start_date::StartDateMap;

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
/// Filters out files already present on disk (via `local` set).
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
    data_dir: &Path,
) -> Vec<DownloadTask> {
    urls::build_urls(symbols, timeframes, config, start_dates, chunk_start, chunk_end)
        .into_iter()
        .filter(|dl| !local.contains(dl.rel_path.trim_end_matches(".zip")))
        .map(|u| DownloadTask {
            url: u.url,
            dest: data_dir.join(&u.rel_path),
            pair: u.pair,
            timeframe: u.timeframe,
        })
        .collect()
}
