pub mod local_file;
pub mod start_date;
pub mod store;
pub mod symbols;
pub mod urls;

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::config::Config;
use start_date::StartDateMap;

#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub url: String,
    pub dest: PathBuf,
    pub pair: String,
    pub timeframe: String,
}

pub fn build_tasks(
    symbols: &[String],
    timeframes: &[String],
    config: &Config,
    start_dates: &StartDateMap,
    end_date: chrono::NaiveDate,
    local: &HashSet<String>,
    data_dir: &Path,
) -> Vec<DownloadTask> {
    urls::build_urls(symbols, timeframes, config, start_dates, end_date)
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
