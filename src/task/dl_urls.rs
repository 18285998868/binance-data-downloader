use std::collections::HashSet;
use std::path::Path;

use super::urls;
use super::DownloadTask;

pub fn build_download_tasks(
    symbol: &str,
    timeframe: &str,
    start: chrono::NaiveDate,
    end: chrono::NaiveDate,
    local: &HashSet<String>,
    data_dir: &Path,
) -> Vec<DownloadTask> {
    urls::build_urls(symbol, timeframe, start, end)
        .into_iter()
        .filter(|dl| !local.contains(&dl.rel_path))
        .map(|dl| DownloadTask {
            url: dl.url,
            dest: data_dir.join(&dl.rel_path),
            pair: dl.pair,
            timeframe: dl.timeframe,
        })
        .collect()
}
