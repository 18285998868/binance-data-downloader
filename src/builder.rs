use std::collections::HashSet;
use std::path::Path;

use crate::DownloadTask;
use crate::urls;

/// Build a list of DownloadTasks for a date range, skipping locally-present files.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn build_tasks_all_missing() {
        let tasks = build_download_tasks(
            "BTCUSDT",
            "1m",
            date(2024, 1, 1),
            date(2024, 1, 2),
            &HashSet::new(),
            Path::new("/tmp"),
        );
        assert_eq!(tasks.len(), 2);
        assert!(tasks[0].url.contains("BTCUSDT"));
        assert_eq!(tasks[0].pair, "BTCUSDT");
    }

    #[test]
    fn build_tasks_skip_existing() {
        let mut local = HashSet::new();
        local.insert("futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip".into());
        let tasks = build_download_tasks(
            "BTCUSDT",
            "1m",
            date(2024, 1, 1),
            date(2024, 1, 2),
            &local,
            Path::new("/tmp"),
        );
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].url.contains("2024-01-02"));
    }

    #[test]
    fn build_tasks_empty_range() {
        let tasks = build_download_tasks(
            "ETHUSDT",
            "5m",
            date(2024, 6, 1),
            date(2024, 5, 31),
            &HashSet::new(),
            Path::new("/tmp"),
        );
        assert!(tasks.is_empty());
    }
}
