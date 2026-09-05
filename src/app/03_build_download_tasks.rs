use std::collections::HashSet;
use std::path::Path;

use crate::config::DownloadTask;

/// A download URL with its relative storage path and metadata.
pub struct DownloadUrl {
    pub url: String,
    pub rel_path: String,
    pub pair: String,
    pub timeframe: String,
}

const REL_PATH_PREFIX: &str = "futures/um/daily/klines";

/// Generate daily kline download URLs for a date range.
pub fn build_urls(
    pair: &str,
    tf: &str,
    date_start: chrono::NaiveDate,
    date_end: chrono::NaiveDate,
) -> Vec<DownloadUrl> {
    if date_start > date_end {
        return Vec::new();
    }
    let n_days = ((date_end - date_start).num_days() as usize).max(1);
    let mut result = Vec::with_capacity(n_days);

    let mut cursor = date_start;
    while cursor <= date_end {
        let date_str = cursor.format("%Y-%m-%d").to_string();
        let filename = format!("{pair}-{tf}-{date_str}.zip");
        let rel = format!("{REL_PATH_PREFIX}/{pair}/{tf}/{filename}");
        let url = format!("https://data.binance.vision/data/{rel}");
        result.push(DownloadUrl {
            url,
            rel_path: rel,
            pair: pair.to_string(),
            timeframe: tf.to_string(),
        });
        cursor += chrono::Duration::days(1);
    }
    result
}

/// Build a list of DownloadTasks for a date range, skipping locally-present files.
pub fn build_download_tasks(
    symbol: &str,
    timeframe: &str,
    start: chrono::NaiveDate,
    end: chrono::NaiveDate,
    local: &HashSet<String>,
    data_dir: &Path,
) -> Vec<DownloadTask> {
    build_urls(symbol, timeframe, start, end)
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

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn build_urls_single_day() {
        let urls = build_urls(
            "BTCUSDT",
            "1m",
            chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        );
        assert_eq!(urls.len(), 1);
        assert!(urls[0].url.contains("BTCUSDT"));
        assert!(urls[0].url.contains("2024-01-02"));
        assert!(urls[0].url.starts_with("https://data.binance.vision/data/"));
        assert!(urls[0].url.ends_with(".zip"));
    }

    #[test]
    fn build_urls_multi_day() {
        let urls = build_urls(
            "BTCUSDT",
            "1m",
            chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        );
        assert_eq!(urls.len(), 3);
        assert!(urls[0].url.contains("2024-01-01"));
        assert!(urls[1].url.contains("2024-01-02"));
        assert!(urls[2].url.contains("2024-01-03"));
    }

    #[test]
    fn build_urls_rel_path_format() {
        let urls = build_urls(
            "ETHUSDT",
            "5m",
            chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
        );
        assert_eq!(
            urls[0].rel_path,
            "futures/um/daily/klines/ETHUSDT/5m/ETHUSDT-5m-2024-06-15.zip"
        );
    }

    #[test]
    fn build_urls_empty_range() {
        assert!(build_urls("BTCUSDT", "1m", date(2024, 6, 1), date(2024, 5, 31),).is_empty());
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
