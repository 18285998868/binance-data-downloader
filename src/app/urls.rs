//! Construction of archive download URLs and their local relative paths.

use crate::config::Timeframe;

/// A download URL with its relative storage path and metadata.
#[derive(Debug)]
pub(crate) struct DownloadUrl {
    /// Full download URL.
    pub url: String,
    /// Relative storage path.
    pub rel_path: String,
    /// Trading pair symbol.
    pub pair: String,
    /// Kline timeframe.
    pub timeframe: Timeframe,
}

/// Server-side path on data.binance.vision (fixed by the exchange).
const SERVER_PREFIX: &str = "data/futures/um/daily/klines";
/// Local storage root below `output_dir` (our choice).
const LOCAL_PREFIX: &str = "binance/data/um/daily/klines";
/// Public data archive base URL.
pub(crate) const ARCHIVE_BASE: &str = "https://data.binance.vision";

/// Generate daily kline download URLs for a date range.
///
/// `archive_base` is a parameter rather than a constant so tests can point the
/// whole pipeline at a mock server; see [`crate::app::Endpoints`].
#[must_use]
pub(crate) fn build_urls(
    archive_base: &str,
    pair: &str,
    tf: Timeframe,
    date_start: chrono::NaiveDate,
    date_end: chrono::NaiveDate,
) -> Vec<DownloadUrl> {
    if date_start > date_end {
        return Vec::new();
    }
    let tf_name = tf.as_str();
    let n_days = usize::try_from((date_end - date_start).num_days())
        .unwrap_or(0)
        .max(1);
    let mut result = Vec::with_capacity(n_days);

    let mut cursor = date_start;
    while cursor <= date_end {
        let date_str = cursor.format("%Y-%m-%d").to_string();
        let filename = format!("{pair}-{tf_name}-{date_str}.zip");
        let server_rel = format!("{SERVER_PREFIX}/{pair}/{tf_name}/{filename}");
        let local_rel = format!("{LOCAL_PREFIX}/{pair}/{tf_name}/{filename}");
        let url = format!("{archive_base}/{server_rel}");
        result.push(DownloadUrl {
            url,
            rel_path: local_rel,
            pair: pair.to_string(),
            timeframe: tf,
        });
        cursor += chrono::Duration::days(1);
    }
    result
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
            ARCHIVE_BASE,
            "BTCUSDT",
            Timeframe::M1,
            chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        );
        assert_eq!(urls.len(), 1);
        assert!(urls[0].url.contains("BTCUSDT"));
        assert!(urls[0].url.contains("2024-01-02"));
        assert!(urls[0].url.starts_with("https://data.binance.vision/data/"));
        // The archive always publishes lowercase extensions.
        #[allow(clippy::case_sensitive_file_extension_comparisons)]
        {
            assert!(urls[0].url.ends_with(".zip"));
        }
        assert!(urls[0].url.contains("data/futures/um/daily/klines/"));
        assert!(
            urls[0]
                .rel_path
                .starts_with("binance/data/um/daily/klines/")
        );
    }

    #[test]
    fn build_urls_multi_day() {
        let urls = build_urls(
            ARCHIVE_BASE,
            "BTCUSDT",
            Timeframe::M1,
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
            ARCHIVE_BASE,
            "ETHUSDT",
            Timeframe::M5,
            chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
        );
        assert_eq!(
            urls[0].rel_path,
            "binance/data/um/daily/klines/ETHUSDT/5m/ETHUSDT-5m-2024-06-15.zip"
        );
    }

    #[test]
    fn build_urls_empty_range() {
        assert!(
            build_urls(
                ARCHIVE_BASE,
                "BTCUSDT",
                Timeframe::M1,
                date(2024, 6, 1),
                date(2024, 5, 31),
            )
            .is_empty()
        );
    }

    #[test]
    fn build_urls_exact_url_and_rel_path() {
        let urls = build_urls(
            ARCHIVE_BASE,
            "BTCUSDT",
            Timeframe::H1,
            chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
        );
        assert_eq!(
            urls[0].url,
            "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-03-15.zip"
        );
        assert_eq!(
            urls[0].rel_path,
            "binance/data/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-03-15.zip"
        );
        assert_eq!(urls[0].pair, "BTCUSDT");
        assert_eq!(urls[0].timeframe, Timeframe::H1);
    }

    #[test]
    fn build_urls_capacity_matches_day_count() {
        let urls = build_urls(
            ARCHIVE_BASE,
            "ETHUSDT",
            Timeframe::D1,
            chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2020, 12, 31).unwrap(),
        );
        // 2020 is a leap year: 366 days.
        assert_eq!(urls.len(), 366);
    }

    #[test]
    fn build_urls_spans_a_leap_day() {
        let urls = build_urls(
            ARCHIVE_BASE,
            "ETHUSDT",
            Timeframe::D1,
            chrono::NaiveDate::from_ymd_opt(2024, 2, 28).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 3, 1).unwrap(),
        );
        assert_eq!(urls.len(), 3);
        assert!(urls[1].rel_path.contains("2024-02-29"));
    }
}
