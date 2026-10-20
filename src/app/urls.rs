//! Construction of archive download URLs and their local relative paths.

use std::fmt::Write as _;

use crate::config::Timeframe;

/// A download URL with its relative storage path and metadata.
#[derive(Debug)]
pub(crate) struct DownloadUrl<'a> {
    /// Full download URL.
    pub url: String,
    /// Relative storage path.
    pub rel_path: String,
    /// Trading pair symbol.
    pub pair: &'a str,
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
/// Returns an **iterator**: a long range (six years of `1m` is 2190 days)
/// would otherwise materialise a vector of URLs that the caller immediately
/// filters and throws away.
///
/// `archive_base` is a parameter rather than a constant so tests can point the
/// whole pipeline at a mock server; see [`crate::app::Endpoints`].
pub(crate) fn build_urls<'a>(
    archive_base: &'a str,
    pair: &'a str,
    tf: Timeframe,
    date_start: chrono::NaiveDate,
    date_end: chrono::NaiveDate,
) -> impl Iterator<Item = DownloadUrl<'a>> + 'a {
    let tf_name = tf.as_str();
    let mut cursor = date_start;
    // A manual cursor: `RangeInclusive<NaiveDate>` is not an iterator in this
    // chrono version. `checked_add_days` stops at the maximum representable
    // date instead of overflowing, which a plain `+= Duration::days(1)` did.
    let mut exhausted = false;
    std::iter::from_fn(move || {
        if exhausted || cursor > date_end {
            return None;
        }
        let date = cursor;
        match cursor.checked_add_days(chrono::Days::new(1)) {
            Some(next) => cursor = next,
            // The cursor cannot advance any further: the range ends with it.
            None => exhausted = true,
        }
        let file_name = format!("{pair}-{tf_name}-{}.zip", date.format("%Y-%m-%d"));
        let rel_path = format!("{LOCAL_PREFIX}/{pair}/{tf_name}/{file_name}");

        // One allocation, sized up front: `format!` would reallocate twice.
        let mut url = String::with_capacity(
            archive_base.len()
                + SERVER_PREFIX.len()
                + pair.len()
                + tf_name.len()
                + file_name.len()
                + 3,
        );
        write!(
            url,
            "{archive_base}/{SERVER_PREFIX}/{pair}/{tf_name}/{file_name}"
        )
        .expect("writing to a String never fails");

        Some(DownloadUrl {
            url,
            rel_path,
            pair,
            timeframe: tf,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn build_urls_single_day() {
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            "BTCUSDT",
            Timeframe::M1,
            chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        )
        .collect();
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
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            "BTCUSDT",
            Timeframe::M1,
            chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        )
        .collect();
        assert_eq!(urls.len(), 3);
        assert!(urls[0].url.contains("2024-01-01"));
        assert!(urls[1].url.contains("2024-01-02"));
        assert!(urls[2].url.contains("2024-01-03"));
    }

    #[test]
    fn build_urls_rel_path_format() {
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            "ETHUSDT",
            Timeframe::M5,
            chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
        )
        .collect();
        assert_eq!(
            urls[0].rel_path,
            "binance/data/um/daily/klines/ETHUSDT/5m/ETHUSDT-5m-2024-06-15.zip"
        );
    }

    #[test]
    fn build_urls_empty_range() {
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            "BTCUSDT",
            Timeframe::M1,
            date(2024, 6, 1),
            date(2024, 5, 31),
        )
        .collect();
        assert!(urls.is_empty());
    }

    #[test]
    fn build_urls_exact_url_and_rel_path() {
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            "BTCUSDT",
            Timeframe::H1,
            chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
        )
        .collect();
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
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            "ETHUSDT",
            Timeframe::D1,
            chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2020, 12, 31).unwrap(),
        )
        .collect();
        // 2020 is a leap year: 366 days.
        assert_eq!(urls.len(), 366);
    }

    #[test]
    fn build_urls_spans_a_leap_day() {
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            "ETHUSDT",
            Timeframe::D1,
            chrono::NaiveDate::from_ymd_opt(2024, 2, 28).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 3, 1).unwrap(),
        )
        .collect();
        assert_eq!(urls.len(), 3);
        assert!(urls[1].rel_path.contains("2024-02-29"));
    }

    #[test]
    fn build_urls_does_not_overflow_at_the_maximum_date() {
        // The cursor used to be advanced with `+= Duration::days(1)`, which
        // panics once it leaves the representable range.
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            "ETHUSDT",
            Timeframe::D1,
            chrono::NaiveDate::MAX,
            chrono::NaiveDate::MAX,
        )
        .collect();
        assert_eq!(urls.len(), 1);
        assert!(
            urls[0]
                .rel_path
                .contains(&chrono::NaiveDate::MAX.format("%Y-%m-%d").to_string())
        );
    }
}
