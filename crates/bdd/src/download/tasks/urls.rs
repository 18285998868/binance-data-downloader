//! Construction of archive download URLs and their local relative paths.
//!
//! Both sides use the same relative path: the public archive path
//! `data/futures/um/daily/klines/...` is also the path below `output_dir`.
//! There is no separate local layout to keep in sync.

use crate::dataset::{DownloadJob, Market};

/// A download URL with its relative storage path and metadata.
#[derive(Debug)]
pub(crate) struct DownloadUrl<'a> {
    /// Full download URL.
    pub url: String,
    /// Path relative to both the archive root and `output_dir`.
    pub rel_path: String,
    /// Trading pair symbol.
    pub pair: &'a str,
    /// Dataset/period/interval this file belongs to.
    pub job: DownloadJob,
}

/// Generate archive download URLs for one symbol/job and date range.
///
/// Returns an **iterator**: a long range (six years of `1m` is 2190 days)
/// would otherwise materialise a vector of URLs that the caller immediately
/// filters and throws away.
///
/// Daily ranges cover every day; monthly ranges are aligned to complete
/// months by [`crate::dataset::Period::dates`].
///
/// `archive_base` is a parameter rather than a constant so tests can point the
/// whole pipeline at a mock server; see [`crate::endpoints::Endpoints`].
pub(crate) fn build_urls<'a>(
    archive_base: &'a str,
    market: Market,
    pair: &'a str,
    job: DownloadJob,
    date_start: chrono::NaiveDate,
    date_end: chrono::NaiveDate,
) -> impl Iterator<Item = DownloadUrl<'a>> + 'a {
    let mut dates = job.period.dates(date_start, date_end);
    std::iter::from_fn(move || {
        let date = dates.next()?;
        let rel_path = job.rel_path(market, pair, date);
        Some(DownloadUrl {
            url: format!("{archive_base}/{rel_path}"),
            rel_path,
            pair,
            job,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{DatasetKind, Interval, Period};
    use crate::endpoints::ARCHIVE_BASE;

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn klines(interval: Interval, period: Period) -> DownloadJob {
        DownloadJob {
            kind: DatasetKind::Klines,
            interval: Some(interval),
            period,
        }
    }

    #[test]
    fn build_urls_single_day() {
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            Market::Um,
            "BTCUSDT",
            klines(Interval::M1, Period::Daily),
            date(2024, 1, 2),
            date(2024, 1, 2),
        )
        .collect();
        assert_eq!(urls.len(), 1);
        assert_eq!(
            urls[0].url,
            "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-02.zip"
        );
        assert_eq!(
            urls[0].rel_path,
            "data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-02.zip"
        );
        assert_eq!(urls[0].pair, "BTCUSDT");
        assert_eq!(urls[0].job, klines(Interval::M1, Period::Daily));
    }

    #[test]
    fn build_urls_multi_day() {
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            Market::Um,
            "BTCUSDT",
            klines(Interval::M1, Period::Daily),
            date(2024, 1, 1),
            date(2024, 1, 3),
        )
        .collect();
        assert_eq!(urls.len(), 3);
        assert!(urls[0].url.ends_with("BTCUSDT-1m-2024-01-01.zip"));
        assert!(urls[1].url.ends_with("BTCUSDT-1m-2024-01-02.zip"));
        assert!(urls[2].url.ends_with("BTCUSDT-1m-2024-01-03.zip"));
    }

    #[test]
    fn spot_files_use_the_spot_path() {
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            Market::Spot,
            "ETHUSDT",
            DownloadJob {
                kind: DatasetKind::AggTrades,
                interval: None,
                period: Period::Daily,
            },
            date(2024, 6, 15),
            date(2024, 6, 15),
        )
        .collect();
        assert_eq!(
            urls[0].rel_path,
            "data/spot/daily/aggTrades/ETHUSDT/ETHUSDT-aggTrades-2024-06-15.zip"
        );
    }

    #[test]
    fn cm_futures_use_the_cm_path() {
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            Market::Cm,
            "BTCUSD_PERP",
            DownloadJob {
                kind: DatasetKind::FundingRate,
                interval: None,
                period: Period::Monthly,
            },
            date(2024, 1, 1),
            date(2024, 1, 31),
        )
        .collect();
        assert_eq!(urls.len(), 1);
        assert_eq!(
            urls[0].rel_path,
            "data/futures/cm/monthly/fundingRate/BTCUSD_PERP/BTCUSD_PERP-fundingRate-2024-01.zip"
        );
    }

    #[test]
    fn monthly_range_skips_an_incomplete_month() {
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            Market::Um,
            "BTCUSDT",
            DownloadJob {
                kind: DatasetKind::Metrics,
                interval: None,
                period: Period::Monthly,
            },
            date(2024, 1, 15),
            date(2024, 3, 15),
        )
        .collect();
        assert_eq!(urls.len(), 2);
        assert!(urls[0].url.ends_with("BTCUSDT-metrics-2024-01.zip"));
        assert!(urls[1].url.ends_with("BTCUSDT-metrics-2024-02.zip"));
    }

    #[test]
    fn monthly_range_includes_a_complete_end_month() {
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            Market::Um,
            "BTCUSDT",
            DownloadJob {
                kind: DatasetKind::Metrics,
                interval: None,
                period: Period::Monthly,
            },
            date(2024, 1, 1),
            date(2024, 3, 31),
        )
        .collect();
        assert_eq!(urls.len(), 3);
        assert!(urls[2].url.ends_with("BTCUSDT-metrics-2024-03.zip"));
    }

    #[test]
    fn build_urls_empty_range() {
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            Market::Um,
            "BTCUSDT",
            klines(Interval::M1, Period::Daily),
            date(2024, 6, 1),
            date(2024, 5, 31),
        )
        .collect();
        assert!(urls.is_empty());
    }

    #[test]
    fn build_urls_capacity_matches_day_count() {
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            Market::Um,
            "ETHUSDT",
            klines(Interval::D1, Period::Daily),
            date(2020, 1, 1),
            date(2020, 12, 31),
        )
        .collect();
        // 2020 is a leap year: 366 days.
        assert_eq!(urls.len(), 366);
    }

    #[test]
    fn build_urls_spans_a_leap_day() {
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            Market::Um,
            "ETHUSDT",
            klines(Interval::D1, Period::Daily),
            date(2024, 2, 28),
            date(2024, 3, 1),
        )
        .collect();
        assert_eq!(urls.len(), 3);
        assert!(urls[1].rel_path.contains("2024-02-29"));
    }

    #[test]
    fn build_urls_does_not_overflow_at_the_maximum_date() {
        let urls: Vec<_> = build_urls(
            ARCHIVE_BASE,
            Market::Um,
            "ETHUSDT",
            klines(Interval::D1, Period::Daily),
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
