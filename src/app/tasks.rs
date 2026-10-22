//! Turning a symbol/job/date range into [`DownloadTask`]s, minus what is
//! already on disk.

use std::path::Path;

use super::urls::build_urls;
use crate::config::DownloadTask;
use crate::dataset::{DownloadJob, Market};
use crate::scan::LocalFileSet;

/// Build a list of [`DownloadTask`]s for a date range, skipping locally-present files.
///
/// `local` holds 128-bit [`crate::scan::rel_path_hash`]es of the files found by
/// [`crate::scan::scan_local_files`].
#[must_use]
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_download_tasks(
    archive_base: &str,
    market: Market,
    symbol: &str,
    job: DownloadJob,
    start: chrono::NaiveDate,
    end: chrono::NaiveDate,
    local: &LocalFileSet,
    data_dir: &Path,
) -> Vec<DownloadTask> {
    build_urls(archive_base, market, symbol, job, start, end)
        .filter(|dl| !local.contains_rel_path(&dl.rel_path))
        .map(|dl| {
            // Defense in depth: `build_urls` only emits plain relative paths
            // (the symbol charset is validated in config), but a bug there
            // must never let a download escape `data_dir`.
            // `assert!`, not `debug_assert!`: an internal invariant violation
            // must stop the run in release builds too, before a malformed path
            // can escape `data_dir`.
            assert!(
                Path::new(&dl.rel_path)
                    .components()
                    .all(|c| matches!(c, std::path::Component::Normal(_))),
                "rel_path is not a plain relative path: {}",
                dl.rel_path,
            );
            DownloadTask {
                url: dl.url,
                dest: data_dir.join(&dl.rel_path),
                pair: dl.pair.to_owned(),
                job: dl.job,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::urls::ARCHIVE_BASE;
    use crate::dataset::{DatasetKind, Interval, Period};

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
    fn build_tasks_all_missing() {
        let job = klines(Interval::M1, Period::Daily);
        let tasks = build_download_tasks(
            ARCHIVE_BASE,
            Market::Um,
            "BTCUSDT",
            job,
            date(2024, 1, 1),
            date(2024, 1, 2),
            &LocalFileSet::default(),
            Path::new("/tmp"),
        );
        assert_eq!(tasks.len(), 2);
        assert!(tasks[0].url.contains("BTCUSDT"));
        assert_eq!(tasks[0].pair, "BTCUSDT");
        assert_eq!(tasks[0].job, job);
        assert_eq!(
            tasks[0].dest,
            Path::new("/tmp/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip")
        );
    }

    #[test]
    fn build_tasks_skip_existing() {
        let mut local = LocalFileSet::default();
        local.insert_hash(crate::scan::rel_path_hash(
            "data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip",
        ));
        let tasks = build_download_tasks(
            ARCHIVE_BASE,
            Market::Um,
            "BTCUSDT",
            klines(Interval::M1, Period::Daily),
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
            ARCHIVE_BASE,
            Market::Um,
            "ETHUSDT",
            klines(Interval::M5, Period::Daily),
            date(2024, 6, 1),
            date(2024, 5, 31),
            &LocalFileSet::default(),
            Path::new("/tmp"),
        );
        assert!(tasks.is_empty());
    }

    #[test]
    fn build_tasks_use_the_strict_web_path() {
        let tasks = build_download_tasks(
            ARCHIVE_BASE,
            Market::Um,
            "BTCUSDT",
            DownloadJob {
                kind: DatasetKind::FundingRate,
                interval: None,
                period: Period::Monthly,
            },
            date(2024, 1, 1),
            date(2024, 1, 31),
            &LocalFileSet::default(),
            Path::new("/srv/data"),
        );
        assert_eq!(tasks.len(), 1);
        assert_eq!(
            tasks[0].dest,
            Path::new(
                "/srv/data/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-01.zip"
            )
        );
    }

    /// The `assert!` guard rejects unvalidated symbols before a malformed path
    /// can escape `data_dir`.
    #[test]
    #[should_panic(expected = "rel_path")]
    fn build_tasks_rejects_path_traversal() {
        let _ = build_download_tasks(
            ARCHIVE_BASE,
            Market::Um,
            "../../etc",
            klines(Interval::M1, Period::Daily),
            date(2024, 1, 1),
            date(2024, 1, 1),
            &LocalFileSet::default(),
            Path::new("/tmp"),
        );
    }
}
