//! Turning a symbol/timeframe/date range into [`DownloadTask`]s, minus what is
//! already on disk.

use std::path::Path;

use super::urls::build_urls;
use crate::config::{DownloadTask, Timeframe};
use crate::scan::LocalFileSet;

/// Build a list of [`DownloadTask`]s for a date range, skipping locally-present files.
///
/// `local` holds 128-bit [`rel_path_hash`]es of the files found by
/// [`crate::scan::scan_local_files`].
#[must_use]
pub(crate) fn build_download_tasks(
    archive_base: &str,
    symbol: &str,
    timeframe: Timeframe,
    start: chrono::NaiveDate,
    end: chrono::NaiveDate,
    local: &LocalFileSet,
    data_dir: &Path,
) -> Vec<DownloadTask> {
    build_urls(archive_base, symbol, timeframe, start, end)
        .into_iter()
        .filter(|dl| !local.contains_rel_path(&dl.rel_path))
        .map(|dl| {
            // Defense in depth: `build_urls` only emits plain relative paths
            // (the symbol charset is validated in config), but a bug there
            // must never let a download escape `data_dir`.
            debug_assert!(
                Path::new(&dl.rel_path)
                    .components()
                    .all(|c| matches!(c, std::path::Component::Normal(_))),
                "rel_path is not a plain relative path: {}",
                dl.rel_path,
            );
            DownloadTask {
                url: dl.url,
                dest: data_dir.join(&dl.rel_path),
                pair: dl.pair,
                timeframe: dl.timeframe,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::urls::ARCHIVE_BASE;

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn build_tasks_all_missing() {
        let tasks = build_download_tasks(
            ARCHIVE_BASE,
            "BTCUSDT",
            Timeframe::M1,
            date(2024, 1, 1),
            date(2024, 1, 2),
            &LocalFileSet::default(),
            Path::new("/tmp"),
        );
        assert_eq!(tasks.len(), 2);
        assert!(tasks[0].url.contains("BTCUSDT"));
        assert_eq!(tasks[0].pair, "BTCUSDT");
    }

    #[test]
    fn build_tasks_skip_existing() {
        let mut local = LocalFileSet::default();
        local.insert_hash(crate::scan::rel_path_hash(
            "binance/data/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip",
        ));
        let tasks = build_download_tasks(
            ARCHIVE_BASE,
            "BTCUSDT",
            Timeframe::M1,
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
            "ETHUSDT",
            Timeframe::M5,
            date(2024, 6, 1),
            date(2024, 5, 31),
            &LocalFileSet::default(),
            Path::new("/tmp"),
        );
        assert!(tasks.is_empty());
    }

    /// The `debug_assert!` guard is active in debug builds (`cargo test`
    /// default); it documents that unvalidated symbols never reach `join`.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "rel_path")]
    fn build_tasks_rejects_path_traversal() {
        let _ = build_download_tasks(
            ARCHIVE_BASE,
            "../../etc",
            Timeframe::M1,
            date(2024, 1, 1),
            date(2024, 1, 1),
            &LocalFileSet::default(),
            Path::new("/tmp"),
        );
    }
}
