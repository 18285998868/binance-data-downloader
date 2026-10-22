//! Bounded-concurrency fan-out over download tasks, plus aggregate statistics.

pub mod stream;

use std::future::Future;

use crate::AppError;
use crate::config::DownloadTask;
use futures::stream::{StreamExt, iter as stream_iter};
use tracing::Instrument;

/// How often a line of progress is logged, in completed files.
const PROGRESS_EVERY: usize = 100;
/// Maximum number of individual failure details logged per batch.
const MAX_LOGGED_FAILURES: usize = 20;

/// Aggregate statistics for a completed batch of downloads.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DownloadStats {
    /// Total number of tasks.
    pub total: usize,
    /// Successfully downloaded.
    pub ok: usize,
    /// Files not found (404). Tolerated: never counted as a failure.
    pub not_found: usize,
    /// Failed downloads.
    pub failed: usize,
    /// Downloads abandoned because the run was cancelled.
    pub cancelled: usize,
}

/// A pair that failed *and* what it managed to do before failing.
///
/// Carrying the statistics alongside the error is what lets the batch totals
/// stay accurate: a pair with 999 successes and 1 failure still reports 1000.
#[derive(Debug)]
pub(crate) struct DownloadFailure {
    /// Statistics collected across the whole pair.
    pub stats: DownloadStats,
    /// Why the pair counts as failed.
    pub error: AppError,
}

impl std::fmt::Display for DownloadFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for DownloadFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl DownloadStats {
    /// Number of tasks that reached a terminal outcome.
    pub(crate) fn done(&self) -> usize {
        self.ok + self.not_found + self.failed + self.cancelled
    }

    /// Fold another batch's stats into this one.
    pub(crate) fn absorb(&mut self, other: Self) {
        self.total += other.total;
        self.ok += other.ok;
        self.not_found += other.not_found;
        self.failed += other.failed;
        self.cancelled += other.cancelled;
    }

    /// `not_found` as a percentage of terminal outcomes (0.0 when none ran).
    ///
    /// Using [`Self::done`] rather than `total` keeps the rate meaningful when
    /// a run is interrupted: tasks that never started must not dilute the
    /// not-found share of the tasks that did finish.
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // file counts stay far below 2^53
    pub(crate) fn not_found_pct(&self) -> f64 {
        let done = self.done();
        if done == 0 {
            0.0
        } else {
            100.0 * (self.not_found as f64) / (done as f64)
        }
    }
}

/// A download that failed, tagged with the URL it happened on.
///
/// The tag matters: without it the batch summary can only say "35 of 71
/// downloads failed" without saying *which* files failed or *why* — and the
/// reason (a full disk, an exhausted descriptor limit, a bad response) is
/// exactly what the reader needs.
#[derive(Debug)]
pub(crate) struct FailedDownload {
    /// The URL that was being downloaded.
    pub url: String,
    /// Why it failed.
    pub error: AppError,
}

impl std::fmt::Display for FailedDownload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.url, self.error)
    }
}

/// Download all tasks with bounded concurrency.
///
/// # Errors
///
/// Returns [`DownloadFailure`] when one or more downloads fail (404s are
/// tolerated and not counted as failures). The embedded stats are always
/// populated, including on the error path.
pub(crate) async fn run_downloads(
    client: &reqwest::Client,
    tasks: Vec<DownloadTask>,
    concurrency: usize,
    cancel: &crate::cancel::CancelToken,
    verify: bool,
) -> Result<DownloadStats, DownloadFailure> {
    let cancel = cancel.clone();
    let download_fn = move |c: reqwest::Client, t: DownloadTask| {
        let cancel = cancel.clone();
        async move {
            let result = stream::download_to_file(&c, &t, &cancel, verify).await;
            // The URL is moved into the failure only when there is one: the
            // happy path stays allocation-free.
            match result {
                Ok(()) => Ok(()),
                Err(error) => Err(FailedDownload { url: t.url, error }),
            }
        }
    };
    run_downloads_inner(client, tasks, concurrency, download_fn).await
}

/// Core download orchestration, parameterized by the download function for testing.
///
/// Streams tasks through `buffer_unordered(concurrency)`, so at most
/// `concurrency` downloads run at once and results are counted incrementally
/// instead of materializing all futures upfront.
async fn run_downloads_inner<F, Fut>(
    client: &reqwest::Client,
    tasks: Vec<DownloadTask>,
    concurrency: usize,
    download_fn: F,
) -> Result<DownloadStats, DownloadFailure>
where
    F: Fn(reqwest::Client, DownloadTask) -> Fut + Send,
    Fut: Future<Output = Result<(), FailedDownload>>,
{
    let total = tasks.len();
    let concurrency = concurrency.max(1);
    if total == 0 {
        return Ok(DownloadStats::default());
    }
    tracing::debug!(total, concurrency, "downloading files");

    let mut stats = DownloadStats {
        total,
        ..DownloadStats::default()
    };
    let mut logged_failures = 0;
    // The reasons of the first few failures, so the batch summary says *why*
    // instead of only *how many*.
    let mut failure_details: Vec<String> = Vec::new();

    let mut stream = stream_iter(tasks)
        .map(|t| {
            let c = client.clone();
            let span = tracing::info_span!("download", pair = %t.pair, job = %t.job);
            let fut = download_fn(c, t);
            fut.instrument(span)
        })
        .buffer_unordered(concurrency);

    while let Some(result) = stream.next().await {
        match result {
            Ok(()) => stats.ok += 1,
            Err(failure) => {
                if matches!(&failure.error, AppError::NotFound { .. }) {
                    stats.not_found += 1;
                } else if matches!(&failure.error, AppError::Cancelled) {
                    // Cancellation is not a failure: stop scheduling at once
                    // and let the remaining temp files be reclaimed by the
                    // next scan.
                    stats.cancelled += 1;
                    break;
                } else {
                    stats.failed += 1;
                    if logged_failures < MAX_LOGGED_FAILURES {
                        logged_failures += 1;
                        tracing::warn!(url = %failure.url, error = ?failure.error, "download failed");
                        failure_details.push(failure.to_string());
                    }
                }
            }
        }

        if stats.done().is_multiple_of(PROGRESS_EVERY) {
            tracing::info!(
                done = stats.done(),
                total = stats.total,
                ok = stats.ok,
                not_found = stats.not_found,
                failed = stats.failed,
                "download progress",
            );
        }
    }

    if stats.not_found > 0 {
        // 404s are tolerated by design (a symbol is not listed on every day of
        // the range, and today's archive is not published yet); they are
        // reported, never treated as failure.
        tracing::warn!(
            not_found = stats.not_found,
            total = stats.total,
            pct = stats.not_found_pct(),
            "files not found on the server (404, expected for edge dates; investigate if unexpected)",
        );
    }
    if stats.failed > logged_failures {
        tracing::warn!(
            hidden = stats.failed - logged_failures,
            "additional failure details not logged",
        );
    }

    if stats.failed > 0 {
        return Err(DownloadFailure {
            stats,
            error: AppError::Data(
                crate::error::FailureReport::new(format!(
                    "{} of {} downloads failed",
                    stats.failed, stats.total
                ))
                .with_details(failure_details),
            ),
        });
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppError;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    // — run_downloads_inner —

    /// A failed download, for the tests.
    fn failed(url: &str, error: AppError) -> FailedDownload {
        FailedDownload {
            url: url.to_owned(),
            error,
        }
    }

    #[tokio::test]
    async fn run_downloads_empty_tasks_returns_ok() {
        let client = reqwest::Client::new();
        let tasks = vec![];
        let download_fn = |_: reqwest::Client, _: DownloadTask| async { unreachable!() };
        let stats = run_downloads_inner(&client, tasks, 16, download_fn)
            .await
            .unwrap();
        assert_eq!(stats.total, 0);
        assert_eq!(stats.ok, 0);
    }

    #[tokio::test]
    async fn run_downloads_all_success() {
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b")];
        let call_count = Arc::new(AtomicUsize::new(0));
        let cnt = call_count.clone();
        let download_fn = move |_: reqwest::Client, _: DownloadTask| {
            let cnt = cnt.clone();
            async move {
                cnt.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        };
        let stats = run_downloads_inner(&client, tasks, 16, download_fn)
            .await
            .unwrap();
        assert_eq!(call_count.load(Ordering::Relaxed), 2);
        assert_eq!(stats.ok, 2);
        assert_eq!(stats.not_found, 0);
        assert_eq!(stats.failed, 0);
    }

    #[tokio::test]
    async fn run_downloads_not_found_tolerated() {
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b")];
        let download_fn = |_: reqwest::Client, t: DownloadTask| async move {
            if t.pair == "a" {
                Err(failed(
                    "http://x",
                    AppError::NotFound {
                        url: "http://x".into(),
                    },
                ))
            } else {
                Ok(())
            }
        };
        let stats = run_downloads_inner(&client, tasks, 16, download_fn)
            .await
            .unwrap();
        assert_eq!(stats.ok, 1);
        assert_eq!(stats.not_found, 1);
        assert_eq!(stats.failed, 0);
    }

    #[tokio::test]
    async fn run_downloads_not_found_not_counted_as_failure() {
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b"), dummy_task("c")];
        let download_fn = |_: reqwest::Client, t: DownloadTask| async move {
            match t.pair.as_str() {
                "a" => Err(failed(
                    "http://x",
                    AppError::NotFound {
                        url: "http://x".into(),
                    },
                )),
                "b" => Err(failed(
                    "x",
                    AppError::DownloadFailed {
                        url: "x".into(),
                        reason: "bad".into(),
                    },
                )),
                _ => Ok(()),
            }
        };
        let err = run_downloads_inner(&client, tasks, 16, download_fn)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("1 of 3 downloads failed"));
    }

    #[tokio::test]
    async fn run_downloads_one_fail_returns_err() {
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b")];
        let download_fn = |_: reqwest::Client, t: DownloadTask| async move {
            if t.pair == "b" {
                Err(failed(
                    "x",
                    AppError::DownloadFailed {
                        url: "x".into(),
                        reason: "bad".into(),
                    },
                ))
            } else {
                Ok(())
            }
        };
        let err = run_downloads_inner(&client, tasks, 16, download_fn)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("1 of 2 downloads failed"));
    }

    #[tokio::test]
    async fn the_failure_summary_names_the_files_and_the_reasons() {
        // Regression guard: the summary used to say only "1 of 2 downloads
        // failed", leaving the reader to grep the log for the reason.
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b")];
        let download_fn = |_: reqwest::Client, t: DownloadTask| async move {
            if t.pair == "b" {
                Err(failed(
                    "http://x/b.zip",
                    AppError::DownloadFailed {
                        url: "http://x/b.zip".into(),
                        reason: "HTTP 500".into(),
                    },
                ))
            } else {
                Ok(())
            }
        };
        let err = run_downloads_inner(&client, tasks, 16, download_fn)
            .await
            .unwrap_err();
        let rendered = err.to_string();
        assert!(
            rendered.contains("1 of 2 downloads failed"),
            "got {rendered}"
        );
        assert!(rendered.contains("http://x/b.zip"), "got {rendered}");
        assert!(rendered.contains("HTTP 500"), "got {rendered}");
    }

    #[tokio::test]
    async fn concurrency_one_runs_sequentially() {
        let client = reqwest::Client::new();
        let tasks: Vec<_> = (0..5).map(|i| dummy_task(&format!("s{i}"))).collect();
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let stats = {
            let active = active.clone();
            let max_active = max_active.clone();
            let download_fn = move |_: reqwest::Client, _: DownloadTask| {
                let active = active.clone();
                let max_active = max_active.clone();
                async move {
                    let cur = active.fetch_add(1, Ordering::Relaxed) + 1;
                    max_active.fetch_max(cur, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    active.fetch_sub(1, Ordering::Relaxed);
                    Ok(())
                }
            };
            run_downloads_inner(&client, tasks, 1, download_fn)
                .await
                .unwrap()
        };
        assert_eq!(stats.ok, 5);
        assert_eq!(max_active.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn concurrency_limit_is_bounded_but_not_serialized() {
        let client = reqwest::Client::new();
        let tasks: Vec<_> = (0..16).map(|i| dummy_task(&format!("s{i}"))).collect();
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let stats = {
            let active = active.clone();
            let max_active = max_active.clone();
            let download_fn = move |_: reqwest::Client, _: DownloadTask| {
                let active = active.clone();
                let max_active = max_active.clone();
                async move {
                    let cur = active.fetch_add(1, Ordering::Relaxed) + 1;
                    max_active.fetch_max(cur, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    active.fetch_sub(1, Ordering::Relaxed);
                    Ok(())
                }
            };
            run_downloads_inner(&client, tasks, 4, download_fn)
                .await
                .unwrap()
        };
        assert_eq!(stats.ok, 16);
        assert!(
            max_active.load(Ordering::Relaxed) <= 4,
            "buffer_unordered must never exceed the concurrency limit"
        );
    }

    #[tokio::test]
    async fn zero_concurrency_is_clamped_to_one() {
        let client = reqwest::Client::new();
        let tasks: Vec<_> = (0..3).map(|i| dummy_task(&format!("s{i}"))).collect();
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        run_downloads_inner(&client, tasks, 0, {
            let active = active.clone();
            let max_active = max_active.clone();
            move |_: reqwest::Client, _: DownloadTask| {
                let active = active.clone();
                let max_active = max_active.clone();
                async move {
                    let cur = active.fetch_add(1, Ordering::Relaxed) + 1;
                    max_active.fetch_max(cur, Ordering::Relaxed);
                    active.fetch_sub(1, Ordering::Relaxed);
                    Ok(())
                }
            }
        })
        .await
        .unwrap();
        // `concurrency.max(1)` keeps `buffer_unordered` from panicking.
        assert_eq!(max_active.load(Ordering::Relaxed), 1);
    }

    // — DownloadStats —

    #[test]
    fn stats_done_sums_all_outcome_buckets() {
        let stats = DownloadStats {
            total: 10,
            ok: 3,
            not_found: 4,
            failed: 2,
            cancelled: 1,
        };
        assert_eq!(stats.done(), 10);
        assert_eq!(DownloadStats::default().done(), 0);
    }

    #[test]
    fn stats_absorb_sums_all_fields() {
        let mut a = DownloadStats {
            total: 10,
            ok: 8,
            not_found: 1,
            failed: 1,
            cancelled: 0,
        };
        a.absorb(DownloadStats {
            total: 5,
            ok: 5,
            not_found: 0,
            failed: 0,
            cancelled: 1,
        });
        assert_eq!(a.total, 15);
        assert_eq!(a.ok, 13);
        assert_eq!(a.not_found, 1);
        assert_eq!(a.failed, 1);
        assert_eq!(a.cancelled, 1);
    }

    #[test]
    fn stats_not_found_pct() {
        let stats = DownloadStats {
            total: 20,
            ok: 15,
            not_found: 5,
            failed: 0,
            cancelled: 0,
        };
        assert!((stats.not_found_pct() - 25.0).abs() < f64::EPSILON);
        assert_eq!(DownloadStats::default().not_found_pct(), 0.0);
    }

    #[test]
    fn stats_not_found_pct_uses_terminal_outcomes() {
        let stats = DownloadStats {
            total: 20,
            ok: 4,
            not_found: 5,
            failed: 1,
            cancelled: 0,
        };
        // `done` is 10, not `total` (20): five not-found out of ten finished.
        assert!((stats.not_found_pct() - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn stats_not_found_pct_is_zero_before_any_outcome() {
        let stats = DownloadStats {
            total: 20,
            ..DownloadStats::default()
        };
        assert_eq!(stats.not_found_pct(), 0.0);
    }

    // — DownloadFailure —

    #[test]
    fn download_failure_renders_the_underlying_error() {
        let failure = DownloadFailure {
            stats: DownloadStats {
                total: 3,
                failed: 1,
                ..DownloadStats::default()
            },
            error: AppError::DownloadFailed {
                url: "http://x/a.zip".into(),
                reason: "HTTP 500".into(),
            },
        };
        assert_eq!(
            failure.to_string(),
            "download failed for http://x/a.zip: HTTP 500"
        );
        // The wrapped error stays reachable through the source chain.
        assert!(std::error::Error::source(&failure).is_some());
    }

    // — helpers —

    fn dummy_task(pair: &str) -> DownloadTask {
        DownloadTask {
            url: format!("http://example.com/{pair}"),
            dest: std::path::PathBuf::from("/tmp"),
            pair: pair.into(),
            job: crate::dataset::DownloadJob {
                kind: crate::dataset::DatasetKind::Klines,
                interval: Some(crate::dataset::Interval::M1),
                period: crate::dataset::Period::Daily,
            },
        }
    }
}
