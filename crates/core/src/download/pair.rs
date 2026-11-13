//! One symbol/job pair: resolve its window, build tasks, download them.

use crate::config::Config;
use crate::dataset::DownloadJob;
use crate::prepare::PreparedRun;

use super::fetch;
use super::tasks;
use super::{DownloadFailure, DownloadStats};

/// Per-pair context: the resolved start date plus shared borrows.
pub(super) struct PairCtx<'a> {
    /// First day this pair should download, already resolved from config and
    /// the symbol's listing/archive dates.
    pub(super) sym_start: chrono::NaiveDate,
    /// Resources shared by every pair in the batch.
    pub(super) prepared: &'a PreparedRun,
    /// Validated configuration.
    pub(super) cfg: &'a Config,
}

/// Download one symbol/job pair.
///
/// Returns the per-pair download stats (all zero when the pair was skipped).
/// A failure still reports what the pair managed to download, so the batch
/// totals stay accurate; only this pair is affected, the batch keeps going.
///
/// # Errors
///
/// Returns [`DownloadFailure`] when at least one file in the pair fails to
/// download.
#[tracing::instrument(
    skip(client, ctx),
    fields(market = %ctx.cfg.market(), symbol = %symbol, job = %job)
)]
pub(super) async fn run_one(
    client: &reqwest::Client,
    symbol: &str,
    job: DownloadJob,
    ctx: &PairCtx<'_>,
    verify: bool,
) -> Result<DownloadStats, DownloadFailure> {
    // Monthly jobs align down to the first day of the month and stop at the
    // last *complete* month; daily jobs use the resolved day range as-is.
    let raw_start = crate::start_dates::resolve_start(ctx.cfg.start(), ctx.sym_start);
    let start = job.period.start_of_period(raw_start);
    let end = job.period.last_complete_start(ctx.prepared.end);

    if start > end {
        tracing::warn!(
            symbol = %symbol,
            job = %job,
            start = %raw_start,
            end = %end,
            "start date after end date, skipping",
        );
        return Ok(DownloadStats::default());
    }

    tracing::info!(
        symbol = %symbol,
        job = %job,
        start = %start,
        end = %end,
        "processing symbol/job pair",
    );

    // `end` is already normalized for logging and the skip check above;
    // `Period::dates` normalizes its own end argument, so passing
    // `ctx.prepared.end` (the raw, inclusive end) avoids applying the monthly
    // rule twice.
    let tasks = tasks::build_download_tasks(
        &ctx.prepared.endpoints.archive_files,
        ctx.cfg.market(),
        symbol,
        job,
        start,
        ctx.prepared.end,
        &ctx.prepared.local,
        ctx.cfg.output_dir(),
    );

    if tasks.is_empty() {
        tracing::debug!(
            symbol = %symbol,
            job = %job,
            start = %start,
            end = %end,
            "no files to download, all cached or out of range",
        );
        return Ok(DownloadStats::default());
    }

    tracing::debug!(
        files = tasks.len(),
        start = %start,
        end = %end,
        "downloading all",
    );
    let result = fetch::run_downloads(
        client,
        tasks,
        ctx.prepared.concurrency,
        &ctx.prepared.cancel,
        verify,
    )
    .await;
    // Log the outcome either way: the stats explain how far the pair got even
    // when it ends in failure.
    let (stats, error) = match result {
        Ok(stats) => (stats, None),
        Err(DownloadFailure { stats, error }) => (stats, Some(error)),
    };
    tracing::info!(
        symbol = %symbol,
        job = %job,
        ok = stats.ok,
        not_found = stats.not_found,
        failed = stats.failed,
        cancelled = stats.cancelled,
        total = stats.total,
        "pair download complete",
    );

    match error {
        None => Ok(stats),
        Some(error) => Err(DownloadFailure { stats, error }),
    }
}
