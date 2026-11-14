//! Step 5: decide the run's outcome and build the failure report.

use crate::AppError;
use crate::config::Config;
use crate::download::{MAX_CONSECUTIVE_PAIR_FAILURES, PairBatchSummary};
use crate::error::FailureReport;
use crate::prepare::PreparedRun;
use crate::start_dates::StartDateResolution;

/// Turn the collected statistics into an exit status.
///
/// A run cancelled with Ctrl-C is not an error: files already on disk stay
/// valid and a re-run resumes from them. A configured not-found threshold does
/// turn excessive 404s into a failure, unless the run was cancelled before the
/// sample was complete.
///
/// # Errors
///
/// Returns [`AppError::Data`] when start-date resolution or one or more pairs
/// failed, or when the not-found share reached the configured threshold.
pub(crate) fn finish(
    prepared: &PreparedRun,
    cfg: &Config,
    resolution: &StartDateResolution,
    summary: &PairBatchSummary,
) -> Result<(), AppError> {
    let totals = summary.totals;
    let failed_pairs = summary.failed_pairs;
    let mut errors = resolution.errors.clone();
    errors.extend(summary.errors.iter().cloned());

    if totals.not_found > 0 {
        // 404s are tolerated by design and never change the exit code; they are
        // surfaced here so an unexpectedly high ratio is still visible.
        tracing::warn!(
            not_found = totals.not_found,
            total = totals.total,
            done = totals.done(),
            pct = totals.not_found_pct(),
            "files not found on the server (404, expected for edge dates; investigate if unexpected)",
        );
    }

    // A degraded (symbol, job) is NOT a failure: the run continued from
    // `config.start`. It is called out explicitly so partial coverage is never
    // mistaken for a clean result.
    let degraded: Vec<String> = resolution
        .degraded()
        .iter()
        .map(|(symbol, job, start)| format!("{symbol}/{job}@{start}"))
        .collect();
    if !degraded.is_empty() {
        tracing::warn!(
            count = degraded.len(),
            detail = degraded.join(", "),
            "start-date lookup failed for these symbol/job pairs; fell back to config \
             start — their earliest days may be missing",
        );
    }

    let total_failures = resolution.failed.len() + failed_pairs;
    // A configured not-found threshold turns "too many files were missing"
    // into a failure, so a partially-covered archive cannot pass unnoticed.
    // A pair cancelled midway has only a partial sample: applying the threshold
    // to it would turn Ctrl-C into a failure just because some early dates were
    // 404. Completed pairs still use their full sample.
    let partial_cancelled = prepared.cancel.is_cancelled() && totals.done() < totals.total;
    let not_found_pct = totals.not_found_pct();
    let too_many_not_found = !partial_cancelled
        && cfg
            .not_found_pct_threshold()
            .is_some_and(|threshold| not_found_pct >= threshold);
    if total_failures == 0 && !too_many_not_found {
        if prepared.cancel.is_cancelled() {
            tracing::warn!("run cancelled by user; rerun to resume where it stopped");
        }
        tracing::info!(
            ok = totals.ok,
            not_found = totals.not_found,
            failed = totals.failed,
            cancelled = totals.cancelled,
            "batch download complete",
        );
        Ok(())
    } else {
        tracing::error!(
            symbols = resolution.failed.len(),
            pairs = failed_pairs,
            "batch download completed with errors",
        );
        let mut causes = Vec::new();
        if !resolution.failed.is_empty() {
            causes.push(format!(
                "{} symbol(s) failed start-date resolution",
                resolution.failed.len()
            ));
        }
        if failed_pairs > 0 {
            causes.push(format!("{failed_pairs} pair(s) failed"));
        }
        if too_many_not_found {
            causes.push(format!(
                "not-found share {not_found_pct:.1}% reached the configured threshold of {:.1}%",
                cfg.not_found_pct_threshold().unwrap_or_default()
            ));
        }
        let listed_errors = errors.len();
        let mut report = FailureReport::new(causes.join("; ")).with_details(errors);
        if listed_errors < total_failures {
            report = report.with_note("(additional failures not listed)");
        }
        if summary.aborted_early {
            report = report.with_note(format!(
                "(aborted early after {MAX_CONSECUTIVE_PAIR_FAILURES} consecutive pair failures; rerun later to resume)"
            ));
        }
        Err(AppError::Data(report))
    }
}
