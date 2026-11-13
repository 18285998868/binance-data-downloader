//! Step 4: run every symbol/job pair.
//!
//! The loop is serial by design: one pair downloads a bounded number of files
//! concurrently, then the next pair starts. This keeps the 404-threshold
//! sample and the circuit breaker meaningful, and avoids thousands of
//! independent file downloads competing for the same descriptors.

mod fetch;
mod pair;
mod tasks;

use crate::config::AppConfig;
use crate::prepare::PreparedRun;
use crate::start_dates::StartDateResolution;

use self::fetch::DownloadFailure;
pub(crate) use self::fetch::DownloadStats;

/// Maximum number of pair-level error details retained in the batch summary.
const MAX_BATCH_ERRORS: usize = 20;

/// Abort the batch after this many consecutive pair failures.
///
/// A systemic outage (network down, rate limiting, server errors) would
/// otherwise burn the full retry window for every remaining pair, turning a
/// single broken run into hours of pointless waiting. Rerunning later resumes
/// from the local files that were already downloaded.
pub(crate) const MAX_CONSECUTIVE_PAIR_FAILURES: usize = 5;

/// Totals and diagnostics collected while running the symbol/job loop.
pub(crate) struct PairBatchSummary {
    /// Aggregated download statistics.
    pub(crate) totals: DownloadStats,
    /// Number of pairs that ended in failure.
    pub(crate) failed_pairs: usize,
    /// Error details, capped at [`MAX_BATCH_ERRORS`].
    pub(crate) errors: Vec<String>,
    /// Whether the circuit breaker stopped the batch.
    pub(crate) aborted_early: bool,
}

/// Run every symbol/job pair, absorbing completed work even when a pair
/// fails or the run is cancelled.
pub(crate) async fn run(
    prepared: &PreparedRun,
    cfg: &AppConfig,
    resolution: &StartDateResolution,
    verify: bool,
) -> PairBatchSummary {
    let mut summary = PairBatchSummary {
        totals: DownloadStats::default(),
        failed_pairs: 0,
        errors: Vec::new(),
        aborted_early: false,
    };
    let mut tracker = ConsecutiveFailureTracker::new(MAX_CONSECUTIVE_PAIR_FAILURES);

    'pairs: for symbol in &prepared.symbols {
        for job in &prepared.jobs {
            // The check lives here, at the pair level: a Ctrl-C must stop the
            // *next* pair, not merely the next symbol. A symbol may have
            // dozens of jobs, each of them thousands of files.
            if prepared.cancel.is_cancelled() {
                summary.aborted_early = true;
                tracing::warn!("cancellation requested, stopping after current pair");
                break 'pairs;
            }
            let Some(sym_start) = resolution.get(symbol, *job) else {
                // The symbol could not be resolved at all: it is counted in
                // `resolution.failed`, and its pairs are skipped.
                continue;
            };
            tracing::info!(symbol = %symbol, job = %job, "running pair");
            let ctx = pair::PairCtx {
                sym_start: sym_start.date(),
                prepared,
                cfg,
            };
            match pair::run_one(&prepared.client, symbol, *job, &ctx, verify).await {
                Ok(stats) => {
                    // A pair that was cancelled is not a success: it must not
                    // reset the consecutive-failure breaker.
                    if stats.cancelled == 0 {
                        tracker.record_success();
                    }
                    summary.totals.absorb(stats);
                }
                Err(DownloadFailure { stats, error }) => {
                    // The stats gathered before the failure still belong to the
                    // batch: dropping them would silently shrink both the
                    // totals and the 404 ratio.
                    summary.totals.absorb(stats);
                    summary.failed_pairs += 1;
                    tracing::error!(symbol = %symbol, job = %job, error = ?error, "pair failed");
                    if summary.errors.len() < MAX_BATCH_ERRORS {
                        summary.errors.push(format!("{symbol}/{job}: {error}"));
                    }
                    if tracker.record_failure() {
                        summary.aborted_early = true;
                        tracing::error!(
                            consecutive = MAX_CONSECUTIVE_PAIR_FAILURES,
                            "aborting batch: too many consecutive pair failures (likely a systemic outage); rerun later to resume",
                        );
                        break 'pairs;
                    }
                }
            }
        }
    }

    summary
}

/// Counts consecutive pair failures to trip the batch circuit breaker.
struct ConsecutiveFailureTracker {
    limit: usize,
    consecutive: usize,
}

impl ConsecutiveFailureTracker {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            consecutive: 0,
        }
    }

    /// Record a failed pair; returns `true` when the limit is reached.
    fn record_failure(&mut self) -> bool {
        self.consecutive += 1;
        self.consecutive >= self.limit
    }

    /// Record a successful pair.
    fn record_success(&mut self) {
        self.consecutive = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::ConsecutiveFailureTracker;

    #[test]
    fn tracker_trips_after_limit_consecutive_failures() {
        let mut tracker = ConsecutiveFailureTracker::new(3);
        assert!(!tracker.record_failure());
        assert!(!tracker.record_failure());
        assert!(tracker.record_failure());
    }

    #[test]
    fn tracker_resets_on_success() {
        let mut tracker = ConsecutiveFailureTracker::new(3);
        assert!(!tracker.record_failure());
        assert!(!tracker.record_failure());
        tracker.record_success();
        assert!(!tracker.record_failure());
        assert!(!tracker.record_failure());
        assert!(tracker.record_failure());
    }
}
