#[path = "02_build_download_tasks.rs"]
pub mod build_download_tasks;
#[path = "01_build_urls.rs"]
pub mod build_urls;
#[path = "03_download/mod.rs"]
pub mod download;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use futures::stream::{StreamExt, iter as stream_iter};

use crate::AppError;
use crate::config::{AppConfig, Timeframe};

/// Run the full batch download pipeline.
///
/// 1. Load config
/// 2. Scan existing local files (once)
/// 3. Iterate symbol (serial) → timeframe (serial), downloading missing files
///
/// # Errors
///
/// Returns [`AppError`] when the config is invalid or when one or more
/// symbol/timeframe pairs fail (the summary lists up to 20 failures). The
/// batch aborts early after a run of consecutive pair failures (likely a
/// systemic outage) instead of retrying every remaining pair.
pub async fn run(config_path: &Path) -> Result<(), AppError> {
    let cfg = AppConfig::load(config_path)?;
    let cache = crate::utils::cache::Cache::new(&cfg.output_dir);

    let client = reqwest::Client::builder()
        .user_agent("binance-data-downloader/0.1")
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(256)
        .pool_idle_timeout(Duration::from_secs(30))
        .build()?;

    let local = crate::utils::scan_local_files::scan_local_files(&cfg.output_dir).await?;

    let concurrency = cfg.concurrency;
    // The archive partitions days by UTC midnight, so "yesterday" must be UTC.
    let end = cfg.resolved_end(chrono::Utc::now().date_naive());

    tracing::info!(
        symbols = cfg.symbol.len(),
        timeframes = cfg.timeframe.len(),
        total = cfg.symbol.len() * cfg.timeframe.len(),
        concurrency,
        "starting batch download",
    );

    if cfg.start.is_none() {
        tracing::info!(
            count = cfg.symbol.len(),
            "first start-date per symbol is resolved by an individual request to the \
             Binance API and cached for 30 days; this can take a while on a fresh run",
        );
    }

    let mut failed_start_dates = 0;
    let mut degraded_start_dates = 0;
    let mut failed_pairs = 0;
    let mut errors = Vec::new();
    let mut totals = download::DownloadStats::default();
    let mut tracker = ConsecutiveFailureTracker::new(MAX_CONSECUTIVE_PAIR_FAILURES);
    let mut aborted_early = false;

    // Fetch one start-date per symbol up front, 8 in flight at most.
    let sym_starts: HashMap<String, chrono::NaiveDate> = {
        let cache = &cache;
        let mut map = HashMap::new();
        let mut pending = stream_iter(cfg.symbol.iter().map(|sym| {
            let c = client.clone();
            let s = sym.clone();
            async move {
                (
                    s.clone(),
                    crate::utils::get_start_date::get_first_kline_date(
                        &c,
                        cache,
                        &s,
                        Timeframe::M1.as_str(),
                    )
                    .await,
                )
            }
        }))
        .buffer_unordered(8);
        while let Some((sym, res)) = pending.next().await {
            match res {
                Ok(d) => {
                    map.insert(sym, d);
                }
                Err(e) => {
                    if let Some(fallback) = start_fallback(&e, cfg.start) {
                        degraded_start_dates += 1;
                        tracing::warn!(
                            symbol = %sym,
                            start = %fallback,
                            "start-date fetch failed ({e}), falling back to config start",
                        );
                        map.insert(sym, fallback);
                    } else {
                        failed_start_dates += 1;
                        tracing::error!(symbol = %sym, "start-date fetch failed: {e}");
                        if errors.len() < MAX_BATCH_ERRORS {
                            errors.push(format!("{sym}/*: {e}"));
                        }
                    }
                }
            }
        }
        map
    };
    tracing::info!(
        resolved = sym_starts.len(),
        degraded = degraded_start_dates,
        total = cfg.symbol.len(),
        "start-dates resolved",
    );

    'pairs: for symbol in &cfg.symbol {
        let Some(sym_start) = sym_starts.get(symbol) else {
            continue;
        };
        for timeframe in &cfg.timeframe {
            tracing::info!(symbol = %symbol, timeframe = %timeframe, "running pair");
            let ctx = PairCtx {
                sym_start: *sym_start,
                end,
                cfg: &cfg,
                local: &local,
            };
            match run_one(&client, symbol, *timeframe, &ctx).await {
                Ok(stats) => {
                    tracker.record_success();
                    totals.absorb(stats);
                }
                Err(e) => {
                    failed_pairs += 1;
                    tracing::error!(symbol = %symbol, timeframe = %timeframe, "failed: {e}");
                    if errors.len() < MAX_BATCH_ERRORS {
                        errors.push(format!("{symbol}/{timeframe}: {e}"));
                    }
                    if tracker.record_failure() {
                        aborted_early = true;
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

    if totals.not_found > 0 {
        tracing::warn!(
            not_found = totals.not_found,
            total = totals.total,
            pct = totals.not_found_pct(),
            "files not found on the server (404, expected for edge dates; investigate if unexpected)",
        );
    }

    let total_failures = failed_start_dates + failed_pairs;
    if total_failures == 0 {
        tracing::info!("batch download complete");
        Ok(())
    } else {
        tracing::error!(
            symbols = failed_start_dates,
            pairs = failed_pairs,
            "batch download completed with errors",
        );
        let mut causes = Vec::new();
        if failed_start_dates > 0 {
            causes.push(format!(
                "{failed_start_dates} symbol(s) failed start-date resolution"
            ));
        }
        if failed_pairs > 0 {
            causes.push(format!("{failed_pairs} pair(s) failed"));
        }
        let truncated = if errors.len() < total_failures {
            "\n(additional failures not listed)"
        } else {
            ""
        };
        let aborted = if aborted_early {
            format!(
                "\n(aborted early after {MAX_CONSECUTIVE_PAIR_FAILURES} consecutive pair failures; rerun later to resume)"
            )
        } else {
            String::new()
        };
        Err(AppError::Data(format!(
            "{}: {}{}{}",
            causes.join("; "),
            errors.join("; "),
            truncated,
            aborted,
        )))
    }
}

/// Maximum number of pair-level error details retained in the batch summary.
const MAX_BATCH_ERRORS: usize = 20;

/// Abort the batch after this many consecutive pair failures.
///
/// A systemic outage (network down, rate limiting, server errors) would
/// otherwise burn the full retry window for every remaining pair, turning a
/// single broken run into hours of pointless waiting. Rerunning later resumes
/// from the local files that were already downloaded.
const MAX_CONSECUTIVE_PAIR_FAILURES: usize = 5;

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

/// Per-pair context: resolved dates plus shared borrows.
struct PairCtx<'a> {
    sym_start: chrono::NaiveDate,
    end: chrono::NaiveDate,
    cfg: &'a AppConfig,
    local: &'a HashSet<u128>,
}

/// Download one symbol/timeframe pair.
///
/// Returns the per-pair download stats (all zero when the pair was skipped);
/// failure fails this pair only, the batch keeps going.
async fn run_one(
    client: &reqwest::Client,
    symbol: &str,
    timeframe: Timeframe,
    ctx: &PairCtx<'_>,
) -> Result<download::DownloadStats, AppError> {
    if let Some(cfg_start) = ctx.cfg.start
        && cfg_start > ctx.end
    {
        tracing::warn!(
            symbol = %symbol,
            timeframe = %timeframe,
            start = %cfg_start,
            end = %ctx.end,
            "config start after end date, skipping",
        );
        return Ok(download::DownloadStats::default());
    }

    let start = resolve_start(ctx.cfg.start, ctx.sym_start);

    if start > ctx.end {
        tracing::warn!(
            symbol = %symbol,
            timeframe = %timeframe,
            start = %start,
            end = %ctx.end,
            "start date after end date, skipping",
        );
        return Ok(download::DownloadStats::default());
    }

    tracing::info!(
        symbol = %symbol,
        timeframe = %timeframe,
        start = %start,
        end = %ctx.end,
        "processing symbol-timeframe pair",
    );

    let tasks = build_download_tasks::build_download_tasks(
        symbol,
        timeframe,
        start,
        ctx.end,
        ctx.local,
        &ctx.cfg.output_dir,
    );

    if tasks.is_empty() {
        tracing::debug!(
            symbol = %symbol,
            timeframe = %timeframe,
            start = %start,
            end = %ctx.end,
            "no files to download, all cached or out of range",
        );
        return Ok(download::DownloadStats::default());
    }

    tracing::debug!(
        files = tasks.len(),
        start = %start,
        end = %ctx.end,
        "downloading all",
    );
    let stats = download::run_downloads(client, tasks, ctx.cfg.concurrency).await?;
    tracing::info!(
        symbol = %symbol,
        timeframe = %timeframe,
        ok = stats.ok,
        not_found = stats.not_found,
        failed = stats.failed,
        total = stats.total,
        "pair download complete",
    );

    Ok(stats)
}

fn resolve_start(
    start: Option<chrono::NaiveDate>,
    sym_start: chrono::NaiveDate,
) -> chrono::NaiveDate {
    start.map_or(sym_start, |cs| cs.max(sym_start))
}

/// Decide whether a failed start-date fetch may degrade to `cfg.start`.
///
/// With an explicit `config.start`, a transient fapi failure is recoverable:
/// the run proceeds from that date (dates before the symbol's actual listing
/// simply 404, which is tolerated and reported). An unknown-symbol error
/// stays fatal, so typos cannot masquerade as success.
fn start_fallback(
    err: &AppError,
    cfg_start: Option<chrono::NaiveDate>,
) -> Option<chrono::NaiveDate> {
    match (cfg_start, err) {
        (Some(start), e) if !matches!(e, AppError::UnknownSymbol { .. }) => Some(start),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{ConsecutiveFailureTracker, resolve_start, start_fallback};
    use crate::AppError;

    #[test]
    fn start_fallback_used_when_cfg_start_is_set() {
        let start = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let err = AppError::Api("network down".into());
        assert_eq!(start_fallback(&err, Some(start)), Some(start));
    }

    #[test]
    fn start_fallback_rejected_for_unknown_symbol() {
        let start = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let err = AppError::UnknownSymbol {
            symbol: "NOPEUSDT".into(),
        };
        assert_eq!(start_fallback(&err, Some(start)), None);
    }

    #[test]
    fn start_fallback_needs_cfg_start() {
        let err = AppError::Api("network down".into());
        assert_eq!(start_fallback(&err, None), None);
    }

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

    #[test]
    fn resolve_start_date_prefers_cfg() {
        let sym = chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let cfg = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(resolve_start(Some(cfg), sym), cfg);
    }

    #[test]
    fn resolve_start_date_prefers_sym_when_later() {
        let sym = chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let cfg = Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        assert_eq!(resolve_start(cfg, sym), sym);
    }

    #[test]
    fn resolve_start_date_no_cfg() {
        let sym = chrono::NaiveDate::from_ymd_opt(2023, 6, 15).unwrap();
        let cfg: Option<chrono::NaiveDate> = None;
        assert_eq!(resolve_start(cfg, sym), sym);
    }
}
