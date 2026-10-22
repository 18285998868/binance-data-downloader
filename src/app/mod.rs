//! Pipeline orchestration: config -> scan -> start dates -> per-pair downloads.

pub(crate) mod download;
pub(crate) mod start_dates;
pub(crate) mod tasks;
pub(crate) mod urls;

use std::borrow::Cow;
use std::path::Path;
use std::time::Duration;

use crate::AppError;
use crate::config::{AppConfig, Timeframe};

use crate::cancel::CancelToken;
use crate::lock::RunLock;
use crate::scan::LocalFileSet;

/// Base URLs the pipeline talks to.
///
/// Production always uses the real exchanges ([`Endpoints::live`]). Tests
/// redirect both at a single `wiremock` server via [`Endpoints::for_tests`],
/// which is what makes the orchestration in [`run_inner`] — start-date
/// resolution, degradation, the pair loop, cancellation and the exit code —
/// reachable from unit tests without adding any user-facing configuration.
#[derive(Debug, Clone)]
pub(crate) struct Endpoints {
    /// Base of the public data archive.
    pub(crate) archive_files: Cow<'static, str>,
    /// Base of the Binance USDⓈ-M futures API.
    pub(crate) exchange_api: Cow<'static, str>,
    /// Base of the archive bucket's S3 endpoint, used for its listing.
    pub(crate) archive_listing: Cow<'static, str>,
}

impl Endpoints {
    /// The real `data.binance.vision` and `fapi.binance.com`.
    pub(crate) fn live() -> Self {
        Self {
            archive_files: Cow::Borrowed(urls::ARCHIVE_BASE),
            exchange_api: Cow::Borrowed(crate::start_date::FAPI_BASE),
            archive_listing: Cow::Borrowed(crate::archive::S3_BASE),
        }
    }

    /// Both bases pointed at one mock server.
    #[cfg(test)]
    pub(crate) fn for_tests(base: String) -> Self {
        Self {
            archive_files: Cow::Owned(base.clone()),
            exchange_api: Cow::Owned(base.clone()),
            archive_listing: Cow::Owned(base),
        }
    }
}

/// Options that tune a single run.
#[derive(Debug, Clone, Copy, Default)]
pub struct RunOptions {
    /// Validate every downloaded archive as a readable zip.
    ///
    /// Off by default: the magic-byte and `Content-Length` checks already
    /// reject truncated bodies and HTML error pages, and reading the central
    /// directory costs an extra pass over every file.
    pub verify: bool,
}

impl RunOptions {
    /// Options with every optional behaviour off.
    #[must_use]
    pub const fn new() -> Self {
        Self { verify: false }
    }

    /// Turn the zip validation on or off.
    #[must_use]
    pub const fn with_verify(mut self, verify: bool) -> Self {
        self.verify = verify;
        self
    }
}

/// Run the full batch download pipeline.
///
/// 1. Load config
/// 2. Scan existing local files (once)
/// 3. Resolve one start date per (symbol, timeframe)
/// 4. Iterate symbol (serial) → timeframe (serial), downloading missing files
///
/// # Errors
///
/// Returns [`AppError`] when the config is invalid, another run already holds
/// the output directory, or one or more symbol/timeframe pairs fail (the
/// summary lists up to 20 failures). The batch aborts early after a run of
/// consecutive pair failures (likely a systemic outage) instead of retrying
/// every remaining pair.
///
/// A run cancelled with Ctrl-C is **not** an error: files already on disk stay
/// valid and a re-run resumes from them.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// # async fn demo() -> Result<(), bdd::AppError> {
/// // Downloads every symbol/timeframe pair declared in `config.toml` into the
/// // configured output directory, skipping the files already on disk.
/// bdd::run(Path::new("config.toml")).await?;
/// # Ok(())
/// # }
/// ```
pub async fn run(config_path: &Path) -> Result<(), AppError> {
    run_with_options(config_path, &RunOptions::default()).await
}

/// Run the pipeline with explicit [`RunOptions`].
///
/// # Errors
///
/// Same as [`run`].
pub async fn run_with_options(config_path: &Path, options: &RunOptions) -> Result<(), AppError> {
    // Graceful shutdown: one Ctrl-C flag, observed both between pairs and by
    // every in-flight download, so a long batch stops promptly.
    let cancel = CancelToken::new();
    let cancel_task = cancel.clone();
    let ctrl_c = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::warn!("cancellation requested; stopping in-flight downloads");
            cancel_task.cancel();
        }
    });
    let result = run_inner(config_path, Endpoints::live(), cancel, options).await;
    // Do not leave a signal-handling task behind when the library is used by a
    // long-lived host process.
    ctrl_c.abort();
    result
}

/// Test-only entry point: the same pipeline, redirected at a mock exchange.
///
/// Compiled only under `cfg(test)`; production has no way to reach it and no
/// configuration exposes the endpoints.
#[cfg(test)]
pub(crate) async fn run_with_endpoints(
    config_path: &Path,
    endpoints: Endpoints,
    cancel: CancelToken,
) -> Result<(), AppError> {
    run_inner(config_path, endpoints, cancel, &RunOptions::default()).await
}

/// Build the shared HTTP client.
///
/// Timeouts matter for the retry window: `read_timeout` only bounds one read,
/// while `timeout` bounds a whole request, including a server that dribbles a
/// byte every 59 seconds. The retry budget is separate (see
/// [`crate::backoff::RETRY_WINDOW`]).
fn build_client() -> Result<reqwest::Client, AppError> {
    let client = reqwest::Client::builder()
        .user_agent("binance-data-downloader/0.1")
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(60))
        // A whole-request timeout, on top of the per-read one: a server that
        // trickles a byte every 59 seconds never trips the read timeout and
        // would hold a concurrency slot forever. Five minutes is generous for
        // any legitimate archive.
        .timeout(REQUEST_TIMEOUT)
        .pool_max_idle_per_host(256)
        .pool_idle_timeout(Duration::from_secs(30))
        .build()?;
    Ok(client)
}

/// The pipeline body, with its two external dependencies passed in.
async fn run_inner(
    config_path: &Path,
    endpoints: Endpoints,
    cancel: CancelToken,
    options: &RunOptions,
) -> Result<(), AppError> {
    let cfg = AppConfig::load(config_path).await?;

    // Concurrent runs over one output directory are not supported: they would
    // contend on the same `redb` cache file and race on destination files. The
    // guard turns that into an immediate, explicit error.
    let _lock = RunLock::acquire(&cfg.output_dir).await?;

    let cache = crate::cache::Cache::new(&cfg.output_dir);

    let client = build_client()?;

    let local = crate::scan::scan_local_files(&cfg.output_dir).await?;

    // The configured concurrency is a request; the descriptor limit has the
    // final say. Every in-flight download holds a socket *and* a temporary
    // file, so exceeding the limit turns into `EMFILE` and a wave of failed
    // downloads rather than a faster run.
    let concurrency = crate::limits::effective_concurrency(cfg.concurrency);
    if concurrency < cfg.concurrency {
        tracing::warn!(
            requested = cfg.concurrency,
            effective = concurrency,
            "concurrency reduced to fit the process descriptor limit",
        );
    }

    // The archive partitions days by UTC midnight, so "yesterday" must be UTC.
    let end = cfg.resolved_end(chrono::Utc::now().date_naive());

    let symbols = cfg.normalized_symbols();
    tracing::info!(
        symbols = symbols.len(),
        timeframes = cfg.timeframe.len(),
        total = symbols.len() * cfg.timeframe.len(),
        concurrency,
        local_files = local.len(),
        "starting batch download",
    );

    if cfg.start.is_none() {
        tracing::info!(
            count = symbols.len(),
            "the first start-date of every symbol is resolved up front -- one request to \
             the Binance API and one bucket listing per timeframe -- and cached for 30 days; \
             this can take a while on a fresh run",
        );
    }

    let mut errors = Vec::new();

    // One start date per (symbol, timeframe), resolved up front.
    let mut resolution = start_dates::resolve_start_dates(
        &client,
        &cache,
        &cfg,
        &symbols,
        &cfg.timeframe,
        &endpoints,
        &cancel,
    )
    .await;
    let degraded = resolution.degraded();
    let degraded_count = degraded.len();
    let resolved_count: usize = resolution
        .dates
        .values()
        .map(std::collections::HashMap::len)
        .sum();
    errors.extend(std::mem::take(&mut resolution.errors));
    tracing::info!(
        resolved = resolved_count.saturating_sub(degraded_count),
        failed = resolution.failed.len(),
        degraded = degraded_count,
        total = symbols.len() * cfg.timeframe.len(),
        "start-dates resolved",
    );

    let pair_summary = run_pairs(
        &client,
        &cfg,
        &symbols,
        &resolution,
        &local,
        concurrency,
        end,
        &endpoints,
        &cancel,
        options,
    )
    .await;
    let totals = pair_summary.totals;
    let failed_pairs = pair_summary.failed_pairs;
    errors.extend(pair_summary.errors);
    let aborted_early = pair_summary.aborted_early;

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

    // A degraded (symbol, timeframe) is NOT a failure: the run continued from
    // `config.start`. It is called out explicitly so partial coverage is never
    // mistaken for a clean result.
    let degraded: Vec<String> = degraded
        .iter()
        .map(|(symbol, timeframe, start)| format!("{symbol}/{timeframe}@{start}"))
        .collect();
    if !degraded.is_empty() {
        tracing::warn!(
            count = degraded.len(),
            detail = degraded.join(", "),
            "start-date lookup failed for these symbol/timeframe pairs; fell back to config \
             start — their earliest days may be missing",
        );
    }

    let total_failures = resolution.failed.len() + failed_pairs;
    // A configured not-found threshold turns "too many files were missing"
    // into a failure, so a partially-covered archive cannot pass unnoticed.
    // A pair cancelled midway has only a partial sample: applying the threshold
    // to it would turn Ctrl-C into a failure just because some early dates were
    // 404. Completed pairs still use their full sample.
    let partial_cancelled = cancel.is_cancelled() && totals.done() < totals.total;
    let not_found_pct = totals.not_found_pct();
    let too_many_not_found = !partial_cancelled
        && cfg
            .not_found_pct_threshold
            .is_some_and(|threshold| not_found_pct >= threshold);
    if total_failures == 0 && !too_many_not_found {
        if cancel.is_cancelled() {
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
                cfg.not_found_pct_threshold.unwrap_or_default()
            ));
        }
        let listed_errors = errors.len();
        let mut report = crate::error::FailureReport::new(causes.join("; ")).with_details(errors);
        if listed_errors < total_failures {
            report = report.with_note("(additional failures not listed)");
        }
        if aborted_early {
            report = report.with_note(format!(
                "(aborted early after {MAX_CONSECUTIVE_PAIR_FAILURES} consecutive pair failures; rerun later to resume)"
            ));
        }
        Err(AppError::Data(report))
    }
}

/// Maximum number of pair-level error details retained in the batch summary.
const MAX_BATCH_ERRORS: usize = 20;

/// Whole-request timeout, on top of the per-read one.
///
/// A server that trickles a byte every 59 seconds never trips the read
/// timeout; without a total timeout it would hold a concurrency slot forever.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

/// Totals and diagnostics collected while running the symbol/timeframe loop.
struct PairBatchSummary {
    totals: download::DownloadStats,
    failed_pairs: usize,
    errors: Vec<String>,
    aborted_early: bool,
}

/// Run every symbol/timeframe pair, absorbing completed work even when a pair
/// fails or the run is cancelled.
#[allow(clippy::too_many_arguments)]
async fn run_pairs(
    client: &reqwest::Client,
    cfg: &AppConfig,
    symbols: &[String],
    resolution: &start_dates::StartDateResolution,
    local: &LocalFileSet,
    concurrency: usize,
    end: chrono::NaiveDate,
    endpoints: &Endpoints,
    cancel: &CancelToken,
    options: &RunOptions,
) -> PairBatchSummary {
    let mut summary = PairBatchSummary {
        totals: download::DownloadStats::default(),
        failed_pairs: 0,
        errors: Vec::new(),
        aborted_early: false,
    };
    let mut tracker = ConsecutiveFailureTracker::new(MAX_CONSECUTIVE_PAIR_FAILURES);

    'pairs: for symbol in symbols {
        for timeframe in &cfg.timeframe {
            // The check lives here, at the pair level: a Ctrl-C must stop the
            // *next* pair, not merely the next symbol. A symbol may have a
            // dozen timeframes, each of them thousands of files.
            if cancel.is_cancelled() {
                summary.aborted_early = true;
                tracing::warn!("cancellation requested, stopping after current pair");
                break 'pairs;
            }
            let Some(sym_start) = resolution.get(symbol, *timeframe) else {
                // The symbol could not be resolved at all: it is counted in
                // `resolution.failed`, and its pairs are skipped.
                continue;
            };
            tracing::info!(symbol = %symbol, timeframe = %timeframe, "running pair");
            let ctx = PairCtx {
                sym_start: sym_start.date(),
                end,
                cfg,
                local,
                endpoints: endpoints.clone(),
                concurrency,
            };
            match run_one(client, symbol, *timeframe, &ctx, cancel, options).await {
                Ok(stats) => {
                    // A pair that was cancelled is not a success: it must not
                    // reset the consecutive-failure breaker.
                    if stats.cancelled == 0 {
                        tracker.record_success();
                    }
                    summary.totals.absorb(stats);
                }
                Err(download::DownloadFailure { stats, error }) => {
                    // The stats gathered before the failure still belong to the
                    // batch: dropping them would silently shrink both the
                    // totals and the 404 ratio.
                    summary.totals.absorb(stats);
                    summary.failed_pairs += 1;
                    tracing::error!(symbol = %symbol, timeframe = %timeframe, error = ?error, "pair failed");
                    if summary.errors.len() < MAX_BATCH_ERRORS {
                        summary
                            .errors
                            .push(format!("{symbol}/{timeframe}: {error}"));
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
    local: &'a LocalFileSet,
    endpoints: Endpoints,
    /// The effective concurrency, already capped by the descriptor limit.
    concurrency: usize,
}

/// Download one symbol/timeframe pair.
///
/// Returns the per-pair download stats (all zero when the pair was skipped).
/// A failure still reports what the pair managed to download, so the batch
/// totals stay accurate; only this pair is affected, the batch keeps going.
///
/// # Errors
///
/// Returns [`download::DownloadFailure`] when at least one file in the pair
/// fails to download.
#[tracing::instrument(skip(client, ctx, cancel, options), fields(symbol = %symbol, timeframe = %timeframe))]
async fn run_one(
    client: &reqwest::Client,
    symbol: &str,
    timeframe: Timeframe,
    ctx: &PairCtx<'_>,
    cancel: &CancelToken,
    options: &RunOptions,
) -> Result<download::DownloadStats, download::DownloadFailure> {
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

    let start = start_dates::resolve_start(ctx.cfg.start, ctx.sym_start);

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

    let tasks = tasks::build_download_tasks(
        &ctx.endpoints.archive_files,
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
    let result =
        download::run_downloads(client, tasks, ctx.concurrency, cancel, options.verify).await;
    // Log the outcome either way: the stats explain how far the pair got even
    // when it ends in failure.
    let (stats, error) = match result {
        Ok(stats) => (stats, None),
        Err(download::DownloadFailure { stats, error }) => (stats, Some(error)),
    };
    tracing::info!(
        symbol = %symbol,
        timeframe = %timeframe,
        ok = stats.ok,
        not_found = stats.not_found,
        failed = stats.failed,
        cancelled = stats.cancelled,
        total = stats.total,
        "pair download complete",
    );

    match error {
        None => Ok(stats),
        Some(error) => Err(download::DownloadFailure { stats, error }),
    }
}

#[cfg(test)]
mod tests {
    use super::start_dates::{SymbolStart, resolve_start_dates};
    use super::{ConsecutiveFailureTracker, Endpoints, run_with_endpoints};
    use crate::cancel::CancelToken;
    use crate::config::AppConfig;
    use crate::{AppError, lock::RunLock};
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{method, path, path_regex, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // — mock exchange —

    /// Minimal body accepted by the zip-magic check.
    const ZIP: &[u8] = b"PK\x03\x04fake";
    /// fapi klines response whose first open time is 2020-01-01T00:00:00Z.
    const KLINE: &str = r#"[[1577836800000,"1","1","1","1","1",1577923199999,"0",1,"0","0","0"]]"#;
    /// fapi klines response whose first open time is 2021-06-01T00:00:00Z.
    const KLINE_2021_06_01: &str =
        r#"[[1622505600000,"1","1","1","1","1",1622591999999,"0",1,"0","0","0"]]"#;

    /// One mock server serving both the archive and `fapi`; they are told apart
    /// by path (`/fapi/...` vs `/data/...`).
    async fn mock_exchange() -> (Endpoints, MockServer) {
        let server = MockServer::start().await;
        (Endpoints::for_tests(server.uri()), server)
    }

    /// Answer `fapi` klines for `symbol` with `template`.
    async fn mock_fapi(server: &MockServer, symbol: &str, template: ResponseTemplate) {
        Mock::given(method("GET"))
            .and(path("/fapi/v1/klines"))
            .and(query_param("symbol", symbol))
            .respond_with(template)
            .mount(server)
            .await;
    }

    /// Answer every archive request with `template`.
    async fn mock_archive(server: &MockServer, template: ResponseTemplate) {
        Mock::given(method("GET"))
            .and(path(
                "/data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2020-01-01.zip",
            ))
            .respond_with(template)
            .mount(server)
            .await;
    }

    /// Answer every archive request with `template`, whatever the archive path
    /// is. The `fapi` path is excluded so the two never overlap.
    async fn mock_archive_any(server: &MockServer, template: ResponseTemplate) {
        Mock::given(method("GET"))
            .and(path_regex("^/data/futures/um/daily/klines/.*"))
            .respond_with(template)
            .mount(server)
            .await;
    }

    /// Answer the archive request for one exact path with `template`.
    async fn mock_archive_path(
        server: &MockServer,
        archive_path: &str,
        template: ResponseTemplate,
    ) {
        Mock::given(method("GET"))
            .and(path(archive_path))
            .respond_with(template)
            .mount(server)
            .await;
    }

    /// Answer the archive bucket listing for `symbol`/`timeframe` with a single
    /// key dated `date`, exactly as S3 answers `max-keys=1`.
    async fn mock_archive_listing(server: &MockServer, symbol: &str, timeframe: &str, date: &str) {
        let prefix = format!("data/futures/um/daily/klines/{symbol}/{timeframe}/");
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>data.binance.vision</Name>
<Prefix>{prefix}</Prefix>
<MaxKeys>1</MaxKeys>
<IsTruncated>true</IsTruncated>
<Contents><Key>{prefix}{symbol}-{timeframe}-{date}.zip</Key></Contents>
</ListBucketResult>"#
        );
        Mock::given(method("GET"))
            .and(query_param("prefix", prefix))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(server)
            .await;
    }

    /// Write a config file; `extra` is appended verbatim.
    fn write_config(dir: &Path, body: &str) -> std::path::PathBuf {
        let path = dir.join("config.toml");
        std::fs::write(&path, body).unwrap();
        path
    }

    /// Config for the 2020-01-01..=2020-01-01 window used by most tests.
    fn config_for(dir: &Path, with_start: bool) -> std::path::PathBuf {
        let start = if with_start {
            "start = \"2020-01-01\"\n"
        } else {
            ""
        };
        write_config(
            dir,
            &format!(
                "symbol = [\"BTCUSDT\"]\ntimeframe = [\"1d\"]\n{start}end = \"2020-01-01\"\noutput_dir = \"{}\"\n",
                dir.join("out").display()
            ),
        )
    }

    fn downloaded(dir: &Path) -> Vec<String> {
        let root = dir.join("out/binance/data/um/daily/klines/BTCUSDT/1d");
        let mut v: Vec<String> = std::fs::read_dir(&root)
            .map(|rd| {
                rd.filter_map(Result::ok)
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        v.sort();
        v
    }

    // — tracker —

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

    #[tokio::test]
    async fn run_downloads_the_configured_window() {
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;
        mock_archive(&server, ResponseTemplate::new(200).set_body_bytes(ZIP)).await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = config_for(dir.path(), true);
        run_with_endpoints(&cfg, endpoints, CancelToken::new())
            .await
            .unwrap();
        assert_eq!(downloaded(dir.path()), vec!["BTCUSDT-1d-2020-01-01.zip"]);
    }

    #[tokio::test]
    async fn run_tolerates_a_missing_archive_file() {
        // D2: 404 is never a failure, so the run still succeeds.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;
        mock_archive(&server, ResponseTemplate::new(404)).await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = config_for(dir.path(), true);
        run_with_endpoints(&cfg, endpoints, CancelToken::new())
            .await
            .unwrap();
        assert_eq!(downloaded(dir.path()), Vec::<String>::new());
    }

    #[tokio::test]
    async fn run_fails_when_a_download_fails_permanently() {
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;
        // 400 is permanent, so this fails fast instead of burning the window.
        mock_archive(&server, ResponseTemplate::new(400)).await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = config_for(dir.path(), true);
        let err = run_with_endpoints(&cfg, endpoints, CancelToken::new())
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::Data(_)), "got {err:?}");
        assert!(err.to_string().contains("1 pair(s) failed"), "got {err}");
    }

    #[tokio::test]
    async fn run_degrades_to_config_start_and_succeeds() {
        // D4: a transient-looking API failure with `start` set degrades, warns,
        // and is NOT counted as a failure.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(400).set_body_string(r#"{"code":-1,"msg":"busy"}"#),
        )
        .await;
        mock_archive(&server, ResponseTemplate::new(200).set_body_bytes(ZIP)).await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = config_for(dir.path(), true);
        run_with_endpoints(&cfg, endpoints, CancelToken::new())
            .await
            .unwrap();
        // The fallback start date is still used for the download.
        assert_eq!(downloaded(dir.path()), vec!["BTCUSDT-1d-2020-01-01.zip"]);
    }

    #[tokio::test]
    async fn run_fails_when_the_start_date_cannot_be_resolved() {
        // No `start` configured: there is nothing to degrade to.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(400).set_body_string(r#"{"code":-1,"msg":"busy"}"#),
        )
        .await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = config_for(dir.path(), false);
        let err = run_with_endpoints(&cfg, endpoints, CancelToken::new())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("start-date resolution"),
            "got {err}"
        );
        assert_eq!(downloaded(dir.path()), Vec::<String>::new());
    }

    #[tokio::test]
    async fn run_rejects_an_unknown_symbol() {
        // -1121 stays fatal: a typo must not masquerade as success.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(400).set_body_string(r#"{"code":-1121,"msg":"Invalid symbol."}"#),
        )
        .await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = config_for(dir.path(), true);
        let err = run_with_endpoints(&cfg, endpoints, CancelToken::new())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("start-date resolution"),
            "got {err}"
        );
    }

    #[tokio::test]
    async fn run_stops_promptly_when_cancelled() {
        // D3 at the orchestration level: the signal is observed between pairs
        // and by the in-flight download.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;
        mock_archive(
            &server,
            ResponseTemplate::new(200)
                .set_body_bytes(ZIP)
                .set_delay(std::time::Duration::from_secs(30)),
        )
        .await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = config_for(dir.path(), true);
        let cancel = CancelToken::new();
        let signaller = cancel.clone();

        let handle =
            tokio::spawn(async move { run_with_endpoints(&cfg, endpoints, signaller).await });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        cancel.cancel();

        let result = tokio::time::timeout(std::time::Duration::from_secs(10), handle)
            .await
            .expect("cancellation must not wait for the delayed response")
            .unwrap();
        // Cancellation is not a failure.
        result.unwrap();
        assert_eq!(downloaded(dir.path()), Vec::<String>::new());
    }

    #[tokio::test]
    async fn run_refuses_a_concurrently_locked_directory() {
        // D5 wiring: the guard is taken inside the pipeline, not by the caller.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = config_for(dir.path(), true);
        let out = dir.path().join("out");
        std::fs::create_dir_all(&out).unwrap();
        let _held = RunLock::acquire(&out).await.unwrap();

        let err = run_with_endpoints(&cfg, endpoints, CancelToken::new())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("not supported"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn resolve_start_dates_sorts_outcomes_per_symbol() {
        // One ok, one unknown (fatal), one transient (degrades to config start).
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "OKUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;
        mock_fapi(
            &server,
            "DEADUSDT",
            ResponseTemplate::new(400).set_body_string(r#"{"code":-1,"msg":"busy"}"#),
        )
        .await;
        mock_fapi(
            &server,
            "GONEUSDT",
            ResponseTemplate::new(400).set_body_string(r#"{"code":-1121,"msg":"Invalid symbol."}"#),
        )
        .await;

        let dir = tempfile::tempdir().unwrap();
        let cache = crate::cache::Cache::new(dir.path());
        let client = reqwest::Client::new();
        let symbols: Vec<String> = ["OKUSDT", "DEADUSDT", "GONEUSDT"]
            .iter()
            .map(ToString::to_string)
            .collect();
        let start = chrono::NaiveDate::from_ymd_opt(2019, 1, 1).unwrap();
        let cfg = AppConfig {
            symbol: symbols.clone(),
            timeframe: vec![crate::config::Timeframe::D1],
            start: Some(start),
            end: None,
            output_dir: dir.path().to_path_buf(),
            concurrency: 1,
            not_found_pct_threshold: None,
        };

        let out = resolve_start_dates(
            &client,
            &cache,
            &cfg,
            &symbols,
            &cfg.timeframe,
            &endpoints,
            &CancelToken::new(),
        )
        .await;

        assert_eq!(
            out.get("OKUSDT", crate::config::Timeframe::D1),
            Some(SymbolStart::Resolved(
                chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()
            ))
        );
        // The degraded symbol still gets a date, from the config — and the
        // type says so.
        assert_eq!(
            out.get("DEADUSDT", crate::config::Timeframe::D1),
            Some(SymbolStart::Degraded(start))
        );
        assert_eq!(
            out.degraded(),
            vec![("DEADUSDT".to_owned(), crate::config::Timeframe::D1, start)]
        );
        // The unknown symbol is fatal and absent from the map.
        assert_eq!(out.failed, vec!["GONEUSDT".to_owned()]);
        assert!(!out.dates.contains_key("GONEUSDT"));
        assert_eq!(out.errors.len(), 1);
    }

    #[tokio::test]
    async fn resolve_start_dates_never_fails_as_a_whole() {
        // Every symbol failing is still reported, not propagated: the caller
        // decides what a total failure means.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "NOPEUSDT",
            ResponseTemplate::new(400).set_body_string(r#"{"code":-1121,"msg":"Invalid symbol."}"#),
        )
        .await;

        let dir = tempfile::tempdir().unwrap();
        let cache = crate::cache::Cache::new(dir.path());
        let client = reqwest::Client::new();
        let symbols = vec!["NOPEUSDT".to_owned()];
        let cfg = AppConfig {
            symbol: symbols.clone(),
            timeframe: vec![crate::config::Timeframe::D1],
            start: None,
            end: None,
            output_dir: dir.path().to_path_buf(),
            concurrency: 1,
            not_found_pct_threshold: None,
        };

        let out = resolve_start_dates(
            &client,
            &cache,
            &cfg,
            &symbols,
            &cfg.timeframe,
            &endpoints,
            &CancelToken::new(),
        )
        .await;
        assert_eq!(out.failed.len(), 1);
        assert!(out.dates.is_empty());
        assert_eq!(out.degraded().len(), 0);
    }

    #[tokio::test]
    async fn resolve_start_dates_stops_when_cancelled() {
        // The pre-flight phase observes the token: a Ctrl-C must not wait for
        // every pending lookup to finish.
        let (endpoints, server) = mock_exchange().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(KLINE)
                    .set_delay(std::time::Duration::from_secs(30)),
            )
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let cache = crate::cache::Cache::new(dir.path());
        let client = reqwest::Client::new();
        let symbols = vec!["AUSDT".to_owned(), "BUSDT".to_owned()];
        let cfg = AppConfig {
            symbol: symbols.clone(),
            timeframe: vec![crate::config::Timeframe::D1],
            start: None,
            end: None,
            output_dir: dir.path().to_path_buf(),
            concurrency: 1,
            not_found_pct_threshold: None,
        };

        let cancel = CancelToken::new();
        let signaller = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            signaller.cancel();
        });

        let out = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            resolve_start_dates(
                &client,
                &cache,
                &cfg,
                &symbols,
                &cfg.timeframe,
                &endpoints,
                &cancel,
            ),
        )
        .await
        .expect("start-date resolution must honour the cancel token");
        assert!(
            out.dates.is_empty(),
            "a cancelled resolution must not resolve any date"
        );
    }

    #[tokio::test]
    async fn resolve_start_dates_prefers_the_archive_first_date() {
        // The API says the symbol started trading in 2020-01-01; the archive
        // says its first *published* file is 2020-06-01. The archive wins: the
        // earlier days could only ever 404.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;
        mock_archive_listing(&server, "BTCUSDT", "1d", "2020-06-01").await;

        let dir = tempfile::tempdir().unwrap();
        let cache = crate::cache::Cache::new(dir.path());
        let client = reqwest::Client::new();
        let symbols = vec!["BTCUSDT".to_owned()];
        let cfg = AppConfig {
            symbol: symbols.clone(),
            timeframe: vec![crate::config::Timeframe::D1],
            start: None,
            end: None,
            output_dir: dir.path().to_path_buf(),
            concurrency: 1,
            not_found_pct_threshold: None,
        };

        let out = resolve_start_dates(
            &client,
            &cache,
            &cfg,
            &symbols,
            &cfg.timeframe,
            &endpoints,
            &CancelToken::new(),
        )
        .await;
        assert_eq!(
            out.get("BTCUSDT", crate::config::Timeframe::D1),
            Some(SymbolStart::Resolved(
                chrono::NaiveDate::from_ymd_opt(2020, 6, 1).unwrap()
            ))
        );
    }

    #[tokio::test]
    async fn resolve_start_dates_keeps_the_api_date_when_the_archive_is_empty() {
        // A freshly listed symbol has no archive file yet: the API date is
        // kept, so the run degrades to the previous behaviour (404s until the
        // archive catches up) instead of failing.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;

        let dir = tempfile::tempdir().unwrap();
        let cache = crate::cache::Cache::new(dir.path());
        let client = reqwest::Client::new();
        let symbols = vec!["BTCUSDT".to_owned()];
        let cfg = AppConfig {
            symbol: symbols.clone(),
            timeframe: vec![crate::config::Timeframe::D1],
            start: None,
            end: None,
            output_dir: dir.path().to_path_buf(),
            concurrency: 1,
            not_found_pct_threshold: None,
        };

        let out = resolve_start_dates(
            &client,
            &cache,
            &cfg,
            &symbols,
            &cfg.timeframe,
            &endpoints,
            &CancelToken::new(),
        )
        .await;
        assert_eq!(
            out.get("BTCUSDT", crate::config::Timeframe::D1),
            Some(SymbolStart::Resolved(
                chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()
            ))
        );
    }

    #[tokio::test]
    async fn a_cancelled_run_schedules_no_further_pair() {
        // The cancel check lives in the timeframe loop, not only in the symbol
        // loop: a Ctrl-C during the first pair must stop the *next* pair of
        // the same symbol, which may be another thousands of files.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;

        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        Mock::given(method("GET"))
            .and(path_regex("^/data/futures/um/daily/klines/.*"))
            .respond_with(move |_request: &wiremock::Request| {
                counter.fetch_add(1, Ordering::Relaxed);
                ResponseTemplate::new(200)
                    .set_body_bytes(ZIP)
                    .set_delay(std::time::Duration::from_secs(30))
            })
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = write_config(
            dir.path(),
            &format!(
                "symbol = [\"BTCUSDT\"]\ntimeframe = [\"1d\", \"1h\"]\nstart = \"2020-01-01\"\n\
                 end = \"2020-01-01\"\noutput_dir = \"{}\"\n",
                dir.path().join("out").display()
            ),
        );

        let cancel = CancelToken::new();
        let signaller = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            signaller.cancel();
        });

        run_with_endpoints(&cfg, endpoints, cancel).await.unwrap();
        // Only the first pair was ever scheduled: the second timeframe was
        // never requested.
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn run_fails_when_the_not_found_share_reaches_the_threshold() {
        // Two files, one present and one missing: half of the requests 404.
        // With a threshold of 50% the run must fail instead of reporting a
        // clean result.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;
        mock_archive_path(
            &server,
            "/data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2020-01-01.zip",
            ResponseTemplate::new(200).set_body_bytes(ZIP),
        )
        .await;
        mock_archive_path(
            &server,
            "/data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2020-01-02.zip",
            ResponseTemplate::new(404),
        )
        .await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = write_config(
            dir.path(),
            &format!(
                "symbol = [\"BTCUSDT\"]\ntimeframe = [\"1d\"]\nstart = \"2020-01-01\"\n\
                 end = \"2020-01-02\"\nnot_found_pct_threshold = 50.0\noutput_dir = \"{}\"\n",
                dir.path().join("out").display()
            ),
        );
        let err = run_with_endpoints(&cfg, endpoints, CancelToken::new())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not-found share"), "got {err}");
    }

    #[tokio::test]
    async fn a_cancelled_run_does_not_apply_the_not_found_threshold_to_a_partial_sample() {
        /// Records that the first (404) request was answered.
        struct First404 {
            seen: Arc<AtomicUsize>,
        }
        impl wiremock::Respond for First404 {
            fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
                self.seen.fetch_add(1, Ordering::Relaxed);
                ResponseTemplate::new(404)
            }
        }

        // One 404 is observed, then the next (slow) request is cancelled. The
        // partial sample is 1/3 = 33%, above the 10% threshold, but Ctrl-C is
        // not a coverage failure and must not turn the run into an error.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;

        let seen = Arc::new(AtomicUsize::new(0));
        Mock::given(method("GET"))
            .and(path(
                "/data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2020-01-01.zip",
            ))
            .respond_with(First404 { seen: seen.clone() })
            .mount(&server)
            .await;
        mock_archive_path(
            &server,
            "/data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2020-01-02.zip",
            ResponseTemplate::new(200)
                .set_body_bytes(ZIP)
                .set_delay(std::time::Duration::from_secs(30)),
        )
        .await;
        mock_archive_path(
            &server,
            "/data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2020-01-03.zip",
            ResponseTemplate::new(404),
        )
        .await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = write_config(
            dir.path(),
            &format!(
                "symbol = [\"BTCUSDT\"]\ntimeframe = [\"1d\"]\nstart = \"2020-01-01\"\n\
                 end = \"2020-01-03\"\nconcurrency = 1\nnot_found_pct_threshold = 10.0\n\
                 output_dir = \"{}\"\n",
                dir.path().join("out").display()
            ),
        );

        let cancel = CancelToken::new();
        let signaller = cancel.clone();
        tokio::spawn(async move {
            while seen.load(Ordering::Relaxed) == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            // Give the first 404 time to be folded into `DownloadStats` before
            // cancelling, so the regression is about the partial sample, not a
            // race with the first response.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            signaller.cancel();
        });

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            run_with_endpoints(&cfg, endpoints, cancel),
        )
        .await
        .expect("cancellation must stop the slow second download");
        result.unwrap();
    }

    #[tokio::test]
    async fn run_succeeds_when_the_not_found_share_stays_below_the_threshold() {
        // The same 50% of missing files, but the threshold is not reached: the
        // run still succeeds, as before.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;
        mock_archive_path(
            &server,
            "/data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2020-01-01.zip",
            ResponseTemplate::new(200).set_body_bytes(ZIP),
        )
        .await;
        mock_archive_path(
            &server,
            "/data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2020-01-02.zip",
            ResponseTemplate::new(404),
        )
        .await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = write_config(
            dir.path(),
            &format!(
                "symbol = [\"BTCUSDT\"]\ntimeframe = [\"1d\"]\nstart = \"2020-01-01\"\n\
                 end = \"2020-01-02\"\nnot_found_pct_threshold = 75.0\noutput_dir = \"{}\"\n",
                dir.path().join("out").display()
            ),
        );
        run_with_endpoints(&cfg, endpoints, CancelToken::new())
            .await
            .unwrap();
    }

    // — skip and circuit-breaker branches —
    #[tokio::test]
    async fn run_skips_the_pair_when_the_config_start_is_after_the_end() {
        // `end` is left unset, so it resolves to "yesterday"; a `start` in the
        // future is then after the end and the pair is skipped outright.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = write_config(
            dir.path(),
            &format!(
                "symbol = [\"BTCUSDT\"]\ntimeframe = [\"1d\"]\nstart = \"2099-01-01\"\n\
                 output_dir = \"{}\"\n",
                dir.path().join("out").display()
            ),
        );
        run_with_endpoints(&cfg, endpoints, CancelToken::new())
            .await
            .unwrap();
        assert_eq!(downloaded(dir.path()), Vec::<String>::new());
    }

    #[tokio::test]
    async fn run_skips_the_pair_when_the_symbol_was_listed_after_the_end() {
        // The symbol's first kline is after the configured end date, so the
        // resolved start is after the end and the pair is skipped.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE_2021_06_01),
        )
        .await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = config_for(dir.path(), false);
        run_with_endpoints(&cfg, endpoints, CancelToken::new())
            .await
            .unwrap();
        assert_eq!(downloaded(dir.path()), Vec::<String>::new());
    }

    #[tokio::test]
    async fn run_trips_the_circuit_breaker_after_consecutive_failures() {
        // Every pair fails permanently; after 5 consecutive failures the batch
        // aborts instead of working through the whole matrix.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;
        mock_archive_any(&server, ResponseTemplate::new(400)).await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = write_config(
            dir.path(),
            &format!(
                "symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\", \"3m\", \"5m\", \"15m\", \"30m\"]\n\
                 start = \"2020-01-01\"\nend = \"2020-01-01\"\noutput_dir = \"{}\"\n",
                dir.path().join("out").display()
            ),
        );
        let err = run_with_endpoints(&cfg, endpoints, CancelToken::new())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("5 pair(s) failed"), "got {err}");
        assert!(err.to_string().contains("aborted early"), "got {err}");
    }

    #[tokio::test]
    async fn run_starts_no_pair_when_the_token_is_already_cancelled() {
        // The token is cancelled before the run starts: the pair loop must not
        // schedule anything. The pre-flight also observes the token and exits
        // without resolving a start date first.
        let (endpoints, server) = mock_exchange().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;
        mock_archive(&server, ResponseTemplate::new(200).set_body_bytes(ZIP)).await;

        let dir = tempfile::tempdir().unwrap();
        let cfg = config_for(dir.path(), true);
        let cancel = CancelToken::new();
        cancel.cancel();

        run_with_endpoints(&cfg, endpoints, cancel).await.unwrap();
        assert_eq!(downloaded(dir.path()), Vec::<String>::new());
    }
}
