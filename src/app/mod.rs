//! Pipeline orchestration: config -> scan -> start dates -> per-pair downloads.

pub(crate) mod download;
pub(crate) mod tasks;
pub(crate) mod urls;

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use futures::stream::{StreamExt, iter as stream_iter};

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
    pub(crate) archive: Cow<'static, str>,
    /// Base of the Binance USDⓈ-M futures API.
    pub(crate) fapi: Cow<'static, str>,
}

impl Endpoints {
    /// The real `data.binance.vision` and `fapi.binance.com`.
    pub(crate) fn live() -> Self {
        Self {
            archive: Cow::Borrowed(urls::ARCHIVE_BASE),
            fapi: Cow::Borrowed(crate::start_date::FAPI_BASE),
        }
    }

    /// Both bases pointed at one mock server.
    #[cfg(test)]
    pub(crate) fn for_tests(base: String) -> Self {
        Self {
            archive: Cow::Owned(base.clone()),
            fapi: Cow::Owned(base),
        }
    }
}

/// Run the full batch download pipeline.
///
/// 1. Load config
/// 2. Scan existing local files (once)
/// 3. Resolve one start date per symbol
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
pub async fn run(config_path: &Path) -> Result<(), AppError> {
    // Graceful shutdown: one Ctrl-C flag, observed both between pairs and by
    // every in-flight download, so a long batch stops promptly.
    let cancel = CancelToken::new();
    let cancel_task = cancel.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::warn!("cancellation requested; stopping in-flight downloads");
            cancel_task.cancel();
        }
    });
    run_inner(config_path, Endpoints::live(), cancel).await
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
    run_inner(config_path, endpoints, cancel).await
}

/// The pipeline body, with its two external dependencies passed in.
async fn run_inner(
    config_path: &Path,
    endpoints: Endpoints,
    cancel: CancelToken,
) -> Result<(), AppError> {
    let cfg = AppConfig::load(config_path).await?;

    // Concurrent runs over one output directory are not supported: they would
    // contend on the same `redb` cache file and race on destination files. The
    // guard turns that into an immediate, explicit error.
    let _lock = RunLock::acquire(&cfg.output_dir)?;

    let cache = crate::cache::Cache::new(&cfg.output_dir);

    let client = reqwest::Client::builder()
        .user_agent("binance-data-downloader/0.1")
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(256)
        .pool_idle_timeout(Duration::from_secs(30))
        .build()?;

    let local = crate::scan::scan_local_files(&cfg.output_dir).await?;

    let concurrency = cfg.concurrency;
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
            "first start-date per symbol is resolved by an individual request to the \
             Binance API and cached for 30 days; this can take a while on a fresh run",
        );
    }

    let mut failed_pairs = 0;
    let mut errors = Vec::new();
    let mut totals = download::DownloadStats::default();
    let mut tracker = ConsecutiveFailureTracker::new(MAX_CONSECUTIVE_PAIR_FAILURES);
    let mut aborted_early = false;

    // One start date per symbol, resolved up front.
    let resolution = resolve_start_dates(&client, &cache, &cfg, &symbols, &endpoints).await;
    errors.extend(resolution.errors);
    tracing::info!(
        resolved = resolution.dates.len(),
        failed = resolution.failed.len(),
        degraded = resolution.degraded.len(),
        total = symbols.len(),
        "start-dates resolved",
    );

    'pairs: for symbol in symbols {
        if cancel.is_cancelled() {
            aborted_early = true;
            tracing::warn!("cancellation requested, stopping after current pair");
            break 'pairs;
        }
        let Some(sym_start) = resolution.dates.get(&symbol) else {
            continue;
        };
        for timeframe in &cfg.timeframe {
            tracing::info!(symbol = %symbol, timeframe = %timeframe, "running pair");
            let ctx = PairCtx {
                sym_start: *sym_start,
                end,
                cfg: &cfg,
                local: &local,
                endpoints: endpoints.clone(),
            };
            match run_one(&client, &symbol, *timeframe, &ctx, &cancel).await {
                Ok(stats) => {
                    tracker.record_success();
                    totals.absorb(stats);
                }
                Err(download::DownloadFailure { stats, error }) => {
                    // The stats gathered before the failure still belong to the
                    // batch: dropping them would silently shrink both the
                    // totals and the 404 ratio.
                    totals.absorb(stats);
                    failed_pairs += 1;
                    tracing::error!(symbol = %symbol, timeframe = %timeframe, error = %error, "pair failed");
                    if errors.len() < MAX_BATCH_ERRORS {
                        errors.push(format!("{symbol}/{timeframe}: {error}"));
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
        // 404s are tolerated by design and never change the exit code; they are
        // surfaced here so an unexpectedly high ratio is still visible.
        tracing::warn!(
            not_found = totals.not_found,
            total = totals.total,
            pct = totals.not_found_pct(),
            "files not found on the server (404, expected for edge dates; investigate if unexpected)",
        );
    }

    // A degraded symbol is NOT a failure: the run continued from `config.start`.
    // It is called out explicitly so partial coverage is never mistaken for a
    // clean result.
    if !resolution.degraded.is_empty() {
        let detail = resolution
            .degraded
            .iter()
            .map(|(sym, start)| format!("{sym}@{start}"))
            .collect::<Vec<_>>()
            .join(", ");
        tracing::warn!(
            count = resolution.degraded.len(),
            detail,
            "start-date lookup failed for these symbols; fell back to config start — \
             their earliest days may be missing",
        );
    }

    let total_failures = resolution.failed.len() + failed_pairs;
    if total_failures == 0 {
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

/// Result of resolving one start date per symbol.
struct StartDateResolution {
    /// Resolved first-kline date, keyed by symbol.
    dates: HashMap<String, chrono::NaiveDate>,
    /// Symbols whose lookup failed with no usable fallback: skipped entirely.
    failed: Vec<String>,
    /// Symbols that fell back to `config.start` after a transient failure.
    degraded: Vec<(String, chrono::NaiveDate)>,
    /// Error details, capped at [`MAX_BATCH_ERRORS`].
    errors: Vec<String>,
}

/// Resolve the first-kline date for every symbol up front.
///
/// Start dates are resolved from **`1m` only**, then reused for every
/// timeframe. The `fapi` klines endpoint answers this question identically for
/// all intervals, and one lookup per symbol (rather than per symbol × timeframe)
/// keeps the pre-flight phase cheap. The cost of that approximation is real but
/// bounded: a symbol's higher-timeframe archive can begin later than its first
/// minute bar (BTCUSDT's first `1m` bar is 2019-09-08, while its first `1d`
/// archive is 2019-12-31), so the earliest days of a coarse timeframe 404.
/// Those 404s are tolerated and reported, never counted as failures.
///
/// `symbols` is passed in rather than recomputed: [`AppConfig::normalized_symbols`]
/// allocates and sorts a fresh vector on every call.
async fn resolve_start_dates(
    client: &reqwest::Client,
    cache: &crate::cache::Cache,
    cfg: &AppConfig,
    symbols: &[String],
    endpoints: &Endpoints,
) -> StartDateResolution {
    let mut out = StartDateResolution {
        dates: HashMap::with_capacity(symbols.len()),
        failed: Vec::new(),
        degraded: Vec::new(),
        errors: Vec::new(),
    };

    let mut pending = stream_iter(symbols.iter().cloned().map(|sym| {
        let c = client.clone();
        async move {
            // D1: one lookup per symbol, always at 1m (see the fn docs).
            let res = crate::start_date::get_first_kline_date(
                &c,
                cache,
                &endpoints.fapi,
                &sym,
                Timeframe::M1.as_str(),
            )
            .await;
            (sym, res)
        }
    }))
    .buffer_unordered(START_DATE_FETCH_CONCURRENCY);

    while let Some((sym, res)) = pending.next().await {
        match res {
            Ok(d) => {
                out.dates.insert(sym, d);
            }
            Err(e) => {
                if let Some(fallback) = start_fallback(&e, cfg.start) {
                    tracing::warn!(
                        symbol = %sym,
                        start = %fallback,
                        error = %e,
                        "start-date fetch failed, falling back to config start",
                    );
                    out.dates.insert(sym.clone(), fallback);
                    out.degraded.push((sym, fallback));
                } else {
                    tracing::error!(symbol = %sym, error = %e, "start-date fetch failed");
                    if out.errors.len() < MAX_BATCH_ERRORS {
                        out.errors.push(format!("{sym}/*: {e}"));
                    }
                    out.failed.push(sym);
                }
            }
        }
    }

    out
}

/// Maximum number of pair-level error details retained in the batch summary.
const MAX_BATCH_ERRORS: usize = 20;

/// Maximum number of concurrent start-date lookups issued before the pair loop.
///
/// Deliberately independent of `config.concurrency`: this phase is a handful of
/// cheap metadata requests and Binance rate-limits by request weight, so the
/// default stays low.
const START_DATE_FETCH_CONCURRENCY: usize = 8;

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
#[tracing::instrument(skip(client, ctx, cancel), fields(symbol = %symbol, timeframe = %timeframe))]
async fn run_one(
    client: &reqwest::Client,
    symbol: &str,
    timeframe: Timeframe,
    ctx: &PairCtx<'_>,
    cancel: &CancelToken,
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

    let tasks = tasks::build_download_tasks(
        &ctx.endpoints.archive,
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
    let result = download::run_downloads(client, tasks, ctx.cfg.concurrency, cancel).await;
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

/// Pick the first day to download: the later of `config.start` and the
/// symbol's own listing date, so dates before listing are never requested.
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
    use super::{ConsecutiveFailureTracker, Endpoints, resolve_start, run_with_endpoints};
    use super::{resolve_start_dates, start_fallback};
    use crate::cancel::CancelToken;
    use crate::config::AppConfig;
    use crate::{AppError, lock::RunLock};
    use std::path::Path;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn start_fallback_used_when_cfg_start_is_set() {
        let start = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let err = AppError::Api {
            status: None,
            message: "network down".into(),
        };
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
        let err = AppError::Api {
            status: None,
            message: "network down".into(),
        };
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

    // — orchestration against a mock exchange —

    /// Minimal body accepted by the zip-magic check.
    const ZIP: &[u8] = b"PK\x03\x04fake";
    /// fapi klines response whose first open time is 2020-01-01T00:00:00Z.
    const KLINE: &str = r#"[[1577836800000,"1","1","1","1","1",1577923199999,"0",1,"0","0","0"]]"#;

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
        let _held = RunLock::acquire(&out).unwrap();

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
        };

        let out = resolve_start_dates(&client, &cache, &cfg, &symbols, &endpoints).await;

        assert_eq!(
            out.dates.get("OKUSDT"),
            Some(&chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap())
        );
        // The degraded symbol still gets a date, from the config.
        assert_eq!(out.dates.get("DEADUSDT"), Some(&start));
        assert_eq!(out.degraded, vec![("DEADUSDT".to_owned(), start)]);
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
        };

        let out = resolve_start_dates(&client, &cache, &cfg, &symbols, &endpoints).await;
        assert_eq!(out.failed.len(), 1);
        assert!(out.dates.is_empty());
        assert_eq!(out.degraded, Vec::<(String, chrono::NaiveDate)>::new());
    }
}
