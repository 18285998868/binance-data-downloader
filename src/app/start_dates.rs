//! Resolving the day each (symbol, job) starts at.
//!
//! Two sources, queried **concurrently** — neither may gate the other:
//!
//! * the configured market's klines API tells when a symbol **started trading**;
//! * the archive's own bucket listing tells when it **started publishing**.
//!
//! The archive is later for the oldest symbols (BTCUSDT: the API says
//! 2019-09-08, the archive 2019-12-31), and starting from the API's date would
//! request ~114 days per symbol that could only ever 404. The archive therefore
//! wins for every job it covers, while the API date stays as a floor: the
//! archive cannot predate the symbol.
//!
//! Each job gets its **own** exact date, rather than one shared approximation:
//! different datasets and intervals can start publishing on different days,
//! especially monthly files. A shared date would request files that were never
//! published — 404s that the `not_found_pct_threshold` would then rightly read
//! as a broken run.
//!
//! The API keeps a job of its own: it proves the symbol **exists**. A transient
//! API failure only matters when the archive has nothing either; an explicit
//! `-1121 unknown symbol` response remains a hard failure so typos cannot
//! masquerade as success.

use std::collections::HashMap;

use futures::StreamExt;
use futures::stream::iter as stream_iter;

use crate::AppError;
use crate::app::Endpoints;
use crate::cache::Cache;
use crate::cancel::CancelToken;
use crate::config::AppConfig;
use crate::dataset::{DatasetKind, DownloadJob};

/// The start date of one (symbol, job).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SymbolStart {
    /// Resolved from the API and/or the archive listing.
    Resolved(chrono::NaiveDate),
    /// The lookup failed and the run fell back to `config.start`.
    Degraded(chrono::NaiveDate),
}

impl SymbolStart {
    /// The day to download from, whatever the outcome was.
    pub(crate) fn date(self) -> chrono::NaiveDate {
        match self {
            Self::Resolved(date) | Self::Degraded(date) => date,
        }
    }
}

/// The result of resolving the start dates.
pub(crate) struct StartDateResolution {
    /// Per symbol, per job: the resolved start date.
    pub(crate) dates: HashMap<String, HashMap<DownloadJob, SymbolStart>>,
    /// Symbols that could not be resolved at all: skipped entirely.
    pub(crate) failed: Vec<String>,
    /// Error details, capped at [`MAX_BATCH_ERRORS`].
    pub(crate) errors: Vec<String>,
}

impl StartDateResolution {
    /// The start date of one (symbol, job), when it is known.
    pub(crate) fn get(&self, symbol: &str, job: DownloadJob) -> Option<SymbolStart> {
        self.dates
            .get(symbol)
            .and_then(|by_job| by_job.get(&job))
            .copied()
    }

    /// The (symbol, job) pairs that fell back to `config.start`, with the date
    /// they use.
    ///
    /// Sorted so callers never inherit `HashMap` iteration order.
    pub(crate) fn degraded(&self) -> Vec<(String, DownloadJob, chrono::NaiveDate)> {
        let mut out = Vec::new();
        for (symbol, by_job) in &self.dates {
            for (job, start) in by_job {
                if let SymbolStart::Degraded(date) = start {
                    out.push((symbol.clone(), *job, *date));
                }
            }
        }
        out.sort();
        out
    }
}

/// Maximum number of symbol-level error details retained in the batch summary.
const MAX_BATCH_ERRORS: usize = 20;

/// Maximum number of concurrent start-date resolutions.
///
/// Deliberately independent of `config.concurrency`: this phase is a handful
/// of cheap metadata requests and Binance rate-limits by request weight, so
/// the default stays low.
const START_DATE_FETCH_CONCURRENCY: usize = 8;

/// Resolve the start date of every (symbol, job) up front.
///
/// `symbols` is passed in rather than recomputed: [`AppConfig::normalized_symbols`]
/// allocates and sorts a fresh vector on every call.
///
/// The phase observes `cancel`: a Ctrl-C stops new lookups and drops the
/// pending stream, which also cancels the in-flight API/archive requests
/// instead of waiting for their timeouts.
pub(crate) async fn resolve_start_dates(
    client: &reqwest::Client,
    cache: &Cache,
    cfg: &AppConfig,
    symbols: &[String],
    jobs: &[DownloadJob],
    endpoints: &Endpoints,
    cancel: &CancelToken,
) -> StartDateResolution {
    let mut out = StartDateResolution {
        dates: HashMap::with_capacity(symbols.len()),
        failed: Vec::new(),
        errors: Vec::new(),
    };

    let market = cfg.market;
    let mut pending = stream_iter(symbols.iter().cloned().map(|sym| {
        let client = client.clone();
        async move {
            // The two sources are queried concurrently: neither may gate the
            // other. The API answers for `1m` only -- it reports the same
            // first kline for every interval (measured: BTCUSDT's first `1m`,
            // `5m`, `1h`, `4h` and `1d` klines are all 2019-09-08), so
            // resolving per interval would issue N×11 extra requests for
            // identical answers.
            let (api, archive) = tokio::join!(
                crate::start_date::get_first_kline_date(
                    &client,
                    cache,
                    market,
                    endpoints.exchange_api(market),
                    &sym,
                ),
                crate::archive::archive_dates(
                    &client,
                    cache,
                    &endpoints.archive_listing,
                    market,
                    &sym,
                    jobs,
                ),
            );
            (sym, api, archive)
        }
    }))
    .buffer_unordered(START_DATE_FETCH_CONCURRENCY);

    loop {
        // `biased` gives cancellation priority: a Ctrl-C must not be swallowed
        // by a lookup that happens to complete at the same time. Dropping the
        // `next()` future is safe -- `StreamExt::next` only consumes an item
        // when it actually produces one.
        let item = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                tracing::warn!("cancellation requested, stopping start-date resolution");
                break;
            }
            item = pending.next() => item,
        };
        let Some((sym, api, archive)) = item else {
            break;
        };
        match api {
            Ok(api_date) => {
                // The API knows when the symbol started trading; the archive
                // knows when it started publishing. The archive is later for
                // the oldest symbols, and using it removes the days that
                // could only ever 404.
                let mut by_job = HashMap::with_capacity(jobs.len());
                for job in jobs {
                    let date =
                        resolve_job_start(*job, api_date, archive.get(job).copied().flatten());
                    by_job.insert(*job, SymbolStart::Resolved(date));
                }
                out.dates.insert(sym, by_job);
            }
            Err(AppError::UnknownSymbol { .. }) => {
                // The exchange does not know the symbol: fatal, so a typo
                // cannot masquerade as success.
                let error = api.expect_err("the arm matches an error");
                tracing::error!(symbol = %sym, error = ?error, "start-date fetch failed");
                out.errors.push(format!("{sym}/*: {error}"));
                out.failed.push(sym);
            }
            Err(error) => {
                // The API failed, but the archive may still know the answer --
                // and an archive file proves the symbol exists, so a
                // rate-limited API must not sink the run.
                let mut by_job = HashMap::with_capacity(jobs.len());
                let mut missing: Vec<DownloadJob> = Vec::new();
                for job in jobs {
                    match archive.get(job).copied().flatten() {
                        Some(archive_date) => {
                            by_job.insert(*job, SymbolStart::Resolved(archive_date));
                        }
                        None => missing.push(*job),
                    }
                }
                if missing.is_empty() {
                    tracing::info!(
                        symbol = %sym,
                        error = ?error,
                        "start-date API lookup failed, but the archive listing answered for every job",
                    );
                    out.dates.insert(sym, by_job);
                } else if let Some(fallback) = start_fallback(&error, cfg.start) {
                    tracing::warn!(
                        symbol = %sym,
                        start = %fallback,
                        error = ?error,
                        "start-date fetch failed, falling back to config start",
                    );
                    for job in missing {
                        by_job.insert(job, SymbolStart::Degraded(fallback));
                    }
                    out.dates.insert(sym, by_job);
                } else {
                    // Nothing known for at least one job and no fallback: the
                    // symbol is skipped entirely.
                    tracing::error!(symbol = %sym, error = ?error, "start-date fetch failed");
                    out.errors.push(format!("{sym}/*: {error}"));
                    out.failed.push(sym);
                }
            }
        }
    }

    // Symbols complete out of order (`buffer_unordered`); sort before taking
    // the cap so which 20 details survive does not depend on completion order.
    out.failed.sort();
    out.errors.sort();
    out.errors.truncate(MAX_BATCH_ERRORS);
    out
}

/// Combine the API listing date and the archive date for one job.
///
/// For ordinary datasets the API date is a floor: the archive cannot predate
/// the symbol. `indexPriceKlines` is the exception — it is keyed by the
/// underlying index, which can exist before the configured contract.
fn resolve_job_start(
    job: DownloadJob,
    api_date: chrono::NaiveDate,
    archive_date: Option<chrono::NaiveDate>,
) -> chrono::NaiveDate {
    match archive_date {
        Some(date) if job.kind == DatasetKind::IndexPriceKlines => date,
        Some(date) => api_date.max(date),
        None => api_date,
    }
}

/// Pick the first day to download: the later of `config.start` and the
/// symbol's own start date, so dates before the listing are never requested.
pub(crate) fn resolve_start(
    start: Option<chrono::NaiveDate>,
    symbol_start: chrono::NaiveDate,
) -> chrono::NaiveDate {
    start.map_or(symbol_start, |configured| configured.max(symbol_start))
}

/// Decide whether a failed start-date fetch may degrade to `cfg.start`.
///
/// With an explicit `config.start`, a transient failure is recoverable: the
/// run proceeds from that date. An unknown-symbol error stays fatal, so typos
/// cannot masquerade as success.
pub(crate) fn start_fallback(
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
    use super::*;
    use crate::config::DatasetConfig;
    use crate::dataset::{DatasetKind, Interval, Market, Period};
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// fapi klines response whose first open time is 2020-01-01T00:00:00Z.
    const KLINE: &str = r#"[[1577836800000,"1","1","1","1","1",1577923199999,"0",1,"0","0","0"]]"#;

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn klines_job(interval: Interval) -> DownloadJob {
        DownloadJob {
            kind: DatasetKind::Klines,
            interval: Some(interval),
            period: Period::Daily,
        }
    }

    fn index_job() -> DownloadJob {
        DownloadJob {
            kind: DatasetKind::IndexPriceKlines,
            interval: Some(Interval::M1),
            period: Period::Daily,
        }
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

    /// Answer the archive bucket listing for `symbol`/`job` with a single key
    /// dated `date`, exactly as S3 answers `max-keys=1`.
    async fn mock_listing(server: &MockServer, symbol: &str, job: DownloadJob, date: &str) {
        let prefix = format!("{}/", job.relative_dir(Market::Um, symbol));
        let file = job.file_name(
            Market::Um,
            symbol,
            chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap(),
        );
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>data.binance.vision</Name>
<Prefix>{prefix}</Prefix>
<MaxKeys>1</MaxKeys>
<IsTruncated>true</IsTruncated>
<Contents><Key>{prefix}{file}</Key></Contents>
</ListBucketResult>"#
        );
        Mock::given(method("GET"))
            .and(query_param("prefix", prefix))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(server)
            .await;
    }

    /// A config over one market and one klines dataset, with the given start.
    fn config(
        symbols: &[String],
        intervals: &[Interval],
        start: Option<chrono::NaiveDate>,
    ) -> AppConfig {
        AppConfig {
            market: Market::Um,
            symbol: symbols.to_vec(),
            start,
            end: None,
            output_dir: std::path::PathBuf::from("/tmp/bdd-start-dates-test"),
            concurrency: 1,
            not_found_pct_threshold: None,
            dataset: vec![DatasetConfig {
                kind: DatasetKind::Klines,
                periods: vec![Period::Daily],
                intervals: intervals.to_vec(),
            }],
        }
    }

    #[tokio::test]
    async fn the_archive_listing_rescues_a_failing_api() {
        // The API is rate-limited, but the archive answered: the symbol is
        // resolved from the archive alone, not degraded and not failed.
        let server = MockServer::start().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(400).set_body_string(r#"{"code":-1,"msg":"busy"}"#),
        )
        .await;
        let job = klines_job(Interval::D1);
        mock_listing(&server, "BTCUSDT", job, "2020-06-01").await;

        let endpoints = Endpoints::for_tests(server.uri());
        let client = reqwest::Client::new();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = Cache::new(cache_dir.path());
        let symbols = vec!["BTCUSDT".to_owned()];
        let jobs = vec![job];
        let cfg = config(&symbols, &[Interval::D1], None);

        let out = resolve_start_dates(
            &client,
            &cache,
            &cfg,
            &symbols,
            &jobs,
            &endpoints,
            &CancelToken::new(),
        )
        .await;
        assert_eq!(
            out.get("BTCUSDT", job),
            Some(SymbolStart::Resolved(date(2020, 6, 1)))
        );
        assert_eq!(
            out.failed.len(),
            0,
            "the archive answers, so nothing failed"
        );
        assert_eq!(out.degraded().len(), 0, "nothing degraded");
    }

    #[tokio::test]
    async fn every_job_keeps_its_own_archive_date() {
        // The 1d archive starts later than the 1m one: a single shared date
        // would make the 1d pair request days that were never published.
        let server = MockServer::start().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(200).set_body_string(KLINE),
        )
        .await;
        let m1 = klines_job(Interval::M1);
        let d1 = klines_job(Interval::D1);
        mock_listing(&server, "BTCUSDT", m1, "2020-01-01").await;
        mock_listing(&server, "BTCUSDT", d1, "2020-06-01").await;

        let endpoints = Endpoints::for_tests(server.uri());
        let client = reqwest::Client::new();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = Cache::new(cache_dir.path());
        let symbols = vec!["BTCUSDT".to_owned()];
        let jobs = vec![m1, d1];
        let cfg = config(&symbols, &[Interval::M1, Interval::D1], None);

        let out = resolve_start_dates(
            &client,
            &cache,
            &cfg,
            &symbols,
            &jobs,
            &endpoints,
            &CancelToken::new(),
        )
        .await;
        assert_eq!(
            out.get("BTCUSDT", m1),
            Some(SymbolStart::Resolved(date(2020, 1, 1)))
        );
        assert_eq!(
            out.get("BTCUSDT", d1),
            Some(SymbolStart::Resolved(date(2020, 6, 1)))
        );
    }

    #[tokio::test]
    async fn a_failing_api_without_any_archive_data_falls_back_to_the_config_start() {
        // Neither source answered: the run continues from `config.start` and
        // says so.
        let server = MockServer::start().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(400).set_body_string(r#"{"code":-1,"msg":"busy"}"#),
        )
        .await;

        let endpoints = Endpoints::for_tests(server.uri());
        let client = reqwest::Client::new();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = Cache::new(cache_dir.path());
        let symbols = vec!["BTCUSDT".to_owned()];
        let job = klines_job(Interval::D1);
        let jobs = vec![job];
        let start = date(2024, 1, 1);
        let cfg = config(&symbols, &[Interval::D1], Some(start));

        let out = resolve_start_dates(
            &client,
            &cache,
            &cfg,
            &symbols,
            &jobs,
            &endpoints,
            &CancelToken::new(),
        )
        .await;
        assert_eq!(out.get("BTCUSDT", job), Some(SymbolStart::Degraded(start)));
        assert_eq!(out.failed.len(), 0);
    }

    #[tokio::test]
    async fn a_failing_api_without_a_fallback_fails_the_symbol() {
        // Nothing known and nowhere to fall back to: the symbol is skipped.
        let server = MockServer::start().await;
        mock_fapi(
            &server,
            "BTCUSDT",
            ResponseTemplate::new(400).set_body_string(r#"{"code":-1,"msg":"busy"}"#),
        )
        .await;

        let endpoints = Endpoints::for_tests(server.uri());
        let client = reqwest::Client::new();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = Cache::new(cache_dir.path());
        let symbols = vec!["BTCUSDT".to_owned()];
        let job = klines_job(Interval::D1);
        let jobs = vec![job];
        let cfg = config(&symbols, &[Interval::D1], None);

        let out = resolve_start_dates(
            &client,
            &cache,
            &cfg,
            &symbols,
            &jobs,
            &endpoints,
            &CancelToken::new(),
        )
        .await;
        assert!(out.get("BTCUSDT", job).is_none());
        assert_eq!(out.failed, vec!["BTCUSDT".to_owned()]);
    }

    #[test]
    fn index_jobs_take_the_earlier_archive_date() {
        // BTCUSD_PERP was listed 2020-08-11, but the BTCUSD index archive
        // starts 2020-06-30. Clamping to the contract's API date would silently
        // drop the first six weeks of index history.
        assert_eq!(
            resolve_job_start(index_job(), date(2020, 8, 11), Some(date(2020, 6, 30))),
            date(2020, 6, 30)
        );
    }

    #[test]
    fn ordinary_jobs_clamp_the_archive_to_the_api_date() {
        assert_eq!(
            resolve_job_start(
                klines_job(Interval::D1),
                date(2020, 8, 11),
                Some(date(2020, 6, 30))
            ),
            date(2020, 8, 11)
        );
    }

    #[test]
    fn a_missing_archive_date_uses_the_api_date() {
        assert_eq!(
            resolve_job_start(index_job(), date(2020, 8, 11), None),
            date(2020, 8, 11)
        );
    }

    #[test]
    fn resolve_start_date_prefers_cfg() {
        let sym = date(2023, 1, 1);
        let cfg = date(2024, 1, 1);
        assert_eq!(resolve_start(Some(cfg), sym), cfg);
    }

    #[test]
    fn resolve_start_date_prefers_sym_when_later() {
        let sym = date(2025, 1, 1);
        let cfg = Some(date(2024, 1, 1));
        assert_eq!(resolve_start(cfg, sym), sym);
    }

    #[test]
    fn resolve_start_date_no_cfg() {
        let sym = date(2023, 6, 15);
        let cfg: Option<chrono::NaiveDate> = None;
        assert_eq!(resolve_start(cfg, sym), sym);
    }

    #[test]
    fn start_fallback_used_when_cfg_start_is_set() {
        let start = date(2024, 1, 1);
        let err = AppError::Api {
            status: None,
            message: "network down".into(),
        };
        assert_eq!(start_fallback(&err, Some(start)), Some(start));
    }

    #[test]
    fn start_fallback_rejected_for_unknown_symbol() {
        let start = date(2024, 1, 1);
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
    fn symbol_start_exposes_its_date_whatever_the_outcome() {
        let date = date(2024, 1, 1);
        assert_eq!(SymbolStart::Resolved(date).date(), date);
        assert_eq!(SymbolStart::Degraded(date).date(), date);
    }
}
