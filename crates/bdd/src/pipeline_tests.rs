//! End-to-end pipeline tests over a mock exchange.
//!
//! These drive `run_with_endpoints`, so config loading, preparation,
//! start-date resolution, the pair loop, downloads, cancellation and the
//! final report are all exercised together.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use wiremock::matchers::{method, path, path_regex, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::AppError;
use crate::app::run_with_endpoints;
use crate::cancel::CancelToken;
use crate::config::{AppConfig, DatasetConfig};
use crate::dataset::{DatasetKind, DownloadJob, Interval, Market, Period};
use crate::endpoints::Endpoints;
use crate::prepare::lock::RunLock;
use crate::start_dates::{SymbolStart, resolve_start_dates};

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
async fn mock_archive_path(server: &MockServer, archive_path: &str, template: ResponseTemplate) {
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
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n{start}end = \"2020-01-01\"\n\
             output_dir = \"{}\"\n\n\
             [[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1d\"]\n",
            dir.join("out").display()
        ),
    )
}

fn klines_dataset(intervals: &[Interval]) -> DatasetConfig {
    DatasetConfig {
        kind: DatasetKind::Klines,
        periods: vec![Period::Daily],
        intervals: intervals.to_vec(),
    }
}

fn d1_job() -> DownloadJob {
    DownloadJob {
        kind: DatasetKind::Klines,
        interval: Some(Interval::D1),
        period: Period::Daily,
    }
}

fn downloaded_under(dir: &Path, segments: &[&str]) -> Vec<String> {
    let mut root = dir.join("out");
    for segment in segments {
        root.push(segment);
    }
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

fn downloaded(dir: &Path) -> Vec<String> {
    downloaded_under(
        dir,
        &["data", "futures", "um", "daily", "klines", "BTCUSDT", "1d"],
    )
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
async fn run_downloads_a_complete_month_once() {
    // Regression: `run_one` normalizes the monthly end for its skip check,
    // and `Period::dates` normalizes again. Passing the already-normalized
    // date as the end dropped the only complete month (start == end).
    let (endpoints, server) = mock_exchange().await;
    mock_fapi(
        &server,
        "BTCUSDT",
        ResponseTemplate::new(200).set_body_string(KLINE),
    )
    .await;
    let prefix = "data/futures/um/monthly/fundingRate/BTCUSDT/";
    let listing = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>data.binance.vision</Name>
<Prefix>{prefix}</Prefix>
<MaxKeys>1</MaxKeys>
<IsTruncated>true</IsTruncated>
<Contents><Key>{prefix}BTCUSDT-fundingRate-2020-01.zip</Key></Contents>
</ListBucketResult>"#
    );
    Mock::given(method("GET"))
        .and(query_param("prefix", prefix))
        .respond_with(ResponseTemplate::new(200).set_body_string(listing))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(
            "/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2020-01.zip",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(ZIP))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        dir.path(),
        &format!(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n\
             start = \"2020-01-01\"\nend = \"2020-01-31\"\n\
             output_dir = \"{}\"\n\n\
             [[dataset]]\ntype = \"fundingRate\"\nperiods = [\"monthly\"]\n",
            dir.path().join("out").display()
        ),
    );
    run_with_endpoints(&cfg, endpoints, CancelToken::new())
        .await
        .unwrap();
    assert_eq!(
        downloaded_under(
            dir.path(),
            &["data", "futures", "um", "monthly", "fundingRate", "BTCUSDT"]
        ),
        vec!["BTCUSDT-fundingRate-2020-01.zip"]
    );
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

    let handle = tokio::spawn(async move { run_with_endpoints(&cfg, endpoints, signaller).await });
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
        market: Market::Um,
        dataset: vec![klines_dataset(&[Interval::D1])],
        start: Some(start),
        end: None,
        output_dir: dir.path().to_path_buf(),
        concurrency: 1,
        not_found_pct_threshold: None,
    };
    let jobs = cfg.jobs();

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
        out.get("OKUSDT", d1_job()),
        Some(SymbolStart::Resolved(
            chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()
        ))
    );
    // The degraded symbol still gets a date, from the config — and the
    // type says so.
    assert_eq!(
        out.get("DEADUSDT", d1_job()),
        Some(SymbolStart::Degraded(start))
    );
    assert_eq!(
        out.degraded(),
        vec![("DEADUSDT".to_owned(), d1_job(), start)]
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
        market: Market::Um,
        dataset: vec![klines_dataset(&[Interval::D1])],
        start: None,
        end: None,
        output_dir: dir.path().to_path_buf(),
        concurrency: 1,
        not_found_pct_threshold: None,
    };
    let jobs = cfg.jobs();

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
        market: Market::Um,
        dataset: vec![klines_dataset(&[Interval::D1])],
        start: None,
        end: None,
        output_dir: dir.path().to_path_buf(),
        concurrency: 1,
        not_found_pct_threshold: None,
    };
    let jobs = cfg.jobs();

    let cancel = CancelToken::new();
    let signaller = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        signaller.cancel();
    });

    let out = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        resolve_start_dates(&client, &cache, &cfg, &symbols, &jobs, &endpoints, &cancel),
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
        market: Market::Um,
        dataset: vec![klines_dataset(&[Interval::D1])],
        start: None,
        end: None,
        output_dir: dir.path().to_path_buf(),
        concurrency: 1,
        not_found_pct_threshold: None,
    };
    let jobs = cfg.jobs();

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
        out.get("BTCUSDT", d1_job()),
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
        market: Market::Um,
        dataset: vec![klines_dataset(&[Interval::D1])],
        start: None,
        end: None,
        output_dir: dir.path().to_path_buf(),
        concurrency: 1,
        not_found_pct_threshold: None,
    };
    let jobs = cfg.jobs();

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
        out.get("BTCUSDT", d1_job()),
        Some(SymbolStart::Resolved(
            chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()
        ))
    );
}

#[tokio::test]
async fn a_cancelled_run_schedules_no_further_pair() {
    // The cancel check lives in the job loop, not only in the symbol
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
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\nstart = \"2020-01-01\"\n\
             end = \"2020-01-01\"\noutput_dir = \"{}\"\n\n\
             [[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\n\
             intervals = [\"1d\", \"1h\"]\n",
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
    // Only the first pair was ever scheduled: the second job was
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
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\nstart = \"2020-01-01\"\n\
             end = \"2020-01-02\"\nnot_found_pct_threshold = 50.0\noutput_dir = \"{}\"\n\n\
             [[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1d\"]\n",
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
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\nstart = \"2020-01-01\"\n\
             end = \"2020-01-03\"\nconcurrency = 1\nnot_found_pct_threshold = 10.0\n\
             output_dir = \"{}\"\n\n\
             [[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1d\"]\n",
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
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\nstart = \"2020-01-01\"\n\
             end = \"2020-01-02\"\nnot_found_pct_threshold = 75.0\noutput_dir = \"{}\"\n\n\
             [[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1d\"]\n",
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
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\nstart = \"2099-01-01\"\n\
             output_dir = \"{}\"\n\n\
             [[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1d\"]\n",
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
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n\
             start = \"2020-01-01\"\nend = \"2020-01-01\"\noutput_dir = \"{}\"\n\n\
             [[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\n\
             intervals = [\"1m\", \"3m\", \"5m\", \"15m\", \"30m\"]\n",
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
