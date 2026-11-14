//! Start-date resolution: the API date, the archive date, and what happens
//! when one of them cannot be read.

use wiremock::ResponseTemplate;

use crate::app::run_with_endpoints;
use crate::cache::Cache;
use crate::cancel::CancelToken;
use crate::config::for_tests;
use crate::dataset::Market;
use crate::start_dates::{SymbolStart, resolve_start_dates};

use super::harness::{
    KLINE, ZIP, config_for, d1_job, downloaded, mock_archive, mock_archive_listing, mock_exchange,
    mock_fapi,
};

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
    let cache = Cache::new(dir.path());
    let client = reqwest::Client::new();
    let symbols: Vec<String> = ["OKUSDT", "DEADUSDT", "GONEUSDT"]
        .iter()
        .map(ToString::to_string)
        .collect();
    let start = chrono::NaiveDate::from_ymd_opt(2019, 1, 1).unwrap();
    let cfg = for_tests(
        Market::Um,
        symbols.clone(),
        vec![d1_job()],
        Some(start),
        dir.path().to_path_buf(),
    );
    let jobs = cfg.jobs().to_vec();

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
    let cache = Cache::new(dir.path());
    let client = reqwest::Client::new();
    let symbols = vec!["NOPEUSDT".to_owned()];
    let cfg = for_tests(
        Market::Um,
        symbols.clone(),
        vec![d1_job()],
        None,
        dir.path().to_path_buf(),
    );
    let jobs = cfg.jobs().to_vec();

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
    let cache = Cache::new(dir.path());
    let client = reqwest::Client::new();
    let symbols = vec!["BTCUSDT".to_owned()];
    let cfg = for_tests(
        Market::Um,
        symbols.clone(),
        vec![d1_job()],
        None,
        dir.path().to_path_buf(),
    );
    let jobs = cfg.jobs().to_vec();

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
    let cache = Cache::new(dir.path());
    let client = reqwest::Client::new();
    let symbols = vec!["BTCUSDT".to_owned()];
    let cfg = for_tests(
        Market::Um,
        symbols.clone(),
        vec![d1_job()],
        None,
        dir.path().to_path_buf(),
    );
    let jobs = cfg.jobs().to_vec();

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
