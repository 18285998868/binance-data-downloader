//! The download window: what gets requested, what a 404 means, and when the
//! batch gives up.

use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, ResponseTemplate};

use crate::AppError;
use crate::app::run_with_endpoints;
use crate::cancel::CancelToken;

use super::harness::{
    KLINE, KLINE_2021_06_01, ZIP, config_for, downloaded, downloaded_under, mock_archive,
    mock_archive_any, mock_archive_path, mock_exchange, mock_fapi, write_config,
};

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
