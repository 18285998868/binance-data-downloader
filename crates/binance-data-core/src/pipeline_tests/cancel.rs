//! Cooperative cancellation: every stage must observe the token, and a
//! cancelled run is not a failed run.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, ResponseTemplate};

use crate::app::run_with_endpoints;
use crate::cache::Cache;
use crate::cancel::CancelToken;
use crate::config::for_tests;
use crate::dataset::Market;
use crate::start_dates::resolve_start_dates;

use super::harness::{
    KLINE, ZIP, config_for, d1_job, downloaded, mock_archive, mock_archive_path, mock_exchange,
    mock_fapi, write_config,
};

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
    let cache = Cache::new(dir.path());
    let client = reqwest::Client::new();
    let symbols = vec!["AUSDT".to_owned(), "BUSDT".to_owned()];
    let cfg = for_tests(
        Market::Um,
        symbols.clone(),
        vec![d1_job()],
        None,
        dir.path().to_path_buf(),
    );
    let jobs = cfg.jobs().to_vec();

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
