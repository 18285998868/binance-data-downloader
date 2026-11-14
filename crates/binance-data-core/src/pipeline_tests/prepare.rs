//! Preparation: the per-run resources and the exclusive output-directory lock.

use wiremock::ResponseTemplate;

use crate::app::run_with_endpoints;
use crate::cancel::CancelToken;
use crate::prepare::lock::RunLock;

use super::harness::{KLINE, config_for, mock_exchange, mock_fapi};

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
