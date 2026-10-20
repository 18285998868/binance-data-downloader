//! Integration tests over the crate's public surface: `run`, `Cli`, `AppError`.
//!
//! These exercise the binary's entry point end to end as far as the public API
//! allows — without touching the network, since `run` resolves its own base
//! URLs. Downloading behaviour itself is covered by the unit tests in
//! `src/app/download/stream.rs`, which drive a `wiremock` server.

use std::path::Path;

use bdd::{AppError, Cli, run};

#[tokio::test]
async fn run_reports_a_missing_config_file() {
    let err = run(Path::new("/nonexistent/definitely/missing.toml"))
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::Config { .. }), "got {err:?}");
    // The path lives in its own field and is rendered exactly once, by Display.
    match &err {
        AppError::Config { path, message } => {
            assert_eq!(
                path.as_deref(),
                Some(Path::new("/nonexistent/definitely/missing.toml"))
            );
            assert!(
                !message.contains("missing.toml"),
                "the path must not be duplicated into the message: {message}"
            );
        }
        _ => unreachable!("matched above"),
    }
    let rendered = err.to_string();
    assert!(rendered.starts_with("config error: /nonexistent/definitely/missing.toml: "));
    assert_eq!(
        rendered.matches("missing.toml").count(),
        1,
        "path should appear exactly once: {rendered}"
    );
}

#[tokio::test]
async fn run_reports_an_invalid_config_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.toml");
    std::fs::write(&path, "symbol = []\ntimeframe = [\"1m\"]\n").unwrap();

    let err = run(&path).await.unwrap_err();
    assert!(
        matches!(err, AppError::Config { .. }),
        "expected Config error, got {err:?}"
    );
}

#[tokio::test]
async fn run_reports_an_unknown_config_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("typo.toml");
    std::fs::write(
        &path,
        "symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]\ntiemframe = [\"1m\"]\n",
    )
    .unwrap();

    assert!(run(&path).await.is_err());
}

#[test]
fn cli_requires_a_config_flag() {
    use clap::Parser;
    assert!(Cli::try_parse_from(["bdd"]).is_err());
    let cli = Cli::try_parse_from(["bdd", "-c", "config.toml"]).unwrap();
    assert_eq!(cli.config, Path::new("config.toml").to_path_buf());
}

#[test]
fn app_error_messages_are_lowercase_and_self_describing() {
    let cases: Vec<(AppError, &str)> = vec![
        (
            AppError::NotFound {
                url: "https://x/y.zip".into(),
            },
            "not found: https://x/y.zip",
        ),
        (
            AppError::DownloadFailed {
                url: "https://x/y.zip".into(),
                reason: "HTTP 503".into(),
            },
            "download failed for https://x/y.zip: HTTP 503",
        ),
        (
            AppError::UnknownSymbol {
                symbol: "NOPEUSDT".into(),
            },
            "unknown symbol: NOPEUSDT",
        ),
        (AppError::Date("bad ts".into()), "date error: bad ts"),
        (AppError::Cache("db gone".into()), "cache error: db gone"),
        (
            AppError::Data(bdd::FailureReport::new("2 of 5 downloads failed")),
            "data error: 2 of 5 downloads failed",
        ),
    ];
    for (err, expected) in cases {
        assert_eq!(err.to_string(), expected);
    }
}

#[test]
fn app_error_from_io_preserves_the_source() {
    use std::error::Error;

    let io = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
    let err = AppError::from(io);
    assert!(matches!(err, AppError::Io(_)));
    assert!(
        err.source().is_some(),
        "Io variant must keep a source chain"
    );
}
