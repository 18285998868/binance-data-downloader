//! Shared mock exchange and configuration fixtures.

use std::path::Path;

use wiremock::matchers::{method, path, path_regex, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::dataset::{DatasetKind, DownloadJob, Interval, Period};
use crate::endpoints::Endpoints;

/// Minimal body accepted by the zip-magic check.
pub(super) const ZIP: &[u8] = b"PK\x03\x04fake";
/// fapi klines response whose first open time is 2020-01-01T00:00:00Z.
pub(super) const KLINE: &str =
    r#"[[1577836800000,"1","1","1","1","1",1577923199999,"0",1,"0","0","0"]]"#;
/// fapi klines response whose first open time is 2021-06-01T00:00:00Z.
pub(super) const KLINE_2021_06_01: &str =
    r#"[[1622505600000,"1","1","1","1","1",1622591999999,"0",1,"0","0","0"]]"#;

/// One mock server serving both the archive and `fapi`; they are told apart
/// by path (`/fapi/...` vs `/data/...`).
pub(super) async fn mock_exchange() -> (Endpoints, MockServer) {
    let server = MockServer::start().await;
    (Endpoints::for_tests(server.uri()), server)
}

/// Answer `fapi` klines for `symbol` with `template`.
pub(super) async fn mock_fapi(server: &MockServer, symbol: &str, template: ResponseTemplate) {
    Mock::given(method("GET"))
        .and(path("/fapi/v1/klines"))
        .and(query_param("symbol", symbol))
        .respond_with(template)
        .mount(server)
        .await;
}

/// Answer the default archive request with `template`.
pub(super) async fn mock_archive(server: &MockServer, template: ResponseTemplate) {
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
pub(super) async fn mock_archive_any(server: &MockServer, template: ResponseTemplate) {
    Mock::given(method("GET"))
        .and(path_regex("^/data/futures/um/daily/klines/.*"))
        .respond_with(template)
        .mount(server)
        .await;
}

/// Answer the archive request for one exact path with `template`.
pub(super) async fn mock_archive_path(
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
pub(super) async fn mock_archive_listing(
    server: &MockServer,
    symbol: &str,
    timeframe: &str,
    date: &str,
) {
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

/// Write a config file and return its path.
pub(super) fn write_config(dir: &Path, body: &str) -> std::path::PathBuf {
    let path = dir.join("config.toml");
    std::fs::write(&path, body).unwrap();
    path
}

/// Config for the 2020-01-01..=2020-01-01 window used by most tests.
pub(super) fn config_for(dir: &Path, with_start: bool) -> std::path::PathBuf {
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

/// The daily `1d` kline job every fixture uses.
pub(super) fn d1_job() -> DownloadJob {
    DownloadJob {
        kind: DatasetKind::Klines,
        interval: Some(Interval::D1),
        period: Period::Daily,
    }
}

/// File names under `{dir}/out/<segments...>`, sorted.
pub(super) fn downloaded_under(dir: &Path, segments: &[&str]) -> Vec<String> {
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

/// File names in the default daily-kline output directory, sorted.
pub(super) fn downloaded(dir: &Path) -> Vec<String> {
    downloaded_under(
        dir,
        &["data", "futures", "um", "daily", "klines", "BTCUSDT", "1d"],
    )
}
