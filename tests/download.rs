mod common;

use std::path::Path;

/// Download a real Binance ZIP file and verify it's a valid archive with a CSV entry.
#[tokio::test]
#[ignore = "requires network; run with --include-ignored"]
async fn download_valid_zip() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let client = common::get_test_client().await;

    let task = binance_data_downloader::task::DownloadTask {
        url: common::download_url("BTCUSDT", "1m", "2024-01-02"),
        dest: Path::new("/tmp").join("test_download_valid.zip"),
        pair: "BTCUSDT".into(),
        timeframe: "1m".into(),
    };

    let bytes = binance_data_downloader::task::download::download_bytes_with_retry(&client, &task)
        .await
        .expect("should download valid zip");

    assert!(!bytes.is_empty(), "downloaded bytes should not be empty");
    assert!(bytes.len() > 100, "downloaded zip should be > 100 bytes");

    let cursor = std::io::Cursor::new(&bytes);
    let mut archive =
        zip::ZipArchive::new(cursor).expect("downloaded bytes should be a valid zip archive");
    assert!(archive.len() > 0, "zip should contain at least one entry");

    let has_csv = (0..archive.len()).any(|i| {
        archive.by_index(i).ok().map_or(false, |e| {
            e.name().ends_with(".csv") || e.name().ends_with(".CSV")
        })
    });
    assert!(has_csv, "zip should contain a CSV entry");
}

/// A non-existent symbol should return `AppError::NotFound` (HTTP 404).
#[tokio::test]
#[ignore = "requires network; run with --include-ignored"]
async fn download_404() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let client = common::get_test_client().await;

    let task = binance_data_downloader::task::DownloadTask {
        url: common::download_url("NONEXISTENT12345", "1m", "2024-01-01"),
        dest: Path::new("/tmp").join("test_404.zip"),
        pair: "NONEXISTENT12345".into(),
        timeframe: "1m".into(),
    };

    let result =
        binance_data_downloader::task::download::download_bytes_with_retry(&client, &task).await;

    match result {
        Err(binance_data_downloader::AppError::NotFound { url }) => {
            assert!(url.contains("NONEXISTENT12345"), "url should match: {url}");
        }
        other => panic!("expected NotFound, got: {other:?}"),
    }
}

/// Batch download: valid dates succeed, a far-future date returns 404.
#[tokio::test]
#[ignore = "requires network; run with --include-ignored"]
async fn download_batch() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let client = std::sync::Arc::new(common::get_test_client().await);

    let dates = ["2024-01-01", "2024-01-02", "2099-12-31"];

    let tasks: Vec<_> = dates
        .iter()
        .map(|date| binance_data_downloader::task::DownloadTask {
            url: common::download_url("BTCUSDT", "1m", date),
            dest: Path::new("/tmp").join(format!("test_batch_{date}.zip")),
            pair: "BTCUSDT".into(),
            timeframe: "1m".into(),
        })
        .collect();

    use futures::stream::{self, StreamExt};

    let results: Vec<_> =
        stream::iter(tasks.iter().map(|t| {
            let c = client.clone();
            async move {
                binance_data_downloader::task::download::download_bytes_with_retry(&c, t).await
            }
        }))
        .buffer_unordered(4)
        .collect()
        .await;

    let ok_count = results.iter().filter(|r| r.is_ok()).count();
    let nf_count = results
        .iter()
        .filter(|r| matches!(r, Err(binance_data_downloader::AppError::NotFound { .. })))
        .count();

    assert_eq!(ok_count, 2, "two known-good dates should succeed");
    assert_eq!(nf_count, 1, "future date should return 404");
    assert_eq!(results.len(), 3);
}
