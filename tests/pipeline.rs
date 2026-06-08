mod common;

use std::collections::HashSet;

/// Full pipeline: build tasks → run_downloads → verify output parquet.
#[tokio::test]
#[ignore = "requires network; run with --include-ignored"]
async fn full_pipeline_single_day() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let client = common::get_test_client().await;
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");

    let symbol = "BTCUSDT";
    let tf = "1m";
    let start = chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let end = chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let tasks = binance_data_downloader::task::builder::build_download_tasks(
        symbol, tf, start, end, &HashSet::new(), &data_dir,
    );
    assert_eq!(tasks.len(), 1, "should produce 1 task for 1 day");

    let parquet_path = tasks[0].dest.with_extension("parquet");

    binance_data_downloader::task::download::run_downloads(&client, tasks, 4)
        .await
        .expect("run_downloads should succeed");

    assert!(
        parquet_path.exists(),
        "parquet file should exist at: {}",
        parquet_path.display()
    );

    use polars::prelude::SerReader;
    let df = polars::prelude::ParquetReader::new(std::io::Cursor::new(
        std::fs::read(&parquet_path).unwrap(),
    ))
    .finish()
    .expect("should read parquet back");

    assert_eq!(df.width(), 12, "Binance kline parquet should have 12 columns");
    assert!(df.height() > 0, "parquet should contain rows");

    let col_names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert!(
        col_names.contains(&"open_time"),
        "should contain open_time, got: {col_names:?}",
    );
    assert!(
        col_names.contains(&"close"),
        "should contain close, got: {col_names:?}",
    );
}

/// If a parquet file already exists, run_downloads should skip it.
#[tokio::test]
#[ignore = "requires network; run with --include-ignored"]
async fn pipeline_skip_existing() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let client = common::get_test_client().await;
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");

    // Pre-seed a stub parquet for 2024-01-01
    let stub_rel =
        "futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.parquet";
    let stub_path = data_dir.join(stub_rel);
    std::fs::create_dir_all(stub_path.parent().unwrap()).unwrap();
    let dummy_df = binance_data_downloader::task::download::csv_to_dataframe_sync(
        b"open_time,open,high,low,close,volume,close_time,quote_vol,trades,taker_buy_vol,taker_buy_quote_vol,ignore\n1,100,101,99,100,1000,2,10000,10,500,5000,0\n",
    )
    .unwrap();
    binance_data_downloader::task::download::df_to_parquet::write_parquet_sync(
        &stub_path,
        dummy_df,
    )
    .unwrap();

    // Scan — should normalize .parquet → .zip key
    let local = binance_data_downloader::task::get_local_files::scan_local_files(&data_dir)
        .await
        .unwrap();
    assert!(
        local.contains::<str>(
            "futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip"
        ),
        "scan should normalize .parquet to .zip key, got: {local:?}",
    );

    // Build tasks for 2 days — 2024-01-01 (cached) and 2024-01-02 (new)
    let start = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let end = chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let tasks = binance_data_downloader::task::builder::build_download_tasks(
        "BTCUSDT", "1m", start, end, &local, &data_dir,
    );
    assert_eq!(
        tasks.len(),
        1,
        "should skip 2024-01-01, only download 2024-01-02",
    );
    assert!(
        tasks[0].url.contains("2024-01-02"),
        "remaining task should be for 2024-01-02, got: {}",
        tasks[0].url,
    );

    let new_parquet = tasks[0].dest.with_extension("parquet");
    assert!(!new_parquet.exists(), "2024-01-02 parquet should not exist yet");

    binance_data_downloader::task::download::run_downloads(&client, tasks, 4)
        .await
        .expect("run_downloads should succeed");

    assert!(new_parquet.exists(), "2024-01-02 parquet should now exist");
}
