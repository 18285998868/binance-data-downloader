use std::collections::HashMap;

/// Core non-HTTP pipeline components work together correctly.
#[tokio::test]
async fn pipeline_local_scan_to_urls_to_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");

    // ── Pre-populate a .parquet file to simulate already converted data ──
    let existing_rel = "futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01";
    let parquet_path = data_dir.join(&format!("{existing_rel}.parquet"));
    std::fs::create_dir_all(parquet_path.parent().unwrap()).unwrap();
    // Write a minimal valid parquet so scan finds it
    {
        let empty = data_downloader::downloader::csv_to_dataframe::csv_to_dataframe_sync(
            b"open_time,open,high,low,close\n1,100,101,99,100\n",
        )
        .unwrap();
        data_downloader::downloader::dataframe_to_parquet::write_parquet_sync(&parquet_path, empty)
            .unwrap();
    }

    // ── Scan local files ──
    let local = data_downloader::task::local_file::scan_local_files(&data_dir)
        .await
        .unwrap();

    // The path should be normalized to .zip suffix
    assert!(
        local.contains(&format!("{existing_rel}.zip")),
        "local should contain .zip-normalized path, got: {local:?}",
    );

    // ── Build URLs for a single chunk ──
    let config = data_downloader::config::Config {
        market_type: vec![data_downloader::config::MarketType::Futures],
        timeframes: Some(vec!["1m".into()]),
        start_date: Some("2024-01-01".into()),
        end_date: Some("2024-01-02".into()),
        data_dir: data_dir.clone(),
        ..Default::default()
    };

    let symbols = vec!["BTCUSDT".into()];
    let timeframes = config.timeframes.as_deref().unwrap_or(&[]);
    let start_dates: HashMap<String, chrono::NaiveDate> = HashMap::new();
    let chunk_start = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let chunk_end = chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();

    let tasks = data_downloader::task::build_tasks(
        &symbols,
        timeframes,
        &config,
        &start_dates,
        chunk_start,
        chunk_end,
        &local,
    );

    // 1 symbol × 1 type × 1 tf × 2 days = 2 tasks, but 1 day filtered by local
    assert_eq!(tasks.len(), 1, "should skip the already-downloaded date");
    assert!(tasks[0].url.contains("2024-01-02"));

    // ── Download URL looks correct ──
    assert!(
        tasks[0]
            .url
            .starts_with("https://data.binance.vision/data/")
    );
    assert!(tasks[0].url.ends_with(".zip"));

    // ── CSV → Parquet conversion pipeline ──
    let csv_bytes = b"open_time,open,high,low,close\n100,50000.0,50100.0,49900.0,50050.0\n";
    let dest = data_dir.join("test.parquet");

    let df =
        data_downloader::downloader::csv_to_dataframe::csv_to_dataframe_sync(csv_bytes).unwrap();
    let df = data_downloader::downloader::csv_to_dataframe::set_column_types::convert_types(&df)
        .unwrap();
    let df = data_downloader::downloader::data_clean::clean_dataframe(df).unwrap();
    data_downloader::downloader::dataframe_to_parquet::write_parquet_sync(&dest, df).unwrap();

    assert!(dest.exists(), "parquet file should exist");

    // ── Cleanup ──
    dir.close().unwrap();
}
