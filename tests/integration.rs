#[tokio::test]
async fn pipeline_local_scan_to_urls_to_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");

    let existing_rel = "futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01";
    let parquet_path = data_dir.join(format!("{existing_rel}.parquet"));
    std::fs::create_dir_all(parquet_path.parent().unwrap()).unwrap();
    {
        let empty =
            binance_data_downloader::task::download::csv_to_dataframe_sync(
                b"open_time,open,high,low,close
1,100,101,99,100
",
            )
            .unwrap();
        binance_data_downloader::task::download::df_to_parquet::write_parquet_sync(
            &parquet_path,
            empty,
        )
        .unwrap();
    }

    let local = binance_data_downloader::task::get_local_files::scan_local_files(&data_dir)
        .await
        .unwrap();

    assert!(
        local.contains(&format!("{existing_rel}.zip")),
        "local should contain .zip-normalized path, got: {local:?}",
    );

    use binance_data_downloader::task::urls;
    let chunk_start = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    let chunk_end = chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();

    let tasks: Vec<_> = urls::build_urls("BTCUSDT", "1m", chunk_start, chunk_end)
        .into_iter()
        .filter(|dl| !local.contains(&dl.rel_path))
        .map(|dl| binance_data_downloader::task::DownloadTask {
            url: dl.url.clone(),
            dest: data_dir.join(&dl.rel_path),
            pair: dl.pair,
            timeframe: dl.timeframe,
        })
        .collect();

    assert_eq!(tasks.len(), 1, "should skip the already-downloaded date");
    assert!(tasks[0].url.contains("2024-01-02"));
    assert!(
        tasks[0]
            .url
            .starts_with("https://data.binance.vision/data/")
    );
    assert!(tasks[0].url.ends_with(".zip"));
}
