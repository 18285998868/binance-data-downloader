mod csv_set_col_types;
mod csv_table_title;
pub mod df_to_parquet;
mod download_bytes;
mod extract_zip;

use std::future::Future;

use polars::prelude::*;

use crate::AppError;
use crate::task::DownloadTask;
use futures::stream::{FuturesUnordered, StreamExt};
use tracing::Instrument;

pub use download_bytes::download_bytes_with_retry;

/// Download all tasks in parallel — no artificial concurrency limit.
///
/// HTTP connection pooling is handled internally by `reqwest::Client`.
pub async fn run_downloads(
    client: &reqwest::Client,
    tasks: Vec<DownloadTask>,
) -> Result<(), AppError> {
    let download_fn = move |c: reqwest::Client, t: DownloadTask| async move {
        let zip_bytes = download_bytes::download_bytes_with_retry(&c, &t).await?;
        process_zip(zip_bytes, t.dest.with_extension("parquet")).await
    };
    run_downloads_inner(client, tasks, download_fn).await
}

/// Core download orchestration, parameterized by the download function for testing.
async fn run_downloads_inner<F, Fut>(
    client: &reqwest::Client,
    tasks: Vec<DownloadTask>,
    download_fn: F,
) -> Result<(), AppError>
where
    F: Fn(reqwest::Client, DownloadTask) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), AppError>> + Send,
{
    let total = tasks.len();
    if total == 0 {
        return Ok(());
    }
    tracing::debug!(total = total, "downloading new files");
    let results: Vec<_> = tasks
        .into_iter()
        .map(|t| {
            let c = client.clone();
            let span = tracing::info_span!("download", pair = %t.pair, tf = %t.timeframe);
            let fut = download_fn(c, t);
            async move { fut.await }.instrument(span)
        })
        .collect::<FuturesUnordered<_>>()
        .collect()
        .await;

    aggregate_results(results)
}

/// Aggregate per-task results into a batch-level outcome.
fn aggregate_results(results: Vec<Result<(), AppError>>) -> Result<(), AppError> {
    let (ok, nf) = results.iter().fold((0, 0), |(ok, nf), r| match r {
        Ok(_) => (ok + 1, nf),
        Err(AppError::NotFound { .. }) => (ok, nf + 1),
        _ => (ok, nf),
    });
    let fail = results.len() - ok - nf;
    if nf > 0 {
        tracing::debug!(
            not_found = nf,
            total = results.len(),
            "files not found on server (404, expected for edge dates)",
        );
    }

    if fail > 0 {
        for r in &results {
            if let Err(e) = r
                && !matches!(e, AppError::NotFound { .. })
            {
                tracing::warn!("download failed: {e}");
            }
        }
        return Err(AppError::DownloadFailed {
            url: "batch".into(),
            reason: format!("{fail}/{} failed", results.len()),
        });
    }
    Ok(())
}

/// Process downloaded zip bytes: extract → parse → convert → write parquet.
async fn process_zip(zip_bytes: Vec<u8>, dest: std::path::PathBuf) -> Result<(), AppError> {
    tokio::task::spawn_blocking(move || {
        let csv_bytes = extract_zip::extract_csv_from_bytes(&zip_bytes)?;
        let df = csv_to_dataframe_sync(&csv_bytes)?;
        let df = csv_set_col_types::convert_types(&df)?;
        df_to_parquet::write_parquet_sync(&dest, df)?;
        Ok::<_, AppError>(())
    })
    .await
    .map_err(AppError::from)?
}

/// Parse raw CSV bytes into a Polars DataFrame, auto-detecting headers.
pub fn csv_to_dataframe_sync(bytes: &[u8]) -> Result<DataFrame, AppError> {
    let has_header = csv_table_title::detect_header(bytes);
    let opts = CsvReadOptions::default().with_infer_schema_length(Some(0));

    if has_header {
        opts.into_reader_with_file_handle(std::io::Cursor::new(bytes))
            .finish()
            .map_err(|e| AppError::Data(format!("csv parse: {e}")))
    } else {
        let content = std::str::from_utf8(bytes)?;
        let num_cols = content
            .lines()
            .next()
            .map(|line| line.split(",").count())
            .unwrap_or(0);
        let header = csv_table_title::default_header(num_cols).join(",");
        let csv_content = format!("{header}\n{content}");
        opts.into_reader_with_file_handle(std::io::Cursor::new(csv_content.as_bytes()))
            .finish()
            .map_err(|e| AppError::Data(format!("csv parse: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppError;
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn parse_csv_with_header() {
        let csv = b"open_time,open,high,low,close\n1,100.0,101.0,99.0,100.5\n";
        let df = csv_to_dataframe_sync(csv).unwrap();
        assert_eq!(df.width(), 5);
        assert_eq!(df.height(), 1);
    }

    #[test]
    fn parse_csv_without_header() {
        let csv = b"1,100.0,101.0,99.0,100.5\n2,101.0,102.0,100.0,101.5\n";
        let df = csv_to_dataframe_sync(csv).unwrap();
        assert_eq!(df.width(), 5);
        assert_eq!(df.height(), 2);
    }

    #[test]
    fn parse_empty_csv_returns_error() {
        let csv = b"";
        assert!(csv_to_dataframe_sync(csv).is_err());
    }

    // — aggregate_results —

    #[test]
    fn aggregate_all_ok() {
        let results = vec![Ok(()), Ok(()), Ok(())];
        assert!(aggregate_results(results).is_ok());
    }

    #[test]
    fn aggregate_not_found_only_ok() {
        let results = vec![
            Ok(()),
            Err(AppError::NotFound {
                url: "http://x".into(),
            }),
            Err(AppError::NotFound {
                url: "http://y".into(),
            }),
        ];
        // 1 ok + 2 not_found → 0 fail → Ok
        assert!(aggregate_results(results).is_ok());
    }

    #[test]
    fn aggregate_one_fail_returns_err() {
        let results = vec![
            Ok(()),
            Err(AppError::DownloadFailed {
                url: "a".into(),
                reason: "boom".into(),
            }),
        ];
        let err = aggregate_results(results).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("1/2 failed"));
    }

    #[test]
    fn aggregate_mix_not_found_and_fail() {
        let results = vec![
            Ok(()),
            Err(AppError::NotFound {
                url: "http://x".into(),
            }),
            Err(AppError::DownloadFailed {
                url: "b".into(),
                reason: "timeout".into(),
            }),
        ];
        // 1 ok + 1 nf + 1 fail → fail = 1
        let err = aggregate_results(results).unwrap_err();
        assert!(err.to_string().contains("1/3 failed"));
    }

    #[test]
    fn aggregate_empty_vec_ok() {
        assert!(aggregate_results(vec![]).is_ok());
    }

    // — run_downloads_inner —

    #[tokio::test]
    async fn run_downloads_empty_tasks_returns_ok() {
        let client = reqwest::Client::new();
        let tasks = vec![];
        let download_fn = |_: reqwest::Client, _: DownloadTask| async { unreachable!() };
        assert!(
            run_downloads_inner(&client, tasks, download_fn)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn run_downloads_all_success() {
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b")];
        let call_count = Arc::new(AtomicUsize::new(0));
        let cnt = call_count.clone();
        let download_fn = move |_: reqwest::Client, _: DownloadTask| {
            let cnt = cnt.clone();
            async move {
                cnt.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };
        assert!(
            run_downloads_inner(&client, tasks, download_fn)
                .await
                .is_ok()
        );
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn run_downloads_not_found_tolerated() {
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b")];
        let download_fn = |_: reqwest::Client, t: DownloadTask| async move {
            if t.pair == "a" {
                Err(AppError::NotFound {
                    url: "http://x".into(),
                })
            } else {
                Ok(())
            }
        };
        assert!(
            run_downloads_inner(&client, tasks, download_fn)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn run_downloads_one_fail_returns_err() {
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b")];
        let download_fn = |_: reqwest::Client, t: DownloadTask| async move {
            if t.pair == "b" {
                Err(AppError::DownloadFailed {
                    url: "x".into(),
                    reason: "bad".into(),
                })
            } else {
                Ok(())
            }
        };
        let err = run_downloads_inner(&client, tasks, download_fn)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("1/2 failed"));
    }

    // — process_zip —

    #[tokio::test]
    async fn process_zip_valid_kline() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("BTCUSDT-1m-2024-01-01.parquet");

        let csv = b"open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n1,100.0,101.0,99.0,100.0,1000.0,2,10000.0,10,500.0,5000.0,0\n";
        let zip_bytes = build_csv_zip("data.csv", csv);

        process_zip(zip_bytes, dest.clone()).await.unwrap();
        assert!(dest.exists());

        let df = polars::prelude::ParquetReader::new(std::fs::File::open(&dest).unwrap())
            .finish()
            .unwrap();
        assert_eq!(df.width(), 12);
        assert_eq!(df.height(), 1);
    }

    #[tokio::test]
    async fn process_zip_invalid_zip_returns_err() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("bad.parquet");
        let result = process_zip(b"not a zip".to_vec(), dest).await;
        assert!(result.is_err());
    }

    // — helpers —

    fn dummy_task(pair: &str) -> DownloadTask {
        DownloadTask {
            url: format!("http://example.com/{pair}"),
            dest: std::path::PathBuf::from("/tmp"),
            pair: pair.into(),
            timeframe: "1m".into(),
        }
    }

    fn build_csv_zip(name: &str, content: &[u8]) -> Vec<u8> {
        use ::zip::write::FileOptions;
        let buf = std::io::Cursor::new(Vec::new());
        let mut w = ::zip::ZipWriter::new(buf);
        w.start_file::<&str, ()>(name, FileOptions::default())
            .unwrap();
        w.write_all(content).unwrap();
        w.finish().unwrap().into_inner()
    }
}
