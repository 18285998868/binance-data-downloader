mod csv_set_col_types;
mod csv_table_title;
pub mod df_to_parquet;
mod download_bytes;
mod extract_zip;

use polars::prelude::*;

use crate::task::DownloadTask;
use crate::AppError;
use futures::stream::{self, StreamExt};
use tracing::Instrument;

pub use download_bytes::download_bytes_with_retry;

/// Download all tasks in parallel with a concurrency limit.
pub async fn run_downloads(
    client: &reqwest::Client,
    tasks: Vec<DownloadTask>,
    max_concurrent: usize,
) -> Result<(), AppError> {
    let total = tasks.len();
    if total == 0 {
        return Ok(());
    }
    let max_concurrent = max_concurrent.min(256);
    tracing::debug!(
        total = total,
        concurrency = max_concurrent,
        "downloading new files",
    );
    let results: Vec<_> = stream::iter(tasks.into_iter().map(|t| {
        let c = client.clone();
        let span = tracing::info_span!("download", pair = %t.pair, tf = %t.timeframe);
        async move { pipeline_inner(&c, &t).await }.instrument(span)
    }))
    .buffer_unordered(max_concurrent)
    .collect()
    .await;

    let (ok, nf) = results.iter().fold((0, 0), |(ok, nf), r| match r {
        Ok(_) => (ok + 1, nf),
        Err(AppError::NotFound { .. }) => (ok, nf + 1),
        _ => (ok, nf),
    });
    let fail = results.len() - ok - nf;
    if nf > 0 {
        tracing::debug!(
            not_found = nf,
            total = total,
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
            reason: format!("{fail}/{total} failed"),
        });
    }
    Ok(())
}

async fn pipeline_inner(
    client: &reqwest::Client,
    task: &DownloadTask,
) -> Result<(), AppError> {
    let zip_bytes = download_bytes::download_bytes_with_retry(client, task).await?;
    let task_dest = task.dest.with_extension("parquet");

    tokio::task::spawn_blocking(move || {
        let csv_bytes = extract_zip::extract_csv_from_bytes(&zip_bytes)?;
        let df = csv_to_dataframe_sync(&csv_bytes)?;
        let df = csv_set_col_types::convert_types(&df)?;
        df_to_parquet::write_parquet_sync(&task_dest, df)?;
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
        let content =
            std::str::from_utf8(bytes).map_err(|e| AppError::Utf8(e.to_string()))?;
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
}
