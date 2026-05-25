pub(crate) mod fetch;
pub(crate) mod convert;

use futures::stream::{self, StreamExt};

/// Executes a batch of download tasks concurrently, returning on first error.
pub(crate) async fn run_downloads(
    client: &reqwest::Client,
    tasks: Vec<crate::DownloadTask>,
    max_concurrent: usize,
) -> Result<(), crate::AppError> {
    let total = tasks.len();
    let results: Vec<_> = stream::iter(tasks.into_iter().map(|t| {
        let c = client.clone();
        async move { pipeline_inner(&c, &t).await }
    }))
    .buffer_unordered(max_concurrent)
    .collect()
    .await;
    let ok = results.iter().filter(|r| r.is_ok()).count();
    let nf = results
        .iter()
        .filter(|r| matches!(r, Err(crate::AppError::NotFound { .. })))
        .count();
    let fail = results.len() - ok - nf;
    if fail > 0 {
        for r in &results {
            if let Err(e) = r
                && !matches!(e, crate::AppError::NotFound { .. })
            {
                log::warn!("download failed: {e}");
            }
        }
        return Err(crate::AppError::DownloadFailed {
            url: "batch".into(),
            reason: format!("{fail}/{total} failed"),
        });
    }
    Ok(())
}

/// Single-pipeline: download ZIP → extract CSV → parse + cast → write parquet.
/// The ZIP is kept in-memory (no disk I/O for the .zip file).
async fn pipeline_inner(
    client: &reqwest::Client,
    task: &crate::DownloadTask,
) -> Result<(), crate::AppError> {
    // 1. Download ZIP bytes directly to memory
    let zip_bytes =
        crate::download::fetch::download_bytes_with_retry(client, task).await?;

    // 2. Extract CSV from in-memory ZIP
    let csv_bytes = crate::download::fetch::extract_csv_from_bytes(&zip_bytes)
        .map_err(|e| crate::AppError::Api(format!("extract: {e}")))?;

    // 3. Parse CSV → DataFrame
    let df = crate::download::convert::csv_to_dataframe(&csv_bytes)
        .await
        .map_err(|e| crate::AppError::Api(format!("csv read: {e}")))?;

    // 4. Cast column types
    let df = crate::download::convert::convert_types(&df)
        .map_err(|e| crate::AppError::Api(format!("convert: {e}")))?;

    // 5. Data cleaning pipeline (dedup, OHLC validation, null handling)
    let df = crate::download::convert::clean_dataframe(df)
        .map_err(|e| crate::AppError::Api(format!("clean: {e}")))?;

    // 6. Atomic parquet write (uses .parquet.tmp + rename internally)
    crate::download::convert::write_parquet(&task.dest.with_extension("parquet"), df)
        .await
        .map_err(|e| crate::AppError::Api(format!("parquet write: {e}")))?;

    Ok(())
}
