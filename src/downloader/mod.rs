pub mod csv_to_dataframe;
pub mod data_clean;
pub mod dataframe_to_parquet;
pub mod extract_zip_from_bytes;
pub mod http;

use crate::task::DownloadTask;
use futures::stream::{self, StreamExt};

pub async fn run_downloads(
    client: &reqwest::Client,
    tasks: Vec<DownloadTask>,
    max_concurrent: usize,
) -> Result<(), crate::AppError> {
    let total = tasks.len();
    if total == 0 {
        return Ok(());
    }
    log::debug!(
        "downloading {} new files with concurrency={}",
        total,
        max_concurrent
    );
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
    if nf > 0 {
        log::debug!(
            "{} / {} files not found on server (404, expected for edge dates)",
            nf,
            total
        );
    }

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

async fn pipeline_inner(
    client: &reqwest::Client,
    task: &DownloadTask,
) -> Result<(), crate::AppError> {
    let zip_bytes = http::download_bytes_with_retry(client, task).await?;
    let task_dest = task.dest.with_extension("parquet");

    tokio::task::spawn_blocking(move || {
        let csv_bytes = extract_zip_from_bytes::extract_csv_from_bytes(&zip_bytes)?;
        let df = csv_to_dataframe::csv_to_dataframe_sync(&csv_bytes)?;
        let df = csv_to_dataframe::set_column_types::convert_types(&df)?;
        let df = data_clean::clean_dataframe(df)?;
        dataframe_to_parquet::write_parquet_sync(&task_dest, df)?;
        Ok::<_, crate::AppError>(())
    })
    .await
    .map_err(|e| crate::AppError::Join(format!("spawn: {e}")))?
}
