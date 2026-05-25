use std::io::Read;
use crate::config::retry::{RetryConfig, retry};
use crate::DownloadTask;

/// Downloads ZIP bytes directly to memory with retry.
/// No .zip file is ever written to disk — saves one write + one read per download.
pub(crate) async fn download_bytes_with_retry(
    client: &reqwest::Client,
    task: &DownloadTask,
) -> Result<Vec<u8>, crate::AppError> {
    retry(
        || {
            let c = client.clone();
            async move {
                let resp = c
                    .get(&task.url)
                    .send()
                    .await
                    .map_err(crate::AppError::Http)?;
                let status = resp.status();
                if !status.is_success() {
                    return if status == reqwest::StatusCode::NOT_FOUND {
                        Err(crate::AppError::NotFound {
                            url: task.url.clone(),
                        })
                    } else {
                        Err(crate::AppError::DownloadFailed {
                            url: task.url.clone(),
                            reason: format!("HTTP {}", status),
                        })
                    };
                }
                let bytes = resp.bytes().await.map_err(crate::AppError::Http)?;
                if bytes.is_empty() {
                    return Err(crate::AppError::DownloadFailed {
                        url: task.url.clone(),
                        reason: "empty".into(),
                    });
                }
                Ok(bytes.to_vec())
            }
        },
        &RetryConfig::default(),
        |e| matches!(e, crate::AppError::Http(_) | crate::AppError::DownloadFailed { .. }),
    )
    .await
}

/// Extracts the CSV entry from an in-memory ZIP archive.
pub(crate) fn extract_csv_from_bytes(zip_bytes: &[u8]) -> Result<Vec<u8>, String> {
    let cursor = std::io::Cursor::new(zip_bytes);
    let mut archive = ::zip::ZipArchive::new(cursor).map_err(|e| format!("zip archive: {e}"))?;
    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|e| format!("entry {i}: {e}"))?;
        if entry.is_dir() {
            continue;
        }
        let name = entry.name();
        if name.ends_with(".csv") || name.ends_with(".CSV") {
            let mut buf = Vec::with_capacity(entry.size() as usize);
            entry.read_to_end(&mut buf).map_err(|e| format!("read csv: {e}"))?;
            return Ok(buf);
        }
    }
    Err("no CSV entry in ZIP".to_string())
}
