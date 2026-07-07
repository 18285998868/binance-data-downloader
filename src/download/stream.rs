use backoff::Error as BackoffError;
use backoff::backoff::Backoff;
use backoff::future::retry;

use crate::DownloadTask;
use crate::backoff::default_backoff;

/// Download a zip file with retry, streaming directly to disk.
///
/// Writes to a `.zip.tmp` file, then atomically renames to `.zip`.
pub async fn download_to_file(
    client: &reqwest::Client,
    task: &DownloadTask,
) -> Result<(), crate::AppError> {
    download_to_file_with_backoff(client, task, default_backoff()).await
}

async fn download_to_file_with_backoff(
    client: &reqwest::Client,
    task: &DownloadTask,
    backoff: impl Backoff + 'static,
) -> Result<(), crate::AppError> {
    let op = || {
        let c = client.clone();
        let u = task.url.clone();
        let dest = task.dest.clone();
        async move {
            let mut resp = match c.get(&u).send().await {
                Ok(r) => r,
                Err(e) => {
                    return Err(BackoffError::Transient {
                        err: crate::AppError::from(e),
                        retry_after: None,
                    });
                }
            };
            let status = resp.status();
            if !status.is_success() {
                return if status == reqwest::StatusCode::NOT_FOUND {
                    Err(BackoffError::Permanent(crate::AppError::NotFound {
                        url: u,
                    }))
                } else {
                    Err(BackoffError::Transient {
                        err: crate::AppError::DownloadFailed {
                            url: u,
                            reason: format!("HTTP {status}"),
                        },
                        retry_after: None,
                    })
                };
            }

            if let Some(p) = dest.parent() {
                tokio::fs::create_dir_all(p).await.map_err(|e| {
                    BackoffError::Permanent(crate::AppError::Io(e))
                })?;
            }

            let tmp_path = dest.with_extension("zip.tmp");
            let mut file = tokio::fs::File::create(&tmp_path).await.map_err(|e| {
                BackoffError::Permanent(crate::AppError::Io(e))
            })?;

            use tokio::io::AsyncWriteExt;
            while let Some(chunk) = resp.chunk().await.map_err(|e| {
                BackoffError::Transient {
                    err: crate::AppError::from(e),
                    retry_after: None,
                }
            })? {
                file.write_all(&chunk).await.map_err(|e| {
                    BackoffError::Permanent(crate::AppError::Io(e))
                })?;
            }

            file.flush().await.map_err(|e| {
                BackoffError::Permanent(crate::AppError::Io(e))
            })?;
            drop(file);

            tokio::fs::rename(&tmp_path, &dest).await.map_err(|e| {
                BackoffError::Permanent(crate::AppError::Io(e))
            })?;

            Ok(())
        }
    };

    retry(backoff, op).await
}
