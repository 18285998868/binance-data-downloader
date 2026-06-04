use backoff::future::retry;
use backoff::Error as BackoffError;

use crate::task::DownloadTask;
use crate::utils::default_backoff::default_backoff;


/// Download a file with exponential backoff retry, returning raw bytes.
pub async fn download_bytes_with_retry(
    client: &reqwest::Client,
    task: &DownloadTask,
) -> Result<Vec<u8>, crate::AppError> {
    let op = || {
        let c = client.clone();
        let u = task.url.clone();
        async move {
            let resp = match c.get(&u).send().await {
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
                    Err(BackoffError::Permanent(crate::AppError::NotFound { url: u }))
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
            let bytes = match resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    return Err(BackoffError::Transient {
                        err: crate::AppError::from(e),
                        retry_after: None,
                    });
                }
            };
            if bytes.is_empty() {
                return Err(BackoffError::Transient {
                    err: crate::AppError::DownloadFailed {
                        url: u,
                        reason: "empty".into(),
                    },
                    retry_after: None,
                });
            }
            Ok(bytes.to_vec())
        }
    };

    retry(default_backoff(), op).await
}
