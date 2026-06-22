use backoff::Error as BackoffError;
use backoff::backoff::Backoff;
use backoff::future::retry;

use crate::DownloadTask;
use common::default_backoff::default_backoff;

/// Download a file with exponential backoff retry, returning raw bytes.
pub async fn download_bytes_with_retry(
    client: &reqwest::Client,
    task: &DownloadTask,
) -> Result<Vec<u8>, crate::AppError> {
    download_bytes_with_backoff(client, task, default_backoff()).await
}

/// Same as [`download_bytes_with_retry`] but with configurable backoff (for testing).
async fn download_bytes_with_backoff(
    client: &reqwest::Client,
    task: &DownloadTask,
    backoff: impl Backoff + 'static,
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
                return Err(BackoffError::Permanent(crate::AppError::DownloadFailed {
                    url: u,
                    reason: "empty response body".into(),
                }));
            }
            Ok(bytes.to_vec())
        }
    };

    retry(backoff, op).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn no_retry() -> backoff::ExponentialBackoff {
        backoff::ExponentialBackoff {
            current_interval: std::time::Duration::ZERO,
            initial_interval: std::time::Duration::ZERO,
            max_elapsed_time: Some(std::time::Duration::ZERO),
            ..Default::default()
        }
    }

    fn make_task(server: &MockServer) -> DownloadTask {
        DownloadTask {
            url: format!(
                "{}/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip",
                server.uri()
            ),
            dest: std::path::PathBuf::from("/tmp"),
            pair: "BTCUSDT".into(),
            timeframe: "1m".into(),
        }
    }

    #[tokio::test]
    async fn download_success() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"zipdata"))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let task = make_task(&server);
        let result = download_bytes_with_backoff(&client, &task, no_retry())
            .await
            .unwrap();
        assert_eq!(result, b"zipdata");
    }

    #[tokio::test]
    async fn download_404_returns_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let task = make_task(&server);
        let result = download_bytes_with_backoff(&client, &task, no_retry()).await;
        assert!(matches!(result, Err(crate::AppError::NotFound { .. })));
    }

    #[tokio::test]
    async fn download_500_returns_download_failed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let task = make_task(&server);
        let result = download_bytes_with_backoff(&client, &task, no_retry()).await;
        assert!(matches!(
            result,
            Err(crate::AppError::DownloadFailed { .. })
        ));
    }

    #[tokio::test]
    async fn download_empty_body_returns_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b""))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let task = make_task(&server);
        let result = download_bytes_with_backoff(&client, &task, no_retry()).await;
        match result {
            Err(crate::AppError::DownloadFailed { reason, .. }) => {
                assert!(reason.contains("empty"));
            }
            other => panic!("expected DownloadFailed, got: {other:?}"),
        }
    }
}
