use backoff::Error as BackoffError;
use backoff::backoff::Backoff;
use backoff::future::retry;
use tokio::io::AsyncWriteExt;

use crate::config::DownloadTask;
use crate::utils::backoff::{RETRY_WINDOW, clamp_retry_after, default_backoff, parse_retry_after};

/// Download a zip file with retry, streaming directly to disk.
///
/// Writes to a `{dest}.{pid}.tmp` file, then atomically renames to `dest`.
///
/// # Errors
///
/// Returns [`crate::AppError::NotFound`] for a 404 (permanent, not retried),
/// [`crate::AppError::DownloadFailed`] for HTTP or content-validation
/// failures, and [`crate::AppError::Io`] / [`crate::AppError::Http`] for
/// local I/O and transport errors.
pub async fn download_to_file(
    client: &reqwest::Client,
    task: &DownloadTask,
) -> Result<(), crate::AppError> {
    download_to_file_with_backoff(client, task, default_backoff(), RETRY_WINDOW).await
}

/// Temp path for an in-flight download: `{dest}.{pid}.tmp`.
///
/// The pid suffix keeps concurrent runs against the same output directory
/// from writing into each other's temp file.
fn tmp_path_for(dest: &std::path::Path) -> std::path::PathBuf {
    let mut p = dest.as_os_str().to_os_string();
    p.push(format!(".{}.tmp", std::process::id()));
    std::path::PathBuf::from(p)
}

/// Retried download with an injectable total retry `window` (production
/// passes [`RETRY_WINDOW`]; tests use a short window to exercise exhaustion).
async fn download_to_file_with_backoff(
    client: &reqwest::Client,
    task: &DownloadTask,
    backoff: impl Backoff + 'static,
    window: std::time::Duration,
) -> Result<(), crate::AppError> {
    let tmp_path = tmp_path_for(&task.dest);
    let deadline = std::time::Instant::now() + window;
    let op = || {
        let c = client.clone();
        let u = task.url.clone();
        let dest = task.dest.clone();
        let tmp = tmp_path.clone();
        async move {
            // Hard stop: `Retry-After` overrides bypass backoff's own
            // `max_elapsed_time` (see [`RETRY_WINDOW`]); the window is
            // enforced here as well, and the override itself is clamped to
            // the remaining window via `clamp_retry_after`.
            if std::time::Instant::now() >= deadline {
                return Err(BackoffError::Permanent(crate::AppError::DownloadFailed {
                    url: u,
                    reason: "retry window elapsed".into(),
                }));
            }
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
                } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    || status.is_server_error()
                {
                    Err(BackoffError::Transient {
                        err: crate::AppError::DownloadFailed {
                            url: u,
                            reason: format!("HTTP {status}"),
                        },
                        retry_after: clamp_retry_after(parse_retry_after(&resp), deadline),
                    })
                } else {
                    Err(BackoffError::Permanent(crate::AppError::DownloadFailed {
                        url: u,
                        reason: format!("HTTP {status}"),
                    }))
                };
            }

            // Fetch the first chunk before touching the filesystem: the body
            // must start with the zip magic, which rejects HTML error pages
            // served with a 200 status without writing anything to disk.
            let expected_len = resp.content_length();
            let first = resp.chunk().await.map_err(|e| BackoffError::Transient {
                err: crate::AppError::from(e),
                retry_after: None,
            })?;
            let Some(first) = first else {
                return Err(BackoffError::Transient {
                    err: crate::AppError::DownloadFailed {
                        url: u,
                        reason: "empty response body".into(),
                    },
                    retry_after: None,
                });
            };
            if first.len() < 4 || &first[..4] != b"PK\x03\x04" {
                return Err(BackoffError::Transient {
                    err: crate::AppError::DownloadFailed {
                        url: u,
                        reason: "response is not a zip archive".into(),
                    },
                    retry_after: None,
                });
            }

            if let Some(p) = dest.parent() {
                tokio::fs::create_dir_all(p)
                    .await
                    .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;
            }

            match tokio::fs::remove_file(&tmp).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(BackoffError::Permanent(crate::AppError::Io(e))),
            }
            let mut file = tokio::fs::File::create(&tmp)
                .await
                .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;

            let mut written: u64 = first.len() as u64;
            file.write_all(&first)
                .await
                .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;

            while let Some(chunk) = resp.chunk().await.map_err(|e| BackoffError::Transient {
                err: crate::AppError::from(e),
                retry_after: None,
            })? {
                written += chunk.len() as u64;
                file.write_all(&chunk)
                    .await
                    .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;
            }

            // The body must match Content-Length when the server provided it.
            if let Some(expected) = expected_len
                && written != expected
            {
                return Err(BackoffError::Transient {
                    err: crate::AppError::DownloadFailed {
                        url: u,
                        reason: format!("incomplete download: {written}/{expected} bytes"),
                    },
                    retry_after: None,
                });
            }

            file.flush()
                .await
                .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;
            drop(file);

            tokio::fs::rename(&tmp, &dest)
                .await
                .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;

            Ok(())
        }
    };

    retry(backoff, op).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppError;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Minimal body accepted by the zip-magic check.
    const ZIP_BODY: &[u8] = b"PK\x03\x04fake-zip-body";

    /// Backoff that never retries (`max_elapsed_time` = 0).
    fn no_retry() -> backoff::ExponentialBackoff {
        backoff::ExponentialBackoff {
            current_interval: std::time::Duration::ZERO,
            initial_interval: std::time::Duration::ZERO,
            max_elapsed_time: Some(std::time::Duration::ZERO),
            ..Default::default()
        }
    }

    fn make_task(server: &MockServer, dir: &std::path::Path) -> DownloadTask {
        DownloadTask {
            url: format!(
                "{}/data/binance/data/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip",
                server.uri()
            ),
            dest: dir.join("BTCUSDT-1m-2024-01-01.zip"),
            pair: "BTCUSDT".into(),
            timeframe: crate::config::Timeframe::M1,
        }
    }

    #[tokio::test]
    async fn download_success() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/data/binance/data/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ZIP_BODY))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let task = make_task(&server, dir.path());
        let result = download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW).await;
        assert!(result.is_ok());
        assert_eq!(std::fs::read(&task.dest).unwrap(), ZIP_BODY);
        assert!(!tmp_path_for(&task.dest).exists());
    }

    #[tokio::test]
    async fn download_html_200_rejected() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"<html>error</html>"))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let task = make_task(&server, dir.path());
        let result = download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW).await;
        assert!(matches!(result, Err(AppError::DownloadFailed { .. })));
        assert!(!task.dest.exists());
        assert!(!tmp_path_for(&task.dest).exists());
    }

    #[tokio::test]
    async fn download_404_returns_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let task = make_task(&server, dir.path());
        let result = download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW).await;
        assert!(matches!(result, Err(AppError::NotFound { .. })));
    }

    #[tokio::test]
    async fn download_429_with_retry_after_retries_then_succeeds() {
        /// Answers 429 (+ `Retry-After`) on the first call, then a zip body.
        struct First429Then200 {
            calls: std::sync::atomic::AtomicUsize,
        }
        impl wiremock::Respond for First429Then200 {
            fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
                if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(429).insert_header("Retry-After", "0")
                } else {
                    ResponseTemplate::new(200).set_body_bytes(ZIP_BODY)
                }
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(First429Then200 {
                calls: std::sync::atomic::AtomicUsize::new(0),
            })
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let task = make_task(&server, dir.path());
        let result =
            download_to_file_with_backoff(&client, &task, default_backoff(), RETRY_WINDOW).await;
        assert!(result.is_ok());
        assert_eq!(std::fs::read(&task.dest).unwrap(), ZIP_BODY);
    }

    #[tokio::test]
    async fn download_oversized_retry_after_is_clamped_to_the_window() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "999999"))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let task = make_task(&server, dir.path());
        // With the clamp, the retry sleeps at most the (short) window and the
        // next attempt short-circuits on the deadline check. Without it, this
        // would sleep for the full `Retry-After` value and hit the timeout.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            download_to_file_with_backoff(
                &client,
                &task,
                default_backoff(),
                std::time::Duration::from_millis(100),
            ),
        )
        .await;
        let result = result.expect("download did not respect the retry window");
        assert!(matches!(result, Err(AppError::DownloadFailed { .. })));
    }

    #[tokio::test]
    async fn download_500_returns_download_failed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let task = make_task(&server, dir.path());
        let result = download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW).await;
        assert!(matches!(result, Err(AppError::DownloadFailed { .. })));
    }
}
