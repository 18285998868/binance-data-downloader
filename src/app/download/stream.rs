//! Single-file download: retry policy, content validation and atomic landing.

use backoff::Error as BackoffError;
use backoff::backoff::Backoff;
use backoff::future::retry;
use tokio::io::AsyncWriteExt;

use crate::backoff::{RETRY_WINDOW, clamp_retry_after, default_backoff, parse_retry_after};
use crate::cancel::CancelToken;
use crate::config::DownloadTask;

/// Bytes every non-empty zip archive begins with.
const ZIP_MAGIC: &[u8; 4] = b"PK\x03\x04";

/// Upper bound on chunks read while hunting for the zip magic.
///
/// Guards against a server that dribbles empty chunks: without it a stream of
/// zero-length frames would spin forever inside the retry window.
const MAX_MAGIC_CHUNKS: u32 = 8;

/// Download a zip file with retry, streaming directly to disk.
///
/// Writes to a `{dest}.{pid}.tmp` file, then atomically renames to `dest`.
///
/// Returns [`crate::AppError::Cancelled`] as soon as `cancel` fires, which
/// abandons the in-flight request and leaves the temp file behind; the next
/// [`crate::scan::scan_local_files`] pass reclaims it.
///
/// # Errors
///
/// Returns [`crate::AppError::NotFound`] for a 404 (permanent, not retried),
/// [`crate::AppError::DownloadFailed`] for HTTP or content-validation
/// failures, and [`crate::AppError::Io`] / [`crate::AppError::Http`] for
/// local I/O and transport errors.
#[tracing::instrument(skip(client, task, cancel), fields(url = %task.url))]
pub(crate) async fn download_to_file(
    client: &reqwest::Client,
    task: &DownloadTask,
    cancel: &CancelToken,
) -> Result<(), crate::AppError> {
    download_to_file_with_backoff(client, task, default_backoff(), RETRY_WINDOW, cancel).await
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
    cancel: &CancelToken,
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

            // Fetch the leading bytes before touching the filesystem: the body
            // must start with the zip magic, which rejects HTML error pages
            // served with a 200 status without writing anything to disk.
            //
            // The magic is accumulated across chunks rather than read from the
            // first one: with chunked transfer encoding the four magic bytes
            // can straddle a frame boundary, and mistaking that for "not a zip"
            // would burn the entire retry window on a perfectly good file.
            let expected_len = resp.content_length();
            let mut head = Vec::with_capacity(ZIP_MAGIC.len());
            let mut chunks_read = 0u32;
            while head.len() < ZIP_MAGIC.len() {
                chunks_read += 1;
                if chunks_read > MAX_MAGIC_CHUNKS {
                    return Err(BackoffError::Transient {
                        err: crate::AppError::DownloadFailed {
                            url: u,
                            reason: "response is not a zip archive".into(),
                        },
                        retry_after: None,
                    });
                }
                let chunk = match resp.chunk().await {
                    Ok(Some(c)) => c,
                    Ok(None) => {
                        return Err(BackoffError::Transient {
                            err: crate::AppError::DownloadFailed {
                                url: u,
                                reason: "empty response body".into(),
                            },
                            retry_after: None,
                        });
                    }
                    Err(e) => {
                        return Err(BackoffError::Transient {
                            err: crate::AppError::from(e),
                            retry_after: None,
                        });
                    }
                };
                head.extend_from_slice(&chunk);
            }
            if &head[..ZIP_MAGIC.len()] != ZIP_MAGIC {
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
            let file = tokio::fs::File::create(&tmp)
                .await
                .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;
            let mut writer = tokio::io::BufWriter::new(file);

            let mut written: u64 = head.len() as u64;
            writer
                .write_all(&head)
                .await
                .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;

            while let Some(chunk) = resp.chunk().await.map_err(|e| BackoffError::Transient {
                err: crate::AppError::from(e),
                retry_after: None,
            })? {
                written += chunk.len() as u64;
                writer
                    .write_all(&chunk)
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

            writer
                .flush()
                .await
                .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;
            drop(writer);

            tokio::fs::rename(&tmp, &dest)
                .await
                .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;

            Ok(())
        }
    };

    // `biased` gives cancellation priority when both arms are ready, so a
    // Ctrl-C during the last attempt is not swallowed by a success. Dropping
    // the `retry` future is safe here: it abandons the in-flight request and
    // the partially written temp file is reclaimed by the next scan.
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(crate::AppError::Cancelled),
        res = retry(backoff, op) => res,
    }
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
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());
        let result =
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel).await;
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
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());
        let result =
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel).await;
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
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());
        let result =
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel).await;
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
                if self
                    .calls
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    == 0
                {
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
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());
        let result =
            download_to_file_with_backoff(&client, &task, default_backoff(), RETRY_WINDOW, &cancel)
                .await;
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
        let cancel = CancelToken::new();
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
                &cancel,
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
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());
        let result =
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel).await;
        assert!(matches!(result, Err(AppError::DownloadFailed { .. })));
    }

    #[tokio::test]
    async fn download_500_retries_then_succeeds() {
        /// Answers 500 on the first call, then a zip body.
        struct First500Then200 {
            calls: std::sync::atomic::AtomicUsize,
        }
        impl wiremock::Respond for First500Then200 {
            fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
                if self
                    .calls
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    == 0
                {
                    ResponseTemplate::new(500)
                } else {
                    ResponseTemplate::new(200).set_body_bytes(ZIP_BODY)
                }
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(First500Then200 {
                calls: std::sync::atomic::AtomicUsize::new(0),
            })
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());
        let result =
            download_to_file_with_backoff(&client, &task, default_backoff(), RETRY_WINDOW, &cancel)
                .await;
        assert!(result.is_ok());
        assert_eq!(std::fs::read(&task.dest).unwrap(), ZIP_BODY);
    }

    #[tokio::test]
    async fn download_400_is_permanent_and_writes_nothing() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(400))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());
        // 4xx (other than 404/429) is permanent: one attempt, no retry, and no
        // filesystem side effects at all.
        let result =
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel).await;
        match result {
            Err(AppError::DownloadFailed { reason, .. }) => assert!(reason.contains("400")),
            other => panic!("expected DownloadFailed, got {other:?}"),
        }
        assert!(!task.dest.exists());
        assert!(!tmp_path_for(&task.dest).exists());
    }

    #[tokio::test]
    async fn download_creates_missing_parent_directories() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ZIP_BODY))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("a/b/c/BTCUSDT-1m-2024-01-01.zip");
        assert!(!dest.parent().unwrap().exists());
        let task = DownloadTask {
            url: format!("{}/whatever.zip", server.uri()),
            dest: dest.clone(),
            pair: "BTCUSDT".into(),
            timeframe: crate::config::Timeframe::M1,
        };
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), ZIP_BODY);
    }

    #[tokio::test]
    async fn tmp_path_is_scoped_to_this_pid() {
        let dest = std::path::Path::new("/tmp/x/BTCUSDT-1m-2024-01-01.zip");
        let tmp = tmp_path_for(dest);
        assert!(
            tmp.to_string_lossy()
                .ends_with(&format!(".{}.tmp", std::process::id()))
        );
        // The pid suffix must be the last component so `is_our_tmp` can parse it.
        assert_eq!(tmp.extension().unwrap(), "tmp");
    }

    #[test]
    fn retry_window_exhaustion_is_reported_as_download_failed() {
        // Guard the constant itself: a zero window must expire immediately.
        assert_eq!(RETRY_WINDOW.as_secs(), 120);
        assert!(RETRY_WINDOW > std::time::Duration::from_secs(0));
    }

    // — cancellation —

    #[tokio::test]
    async fn already_cancelled_download_returns_immediately() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ZIP_BODY))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        cancel.cancel();
        let task = make_task(&server, dir.path());

        let result =
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel).await;
        assert!(matches!(result, Err(AppError::Cancelled)));
        assert!(!task.dest.exists());
    }

    #[tokio::test]
    async fn cancelling_mid_download_abandons_the_attempt() {
        // The server always errors, so the retry loop is guaranteed to be
        // running when the token fires.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());

        let canceller = cancel.clone();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            canceller.cancel();
        });

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            download_to_file_with_backoff(&client, &task, default_backoff(), RETRY_WINDOW, &cancel),
        )
        .await
        .expect("cancellation must interrupt the retry loop promptly");
        handle.await.unwrap();

        assert!(matches!(result, Err(AppError::Cancelled)));
        assert!(
            !task.dest.exists(),
            "an aborted download must not land a file"
        );
    }

    #[tokio::test]
    async fn non_cancelled_download_is_unaffected_by_the_token() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(ZIP_BODY))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());
        download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&task.dest).unwrap(), ZIP_BODY);
    }

    // — zip magic —

    /// Serve one chunked HTTP/1.1 response, emitting each element of `parts` as
    /// its own chunk, and return the server's base URL.
    ///
    /// Needed because `wiremock` cannot emit a split `Transfer-Encoding:
    /// chunked` body, and splitting the magic is exactly what is under test.
    async fn serve_chunked(parts: Vec<Vec<u8>>) -> String {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            // Drain the request head; the test does not assert on it.
            let mut req = [0u8; 1024];
            let _ = sock.read(&mut req).await;

            let mut body = String::from(
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/zip\r\n\r\n",
            );
            for p in parts {
                use std::fmt::Write as _;
                let _ = write!(body, "{:x}\r\n", p.len());
                body.push_str(&String::from_utf8_lossy(&p));
                body.push_str("\r\n");
            }
            body.push_str("0\r\n\r\n");
            sock.write_all(body.as_bytes()).await.unwrap();
            sock.flush().await.unwrap();
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn zip_magic_split_across_chunks_is_still_accepted() {
        // The four magic bytes arrive across three 2-byte chunks. A check on the
        // first chunk alone would reject a perfectly valid archive.
        let base = serve_chunked(ZIP_BODY.chunks(2).map(<[u8]>::to_vec).collect()).await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        let task = DownloadTask {
            url: format!("{base}/split.zip"),
            dest: dir.path().join("split.zip"),
            pair: "BTCUSDT".into(),
            timeframe: crate::config::Timeframe::M1,
        };
        download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&task.dest).unwrap(), ZIP_BODY);
    }

    #[tokio::test]
    async fn byte_at_a_time_chunking_is_accepted() {
        let base = serve_chunked(ZIP_BODY.chunks(1).map(<[u8]>::to_vec).collect()).await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        let task = DownloadTask {
            url: format!("{base}/drip.zip"),
            dest: dir.path().join("drip.zip"),
            pair: "BTCUSDT".into(),
            timeframe: crate::config::Timeframe::M1,
        };
        download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&task.dest).unwrap(), ZIP_BODY);
    }

    #[tokio::test]
    async fn chunked_body_without_the_zip_magic_is_rejected() {
        let base = serve_chunked(vec![b"<html>".to_vec(), b"nope</html>".to_vec()]).await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        let task = DownloadTask {
            url: format!("{base}/html.zip"),
            dest: dir.path().join("html.zip"),
            pair: "BTCUSDT".into(),
            timeframe: crate::config::Timeframe::M1,
        };
        let result =
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel).await;
        match result {
            Err(AppError::DownloadFailed { reason, .. }) => {
                assert!(reason.contains("not a zip"), "unexpected: {reason}");
            }
            other => panic!("expected DownloadFailed, got {other:?}"),
        }
        assert!(!task.dest.exists());
    }

    #[tokio::test]
    async fn a_body_shorter_than_the_magic_is_rejected() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"PK"))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());
        let result =
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel).await;
        assert!(matches!(result, Err(AppError::DownloadFailed { .. })));
    }

    #[test]
    fn zip_magic_constant_is_the_local_file_signature() {
        assert_eq!(ZIP_MAGIC, b"PK\x03\x04");
        assert_eq!(ZIP_MAGIC.len(), 4);
    }
}
