//! Single-file download: retry policy, content validation and atomic landing.

use tokio::io::AsyncWriteExt;

use super::validate;
use crate::backoff::{
    RETRY_WINDOW, RetryError, RetryPolicy, default_retry_policy, is_transient_status,
    parse_retry_after, retry_operation,
};
use crate::cancel::CancelToken;
use crate::download::tasks::DownloadTask;

/// Download a zip file with retry, streaming directly to disk.
///
/// Writes to a `{dest}.{pid}.tmp` file, then atomically renames to `dest`.
/// A failed or cancelled attempt removes its temp file immediately; a process
/// killed mid-download can still leave one behind, which the next
/// [`crate::scan::scan_local_files`] pass reclaims after the liveness window.
///
/// When `verify` is set, the finished file is additionally opened as a zip
/// archive and checked to hold at least one entry; a corrupt archive is
/// retried like any other transient failure.
///
/// Returns [`crate::AppError::Cancelled`] as soon as `cancel` fires, which
/// abandons the in-flight request without landing a partial destination file.
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
    verify: bool,
) -> Result<(), crate::AppError> {
    download_to_file_with_backoff(
        client,
        task,
        default_retry_policy(),
        RETRY_WINDOW,
        cancel,
        verify,
    )
    .await
}

/// Make a completed `rename` durable by fsyncing the containing directory.
///
/// Unix needs this separate step because the file's own `fsync` only covers its
/// data and inode, not the directory entry that makes the new name visible.
/// Windows has no portable equivalent through `std`; the file handle's
/// `sync_all` already covers the data there.
async fn sync_parent_dir(path: &std::path::Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let parent = path.parent().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has no parent")
        })?;
        let dir = tokio::fs::File::open(parent).await?;
        dir.sync_all().await
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

/// Temp path for an in-flight download: `{dest}.{pid}.tmp`.
///
/// The pid suffix makes the owner visible to the scan pass and prevents two
/// processes that happen to target the same path from clobbering each other's
/// temp file.
fn tmp_path_for(dest: &std::path::Path) -> std::path::PathBuf {
    let mut p = dest.as_os_str().to_os_string();
    p.push(format!(".{}.tmp", std::process::id()));
    std::path::PathBuf::from(p)
}

/// Removes a half-written temp file unless the download is renamed into place.
///
/// The guard is declared before the writer so drop order closes the file
/// first. This also covers cancellation: dropping the retry future drops the
/// guard, so a Ctrl-C leaves no `.tmp` behind.
struct TempFileGuard {
    path: std::path::PathBuf,
    keep: bool,
}

impl TempFileGuard {
    fn new(path: std::path::PathBuf) -> Self {
        Self { path, keep: false }
    }

    /// Mark the temp file as successfully renamed; `Drop` then does nothing.
    fn keep(mut self) {
        self.keep = true;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if !self.keep {
            // One unlink on the failure path. Ignore the error: a process killed
            // before this point is covered by the scan pass, and a missing file
            // means another actor already cleaned it up.
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Retried download with an injectable total retry `window` (production
/// passes [`RETRY_WINDOW`]; tests use a short window to exercise exhaustion).
async fn download_to_file_with_backoff(
    client: &reqwest::Client,
    task: &DownloadTask,
    policy: RetryPolicy,
    window: std::time::Duration,
    cancel: &CancelToken,
    verify: bool,
) -> Result<(), crate::AppError> {
    let tmp_path = tmp_path_for(&task.dest);
    let deadline = std::time::Instant::now() + window;
    let op = || {
        let c = client.clone();
        let u = task.url.clone();
        let dest = task.dest.clone();
        let tmp = tmp_path.clone();
        async move {
            // Do not start another attempt after the retry window. The shared
            // retry loop caps every backon/Retry-After delay at the same
            // deadline; this check also covers a retry that became ready just
            // at the cutoff. An already in-flight request is bounded by the
            // client's 300-second whole-request timeout instead.
            if std::time::Instant::now() >= deadline {
                return Err(RetryError::Permanent(crate::AppError::DownloadFailed {
                    url: u,
                    reason: "retry window elapsed".into(),
                }));
            }
            let mut resp = match c.get(&u).send().await {
                Ok(r) => r,
                Err(e) => {
                    return Err(RetryError::Transient {
                        err: crate::AppError::from(e),
                        retry_after: None,
                    });
                }
            };
            let status = resp.status();
            if status != reqwest::StatusCode::OK {
                // Only a full `200 OK` is a complete archive. In particular,
                // accepting every 2xx would let an unexpected `206 Partial
                // Content` land a truncated file with a valid zip magic.
                return if status == reqwest::StatusCode::NOT_FOUND {
                    Err(RetryError::Permanent(crate::AppError::NotFound { url: u }))
                } else if is_transient_status(status) {
                    Err(RetryError::Transient {
                        err: crate::AppError::DownloadFailed {
                            url: u,
                            reason: format!("HTTP {status}"),
                        },
                        retry_after: parse_retry_after(&resp),
                    })
                } else {
                    Err(RetryError::Permanent(crate::AppError::DownloadFailed {
                        url: u,
                        reason: format!("HTTP {status}"),
                    }))
                };
            }

            // Fetch the leading bytes before touching the filesystem: the body
            // must start with the zip magic, which rejects HTML error pages
            // served with a 200 status without writing anything to disk.
            let expected_len = resp.content_length();
            let head = match validate::read_zip_head(&mut resp, &u).await {
                Ok(head) => head,
                Err(e) => {
                    return Err(RetryError::Transient {
                        err: e,
                        retry_after: None,
                    });
                }
            };

            if let Some(p) = dest.parent() {
                tokio::fs::create_dir_all(p)
                    .await
                    .map_err(|e| RetryError::Permanent(crate::AppError::Io(e)))?;
            }

            match tokio::fs::remove_file(&tmp).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(RetryError::Permanent(crate::AppError::Io(e))),
            }
            // Declared before the writer: locals drop in reverse order, so the
            // file is closed before the guard tries to unlink it.
            let temp_guard = TempFileGuard::new(tmp.clone());
            let file = tokio::fs::File::create(&tmp)
                .await
                .map_err(|e| RetryError::Permanent(crate::AppError::Io(e)))?;
            let mut writer = tokio::io::BufWriter::new(file);

            let mut written: u64 = head.len() as u64;
            writer
                .write_all(&head)
                .await
                .map_err(|e| RetryError::Permanent(crate::AppError::Io(e)))?;

            while let Some(chunk) = resp.chunk().await.map_err(|e| RetryError::Transient {
                err: crate::AppError::from(e),
                retry_after: None,
            })? {
                written += chunk.len() as u64;
                writer
                    .write_all(&chunk)
                    .await
                    .map_err(|e| RetryError::Permanent(crate::AppError::Io(e)))?;
            }

            // The body must match Content-Length when the server provided it.
            if let Err(e) = validate::check_content_length(written, expected_len, &u) {
                return Err(RetryError::Transient {
                    err: e,
                    retry_after: None,
                });
            }

            writer
                .flush()
                .await
                .map_err(|e| RetryError::Permanent(crate::AppError::Io(e)))?;
            // Persist the payload before the rename. Without this, a power loss
            // can leave a correctly named file whose contents are empty or
            // partially written.
            writer
                .get_ref()
                .sync_all()
                .await
                .map_err(|e| RetryError::Permanent(crate::AppError::Io(e)))?;
            drop(writer);

            // Optional structural check of the finished file. A corrupt
            // archive is worth another attempt, so it is transient.
            if let Err(e) = validate::check_archive(&tmp, &u, verify).await {
                return Err(RetryError::Transient {
                    err: e,
                    retry_after: None,
                });
            }

            tokio::fs::rename(&tmp, &dest)
                .await
                .map_err(|e| RetryError::Permanent(crate::AppError::Io(e)))?;
            temp_guard.keep();
            // The rename itself must be durable too; on Unix that requires an
            // fsync of the directory entry.
            sync_parent_dir(&dest)
                .await
                .map_err(|e| RetryError::Permanent(crate::AppError::Io(e)))?;

            Ok(())
        }
    };

    // `biased` gives cancellation priority when both arms are ready, so a
    // Ctrl-C during the last attempt is not swallowed by a success. Dropping
    // the retry future abandons the in-flight request; the attempt's
    // `TempFileGuard` then removes its partial file as it drops.
    tokio::select! {
        biased;
        () = cancel.cancelled() => Err(crate::AppError::Cancelled),
        res = retry_operation(policy, deadline, op) => res,
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

    /// Policy that never retries (one attempt).
    fn no_retry() -> RetryPolicy {
        crate::backoff::no_retry_policy()
    }

    fn m1_job() -> crate::dataset::DownloadJob {
        use crate::dataset::{DatasetKind, Interval, Period};

        crate::dataset::DownloadJob {
            kind: DatasetKind::Klines,
            interval: Some(Interval::M1),
            period: Period::Daily,
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
            job: m1_job(),
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
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, false)
                .await;
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
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, false)
                .await;
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
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, false)
                .await;
        assert!(matches!(result, Err(AppError::NotFound { .. })));
    }

    #[tokio::test]
    async fn download_206_partial_content_is_rejected() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(206).set_body_bytes(ZIP_BODY))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());
        let result =
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, false)
                .await;
        match result {
            Err(AppError::DownloadFailed { reason, .. }) => assert!(reason.contains("206")),
            other => panic!("expected DownloadFailed, got {other:?}"),
        }
        assert!(!task.dest.exists());
        assert!(!tmp_path_for(&task.dest).exists());
    }

    #[tokio::test]
    async fn download_408_retries_then_succeeds() {
        /// Answers 408 on the first call, then a zip body.
        struct First408Then200 {
            calls: std::sync::atomic::AtomicUsize,
        }
        impl wiremock::Respond for First408Then200 {
            fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
                if self
                    .calls
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    == 0
                {
                    ResponseTemplate::new(408).insert_header("Retry-After", "0")
                } else {
                    ResponseTemplate::new(200).set_body_bytes(ZIP_BODY)
                }
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(First408Then200 {
                calls: std::sync::atomic::AtomicUsize::new(0),
            })
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());
        let result = download_to_file_with_backoff(
            &client,
            &task,
            default_retry_policy(),
            RETRY_WINDOW,
            &cancel,
            false,
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(std::fs::read(&task.dest).unwrap(), ZIP_BODY);
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
        let result = download_to_file_with_backoff(
            &client,
            &task,
            default_retry_policy(),
            RETRY_WINDOW,
            &cancel,
            false,
        )
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
                default_retry_policy(),
                std::time::Duration::from_millis(100),
                &cancel,
                false,
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
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, false)
                .await;
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
        let result = download_to_file_with_backoff(
            &client,
            &task,
            default_retry_policy(),
            RETRY_WINDOW,
            &cancel,
            false,
        )
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
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, false)
                .await;
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
            job: m1_job(),
        };
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, false)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), ZIP_BODY);
    }

    #[test]
    fn temp_file_guard_removes_the_file_unless_kept() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial.zip.1.tmp");

        std::fs::write(&path, b"partial").unwrap();
        drop(TempFileGuard::new(path.clone()));
        assert!(!path.exists(), "a failed attempt must remove its temp file");

        std::fs::write(&path, b"complete").unwrap();
        TempFileGuard::new(path.clone()).keep();
        assert!(path.exists(), "a renamed file must not be removed");
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

    #[tokio::test]
    async fn retry_window_exhaustion_is_reported_as_download_failed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());
        // The first backoff delay is clamped to the short window; the next
        // attempt then short-circuits on the deadline check.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            download_to_file_with_backoff(
                &client,
                &task,
                default_retry_policy(),
                std::time::Duration::from_millis(100),
                &cancel,
                false,
            ),
        )
        .await
        .expect("the retry loop must stop at the window");
        match result {
            Err(AppError::DownloadFailed { reason, .. }) => {
                assert!(reason.contains("retry window elapsed"), "{reason}");
            }
            other => panic!("expected a download failure, got {other:?}"),
        }
        assert!(!task.dest.exists());
        assert!(!tmp_path_for(&task.dest).exists());
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
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, false)
                .await;
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
            download_to_file_with_backoff(
                &client,
                &task,
                default_retry_policy(),
                RETRY_WINDOW,
                &cancel,
                false,
            ),
        )
        .await
        .expect("cancellation must interrupt the retry loop promptly");
        handle.await.unwrap();

        assert!(matches!(result, Err(AppError::Cancelled)));
        assert!(
            !task.dest.exists(),
            "an aborted download must not land a file"
        );
        assert!(
            !tmp_path_for(&task.dest).exists(),
            "the temp-file guard must clean up on cancellation"
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
        download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, false)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&task.dest).unwrap(), ZIP_BODY);
    }

    // — zip verification —

    /// A minimal but valid zip archive holding one stored entry.
    fn valid_zip() -> Vec<u8> {
        let mut buf = std::io::Cursor::new(Vec::new());
        {
            let mut writer = zip::ZipWriter::new(&mut buf);
            writer
                .start_file("BTCUSDT-1d.csv", zip::write::SimpleFileOptions::default())
                .unwrap();
            std::io::Write::write_all(&mut writer, b"open_time,open\n").unwrap();
            writer.finish().unwrap();
        }
        buf.into_inner()
    }

    /// A body with a valid zip magic but a broken central directory.
    fn corrupt_zip() -> Vec<u8> {
        let mut bytes = valid_zip();
        // Wreck the central directory, which lives at the end of the file.
        let len = bytes.len();
        bytes[len - 6] = 0xff;
        bytes
    }

    #[tokio::test]
    async fn verify_accepts_a_valid_archive() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(valid_zip()))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());
        download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, true)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&task.dest).unwrap(), valid_zip());
    }

    #[tokio::test]
    async fn verify_rejects_a_corrupt_archive() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(corrupt_zip()))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());
        let result =
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, true)
                .await;
        match result {
            Err(AppError::DownloadFailed { reason, .. }) => {
                assert!(reason.contains("zip verification failed"), "got: {reason}");
            }
            other => panic!("expected DownloadFailed, got {other:?}"),
        }
        // The broken archive must not land.
        assert!(!task.dest.exists());
    }

    #[tokio::test]
    async fn verification_is_opt_in() {
        // The same corrupt body is accepted when the check is off: the magic
        // bytes and the content length are still fine.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(corrupt_zip()))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let client = reqwest::Client::new();
        let cancel = CancelToken::new();
        let task = make_task(&server, dir.path());
        download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, false)
            .await
            .unwrap();
        assert!(task.dest.exists());
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
            job: m1_job(),
        };
        download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, false)
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
            job: m1_job(),
        };
        download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, false)
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
            job: m1_job(),
        };
        let result =
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, false)
                .await;
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
            download_to_file_with_backoff(&client, &task, no_retry(), RETRY_WINDOW, &cancel, false)
                .await;
        assert!(matches!(result, Err(AppError::DownloadFailed { .. })));
    }
}
