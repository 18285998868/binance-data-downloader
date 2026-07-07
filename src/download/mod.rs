pub mod stream;

use std::future::Future;

use crate::AppError;
use crate::DownloadTask;
use futures::stream::{FuturesUnordered, StreamExt};
use tracing::Instrument;

/// Download all tasks in parallel with no concurrency limit.
pub async fn run_downloads(
    client: &reqwest::Client,
    tasks: Vec<DownloadTask>,
) -> Result<(), AppError> {
    let download_fn = move |c: reqwest::Client, t: DownloadTask| async move {
        stream::download_to_file(&c, &t).await
    };
    run_downloads_inner(client, tasks, download_fn).await
}

/// Core download orchestration, parameterized by the download function for testing.
async fn run_downloads_inner<F, Fut>(
    client: &reqwest::Client,
    tasks: Vec<DownloadTask>,
    download_fn: F,
) -> Result<(), AppError>
where
    F: Fn(reqwest::Client, DownloadTask) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), AppError>> + Send,
{
    let total = tasks.len();
    if total == 0 {
        return Ok(());
    }
    tracing::debug!(total = total, "downloading new files");
    let results: Vec<_> = tasks
        .into_iter()
        .map(|t| {
            let c = client.clone();
            let span = tracing::info_span!("download", pair = %t.pair, tf = %t.timeframe);
            let fut = download_fn(c, t);
            fut.instrument(span)
        })
        .collect::<FuturesUnordered<_>>()
        .collect()
        .await;

    aggregate_results(results)
}

/// Aggregate per-task results into a batch-level outcome.
fn aggregate_results(results: Vec<Result<(), AppError>>) -> Result<(), AppError> {
    let (_ok, nf, fail) = results.iter().fold((0, 0, 0), |(ok, nf, fail), r| match r {
        Ok(_) => (ok + 1, nf, fail),
        Err(AppError::NotFound { .. }) => (ok, nf + 1, fail),
        _ => (ok, nf, fail + 1),
    });
    if nf > 0 {
        tracing::debug!(
            not_found = nf,
            total = results.len(),
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
            reason: format!("{fail}/{} failed", results.len()),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppError;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // — aggregate_results —

    #[test]
    fn aggregate_all_ok() {
        let results = vec![Ok(()), Ok(()), Ok(())];
        assert!(aggregate_results(results).is_ok());
    }

    #[test]
    fn aggregate_not_found_only_ok() {
        let results = vec![
            Ok(()),
            Err(AppError::NotFound {
                url: "http://x".into(),
            }),
            Err(AppError::NotFound {
                url: "http://y".into(),
            }),
        ];
        assert!(aggregate_results(results).is_ok());
    }

    #[test]
    fn aggregate_one_fail_returns_err() {
        let results = vec![
            Ok(()),
            Err(AppError::DownloadFailed {
                url: "a".into(),
                reason: "boom".into(),
            }),
        ];
        let err = aggregate_results(results).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("1/2 failed"));
    }

    #[test]
    fn aggregate_mix_not_found_and_fail() {
        let results = vec![
            Ok(()),
            Err(AppError::NotFound {
                url: "http://x".into(),
            }),
            Err(AppError::DownloadFailed {
                url: "b".into(),
                reason: "timeout".into(),
            }),
        ];
        let err = aggregate_results(results).unwrap_err();
        assert!(err.to_string().contains("1/3 failed"));
    }

    #[test]
    fn aggregate_empty_vec_ok() {
        assert!(aggregate_results(vec![]).is_ok());
    }

    // — run_downloads_inner —

    #[tokio::test]
    async fn run_downloads_empty_tasks_returns_ok() {
        let client = reqwest::Client::new();
        let tasks = vec![];
        let download_fn = |_: reqwest::Client, _: DownloadTask| async { unreachable!() };
        assert!(
            run_downloads_inner(&client, tasks, download_fn)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn run_downloads_all_success() {
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b")];
        let call_count = Arc::new(AtomicUsize::new(0));
        let cnt = call_count.clone();
        let download_fn = move |_: reqwest::Client, _: DownloadTask| {
            let cnt = cnt.clone();
            async move {
                cnt.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };
        assert!(
            run_downloads_inner(&client, tasks, download_fn)
                .await
                .is_ok()
        );
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn run_downloads_not_found_tolerated() {
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b")];
        let download_fn = |_: reqwest::Client, t: DownloadTask| async move {
            if t.pair == "a" {
                Err(AppError::NotFound {
                    url: "http://x".into(),
                })
            } else {
                Ok(())
            }
        };
        assert!(
            run_downloads_inner(&client, tasks, download_fn)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn run_downloads_one_fail_returns_err() {
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b")];
        let download_fn = |_: reqwest::Client, t: DownloadTask| async move {
            if t.pair == "b" {
                Err(AppError::DownloadFailed {
                    url: "x".into(),
                    reason: "bad".into(),
                })
            } else {
                Ok(())
            }
        };
        let err = run_downloads_inner(&client, tasks, download_fn)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("1/2 failed"));
    }

    // — helpers —

    fn dummy_task(pair: &str) -> DownloadTask {
        DownloadTask {
            url: format!("http://example.com/{pair}"),
            dest: std::path::PathBuf::from("/tmp"),
            pair: pair.into(),
            timeframe: "1m".into(),
        }
    }
}
