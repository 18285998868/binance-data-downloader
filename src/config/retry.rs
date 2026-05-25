//! Generic async retry with exponential back-off.
//!
//! Provides a [`retry`] function that attempts an async operation up to
//! `max_attempts` times with exponential delay, only retrying when the
//! caller's `should_retry` predicate returns `true`.

use std::future::Future;
use std::time::Duration;

/// Configuration for retry behaviour.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    /// Maximum number of attempts (default: 3).
    pub max_attempts: usize,
    /// Base delay in ms (doubled on each retry, default: 2000).
    pub base_delay_ms: u64,
    /// Maximum delay cap in ms (default: 30_000).
    pub max_delay_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 2000,
            max_delay_ms: 30_000,
        }
    }
}

/// Executes an async operation with retry and exponential back-off.
///
/// # Arguments
///
/// * `op` — The operation to retry, as a `FnMut` returning a future.
/// * `config` — [`RetryConfig`] controlling attempt count and delays.
/// * `should_retry` — Predicate that decides whether a given error is
///   retryable.  Non-retryable errors cause immediate failure.
pub async fn retry<F, Fut, T, E, S>(
    mut op: F,
    config: &RetryConfig,
    should_retry: S,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    S: Fn(&E) -> bool,
{
    assert!(config.max_attempts > 0, "max_attempts must be >= 1");
    let mut last_err = None;
    for attempt in 1..=config.max_attempts {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if !should_retry(&e) || attempt == config.max_attempts {
                    return Err(e);
                }
                last_err = Some(e);
                let delay_ms =
                    (config.base_delay_ms * (1 << (attempt - 1))).min(config.max_delay_ms);
                log::debug!("retry attempt={} delay={}ms", attempt + 1, delay_ms);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| unreachable!("retry loop must have at least one error")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_retry_ok_first_try() {
        let mut count = 0;
        let result = retry(
            || {
                count += 1;
                let c = count;
                async move { Ok::<_, &str>(c) }
            },
            &RetryConfig::default(),
            |_| true,
        )
        .await;
        assert_eq!(result, Ok(1));
    }

    #[tokio::test]
    async fn test_retry_succeeds_after_failures() {
        let mut count = 0;
        let result = retry(
            || {
                count += 1;
                let c = count;
                async move {
                    if c < 3 {
                        Err::<&str, &str>("not yet")
                    } else {
                        Ok::<_, &str>("done")
                    }
                }
            },
            &RetryConfig::default(),
            |_| true,
        )
        .await;
        assert_eq!(result, Ok("done"));
    }

    #[tokio::test]
    async fn test_retry_exhausts_attempts() {
        let mut count = 0;
        let result = retry(
            || {
                count += 1;
                async { Err::<&str, &str>("always fails") }
            },
            &RetryConfig {
                max_attempts: 3,
                ..Default::default()
            },
            |_| true,
        )
        .await;
        assert_eq!(result, Err("always fails"));
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_retry_non_retryable_immediate_fail() {
        let mut count = 0;
        let result = retry(
            || {
                count += 1;
                async { Err::<&str, &str>("fatal") }
            },
            &RetryConfig::default(),
            |_| false,
        )
        .await;
        assert_eq!(result, Err("fatal"));
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_retry_config_default() {
        let cfg = RetryConfig::default();
        assert_eq!(cfg.max_attempts, 3);
        assert_eq!(cfg.base_delay_ms, 2000);
        assert_eq!(cfg.max_delay_ms, 30_000);
    }
}
