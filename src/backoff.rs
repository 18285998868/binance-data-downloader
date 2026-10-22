//! Shared retry policy: the backon schedule plus `Retry-After` handling.
//!
//! Retries are explicit and only used for idempotent GETs. Callers return
//! [`RetryError::Transient`] for errors that may recover and
//! [`RetryError::Permanent`] for everything else; [`retry_operation`] applies
//! the shared deadline, schedule, `Retry-After` override and warning log.

use std::future::Future;
use std::time::{Duration, Instant};

use backon::Retryable as _;

/// Retry scheduling budget for a single operation.
///
/// Enforced in three places: the backon schedule caps cumulative sleep with
/// `total_delay`, [`retry_operation`] clamps every delay to the remaining
/// window, and the retried operation checks the same deadline before each
/// attempt. This window is *not* a hard cap on one attempt: a request already
/// in flight is bounded separately by the client's 300-second whole-request
/// timeout. The window only controls retries between attempts.
pub(crate) const RETRY_WINDOW: std::time::Duration = std::time::Duration::from_secs(120);

/// Backon's builder, re-exported under a local name so call sites do not depend
/// on the backon API directly.
pub(crate) use backon::ExponentialBuilder as RetryPolicy;

/// Whether an HTTP status is worth retrying for an idempotent GET.
///
/// `408` and `429` are explicitly transient in addition to `5xx`: Binance and
/// S3 both use them for temporary back-pressure. Other 4xx responses are
/// permanent (a 403, for example, will not become a 200 by waiting).
#[must_use]
pub(crate) fn is_transient_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

/// Default retry policy: 500 ms initial delay, 1.5x growth, 60 s cap, bounded
/// by the 120 s cumulative-sleep budget.
#[must_use]
pub(crate) fn default_retry_policy() -> RetryPolicy {
    RetryPolicy::new()
        .with_min_delay(Duration::from_millis(500))
        .with_max_delay(Duration::from_secs(60))
        .with_factor(1.5)
        .with_total_delay(Some(RETRY_WINDOW))
        .without_max_times()
}

/// Policy used by tests that need exactly one attempt.
#[cfg(test)]
#[must_use]
pub(crate) fn no_retry_policy() -> RetryPolicy {
    RetryPolicy::new()
        .with_min_delay(Duration::ZERO)
        .with_max_delay(Duration::ZERO)
        .with_total_delay(Some(Duration::ZERO))
        .with_max_times(0)
}

/// The result of one attempt, with enough metadata for the shared retry loop.
#[derive(Debug)]
pub(crate) enum RetryError<E> {
    /// The operation may succeed if tried again.
    Transient {
        /// Underlying error.
        err: E,
        /// Delay requested by the server, if any.
        retry_after: Option<Duration>,
    },
    /// The operation must not be retried.
    Permanent(E),
}

impl<E> RetryError<E> {
    /// Mark an error as retryable.
    pub(crate) fn transient(err: E, retry_after: Option<Duration>) -> Self {
        Self::Transient { err, retry_after }
    }

    /// Mark an error as permanent.
    pub(crate) fn permanent(err: E) -> Self {
        Self::Permanent(err)
    }
}

/// Retry an operation with the shared policy and deadline.
///
/// `operation` must only perform idempotent work. Every delay, including a
/// server-provided `Retry-After`, is capped at `deadline`; a permanent error
/// stops immediately. The returned error is the underlying [`AppError`], so
/// callers do not need to unwrap the retry wrapper.
///
/// # Errors
///
/// Returns the final underlying error when the operation is permanent or the
/// retry budget is exhausted.
pub(crate) async fn retry_operation<T, F, Fut>(
    policy: RetryPolicy,
    deadline: Instant,
    operation: F,
) -> Result<T, crate::AppError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, RetryError<crate::AppError>>>,
{
    let result = operation
        .retry(policy)
        .when(|error| matches!(error, RetryError::Transient { .. }))
        .adjust(move |error, delay| {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let retry_after = match error {
                RetryError::Transient {
                    retry_after: Some(delay),
                    ..
                } => Some(*delay),
                RetryError::Transient { .. } | RetryError::Permanent(_) => None,
            };
            clamp_retry_after(retry_after.or(delay), deadline)
        })
        .notify(|error, delay| {
            if let RetryError::Transient { err, .. } = error {
                tracing::warn!(
                    error = %err,
                    ?delay,
                    "request failed; retrying",
                );
            }
        })
        .await;

    match result {
        Ok(value) => Ok(value),
        Err(RetryError::Transient { err, .. } | RetryError::Permanent(err)) => Err(err),
    }
}

/// Parse a `Retry-After` response header.
///
/// Both RFC 9110 forms are accepted: delay-seconds (`Retry-After: 120`) and
/// HTTP-date (`Retry-After: Wed, 21 Oct 2026 07:28:00 GMT`). A date in the
/// past maps to a zero delay. A malformed or missing header returns `None`, so
/// callers fall back to the regular backoff schedule.
pub(crate) fn parse_retry_after(resp: &reqwest::Response) -> Option<std::time::Duration> {
    let raw = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if let Ok(seconds) = raw.parse::<u64>() {
        return Some(std::time::Duration::from_secs(seconds));
    }
    let when = chrono::DateTime::parse_from_rfc2822(raw)
        .ok()?
        .with_timezone(&chrono::Utc);
    let delay = when.signed_duration_since(chrono::Utc::now());
    Some(delay.to_std().unwrap_or(std::time::Duration::ZERO))
}

/// Clamp a server-provided delay to the remaining retry window.
///
/// The shared retry loop uses this for both `Retry-After` overrides and the
/// normal backon delay, so an oversized header (e.g. `Retry-After: 86400`)
/// cannot stall the operation past its deadline.
#[must_use]
pub(crate) fn clamp_retry_after(
    retry_after: Option<std::time::Duration>,
    deadline: std::time::Instant,
) -> Option<std::time::Duration> {
    retry_after
        .map(|delay| delay.min(deadline.saturating_duration_since(std::time::Instant::now())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Fetch `url` from a mock server that answers with `template`.
    async fn response_for(template: ResponseTemplate) -> reqwest::Response {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(template)
            .mount(&server)
            .await;
        reqwest::get(format!("{}/x", server.uri()))
            .await
            .expect("the mock server must answer")
    }

    fn instant_policy() -> RetryPolicy {
        RetryPolicy::new()
            .with_min_delay(Duration::ZERO)
            .with_max_delay(Duration::ZERO)
            .with_total_delay(Some(Duration::from_secs(1)))
            .with_max_times(2)
    }

    #[test]
    fn transient_statuses_cover_back_pressure_and_server_errors() {
        for status in [
            reqwest::StatusCode::REQUEST_TIMEOUT,
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            reqwest::StatusCode::BAD_GATEWAY,
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            reqwest::StatusCode::GATEWAY_TIMEOUT,
        ] {
            assert!(is_transient_status(status), "{status} should be transient");
        }
        for status in [
            reqwest::StatusCode::OK,
            reqwest::StatusCode::NOT_MODIFIED,
            reqwest::StatusCode::BAD_REQUEST,
            reqwest::StatusCode::FORBIDDEN,
            reqwest::StatusCode::NOT_FOUND,
        ] {
            assert!(!is_transient_status(status), "{status} should be permanent");
        }
    }

    #[test]
    fn retry_window_is_two_minutes() {
        assert_eq!(RETRY_WINDOW.as_secs(), 120);
    }

    #[tokio::test]
    async fn retry_operation_retries_transient_errors_then_succeeds() {
        let calls = AtomicUsize::new(0);
        let deadline = Instant::now() + Duration::from_secs(5);
        let result = retry_operation(instant_policy(), deadline, || {
            let call = calls.fetch_add(1, Ordering::Relaxed);
            async move {
                if call < 2 {
                    Err(RetryError::transient(
                        crate::AppError::Api {
                            status: Some(500),
                            message: "transient".into(),
                        },
                        None,
                    ))
                } else {
                    Ok(7_u8)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 7);
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn retry_operation_does_not_retry_permanent_errors() {
        let calls = AtomicUsize::new(0);
        let deadline = Instant::now() + Duration::from_secs(5);
        let result: Result<(), _> = retry_operation(instant_policy(), deadline, || {
            calls.fetch_add(1, Ordering::Relaxed);
            async move {
                Err(RetryError::permanent(crate::AppError::Api {
                    status: Some(400),
                    message: "permanent".into(),
                }))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn retry_operation_stops_when_policy_has_no_retries() {
        let calls = AtomicUsize::new(0);
        let deadline = Instant::now() + Duration::from_secs(5);
        let result: Result<(), _> = retry_operation(no_retry_policy(), deadline, || {
            calls.fetch_add(1, Ordering::Relaxed);
            async move {
                Err(RetryError::transient(
                    crate::AppError::Api {
                        status: Some(500),
                        message: "transient".into(),
                    },
                    None,
                ))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn clamp_retry_after_caps_oversized_value_to_remaining_window() {
        let deadline = std::time::Instant::now() + RETRY_WINDOW;
        let clamped = clamp_retry_after(Some(std::time::Duration::from_hours(24)), deadline);
        assert!(clamped.unwrap() <= RETRY_WINDOW);
    }

    #[test]
    fn clamp_retry_after_keeps_small_values() {
        let deadline = std::time::Instant::now() + RETRY_WINDOW;
        let small = std::time::Duration::from_secs(1);
        assert_eq!(clamp_retry_after(Some(small), deadline), Some(small));
    }

    #[test]
    fn clamp_retry_after_none_stays_none() {
        let deadline = std::time::Instant::now() + RETRY_WINDOW;
        assert_eq!(clamp_retry_after(None, deadline), None);
    }

    #[test]
    fn clamp_retry_after_expired_deadline_yields_zero() {
        let deadline = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(1))
            .expect("monotonic clock is well past one second after boot");
        assert_eq!(
            clamp_retry_after(Some(std::time::Duration::from_secs(60)), deadline),
            Some(std::time::Duration::ZERO)
        );
    }

    // — parse_retry_after —

    #[tokio::test]
    async fn parse_retry_after_reads_the_seconds_form() {
        let resp =
            response_for(ResponseTemplate::new(429).insert_header("Retry-After", "120")).await;
        assert_eq!(
            parse_retry_after(&resp),
            Some(std::time::Duration::from_secs(120))
        );
    }

    #[tokio::test]
    async fn parse_retry_after_tolerates_surrounding_whitespace() {
        let resp =
            response_for(ResponseTemplate::new(429).insert_header("Retry-After", " 7 ")).await;
        assert_eq!(
            parse_retry_after(&resp),
            Some(std::time::Duration::from_secs(7))
        );
    }

    #[tokio::test]
    async fn parse_retry_after_ignores_a_missing_header() {
        let resp = response_for(ResponseTemplate::new(429)).await;
        assert_eq!(parse_retry_after(&resp), None);
    }

    #[tokio::test]
    async fn parse_retry_after_ignores_a_non_numeric_header() {
        let resp =
            response_for(ResponseTemplate::new(429).insert_header("Retry-After", "soon")).await;
        assert_eq!(parse_retry_after(&resp), None);
    }

    #[tokio::test]
    async fn parse_retry_after_reads_a_future_http_date() {
        let future = (chrono::Utc::now() + chrono::TimeDelta::hours(1))
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let resp =
            response_for(ResponseTemplate::new(429).insert_header("Retry-After", &future)).await;
        let delay = parse_retry_after(&resp).expect("a future HTTP-date must be honoured");
        assert!(
            delay >= std::time::Duration::from_mins(59)
                && delay <= std::time::Duration::from_mins(61),
            "unexpected delay: {delay:?}"
        );
    }

    #[tokio::test]
    async fn parse_retry_after_clamps_a_past_http_date_to_zero() {
        let past = (chrono::Utc::now() - chrono::TimeDelta::hours(1))
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let resp =
            response_for(ResponseTemplate::new(429).insert_header("Retry-After", &past)).await;
        assert_eq!(parse_retry_after(&resp), Some(std::time::Duration::ZERO));
    }
}
