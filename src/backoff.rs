//! Shared retry policy: the backoff schedule plus `Retry-After` handling.

use backoff::ExponentialBackoff;

/// Total wall-clock retry budget for a single operation.
///
/// Enforced in three places: `max_elapsed_time` bounds the regular backoff
/// schedule, the retried operation checks a matching deadline (hard stop),
/// and `Retry-After` delays are clamped to the remaining window via
/// [`clamp_retry_after`]. The extra guards are needed because `backoff`
/// treats a `Retry-After` override as unconditional
/// (`retry_after.or_else(|| backoff.next_backoff())`), ignoring
/// `max_elapsed_time`: a server answering with `Retry-After` would otherwise
/// retry forever, or sleep for as long as the header demands.
pub(crate) const RETRY_WINDOW: std::time::Duration = std::time::Duration::from_secs(120);

/// Exponential backoff capped at [`RETRY_WINDOW`] of total retry time.
#[must_use]
pub(crate) fn default_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        max_elapsed_time: Some(RETRY_WINDOW),
        ..Default::default()
    }
}

/// Parse a `Retry-After` response header in seconds form.
///
/// HTTP-date values are ignored (mapped to `None`), so callers fall back to
/// the regular backoff schedule.
pub(crate) fn parse_retry_after(resp: &reqwest::Response) -> Option<std::time::Duration> {
    resp.headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(std::time::Duration::from_secs)
}

/// Clamp a server-provided `Retry-After` delay to the remaining retry window.
///
/// `backoff` sleeps the full `Retry-After` value without consulting
/// `max_elapsed_time` (see [`RETRY_WINDOW`]), so an oversized header (e.g.
/// `Retry-After: 86400`) would otherwise stall the operation for that long.
/// Capping the delay at `deadline - now` keeps the wall-clock budget honest;
/// once the window is spent the next attempt short-circuits via the caller's
/// deadline check.
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

    #[test]
    fn default_backoff_has_max_elapsed() {
        let b = default_backoff();
        assert_eq!(b.max_elapsed_time, Some(RETRY_WINDOW));
    }

    #[test]
    fn default_backoff_has_initial_interval() {
        let b = default_backoff();
        assert!(b.initial_interval.as_millis() >= 100);
    }

    #[test]
    fn default_backoff_multiplication() {
        let b = default_backoff();
        assert!(b.multiplier > 1.0);
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
    async fn parse_retry_after_ignores_the_http_date_form() {
        // HTTP-date values are deliberately not parsed: the caller falls back
        // to the regular backoff schedule.
        let resp = response_for(
            ResponseTemplate::new(429)
                .insert_header("Retry-After", "Wed, 21 Oct 2026 07:28:00 GMT"),
        )
        .await;
        assert_eq!(parse_retry_after(&resp), None);
    }
}
