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
pub const RETRY_WINDOW: std::time::Duration = std::time::Duration::from_secs(120);

/// Exponential backoff capped at [`RETRY_WINDOW`] of total retry time.
#[must_use]
pub fn default_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        max_elapsed_time: Some(RETRY_WINDOW),
        ..Default::default()
    }
}

/// Parse a `Retry-After` response header in seconds form.
///
/// HTTP-date values are ignored (mapped to `None`), so callers fall back to
/// the regular backoff schedule.
pub fn parse_retry_after(resp: &reqwest::Response) -> Option<std::time::Duration> {
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
pub fn clamp_retry_after(
    retry_after: Option<std::time::Duration>,
    deadline: std::time::Instant,
) -> Option<std::time::Duration> {
    retry_after
        .map(|delay| delay.min(deadline.saturating_duration_since(std::time::Instant::now())))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let clamped = clamp_retry_after(Some(std::time::Duration::from_secs(86_400)), deadline);
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
        let deadline = std::time::Instant::now() - std::time::Duration::from_secs(1);
        assert_eq!(
            clamp_retry_after(Some(std::time::Duration::from_secs(60)), deadline),
            Some(std::time::Duration::ZERO)
        );
    }
}
