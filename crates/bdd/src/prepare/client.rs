//! Shared HTTP client construction.

use std::time::Duration;

use crate::AppError;

/// Whole-request timeout, on top of the per-read one.
///
/// A server that trickles a byte every 59 seconds never trips the read
/// timeout; without a total timeout it would hold a concurrency slot forever.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

/// Build the shared HTTP client.
///
/// Timeouts matter for the retry window: `read_timeout` only bounds one read,
/// while `timeout` bounds a whole request, including a server that dribbles a
/// byte every 59 seconds. The retry budget is separate (see
/// [`crate::backoff::RETRY_WINDOW`]).
pub(super) fn build() -> Result<reqwest::Client, AppError> {
    let client = reqwest::Client::builder()
        .user_agent("binance-data-downloader/0.1")
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(60))
        .timeout(REQUEST_TIMEOUT)
        .pool_max_idle_per_host(256)
        .pool_idle_timeout(Duration::from_secs(30))
        .build()?;
    Ok(client)
}
