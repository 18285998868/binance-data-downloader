//! Application-wide error types.
//!
//! All errors use thiserror for ergonomic `From`/`Display` derives.

use thiserror::Error;

/// Unified error type returned by all public API functions.
#[derive(Debug, Error)]
pub enum AppError {
    /// An HTTP request failed (transient network issue, timeout, etc.).
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// A filesystem I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The remote API returned an unexpected response (parse error, etc.).
    #[error("API error: {0}")]
    Api(String),

    /// A date value could not be parsed or overflowed.
    #[error("Date error: {0}")]
    Date(String),

    /// A download finished with a non-OK HTTP status (except 404).
    #[error("Download failed for {url}: {reason}")]
    DownloadFailed { url: String, reason: String },

    /// The requested resource returned HTTP 404 Not Found.
    #[error("Not found: {url}")]
    NotFound { url: String },
}
