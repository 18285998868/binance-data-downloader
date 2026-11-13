//! Errors raised while downloading one file.
//!
//! The batch loop classifies these variants directly (a 404 is tolerated, a
//! cancellation stops scheduling) and converts the rest into [`AppError`] at
//! the pair boundary, so callers of the crate keep their existing error type.

use crate::AppError;

/// A single download failure.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DownloadError {
    /// HTTP transport error.
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    /// Local filesystem error.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    /// The server answered 404.
    #[error("not found: {url}")]
    NotFound {
        /// URL the archive does not have.
        url: String,
    },
    /// The response was incomplete, not a zip, or failed verification.
    #[error("download failed for {url}: {reason}")]
    Failed {
        /// URL that could not be downloaded.
        url: String,
        /// Why it failed.
        reason: String,
    },
    /// The run was cancelled before the file landed.
    #[error("cancelled")]
    Cancelled,
}

impl From<DownloadError> for AppError {
    fn from(error: DownloadError) -> Self {
        match error {
            DownloadError::Http(error) => Self::Http(error),
            DownloadError::Io(error) => Self::Io(error),
            DownloadError::NotFound { url } => Self::NotFound { url },
            DownloadError::Failed { url, reason } => Self::DownloadFailed { url, reason },
            DownloadError::Cancelled => Self::Cancelled,
        }
    }
}
