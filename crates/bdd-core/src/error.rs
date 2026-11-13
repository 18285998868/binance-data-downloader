//! The crate's single error type, built with `thiserror`.
//!
//! [`AppError`] is `#[non_exhaustive]`, so new variants can be added without a
//! breaking change; callers must therefore keep a wildcard arm.

use thiserror::Error;

/// A failure report: what went wrong, in a machine-readable shape.
///
/// The rendered message keeps everything, but the pieces are separate fields so
/// a caller (a CI check, a wrapper crate) can consume the failures without
/// parsing a message string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureReport {
    /// One-line summary of what went wrong.
    pub summary: String,
    /// The individual failures, in the order they happened.
    pub details: Vec<String>,
    /// Trailing notes: truncation, early abort, and similar.
    pub notes: Vec<String>,
}

impl FailureReport {
    /// A report with only a summary.
    #[must_use]
    pub fn new(summary: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
            details: Vec::new(),
            notes: Vec::new(),
        }
    }

    /// Add an individual failure detail.
    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.details.push(detail.into());
        self
    }

    /// Add every failure detail of an iterator.
    #[must_use]
    pub fn with_details(mut self, details: impl IntoIterator<Item = String>) -> Self {
        self.details.extend(details);
        self
    }

    /// Add a trailing note.
    #[must_use]
    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.notes.push(note.into());
        self
    }
}

impl std::fmt::Display for FailureReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.summary)?;
        if !self.details.is_empty() {
            write!(f, ": {}", self.details.join("; "))?;
        }
        for note in &self.notes {
            write!(f, "\n{note}")?;
        }
        Ok(())
    }
}

impl std::error::Error for FailureReport {}

/// Application error type.
///
/// # Examples
///
/// ```
/// use bdd_core::AppError;
///
/// let err = AppError::NotFound {
///     url: "https://data.binance.vision/x.zip".into(),
/// };
/// assert_eq!(err.to_string(), "not found: https://data.binance.vision/x.zip");
/// ```
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AppError {
    /// HTTP transport error.
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    /// I/O error.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON error (a malformed or unexpected API response body).
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Configuration error.
    #[error("config error: {}", AppError::config_text(.path.as_deref(), .message))]
    Config {
        /// Configuration file the failure is tied to, when there is one.
        path: Option<std::path::PathBuf>,
        /// Human-readable error message, without the file path.
        message: String,
    },

    /// Binance API error.
    #[error("api error: {message}")]
    Api {
        /// HTTP status code, when available.
        status: Option<u16>,
        /// Human-readable error message.
        message: String,
    },

    /// Unknown symbol.
    #[error("unknown symbol: {symbol}")]
    UnknownSymbol {
        /// The symbol the exchange rejected.
        symbol: String,
    },

    /// Data processing error.
    ///
    /// The payload is structured (summary + details + notes) so the individual
    /// failures stay consumable without parsing the rendered message.
    #[error("data error: {0}")]
    Data(#[from] FailureReport),

    /// Date parsing error.
    #[error("date error: {0}")]
    Date(String),

    /// Cache error.
    #[error("cache error: {0}")]
    Cache(String),

    /// Download failed.
    #[error("download failed for {url}: {reason}")]
    DownloadFailed {
        /// URL that could not be downloaded.
        url: String,
        /// Why it failed (HTTP status, content validation, ...).
        reason: String,
    },

    /// Resource not found (404).
    #[error("not found: {url}")]
    NotFound {
        /// URL the archive does not have.
        url: String,
    },

    /// Task join error.
    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    /// The run was cancelled (Ctrl-C) before this operation finished.
    ///
    /// Cancellation is not a failure: it propagates so an in-flight download can
    /// abandon its temp file, and callers count it separately from
    /// [`AppError::DownloadFailed`].
    #[error("cancelled")]
    Cancelled,
}

impl AppError {
    /// Render a [`AppError::Config`] payload: prefix the message with the file
    /// path when the failure is tied to one, so the path lives in exactly one
    /// place and every config error names its file.
    fn config_text(path: Option<&std::path::Path>, message: &str) -> String {
        match path {
            Some(p) => format!("{}: {message}", p.display()),
            None => message.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_lowercase() {
        let cases = [
            (
                AppError::Config {
                    path: None,
                    message: "bad".into(),
                }
                .to_string(),
                "config error: bad",
            ),
            (
                AppError::Api {
                    status: None,
                    message: "boom".into(),
                }
                .to_string(),
                "api error: boom",
            ),
            (
                AppError::Data(FailureReport::new("x")).to_string(),
                "data error: x",
            ),
        ];
        for (actual, expected) in cases {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        let err: AppError = io_err.into();
        assert!(matches!(err, AppError::Io(_)));
    }

    #[test]
    fn unknown_symbol_variant() {
        let err = AppError::UnknownSymbol {
            symbol: "NOPE".into(),
        };
        assert!(err.to_string().contains("unknown symbol: NOPE"));
    }

    #[test]
    fn cancelled_is_rendered_without_detail() {
        assert_eq!(AppError::Cancelled.to_string(), "cancelled");
    }

    #[test]
    fn from_json_error() {
        let json_err = serde_json::from_str::<u32>("not json").unwrap_err();
        let err: AppError = json_err.into();
        assert!(matches!(err, AppError::Json(_)));
    }

    #[tokio::test]
    async fn from_join_error() {
        let handle = tokio::spawn(async { panic!("boom") });
        let join_err = handle.await.unwrap_err();
        let err: AppError = join_err.into();
        assert!(matches!(err, AppError::Join(_)));
    }

    #[test]
    fn api_error_keeps_the_status_code() {
        let err = AppError::Api {
            status: Some(503),
            message: "service unavailable".into(),
        };
        assert_eq!(err.to_string(), "api error: service unavailable");
        // The status is part of the payload, so it stays inspectable even
        // though `Display` does not render it.
        match err {
            AppError::Api { status, .. } => assert_eq!(status, Some(503)),
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn failure_report_renders_summary_details_and_notes() {
        let report = FailureReport::new("2 pair(s) failed")
            .with_detail("BTCUSDT/1m: boom")
            .with_detail("ETHUSDT/1d: bang")
            .with_note("(additional failures not listed)");
        assert_eq!(
            report.to_string(),
            "2 pair(s) failed: BTCUSDT/1m: boom; ETHUSDT/1d: bang\n(additional failures not listed)"
        );
        // The pieces stay consumable without parsing the message.
        assert_eq!(report.details.len(), 2);
        assert_eq!(report.notes.len(), 1);
    }

    #[test]
    fn failure_report_without_details_renders_the_summary_alone() {
        assert_eq!(FailureReport::new("all good").to_string(), "all good");
    }
}
