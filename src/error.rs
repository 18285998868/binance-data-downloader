//! The crate's single error type, built with `thiserror`.
//!
//! [`AppError`] is `#[non_exhaustive]`, so new variants can be added without a
//! breaking change; callers must therefore keep a wildcard arm.

use thiserror::Error;

/// Application error type.
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
    #[error("data error: {0}")]
    Data(String),

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
            (AppError::Data("x".into()).to_string(), "data error: x"),
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
}
