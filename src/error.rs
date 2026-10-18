use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
/// Application error type.
pub enum AppError {
    /// HTTP transport error.
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    /// I/O error.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Configuration error.
    #[error("config error: {0}")]
    Config(String),

    /// Binance API error.
    #[error("api error: {0}")]
    Api(String),

    /// Unknown symbol.
    #[error("unknown symbol: {symbol}")]
    UnknownSymbol { symbol: String },

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
    DownloadFailed { url: String, reason: String },

    /// Resource not found (404).
    #[error("not found: {url}")]
    NotFound { url: String },

    /// Task join error.
    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_lowercase() {
        let cases = [
            (
                AppError::Config("bad".into()).to_string(),
                "config error: bad",
            ),
            (AppError::Api("boom".into()).to_string(), "api error: boom"),
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
