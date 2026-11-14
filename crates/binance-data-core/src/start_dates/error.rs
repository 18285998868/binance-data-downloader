//! Errors raised while resolving a symbol's first archive date.
//!
//! [`StartDateError::UnknownSymbol`] is matched by the resolution loop (a typo
//! stays fatal) and the rest degrade to the configured start date when one is
//! available. The pair/run boundary converts them into [`AppError`].

use crate::AppError;
use crate::error::FailureReport;

/// A start-date lookup failure.
#[derive(Debug, thiserror::Error)]
pub(crate) enum StartDateError {
    /// HTTP transport error.
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    /// Malformed JSON in the API response.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// The exchange API rejected the request.
    #[error("api error: {message}")]
    Api {
        /// HTTP status code, when available.
        status: Option<u16>,
        /// Human-readable error message.
        message: String,
    },
    /// The exchange does not know the symbol.
    #[error("unknown symbol: {symbol}")]
    UnknownSymbol {
        /// The symbol the exchange rejected.
        symbol: String,
    },
    /// A timestamp in the response is not a valid date.
    #[error("date error: {0}")]
    Date(String),
    /// The S3 bucket listing is not parseable.
    #[error("archive listing is unparseable: {0}")]
    Listing(String),
}

impl From<StartDateError> for AppError {
    fn from(error: StartDateError) -> Self {
        match error {
            StartDateError::Http(error) => Self::Http(error),
            StartDateError::Json(error) => Self::Json(error),
            StartDateError::Api { status, message } => Self::Api { status, message },
            StartDateError::UnknownSymbol { symbol } => Self::UnknownSymbol { symbol },
            StartDateError::Date(message) => Self::Date(message),
            StartDateError::Listing(message) => Self::Data(FailureReport::new(format!(
                "archive listing is unparseable: {message}"
            ))),
        }
    }
}
