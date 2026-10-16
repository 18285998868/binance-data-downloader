use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AppError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("config error: {0}")]
    Config(String),

    #[error("api error: {0}")]
    Api(String),

    #[error("unknown symbol: {symbol}")]
    UnknownSymbol { symbol: String },

    #[error("data error: {0}")]
    Data(String),

    #[error("date error: {0}")]
    Date(String),

    #[error("cache error: {0}")]
    Cache(String),

    #[error("download failed for {url}: {reason}")]
    DownloadFailed { url: String, reason: String },

    #[error("not found: {url}")]
    NotFound { url: String },

    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}
