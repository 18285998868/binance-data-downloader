use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Data clean error: {0}")]
    DataClean(#[from] common::data_clean::Error),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("API error: {0}")]
    Api(String),

    #[error("Data error: {0}")]
    Data(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Date error: {0}")]
    Date(String),

    #[error("UTF-8 error: {0}")]
    Utf8(String),

    #[error("Download failed for {url}: {reason}")]
    DownloadFailed { url: String, reason: String },

    #[error("Not found: {url}")]
    NotFound { url: String },

    #[error("Task join error: {0}")]
    Join(String),
}
