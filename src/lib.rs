//! Binance Futures daily kline downloader.
//!
//! Downloads historical kline data from Binance's public data archive,
//! converts CSV → Parquet, and supports parallel batch processing.

pub mod app;
pub mod builder;
pub mod cli;
pub mod config;
pub mod download;
pub mod error;
pub mod get_local_files;
pub mod get_start_date;
pub mod get_symbols;
pub mod urls;

use std::path::PathBuf;

pub use app::run;
pub use error::AppError;

#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub url: String,
    pub dest: PathBuf,
    pub pair: String,
    pub timeframe: String,
}
