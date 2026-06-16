//! Binance Futures daily kline downloader.
//!
//! Downloads historical kline data from Binance's public data archive,
//! converts CSV → Parquet, and supports parallel batch processing.

pub mod config;
pub mod error;
pub mod run_one;
pub mod runner;
pub mod task;

pub use error::AppError;
pub use runner::run;
