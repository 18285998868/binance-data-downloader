//! Binance Futures daily kline downloader.
//!
//! Downloads historical kline data from Binance's public data archive
//! and saves as zip files.
//!
//! Module layout: `app/` holds the naive pipeline (`01_build_urls`,
//! `02_build_download_tasks`, `03_download/`); patches live in `utils/`
//! (`get_start_date`, `scan_local_files`, `cache`, `backoff`).

pub mod app;

pub mod cli;
pub mod config;
pub mod error;
pub mod utils;

pub use app::run;
pub use error::AppError;
