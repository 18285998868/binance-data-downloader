//! Binance Futures daily kline downloader.
//!
//! Downloads historical kline data from Binance's public data archive
//! and saves as zip files.
//!
//! Module layout mirrors the download pipeline in `app/`; on-disk files
//! carry a `NN_` step prefix and the Rust module names match the readable
//! step name (`cli`, `config`, `scan_local_files`, ...), wired via `#[path]`.

pub mod app;
#[path = "app/05_build_download_tasks.rs"]
pub mod build_download_tasks;
#[path = "app/01_cli.rs"]
pub mod cli;
#[path = "app/02_config.rs"]
pub mod config;
#[path = "app/04_get_start_date.rs"]
pub mod get_start_date;
#[path = "app/06_run_downloads/mod.rs"]
pub mod run_downloads;
#[path = "app/03_scan_local_files.rs"]
pub mod scan_local_files;

pub mod backoff;
pub mod cache;
pub mod error;

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
