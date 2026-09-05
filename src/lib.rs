//! Binance Futures daily kline downloader.
//!
//! Downloads historical kline data from Binance's public data archive
//! and saves as zip files.
//!
//! Module layout: support modules (`cli`, `config`, `error`, `cache`,
//! `backoff`) live at the crate root; the download pipeline lives in
//! `app/`, whose numbered files (`NN_` prefix) encode execution order and
//! are wired via `#[path]`.

pub mod app;
#[path = "app/03_build_download_tasks.rs"]
pub mod build_download_tasks;
#[path = "app/02_get_start_date.rs"]
pub mod get_start_date;
#[path = "app/04_run_downloads/mod.rs"]
pub mod run_downloads;
#[path = "app/01_scan_local_files.rs"]
pub mod scan_local_files;

pub mod backoff;
pub mod cache;
pub mod cli;
pub mod config;
pub mod error;

pub use app::run;
pub use error::AppError;
