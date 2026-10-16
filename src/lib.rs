//! Binance Futures daily kline downloader.
//!
//! Downloads historical kline data from Binance's public data archive
//! and saves as zip files.
//!
//! Module layout: `app/` holds the naive pipeline (`01_build_urls`,
//! `02_build_download_tasks`, `03_download/`); patches live in `utils/`
//! (`get_start_date`, `scan_local_files`, `cache`, `backoff`).
//!
//! The modules are an implementation detail; the public surface is
//! [`run`], [`Cli`] and [`AppError`].

mod app;
mod cli;
mod config;
mod error;
mod utils;

pub use app::run;
pub use cli::Cli;
pub use error::AppError;
