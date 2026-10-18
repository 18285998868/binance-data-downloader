//! Binance Futures daily kline downloader.
//!
//! Downloads historical kline data from Binance's public data archive
//! and saves as zip files.
//!
//! Module layout: `app/` holds the pipeline (`urls`, `tasks`,
//! `download/`); supporting modules are `backoff`, `cache`, `cancel`,
//! `lock`, `start_date` and `scan`.
//!
//! The modules are an implementation detail; the public surface is
//! [`run`], [`Cli`] and [`AppError`].

mod app;
mod backoff;
mod cache;
mod cancel;
mod cli;
mod config;
mod error;
mod lock;
mod scan;
mod start_date;

pub use app::run;
pub use cli::Cli;
pub use error::AppError;
