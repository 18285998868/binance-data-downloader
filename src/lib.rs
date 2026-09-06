//! Binance Futures daily kline downloader.
//!
//! Downloads historical kline data from Binance's public data archive
//! and saves as zip files.
//!
//! Module layout: support modules (`cli`, `config`, `error`, `cache`,
//! `backoff`) live at the crate root; the download pipeline lives in
//! `app/`, whose numbered files (`NN_` prefix) encode execution order and
//! are wired via `#[path]` as submodules of `app`.

pub mod app;

pub mod backoff;
pub mod cache;
pub mod cli;
pub mod config;
pub mod error;

pub use app::run;
pub use error::AppError;
