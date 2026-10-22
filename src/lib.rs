//! Binance Futures daily kline downloader.
//!
//! Downloads historical kline data from Binance's public data archive
//! and saves as zip files.
//!
//! The crate documentation is the project README, so that the published docs
//! and the repository README can never drift apart.
//!
//! Module layout: `app/` holds the pipeline (`urls`, `tasks`,
//! `download/`, `start_dates`); supporting modules are `archive`, `backoff`,
//! `cache`, `cancel`, `cli`, `config`, `error`, `limits`, `lock`,
//! `start_date`, `scan` and `verify`. The `binance-data-verify` binary lives
//! in `src/bin/` and shares [`verify_zip`] with the download pipeline.
//!
//! The modules are an implementation detail; the public surface is [`run`],
//! [`run_with_options`], [`RunOptions`], [`Cli`], [`verify_zip`], [`AppError`]
//! and [`FailureReport`].

#![doc = include_str!("../README.md")]

mod app;
mod archive;
mod backoff;
mod cache;
mod cancel;
mod cli;
mod config;
mod error;
mod limits;
mod lock;
mod scan;
mod start_date;
mod verify;

pub use app::{RunOptions, run, run_with_options};
pub use cli::Cli;
pub use error::{AppError, FailureReport};
pub use verify::verify_zip;
