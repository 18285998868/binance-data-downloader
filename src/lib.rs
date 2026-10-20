//! Binance Futures daily kline downloader.
//!
//! Downloads historical kline data from Binance's public data archive
//! and saves as zip files.
//!
//! The crate documentation is the project README, so that the published docs
//! and the repository README can never drift apart.
//!
//! Module layout: `app/` holds the pipeline (`urls`, `tasks`,
//! `download/`); supporting modules are `backoff`, `cache`, `cancel`,
//! `lock`, `start_date` and `scan`.
//!
//! The modules are an implementation detail; the public surface is
//! [`run`], [`Cli`] and [`AppError`].

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

pub use app::{RunOptions, run, run_with_options};
pub use cli::Cli;
pub use error::{AppError, FailureReport};
