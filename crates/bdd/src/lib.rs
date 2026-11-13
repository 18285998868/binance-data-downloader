//! Binance public data archive downloader for spot, USDⓈ-M and COIN-M.
//!
//! Downloads any of the eleven archive datasets (aggTrades, trades, klines,
//! bookDepth, bookTicker, fundingRate, liquidationSnapshot, metrics and the
//! three futures price-kline families) and saves the zip files in a tree that
//! mirrors the web page path exactly.
//!
//! The crate documentation is the project README, so that the published docs
//! and the repository README can never drift apart.
//!
//! Module layout: `app` is the five-step pipeline — `config`, `prepare`,
//! `start_dates`, `download` and `report`; supporting modules are `backoff`,
//! `cache`, `cancel`, `dataset`, `endpoints`, `error`, `limits`, `scan` and
//! `verify`. The `binance-data-verify` binary lives in the `bdd-cli` package
//! and shares [`verify_zip`] with the download pipeline.
//!
//! The modules are an implementation detail; the public surface is [`run`],
//! [`run_with_options`], [`RunOptions`], [`verify_zip`], [`AppError`] and
//! [`FailureReport`].

#![doc = include_str!("../../../README.md")]

mod app;
mod backoff;
mod cache;
mod cancel;
mod config;
mod dataset;
mod download;
mod endpoints;
mod error;
mod limits;
mod prepare;
mod report;
mod scan;
mod start_dates;
mod verify;

#[cfg(test)]
mod pipeline_tests;

pub use app::{RunOptions, run, run_with_options};
pub use error::{AppError, FailureReport};
pub use verify::verify_zip;
