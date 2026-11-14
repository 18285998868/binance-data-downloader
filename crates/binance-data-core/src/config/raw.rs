//! Raw TOML schema and defaults.
//!
//! These types only describe the file. Semantic checks, normalization and the
//! expansion into concrete jobs live in [`super::validate`]; business code only
//! ever sees [`super::Config`].

use std::path::PathBuf;

use serde::Deserialize;

use crate::dataset::{DatasetKind, Interval, Market, Period};

/// One `[[dataset]]` entry.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawDataset {
    /// Dataset family, e.g. `klines` or `fundingRate`.
    #[serde(rename = "type")]
    pub(crate) kind: DatasetKind,
    /// Archive cadences to download.
    pub(crate) periods: Vec<Period>,
    /// Kline intervals; required for K-line datasets, forbidden otherwise.
    #[serde(default)]
    pub(crate) intervals: Vec<Interval>,
}

/// Application configuration as written in TOML.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawConfig {
    /// Market path: `spot`, `futures/um` or `futures/cm`.
    pub(crate) market: Market,
    /// Symbols as the user wrote them; validation uppercases and deduplicates
    /// them, but the raw spelling is kept here so errors can quote it.
    pub(crate) symbol: Vec<String>,
    /// Optional inclusive start date override.
    #[serde(default)]
    pub(crate) start: Option<chrono::NaiveDate>,
    /// Optional inclusive end date override.
    #[serde(default)]
    pub(crate) end: Option<chrono::NaiveDate>,
    /// Parent directory of the archive's `data/` tree.
    #[serde(default = "default_output_dir")]
    pub(crate) output_dir: PathBuf,
    /// Maximum concurrent downloads.
    #[serde(default = "default_concurrency")]
    pub(crate) concurrency: usize,
    /// Optional threshold on the share of files the server did not have, in
    /// percent (`0 < t ≤ 100`).
    #[serde(default)]
    pub(crate) not_found_pct_threshold: Option<f64>,
    /// Dataset entries to download.
    pub(crate) dataset: Vec<RawDataset>,
}

fn default_output_dir() -> PathBuf {
    PathBuf::from(".")
}

fn default_concurrency() -> usize {
    64
}
