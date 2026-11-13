//! Raw TOML schema and defaults.
//!
//! `AppConfig` is the deserialized shape; semantic checks live in
//! [`super::validate`].

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::AppError;
use crate::dataset::{DatasetKind, Interval, Market, Period};

/// One `[[dataset]]` entry.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DatasetConfig {
    /// Dataset family, e.g. `klines` or `fundingRate`.
    #[serde(rename = "type")]
    pub(crate) kind: DatasetKind,
    /// Archive cadences to download.
    pub(crate) periods: Vec<Period>,
    /// Kline intervals; required for K-line datasets, forbidden otherwise.
    #[serde(default)]
    pub(crate) intervals: Vec<Interval>,
}

/// Application configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AppConfig {
    /// Market path: `spot`, `futures/um` or `futures/cm`.
    pub(crate) market: Market,
    /// Symbols to download, kept **verbatim** as the user wrote them.
    ///
    /// The uppercased / sorted / deduplicated view is produced on demand by
    /// [`AppConfig::normalized_symbols`]; the raw value is kept so validation
    /// errors can quote exactly what the user typed.
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
    pub(crate) dataset: Vec<DatasetConfig>,
}

fn default_output_dir() -> PathBuf {
    PathBuf::from(".")
}

fn default_concurrency() -> usize {
    64
}

impl AppConfig {
    /// Load and validate configuration from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Config`] when the file cannot be read, the TOML is
    /// invalid, or validation fails. Every such error carries `path`.
    pub(crate) async fn load(path: &Path) -> Result<Self, AppError> {
        let raw = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| AppError::Config {
                path: Some(path.to_path_buf()),
                message: e.to_string(),
            })?;
        Self::parse_str(&raw, path)
    }

    pub(super) fn parse_str(raw: &str, path: &Path) -> Result<Self, AppError> {
        let cfg: Self = toml::from_str(raw).map_err(|e| AppError::Config {
            path: Some(path.to_path_buf()),
            message: e.to_string(),
        })?;
        cfg.validate(path)
    }
}
