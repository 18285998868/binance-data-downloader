//! Semantic validation and compilation of [`RawConfig`] into [`Config`].
//!
//! Deserialization only proves the TOML has the right shape. This module adds
//! the rules that depend on combinations of fields (market/dataset/interval
//! support, duplicate jobs, colliding archive paths), normalizes the symbol
//! list and expands every `[[dataset]]` entry into concrete jobs, so business
//! code never re-derives any of it.

use std::collections::HashSet;

use crate::dataset::{DownloadJob, Market};

use super::error::ConfigError;
use super::raw::{RawConfig, RawDataset};
use super::{Config, MAX_CONCURRENCY};

impl TryFrom<RawConfig> for Config {
    type Error = ConfigError;

    fn try_from(raw: RawConfig) -> Result<Self, Self::Error> {
        let RawConfig {
            market,
            symbol,
            start,
            end,
            output_dir,
            concurrency,
            not_found_pct_threshold,
            dataset,
        } = raw;

        validate_scalars(
            &symbol,
            &dataset,
            &output_dir,
            concurrency,
            start,
            end,
            not_found_pct_threshold,
        )?;
        let mut dataset = dataset;
        let jobs = expand_jobs(&mut dataset, market)?;
        let symbols = normalize_symbols(symbol);

        // `indexPriceKlines` uses the underlying index symbol, so a perpetual
        // and a dated contract of the same underlying map to one archive path.
        // Reject that instead of racing two downloads on one destination.
        reject_colliding_archives(&symbols, &jobs, market)?;

        Ok(Self {
            market,
            symbols,
            start,
            end,
            output_dir,
            concurrency,
            not_found_pct_threshold,
            jobs,
        })
    }
}

/// Checks that need only the scalar fields.
fn validate_scalars(
    symbol: &[String],
    dataset: &[RawDataset],
    output_dir: &std::path::Path,
    concurrency: usize,
    start: Option<chrono::NaiveDate>,
    end: Option<chrono::NaiveDate>,
    not_found_pct_threshold: Option<f64>,
) -> Result<(), ConfigError> {
    if symbol.is_empty() {
        return Err(ConfigError::invalid(
            "symbol must contain at least one symbol",
        ));
    }
    if dataset.is_empty() {
        return Err(ConfigError::invalid(
            "dataset must contain at least one entry",
        ));
    }
    if output_dir.as_os_str().is_empty() {
        return Err(ConfigError::invalid("output_dir must not be empty"));
    }
    if concurrency == 0 {
        return Err(ConfigError::invalid(
            "concurrency must be greater than zero",
        ));
    }
    if concurrency > MAX_CONCURRENCY {
        return Err(ConfigError::invalid(format!(
            "concurrency must not exceed {MAX_CONCURRENCY}, got {concurrency}"
        )));
    }
    if let (Some(start), Some(end)) = (start, end)
        && start > end
    {
        return Err(ConfigError::invalid(format!(
            "start {start} is after end {end}"
        )));
    }
    if let Some(threshold) = not_found_pct_threshold
        && !(threshold > 0.0 && threshold <= 100.0)
    {
        // NaN is rejected as well: both comparisons are false for it.
        return Err(ConfigError::invalid(format!(
            "not_found_pct_threshold must be in (0, 100], got {threshold}"
        )));
    }
    if let Some(bad) = symbol.iter().find(|s| !is_valid_symbol(s)) {
        return Err(ConfigError::invalid(format!(
            "invalid symbol: {bad:?} (expected ASCII letters, digits or underscore)"
        )));
    }
    Ok(())
}

/// Normalize symbols: uppercase, sort and deduplicate.
fn normalize_symbols(symbol: Vec<String>) -> Vec<String> {
    let mut out: Vec<String> = symbol.into_iter().map(|s| s.to_ascii_uppercase()).collect();
    out.sort();
    out.dedup();
    out
}

/// Validate every `[[dataset]]` entry and expand it into concrete jobs.
///
/// A dataset with two periods and three intervals yields six jobs; a dataset
/// without intervals yields one job per period. The resulting jobs are sorted
/// by `(kind, period, interval)` so runs are reproducible.
fn expand_jobs(
    datasets: &mut [RawDataset],
    market: Market,
) -> Result<Vec<DownloadJob>, ConfigError> {
    let mut seen = HashSet::new();
    let mut jobs = Vec::new();

    for dataset in datasets {
        let kind = dataset.kind;
        if dataset.periods.is_empty() {
            return Err(ConfigError::invalid(format!(
                "dataset {kind} must contain at least one period"
            )));
        }
        dataset.periods.sort();
        dataset.periods.dedup();

        if kind.needs_interval() {
            if dataset.intervals.is_empty() {
                return Err(ConfigError::invalid(format!(
                    "dataset {kind} requires at least one interval"
                )));
            }
            dataset.intervals.sort();
            dataset.intervals.dedup();
        } else if !dataset.intervals.is_empty() {
            return Err(ConfigError::invalid(format!(
                "dataset {kind} does not take intervals (got {:?})",
                dataset.intervals
            )));
        }

        for period in &dataset.periods {
            if !kind.supports(market, *period) {
                return Err(ConfigError::invalid(format!(
                    "dataset {kind} is not published for {market} {period}"
                )));
            }
            if kind.needs_interval() {
                for interval in &dataset.intervals {
                    if !interval.available_for(market, *period) {
                        return Err(ConfigError::invalid(format!(
                            "interval {interval} is not published for {market} {period}"
                        )));
                    }
                    push_job(
                        &mut jobs,
                        &mut seen,
                        DownloadJob {
                            kind,
                            interval: Some(*interval),
                            period: *period,
                        },
                    )?;
                }
            } else {
                push_job(
                    &mut jobs,
                    &mut seen,
                    DownloadJob {
                        kind,
                        interval: None,
                        period: *period,
                    },
                )?;
            }
        }
    }

    jobs.sort_by_key(|job| (job.kind, job.period, job.interval));
    Ok(jobs)
}

/// Record one job, rejecting a duplicate declaration.
fn push_job(
    jobs: &mut Vec<DownloadJob>,
    seen: &mut HashSet<DownloadJob>,
    job: DownloadJob,
) -> Result<(), ConfigError> {
    if !seen.insert(job) {
        return Err(ConfigError::invalid(format!(
            "duplicate dataset entry: {job}"
        )));
    }
    jobs.push(job);
    Ok(())
}

/// Reject two symbols whose jobs would write to the same archive directory.
fn reject_colliding_archives(
    symbols: &[String],
    jobs: &[DownloadJob],
    market: Market,
) -> Result<(), ConfigError> {
    let mut archive_dirs = HashSet::new();
    for symbol in symbols {
        for job in jobs {
            let dir = job.relative_dir(market, symbol);
            if !archive_dirs.insert(dir.clone()) {
                return Err(ConfigError::invalid(format!(
                    "symbols map to the same archive directory {dir} for {job}; \
                     indexPriceKlines uses the underlying index, so configure only one \
                     contract per underlying"
                )));
            }
        }
    }
    Ok(())
}

/// Accepted symbol characters: ASCII letters (either case), digits, and `_`
/// (COIN-M contracts use `_`, e.g. `BTCUSD_PERP`; USDⓈ-M quarterly contracts
/// use `_` too, e.g. `BTCUSDT_250926`). Hyphens are not allowed, which keeps
/// the date suffix unambiguous.
///
/// Validation is case-insensitive because symbols are uppercased afterwards by
/// [`normalize_symbols`]. Validating locally turns typos into a clear config
/// error instead of broken URLs, and keeps symbols safe as path components (no
/// traversal via `..`).
fn is_valid_symbol(symbol: &str) -> bool {
    !symbol.is_empty()
        && symbol
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
}
