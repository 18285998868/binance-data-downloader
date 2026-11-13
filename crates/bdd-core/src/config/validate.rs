//! Semantic validation of the deserialized [`AppConfig`].

use std::path::Path;

use crate::AppError;

use super::{AppConfig, MAX_CONCURRENCY};

impl AppConfig {
    /// Check every semantic rule, tagging each failure with `path`.
    pub(super) fn validate(mut self, path: &Path) -> Result<Self, AppError> {
        let err = |message: String| AppError::Config {
            path: Some(path.to_path_buf()),
            message,
        };
        if self.symbol.is_empty() {
            return Err(err("symbol must contain at least one symbol".into()));
        }
        if self.dataset.is_empty() {
            return Err(err("dataset must contain at least one entry".into()));
        }
        if self.output_dir.as_os_str().is_empty() {
            return Err(err("output_dir must not be empty".into()));
        }
        if self.concurrency == 0 {
            return Err(err("concurrency must be greater than zero".into()));
        }
        if self.concurrency > MAX_CONCURRENCY {
            return Err(err(format!(
                "concurrency must not exceed {MAX_CONCURRENCY}, got {}",
                self.concurrency
            )));
        }
        if let (Some(start), Some(end)) = (self.start, self.end)
            && start > end
        {
            return Err(err(format!("start {start} is after end {end}")));
        }
        if let Some(threshold) = self.not_found_pct_threshold
            && !(threshold > 0.0 && threshold <= 100.0)
        {
            // NaN is rejected as well: both comparisons are false for it.
            return Err(err(format!(
                "not_found_pct_threshold must be in (0, 100], got {threshold}"
            )));
        }
        if let Some(bad) = self.symbol.iter().find(|s| !is_valid_symbol(s)) {
            return Err(err(format!(
                "invalid symbol: {bad:?} (expected ASCII letters, digits or underscore)"
            )));
        }

        let mut seen = std::collections::HashSet::new();
        for dataset in &mut self.dataset {
            let kind = dataset.kind;
            if dataset.periods.is_empty() {
                return Err(err(format!(
                    "dataset {kind} must contain at least one period"
                )));
            }
            dataset.periods.sort();
            dataset.periods.dedup();

            if kind.needs_interval() {
                if dataset.intervals.is_empty() {
                    return Err(err(format!(
                        "dataset {kind} requires at least one interval"
                    )));
                }
                dataset.intervals.sort();
                dataset.intervals.dedup();
            } else if !dataset.intervals.is_empty() {
                return Err(err(format!(
                    "dataset {kind} does not take intervals (got {:?})",
                    dataset.intervals
                )));
            }

            for period in &dataset.periods {
                if !kind.supports(self.market, *period) {
                    return Err(err(format!(
                        "dataset {kind} is not published for {} {period}",
                        self.market
                    )));
                }
                if kind.needs_interval() {
                    for interval in &dataset.intervals {
                        if !interval.available_for(self.market, *period) {
                            return Err(err(format!(
                                "interval {interval} is not published for {} {period}",
                                self.market
                            )));
                        }
                    }
                }
            }

            for period in &dataset.periods {
                if kind.needs_interval() {
                    for interval in &dataset.intervals {
                        if !seen.insert((kind, Some(*interval), *period)) {
                            return Err(err(format!(
                                "duplicate dataset entry: {kind}/{interval}/{period}"
                            )));
                        }
                    }
                } else if !seen.insert((kind, None, *period)) {
                    return Err(err(format!("duplicate dataset entry: {kind}/{period}")));
                }
            }
        }

        // `indexPriceKlines` uses the underlying index symbol, so a perpetual
        // and a dated contract of the same underlying map to one archive path.
        // Reject that instead of racing two downloads on one destination.
        let symbols = self.normalized_symbols();
        let jobs = self.jobs();
        let mut archive_dirs = std::collections::HashSet::new();
        for symbol in &symbols {
            for job in &jobs {
                let dir = job.relative_dir(self.market, symbol);
                if !archive_dirs.insert(dir.clone()) {
                    return Err(err(format!(
                        "symbols map to the same archive directory {dir} for {job}; \
                         indexPriceKlines uses the underlying index, so configure only one \
                         contract per underlying"
                    )));
                }
            }
        }

        Ok(self)
    }
}

/// Accepted symbol characters: ASCII letters (either case), digits, and `_`
/// (COIN-M contracts use `_`, e.g. `BTCUSD_PERP`; USDⓈ-M quarterly contracts
/// use `_` too, e.g. `BTCUSDT_250926`). Hyphens are not allowed, which keeps
/// the date suffix unambiguous.
///
/// Validation is case-insensitive because symbols are uppercased afterwards by
/// [`AppConfig::normalized_symbols`]. Validating locally turns typos into a
/// clear config error instead of broken URLs, and keeps symbols safe as path
/// components (no traversal via `..`).
fn is_valid_symbol(symbol: &str) -> bool {
    !symbol.is_empty()
        && symbol
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
}
