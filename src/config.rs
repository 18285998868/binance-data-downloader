//! TOML configuration: schema, defaults and validation.
//!
//! The configuration mirrors the public archive layout:
//!
//! ```toml
//! market = "futures/um"
//! symbol = ["BTCUSDT"]
//!
//! [[dataset]]
//! type = "klines"
//! periods = ["daily", "monthly"]
//! intervals = ["1m", "1d"]
//! ```
//!
//! `output_dir` is the parent of the archive's `data/` directory. The final
//! path is `output_dir/data/futures/um/daily/klines/...`, exactly the path the
//! web page uses.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::AppError;
use crate::dataset::{DatasetKind, DownloadJob, Interval, Market, Period};

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

/// Sanity ceiling for `concurrency`: each in-flight download costs a socket,
/// a file handle and buffers, so anything beyond this is almost certainly a
/// typo rather than intent.
///
/// The effective ceiling is lower still: see [`crate::limits`], which caps the
/// concurrency by the descriptors the process may actually open.
const MAX_CONCURRENCY: usize = crate::limits::MAX_CONCURRENCY;

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

    fn parse_str(raw: &str, path: &Path) -> Result<Self, AppError> {
        let cfg: Self = toml::from_str(raw).map_err(|e| AppError::Config {
            path: Some(path.to_path_buf()),
            message: e.to_string(),
        })?;
        cfg.validate(path)
    }

    /// Resolve the daily download end date: `min(config.end, yesterday)`.
    /// Computed once by the caller; downstream only consumes the value.
    #[must_use]
    pub(crate) fn resolved_end(&self, today: chrono::NaiveDate) -> chrono::NaiveDate {
        // Saturate instead of panicking: `NaiveDate - Duration` overflows at the
        // representable minimum, and a caller must never crash the run over it.
        let yesterday = today
            .checked_sub_days(chrono::Days::new(1))
            .unwrap_or(today);
        self.end.map_or(yesterday, |e| e.min(yesterday))
    }

    /// Normalized symbols: uppercased, sorted and deduplicated.
    #[must_use]
    pub(crate) fn normalized_symbols(&self) -> Vec<String> {
        let mut out: Vec<String> = self.symbol.iter().map(|s| s.to_ascii_uppercase()).collect();
        out.sort();
        out.dedup();
        out
    }

    /// Expand every `[[dataset]]` entry into concrete jobs.
    ///
    /// A dataset with two periods and three intervals yields six jobs; a
    /// dataset without intervals yields one job per period.
    #[must_use]
    pub(crate) fn jobs(&self) -> Vec<DownloadJob> {
        let mut jobs = Vec::new();
        for dataset in &self.dataset {
            for period in &dataset.periods {
                if dataset.kind.needs_interval() {
                    for interval in &dataset.intervals {
                        jobs.push(DownloadJob {
                            kind: dataset.kind,
                            interval: Some(*interval),
                            period: *period,
                        });
                    }
                } else {
                    jobs.push(DownloadJob {
                        kind: dataset.kind,
                        interval: None,
                        period: *period,
                    });
                }
            }
        }
        jobs.sort_by_key(|job| (job.kind, job.period, job.interval));
        jobs
    }

    /// Check every semantic rule, tagging each failure with `path`.
    fn validate(mut self, path: &Path) -> Result<Self, AppError> {
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

/// A download task: source URL, destination path and identifying metadata.
#[derive(Debug, Clone)]
pub(crate) struct DownloadTask {
    /// Source URL.
    pub url: String,
    /// Destination path on disk.
    pub dest: PathBuf,
    /// Trading pair symbol.
    pub pair: String,
    /// Dataset/period/interval this file belongs to.
    pub job: DownloadJob,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const BASE: &str = "\
market = \"futures/um\"
symbol = [\"BTCUSDT\"]

[[dataset]]
type = \"klines\"
periods = [\"daily\"]
intervals = [\"1m\"]
";

    fn write_config(contents: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "{contents}").unwrap();
        drop(f);
        (dir, path)
    }

    fn load(contents: &str) -> Result<AppConfig, AppError> {
        let (_dir, path) = write_config(contents);
        let raw = std::fs::read_to_string(&path).unwrap();
        AppConfig::parse_str(&raw, &path)
    }

    #[test]
    fn load_full_config() {
        let cfg = load(
            "market = \"futures/um\"\n\
             symbol = [\"BTCUSDT\", \"ETHUSDT\"]\n\
             start = \"2024-01-01\"\n\
             end = \"2025-01-01\"\n\
             output_dir = \"/tmp/data\"\n\
             \n\
             [[dataset]]\n\
             type = \"klines\"\n\
             periods = [\"daily\", \"monthly\"]\n\
             intervals = [\"1m\", \"1d\"]\n",
        )
        .unwrap();
        assert_eq!(cfg.symbol, vec!["BTCUSDT", "ETHUSDT"]);
        assert_eq!(cfg.normalized_symbols(), vec!["BTCUSDT", "ETHUSDT"]);
        assert_eq!(cfg.market, Market::Um);
        assert_eq!(
            cfg.start,
            Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        );
        assert_eq!(
            cfg.end,
            Some(chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap())
        );
        assert_eq!(cfg.output_dir, PathBuf::from("/tmp/data"));
        assert_eq!(cfg.concurrency, 64);
        assert_eq!(cfg.jobs().len(), 4);
    }

    #[test]
    fn the_shipped_example_config_is_valid() {
        let raw = include_str!("../config.example.toml");
        let cfg = AppConfig::parse_str(raw, Path::new("config.example.toml")).unwrap();
        assert_eq!(cfg.market, Market::Um);
        assert_eq!(cfg.jobs().len(), 2);
    }

    #[test]
    fn load_minimal_config() {
        let cfg = load(BASE).unwrap();
        assert_eq!(cfg.symbol, vec!["BTCUSDT"]);
        assert_eq!(cfg.start, None);
        assert_eq!(cfg.end, None);
        assert_eq!(cfg.output_dir, PathBuf::from("."));
        assert_eq!(cfg.concurrency, 64);
        assert_eq!(cfg.jobs().len(), 1);
    }

    #[test]
    fn market_is_required() {
        let err = load("symbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n").unwrap_err();
        assert!(err.to_string().contains("market"), "got {err}");
    }

    #[test]
    fn dataset_is_required() {
        let err = load("market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n").unwrap_err();
        assert!(err.to_string().contains("dataset"), "got {err}");
    }

    #[test]
    fn legacy_timeframe_key_is_rejected() {
        let err = load(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("timeframe"), "got {err}");
    }

    #[test]
    fn invalid_market_fails() {
        let err = load(
            "market = \"fapi\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("market"), "got {err}");
    }

    #[test]
    fn market_accepts_the_canonical_archive_path() {
        for (value, expected) in [
            ("spot", Market::Spot),
            ("futures/um", Market::Um),
            ("futures/cm", Market::Cm),
            ("um", Market::Um),
            ("cm", Market::Cm),
        ] {
            let cfg = load(&format!(
                "market = \"{value}\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n"
            ))
            .unwrap();
            assert_eq!(cfg.market, expected);
        }
    }

    #[test]
    fn empty_output_dir_is_rejected() {
        let err = load(&format!("output_dir = \"\"\n{BASE}")).unwrap_err();
        assert!(err.to_string().contains("output_dir"), "got {err}");
    }

    #[test]
    fn load_with_concurrency_override() {
        let cfg = load(&format!("concurrency = 8\n{BASE}")).unwrap();
        assert_eq!(cfg.concurrency, 8);
    }

    #[test]
    fn zero_concurrency_fails() {
        let err = load(&format!("concurrency = 0\n{BASE}")).unwrap_err();
        assert!(err.to_string().contains("concurrency"));
    }

    #[test]
    fn excessive_concurrency_fails() {
        let err = load(&format!("concurrency = 4096\n{BASE}")).unwrap_err();
        assert!(err.to_string().contains("concurrency"));
    }

    #[test]
    fn missing_symbol_fails() {
        let err = load("market = \"futures/um\"\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n").unwrap_err();
        assert!(err.to_string().contains("symbol"));
    }

    #[test]
    fn empty_symbol_fails() {
        let err = load("market = \"futures/um\"\nsymbol = []\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n").unwrap_err();
        assert!(err.to_string().contains("symbol"));
    }

    #[test]
    fn empty_dataset_fails() {
        let err =
            load("market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\ndataset = []\n").unwrap_err();
        assert!(err.to_string().contains("dataset"));
    }

    #[test]
    fn unknown_dataset_type_fails() {
        let err = load(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"nope\"\nperiods = [\"daily\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("dataset type"), "got {err}");
    }

    #[test]
    fn unknown_dataset_key_fails() {
        let err = load(&format!("{BASE}unknown = 1\n")).unwrap_err();
        assert!(err.to_string().contains("unknown"), "got {err}");
    }

    #[test]
    fn empty_periods_fail() {
        let err = load(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = []\nintervals = [\"1m\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("period"), "got {err}");
    }

    #[test]
    fn unsupported_dataset_for_market_fails() {
        let err = load(
            "market = \"spot\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"fundingRate\"\nperiods = [\"monthly\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("not published"), "got {err}");
    }

    #[test]
    fn unsupported_dataset_period_fails() {
        let err = load(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"bookDepth\"\nperiods = [\"monthly\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("not published"), "got {err}");
    }

    #[test]
    fn kline_datasets_require_intervals() {
        let err = load(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("requires"), "got {err}");
    }

    #[test]
    fn non_kline_datasets_reject_intervals() {
        let err = load(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"aggTrades\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not take"), "got {err}");
    }

    #[test]
    fn invalid_interval_fails() {
        let err = load(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1y\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("1y"), "got {err}");
    }

    #[test]
    fn interval_availability_is_validated_per_period() {
        let err = load(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1w\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("not published"), "got {err}");
    }

    #[test]
    fn spot_one_second_klines_are_accepted() {
        let cfg = load(
            "market = \"spot\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\", \"monthly\"]\nintervals = [\"1s\"]\n",
        )
        .unwrap();
        assert_eq!(cfg.jobs().len(), 2);
    }

    #[test]
    fn spot_monthly_one_month_klines_are_accepted() {
        let cfg = load(
            "market = \"spot\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"monthly\"]\nintervals = [\"1mo\"]\n",
        )
        .unwrap();
        assert_eq!(cfg.jobs().len(), 1);
    }

    #[test]
    fn duplicate_dataset_entries_fail() {
        let err = load(&format!(
            "{BASE}\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n"
        ))
        .unwrap_err();
        assert!(err.to_string().contains("duplicate"), "got {err}");
    }

    #[test]
    fn colliding_index_symbols_are_rejected() {
        // Both contracts share the BTCUSD index path; two pairs would race on
        // the same destination file.
        let err = load(
            "market = \"futures/cm\"\nsymbol = [\"BTCUSD_PERP\", \"BTCUSD_250926\"]\n[[dataset]]\ntype = \"indexPriceKlines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n",
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("same archive directory"),
            "got {err}"
        );
    }

    #[test]
    fn jobs_expand_periods_and_intervals() {
        let cfg = load(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"monthly\", \"daily\"]\nintervals = [\"1d\", \"1m\"]\n",
        )
        .unwrap();
        let jobs: Vec<String> = cfg.jobs().iter().map(ToString::to_string).collect();
        assert_eq!(
            jobs,
            vec![
                "klines/1m/daily",
                "klines/1d/daily",
                "klines/1m/monthly",
                "klines/1d/monthly",
            ]
        );
    }

    #[test]
    fn non_kline_jobs_have_no_interval() {
        let cfg = load(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"fundingRate\"\nperiods = [\"monthly\"]\n",
        )
        .unwrap();
        let jobs = cfg.jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].interval, None);
        assert_eq!(jobs[0].kind, DatasetKind::FundingRate);
    }

    #[test]
    fn invalid_date_fails() {
        let err = load(&format!("start = \"not-a-date\"\n{BASE}")).unwrap_err();
        assert!(err.to_string().contains("start"), "got {err}");
    }

    #[test]
    fn resolved_end_clamps_future_to_yesterday() {
        let cfg = load(&format!("end = \"2030-01-10\"\n{BASE}")).unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2030, 1, 10).unwrap();
        assert_eq!(
            cfg.resolved_end(today),
            chrono::NaiveDate::from_ymd_opt(2030, 1, 9).unwrap()
        );
    }

    #[test]
    fn resolved_end_defaults_to_yesterday() {
        let cfg = load(BASE).unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        assert_eq!(
            cfg.resolved_end(today),
            chrono::NaiveDate::from_ymd_opt(2024, 6, 14).unwrap()
        );
    }

    #[test]
    fn start_after_end_fails() {
        let err = load(&format!(
            "start = \"2024-02-01\"\nend = \"2024-01-01\"\n{BASE}"
        ))
        .unwrap_err();
        assert!(err.to_string().contains("start"));
    }

    #[test]
    fn duplicates_are_sorted_and_deduped() {
        let cfg = load(
            "market = \"futures/um\"\n\
             symbol = [\"ETHUSDT\", \"BTCUSDT\", \"ETHUSDT\"]\n\
             [[dataset]]\n\
             type = \"klines\"\n\
             periods = [\"monthly\", \"daily\", \"monthly\"]\n\
             intervals = [\"1d\", \"1m\", \"1m\"]\n",
        )
        .unwrap();
        assert_eq!(cfg.symbol, vec!["ETHUSDT", "BTCUSDT", "ETHUSDT"]);
        assert_eq!(cfg.normalized_symbols(), vec!["BTCUSDT", "ETHUSDT"]);
        assert_eq!(cfg.jobs().len(), 4);
    }

    #[test]
    fn symbols_are_uppercased() {
        let cfg = load(
            "market = \"futures/um\"\nsymbol = [\"btcusdt\", \"ethusdt\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n",
        )
        .unwrap();
        assert_eq!(cfg.symbol, vec!["btcusdt", "ethusdt"]);
        assert_eq!(cfg.normalized_symbols(), vec!["BTCUSDT", "ETHUSDT"]);
    }

    #[test]
    fn digit_and_underscore_symbols_are_valid() {
        let cfg = load(
            "market = \"futures/cm\"\nsymbol = [\"1000pepeusdt\", \"btcusd_perp\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n",
        )
        .unwrap();
        assert_eq!(
            cfg.normalized_symbols(),
            vec!["1000PEPEUSDT", "BTCUSD_PERP"]
        );
    }

    #[test]
    fn symbols_with_invalid_characters_fail() {
        for sym in ["BTC/USDT", "../etc", "BTC USDT", "BTC-USDT", ""] {
            let err = load(&format!(
                "market = \"futures/um\"\nsymbol = [\"{sym}\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n"
            ))
            .unwrap_err();
            assert!(
                err.to_string().contains("symbol"),
                "symbol {sym:?} slipped through"
            );
        }
    }

    #[tokio::test]
    async fn nonexistent_path_fails() {
        assert!(
            AppConfig::load(Path::new("/nonexistent/config.toml"))
                .await
                .is_err()
        );
    }

    #[test]
    fn concurrency_boundary_is_inclusive_at_max() {
        let cfg = load(&format!("concurrency = {MAX_CONCURRENCY}\n{BASE}")).unwrap();
        assert_eq!(cfg.concurrency, MAX_CONCURRENCY);
    }

    #[test]
    fn concurrency_one_above_max_fails() {
        let err = load(&format!("concurrency = {}\n{BASE}", MAX_CONCURRENCY + 1)).unwrap_err();
        assert!(err.to_string().contains("concurrency"));
    }

    #[test]
    fn resolved_end_keeps_a_past_end_untouched() {
        let cfg = load(&format!("end = \"2020-05-05\"\n{BASE}")).unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        assert_eq!(
            cfg.resolved_end(today),
            chrono::NaiveDate::from_ymd_opt(2020, 5, 5).unwrap()
        );
    }

    #[test]
    fn resolved_end_saturates_at_the_minimum_date_instead_of_panicking() {
        let cfg = load(BASE).unwrap();
        assert_eq!(
            cfg.resolved_end(chrono::NaiveDate::MIN),
            chrono::NaiveDate::MIN
        );
    }

    #[test]
    fn not_found_threshold_defaults_to_none() {
        let cfg = load(BASE).unwrap();
        assert_eq!(cfg.not_found_pct_threshold, None);
    }

    #[test]
    fn a_not_found_threshold_in_range_is_accepted() {
        for threshold in ["0.1", "1", "50", "100"] {
            let cfg = load(&format!("not_found_pct_threshold = {threshold}\n{BASE}")).unwrap();
            assert!(cfg.not_found_pct_threshold.is_some());
        }
    }

    #[test]
    fn an_out_of_range_not_found_threshold_fails() {
        for threshold in ["0", "-1", "100.1", "1000"] {
            let err = load(&format!("not_found_pct_threshold = {threshold}\n{BASE}")).unwrap_err();
            assert!(
                err.to_string().contains("not_found_pct_threshold"),
                "threshold {threshold} slipped through: {err}"
            );
        }
    }
}
