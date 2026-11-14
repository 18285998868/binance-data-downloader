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
//!
//! The module is split in three layers:
//!
//! - [`raw`] describes the file (`RawConfig`, `[[dataset]]` entries).
//! - [`validate`] checks the semantics and compiles it into [`Config`].
//! - [`Config`] is what the rest of the crate sees: fields are private and
//!   everything derived (normalized symbols, expanded jobs) is precomputed.

mod error;
mod raw;
mod validate;

use std::path::{Path, PathBuf};

use crate::dataset::{DownloadJob, Market};

use self::error::ConfigError;
use self::raw::RawConfig;

/// Load and validate configuration from a TOML file.
///
/// # Errors
///
/// Returns [`ConfigError`] when the file cannot be read, the TOML is invalid,
/// or validation fails. The error carries the file path.
pub(crate) async fn load(path: &Path) -> Result<Config, ConfigError> {
    let raw = tokio::fs::read_to_string(path)
        .await
        .map_err(|error| ConfigError::io(path, &error))?;
    parse_str(&raw, path)
}

/// Parse and validate an in-memory configuration.
fn parse_str(raw: &str, path: &Path) -> Result<Config, ConfigError> {
    let raw: RawConfig =
        toml::from_str(raw).map_err(|error| ConfigError::invalid(error.to_string()))?;
    Config::try_from(raw).map_err(|error| error.with_path(path))
}

/// Validated, normalized configuration.
///
/// Constructed with `TryFrom<RawConfig>`, so every invariant holds by the time
/// business code receives one: symbols are non-empty, uppercased, sorted and
/// unique, and `jobs` is expanded and sorted.
#[derive(Debug, Clone)]
pub(crate) struct Config {
    market: Market,
    symbols: Vec<String>,
    start: Option<chrono::NaiveDate>,
    end: Option<chrono::NaiveDate>,
    output_dir: PathBuf,
    concurrency: usize,
    not_found_pct_threshold: Option<f64>,
    jobs: Vec<DownloadJob>,
}

/// Sanity ceiling for `concurrency`: each in-flight download costs a socket,
/// a file handle and buffers, so anything beyond this is almost certainly a
/// typo rather than intent.
///
/// The effective ceiling is lower still: see [`crate::limits`], which caps the
/// concurrency by the descriptors the process may actually open.
const MAX_CONCURRENCY: usize = crate::limits::MAX_CONCURRENCY;

impl Config {
    /// Archive market.
    #[must_use]
    pub(crate) fn market(&self) -> Market {
        self.market
    }

    /// Normalized symbols: uppercased, sorted and deduplicated.
    #[must_use]
    pub(crate) fn symbols(&self) -> &[String] {
        &self.symbols
    }

    /// Optional inclusive start date override.
    #[must_use]
    pub(crate) fn start(&self) -> Option<chrono::NaiveDate> {
        self.start
    }

    /// Parent directory of the archive's `data/` tree.
    #[must_use]
    pub(crate) fn output_dir(&self) -> &Path {
        &self.output_dir
    }

    /// Requested maximum number of concurrent downloads.
    #[must_use]
    pub(crate) fn concurrency(&self) -> usize {
        self.concurrency
    }

    /// Threshold on the share of files the server did not have, in percent.
    #[must_use]
    pub(crate) fn not_found_pct_threshold(&self) -> Option<f64> {
        self.not_found_pct_threshold
    }

    /// Expanded dataset jobs, sorted by `(kind, period, interval)`.
    #[must_use]
    pub(crate) fn jobs(&self) -> &[DownloadJob] {
        &self.jobs
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
}

/// A `Config` for tests outside this module, skipping file and semantic
/// validation. Only the fields those tests exercise are configurable; the
/// defaults match the values used across the pipeline tests.
#[cfg(test)]
#[must_use]
pub(crate) fn for_tests(
    market: Market,
    symbols: Vec<String>,
    jobs: Vec<DownloadJob>,
    start: Option<chrono::NaiveDate>,
    output_dir: PathBuf,
) -> Config {
    Config {
        market,
        symbols: {
            let mut symbols: Vec<String> = symbols
                .into_iter()
                .map(|symbol| symbol.to_ascii_uppercase())
                .collect();
            symbols.sort();
            symbols.dedup();
            symbols
        },
        start,
        end: None,
        output_dir,
        concurrency: 1,
        not_found_pct_threshold: None,
        jobs,
    }
}

#[cfg(test)]
mod tests {
    use crate::dataset::{DatasetKind, Market};
    use std::io::Write;
    use std::path::{Path, PathBuf};

    use super::*;

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

    fn parse(contents: &str) -> Result<Config, ConfigError> {
        let (_dir, path) = write_config(contents);
        let raw = std::fs::read_to_string(&path).unwrap();
        parse_str(&raw, &path)
    }

    #[test]
    fn load_full_config() {
        let cfg = parse(
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
        assert_eq!(cfg.symbols(), ["BTCUSDT", "ETHUSDT"]);
        assert_eq!(cfg.market(), Market::Um);
        assert_eq!(
            cfg.start(),
            Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        );
        assert_eq!(cfg.output_dir(), Path::new("/tmp/data"));
        assert_eq!(cfg.concurrency(), 64);
        assert_eq!(cfg.jobs().len(), 4);
    }

    #[test]
    fn the_shipped_example_config_is_valid() {
        let raw = include_str!("../../../../config/00.example.toml");
        let cfg = parse_str(raw, Path::new("config/00.example.toml")).unwrap();
        assert_eq!(cfg.market(), Market::Um);
        assert_eq!(cfg.jobs().len(), 2);
    }

    #[test]
    fn load_minimal_config() {
        let cfg = parse(BASE).unwrap();
        assert_eq!(cfg.symbols(), ["BTCUSDT"]);
        assert_eq!(cfg.start(), None);
        assert_eq!(cfg.output_dir(), Path::new("."));
        assert_eq!(cfg.concurrency(), 64);
        assert_eq!(cfg.jobs().len(), 1);
    }

    #[test]
    fn market_is_required() {
        let err = parse("symbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n").unwrap_err();
        assert!(err.to_string().contains("market"), "got {err}");
    }

    #[test]
    fn dataset_is_required() {
        let err = parse("market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n").unwrap_err();
        assert!(err.to_string().contains("dataset"), "got {err}");
    }

    #[test]
    fn legacy_timeframe_key_is_rejected() {
        let err = parse(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("timeframe"), "got {err}");
    }

    #[test]
    fn invalid_market_fails() {
        let err = parse(
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
            let cfg = parse(&format!(
                "market = \"{value}\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n"
            ))
            .unwrap();
            assert_eq!(cfg.market(), expected);
        }
    }

    #[test]
    fn empty_output_dir_is_rejected() {
        let err = parse(&format!("output_dir = \"\"\n{BASE}")).unwrap_err();
        assert!(err.to_string().contains("output_dir"), "got {err}");
    }

    #[test]
    fn load_with_concurrency_override() {
        let cfg = parse(&format!("concurrency = 8\n{BASE}")).unwrap();
        assert_eq!(cfg.concurrency(), 8);
    }

    #[test]
    fn zero_concurrency_fails() {
        let err = parse(&format!("concurrency = 0\n{BASE}")).unwrap_err();
        assert!(err.to_string().contains("concurrency"));
    }

    #[test]
    fn excessive_concurrency_fails() {
        let err = parse(&format!("concurrency = 4096\n{BASE}")).unwrap_err();
        assert!(err.to_string().contains("concurrency"));
    }

    #[test]
    fn missing_symbol_fails() {
        let err = parse("market = \"futures/um\"\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n").unwrap_err();
        assert!(err.to_string().contains("symbol"));
    }

    #[test]
    fn empty_symbol_fails() {
        let err = parse("market = \"futures/um\"\nsymbol = []\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n").unwrap_err();
        assert!(err.to_string().contains("symbol"));
    }

    #[test]
    fn empty_dataset_fails() {
        let err =
            parse("market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\ndataset = []\n").unwrap_err();
        assert!(err.to_string().contains("dataset"));
    }

    #[test]
    fn unknown_dataset_type_fails() {
        let err = parse(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"nope\"\nperiods = [\"daily\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("dataset type"), "got {err}");
    }

    #[test]
    fn unknown_dataset_key_fails() {
        let err = parse(&format!("{BASE}unknown = 1\n")).unwrap_err();
        assert!(err.to_string().contains("unknown"), "got {err}");
    }

    #[test]
    fn empty_periods_fail() {
        let err = parse(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = []\nintervals = [\"1m\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("period"), "got {err}");
    }

    #[test]
    fn unsupported_dataset_for_market_fails() {
        let err = parse(
            "market = \"spot\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"fundingRate\"\nperiods = [\"monthly\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("not published"), "got {err}");
    }

    #[test]
    fn unsupported_dataset_period_fails() {
        let err = parse(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"bookDepth\"\nperiods = [\"monthly\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("not published"), "got {err}");
    }

    #[test]
    fn kline_datasets_require_intervals() {
        let err = parse(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("requires"), "got {err}");
    }

    #[test]
    fn non_kline_datasets_reject_intervals() {
        let err = parse(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"aggTrades\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not take"), "got {err}");
    }

    #[test]
    fn invalid_interval_fails() {
        let err = parse(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1y\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("1y"), "got {err}");
    }

    #[test]
    fn interval_availability_is_validated_per_period() {
        let err = parse(
            "market = \"futures/um\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1w\"]\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("not published"), "got {err}");
    }

    #[test]
    fn spot_one_second_klines_are_accepted() {
        let cfg = parse(
            "market = \"spot\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\", \"monthly\"]\nintervals = [\"1s\"]\n",
        )
        .unwrap();
        assert_eq!(cfg.jobs().len(), 2);
    }

    #[test]
    fn spot_monthly_one_month_klines_are_accepted() {
        let cfg = parse(
            "market = \"spot\"\nsymbol = [\"BTCUSDT\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"monthly\"]\nintervals = [\"1mo\"]\n",
        )
        .unwrap();
        assert_eq!(cfg.jobs().len(), 1);
    }

    #[test]
    fn duplicate_dataset_entries_fail() {
        let err = parse(&format!(
            "{BASE}\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n"
        ))
        .unwrap_err();
        assert!(err.to_string().contains("duplicate"), "got {err}");
    }

    #[test]
    fn colliding_index_symbols_are_rejected() {
        // Both contracts share the BTCUSD index path; two pairs would race on
        // the same destination file.
        let err = parse(
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
        let cfg = parse(
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
        let cfg = parse(
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
        let err = parse(&format!("start = \"not-a-date\"\n{BASE}")).unwrap_err();
        assert!(err.to_string().contains("start"), "got {err}");
    }

    #[test]
    fn resolved_end_clamps_future_to_yesterday() {
        let cfg = parse(&format!("end = \"2030-01-10\"\n{BASE}")).unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2030, 1, 10).unwrap();
        assert_eq!(
            cfg.resolved_end(today),
            chrono::NaiveDate::from_ymd_opt(2030, 1, 9).unwrap()
        );
    }

    #[test]
    fn resolved_end_defaults_to_yesterday() {
        let cfg = parse(BASE).unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        assert_eq!(
            cfg.resolved_end(today),
            chrono::NaiveDate::from_ymd_opt(2024, 6, 14).unwrap()
        );
    }

    #[test]
    fn start_after_end_fails() {
        let err = parse(&format!(
            "start = \"2024-02-01\"\nend = \"2024-01-01\"\n{BASE}"
        ))
        .unwrap_err();
        assert!(err.to_string().contains("start"));
    }

    #[test]
    fn duplicates_are_sorted_and_deduped() {
        let cfg = parse(
            "market = \"futures/um\"\n\
             symbol = [\"ETHUSDT\", \"BTCUSDT\", \"ETHUSDT\"]\n\
             [[dataset]]\n\
             type = \"klines\"\n\
             periods = [\"monthly\", \"daily\", \"monthly\"]\n\
             intervals = [\"1d\", \"1m\", \"1m\"]\n",
        )
        .unwrap();
        assert_eq!(cfg.symbols(), ["BTCUSDT", "ETHUSDT"]);
        assert_eq!(cfg.jobs().len(), 4);
    }

    #[test]
    fn symbols_are_uppercased() {
        let cfg = parse(
            "market = \"futures/um\"\nsymbol = [\"btcusdt\", \"ethusdt\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n",
        )
        .unwrap();
        assert_eq!(cfg.symbols(), ["BTCUSDT", "ETHUSDT"]);
    }

    #[test]
    fn digit_and_underscore_symbols_are_valid() {
        let cfg = parse(
            "market = \"futures/cm\"\nsymbol = [\"1000pepeusdt\", \"btcusd_perp\"]\n[[dataset]]\ntype = \"klines\"\nperiods = [\"daily\"]\nintervals = [\"1m\"]\n",
        )
        .unwrap();
        assert_eq!(cfg.symbols(), ["1000PEPEUSDT", "BTCUSD_PERP"]);
    }

    #[test]
    fn symbols_with_invalid_characters_fail() {
        for sym in ["BTC/USDT", "../etc", "BTC USDT", "BTC-USDT", ""] {
            let err = parse(&format!(
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
        assert!(load(Path::new("/nonexistent/config.toml")).await.is_err());
    }

    #[test]
    fn concurrency_boundary_is_inclusive_at_max() {
        let cfg = parse(&format!("concurrency = {MAX_CONCURRENCY}\n{BASE}")).unwrap();
        assert_eq!(cfg.concurrency(), MAX_CONCURRENCY);
    }

    #[test]
    fn concurrency_one_above_max_fails() {
        let err = parse(&format!("concurrency = {}\n{BASE}", MAX_CONCURRENCY + 1)).unwrap_err();
        assert!(err.to_string().contains("concurrency"));
    }

    #[test]
    fn resolved_end_keeps_a_past_end_untouched() {
        let cfg = parse(&format!("end = \"2020-05-05\"\n{BASE}")).unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        assert_eq!(
            cfg.resolved_end(today),
            chrono::NaiveDate::from_ymd_opt(2020, 5, 5).unwrap()
        );
    }

    #[test]
    fn resolved_end_saturates_at_the_minimum_date_instead_of_panicking() {
        let cfg = parse(BASE).unwrap();
        assert_eq!(
            cfg.resolved_end(chrono::NaiveDate::MIN),
            chrono::NaiveDate::MIN
        );
    }

    #[test]
    fn not_found_threshold_defaults_to_none() {
        let cfg = parse(BASE).unwrap();
        assert_eq!(cfg.not_found_pct_threshold(), None);
    }

    #[test]
    fn a_not_found_threshold_in_range_is_accepted() {
        for threshold in ["0.1", "1", "50", "100"] {
            let cfg = parse(&format!("not_found_pct_threshold = {threshold}\n{BASE}")).unwrap();
            assert!(cfg.not_found_pct_threshold().is_some());
        }
    }

    #[test]
    fn an_out_of_range_not_found_threshold_fails() {
        for threshold in ["0", "-1", "100.1", "1000"] {
            let err = parse(&format!("not_found_pct_threshold = {threshold}\n{BASE}")).unwrap_err();
            assert!(
                err.to_string().contains("not_found_pct_threshold"),
                "threshold {threshold} slipped through: {err}"
            );
        }
    }
}
