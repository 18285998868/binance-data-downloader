//! TOML configuration: schema, defaults and validation.
//!
//! The parsed value keeps the *raw* input ([`AppConfig::symbol`] is stored
//! verbatim) and exposes normalized views on top, so validation errors can quote
//! exactly what the user wrote.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::AppError;

/// Binance kline interval, matching the archive's directory/interval names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Timeframe {
    /// 1-minute klines.
    M1,
    /// 3-minute klines.
    M3,
    /// 5-minute klines.
    M5,
    /// 15-minute klines.
    M15,
    /// 30-minute klines.
    M30,
    /// 1-hour klines.
    H1,
    /// 2-hour klines.
    H2,
    /// 4-hour klines.
    H4,
    /// 6-hour klines.
    H6,
    /// 8-hour klines.
    H8,
    /// 12-hour klines.
    H12,
    /// 1-day klines.
    D1,
}

impl Timeframe {
    /// All accepted timeframes, ordered by duration.
    pub(crate) const ALL: [Self; 12] = [
        Self::M1,
        Self::M3,
        Self::M5,
        Self::M15,
        Self::M30,
        Self::H1,
        Self::H2,
        Self::H4,
        Self::H6,
        Self::H8,
        Self::H12,
        Self::D1,
    ];

    /// Archive name, e.g. `1m` or `1d`.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::M1 => "1m",
            Self::M3 => "3m",
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::M30 => "30m",
            Self::H1 => "1h",
            Self::H2 => "2h",
            Self::H4 => "4h",
            Self::H6 => "6h",
            Self::H8 => "8h",
            Self::H12 => "12h",
            Self::D1 => "1d",
        }
    }
}

impl std::fmt::Display for Timeframe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Timeframe {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .iter()
            .copied()
            .find(|tf| tf.as_str() == s)
            .ok_or_else(|| {
                let valid: Vec<&str> = Self::ALL.iter().map(|tf| tf.as_str()).collect();
                format!("unsupported timeframe: {s}, valid values: {valid:?}")
            })
    }
}

impl<'de> Deserialize<'de> for Timeframe {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Application configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct AppConfig {
    /// Symbols to download (uppercased, sorted, deduplicated).
    pub symbol: Vec<String>,
    /// Timeframes to download.
    pub timeframe: Vec<Timeframe>,
    /// Optional inclusive start date override.
    pub start: Option<chrono::NaiveDate>,
    /// Optional inclusive end date override.
    pub end: Option<chrono::NaiveDate>,
    /// Output directory for downloaded files.
    pub output_dir: PathBuf,
    /// Maximum concurrent downloads.
    pub concurrency: usize,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            symbol: Vec::new(),
            timeframe: Vec::new(),
            start: None,
            end: None,
            output_dir: PathBuf::from("data"),
            concurrency: 64,
        }
    }
}

/// Sanity ceiling for `concurrency`: each in-flight download costs a socket,
/// a file handle and buffers, so anything beyond this is almost certainly a
/// typo rather than intent.
const MAX_CONCURRENCY: usize = 1024;

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

    /// Resolve the download end date: `min(config.end, yesterday)`.
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

    /// Check every semantic rule, tagging each failure with `path`.
    fn validate(mut self, path: &Path) -> Result<Self, AppError> {
        let err = |message: String| AppError::Config {
            path: Some(path.to_path_buf()),
            message,
        };
        if self.symbol.is_empty() {
            return Err(err("symbol must contain at least one symbol".into()));
        }
        if self.timeframe.is_empty() {
            return Err(err("timeframe must contain at least one timeframe".into()));
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
        if let Some(bad) = self.symbol.iter().find(|s| !is_valid_symbol(s)) {
            return Err(err(format!(
                "invalid symbol: {bad:?} (expected ASCII letters, digits or underscore)"
            )));
        }
        self.timeframe.sort();
        self.timeframe.dedup();
        Ok(self)
    }
}

/// Accepted symbol characters: ASCII letters (either case), digits, and `_`
/// (USDⓈ-M quarterly contracts use `_`, e.g. `BTCUSDT_250926`).
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
/// A download task: source URL, destination path and identifying metadata.
#[derive(Debug, Clone)]
pub(crate) struct DownloadTask {
    /// Source URL.
    pub url: String,
    /// Destination path on disk.
    pub dest: PathBuf,
    /// Trading pair symbol.
    pub pair: String,
    /// Kline timeframe.
    pub timeframe: Timeframe,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

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
            "symbol = [\"BTCUSDT\", \"ETHUSDT\"]\n\
             timeframe = [\"1m\", \"1d\"]\n\
             start = \"2024-01-01\"\n\
             end = \"2025-01-01\"\n\
             output_dir = \"/tmp/data\"",
        )
        .unwrap();
        assert_eq!(cfg.symbol, vec!["BTCUSDT", "ETHUSDT"]);
        assert_eq!(cfg.normalized_symbols(), vec!["BTCUSDT", "ETHUSDT"]);
        assert_eq!(cfg.timeframe, vec![Timeframe::M1, Timeframe::D1]);
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
    }

    #[test]
    fn load_minimal_config() {
        let cfg = load("symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]").unwrap();
        assert_eq!(cfg.symbol, vec!["BTCUSDT"]);
        assert_eq!(cfg.timeframe, vec![Timeframe::M1]);
        assert_eq!(cfg.start, None);
        assert_eq!(cfg.end, None);
        assert_eq!(cfg.output_dir, PathBuf::from("data"));
    }

    #[test]
    fn load_with_concurrency_override() {
        let cfg = load("symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]\nconcurrency = 8").unwrap();
        assert_eq!(cfg.concurrency, 8);
    }

    #[test]
    fn zero_concurrency_fails() {
        let err =
            load("symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]\nconcurrency = 0").unwrap_err();
        assert!(err.to_string().contains("concurrency"));
    }

    #[test]
    fn excessive_concurrency_fails() {
        let err =
            load("symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]\nconcurrency = 4096").unwrap_err();
        assert!(err.to_string().contains("concurrency"));
    }

    #[test]
    fn missing_symbol_fails() {
        let err = load("timeframe = [\"1m\"]").unwrap_err();
        assert!(err.to_string().contains("symbol"));
    }

    #[test]
    fn empty_symbol_fails() {
        let err = load("symbol = []\ntimeframe = [\"1m\"]").unwrap_err();
        assert!(err.to_string().contains("symbol"));
    }

    #[test]
    fn missing_timeframe_fails() {
        let err = load("symbol = [\"BTCUSDT\"]").unwrap_err();
        assert!(err.to_string().contains("timeframe"));
    }

    #[test]
    fn empty_timeframe_fails() {
        let err = load("symbol = [\"BTCUSDT\"]\ntimeframe = []").unwrap_err();
        assert!(err.to_string().contains("timeframe"));
    }

    #[test]
    fn invalid_timeframe_fails() {
        let err = load("symbol = [\"BTCUSDT\"]\ntimeframe = [\"1y\"]").unwrap_err();
        assert!(err.to_string().contains("1y"));
    }

    #[test]
    fn invalid_date_fails() {
        assert!(
            load("symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]\nstart = \"not-a-date\"").is_err()
        );
    }

    #[test]
    fn unknown_field_fails() {
        assert!(load("symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]\ntomeframe = \"1m\"").is_err());
    }

    #[test]
    fn resolved_end_clamps_future_to_yesterday() {
        let cfg =
            load("symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]\nend = \"2030-01-10\"").unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2030, 1, 10).unwrap();
        assert_eq!(
            cfg.resolved_end(today),
            chrono::NaiveDate::from_ymd_opt(2030, 1, 9).unwrap()
        );
    }

    #[test]
    fn resolved_end_defaults_to_yesterday() {
        let cfg = load("symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]").unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        assert_eq!(
            cfg.resolved_end(today),
            chrono::NaiveDate::from_ymd_opt(2024, 6, 14).unwrap()
        );
    }

    #[test]
    fn start_after_end_fails() {
        let err = load(
            "symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]\nstart = \"2024-02-01\"\nend = \"2024-01-01\"",
        )
        .unwrap_err();
        assert!(err.to_string().contains("start"));
    }

    #[test]
    fn duplicates_are_sorted_and_deduped() {
        let cfg = load(
            "symbol = [\"ETHUSDT\", \"BTCUSDT\", \"ETHUSDT\"]\ntimeframe = [\"1d\", \"1m\", \"1m\"]",
        )
        .unwrap();
        assert_eq!(cfg.symbol, vec!["ETHUSDT", "BTCUSDT", "ETHUSDT"]);
        assert_eq!(cfg.normalized_symbols(), vec!["BTCUSDT", "ETHUSDT"]);
        assert_eq!(cfg.timeframe, vec![Timeframe::M1, Timeframe::D1]);
    }

    #[test]
    fn timeframe_round_trip() {
        for tf in Timeframe::ALL {
            assert_eq!(tf.as_str().parse::<Timeframe>().unwrap(), tf);
            assert_eq!(tf.to_string(), tf.as_str());
        }
        assert_eq!(Timeframe::ALL.len(), 12);
        assert!("1y".parse::<Timeframe>().is_err());
        assert_eq!("15m".parse::<Timeframe>().unwrap(), Timeframe::M15);
    }

    #[test]
    fn symbols_are_uppercased() {
        let cfg = load("symbol = [\"btcusdt\", \"ethusdt\"]\ntimeframe = [\"1m\"]").unwrap();
        assert_eq!(cfg.symbol, vec!["btcusdt", "ethusdt"]);
        assert_eq!(cfg.normalized_symbols(), vec!["BTCUSDT", "ETHUSDT"]);
    }

    #[test]
    fn digit_and_underscore_symbols_are_valid() {
        let cfg =
            load("symbol = [\"1000pepeusdt\", \"btcusdt_250926\"]\ntimeframe = [\"1m\"]").unwrap();
        assert_eq!(cfg.symbol, vec!["1000pepeusdt", "btcusdt_250926"]);
        assert_eq!(
            cfg.normalized_symbols(),
            vec!["1000PEPEUSDT", "BTCUSDT_250926"]
        );
    }

    #[test]
    fn symbols_with_invalid_characters_fail() {
        for sym in ["BTC/USDT", "../etc", "BTC USDT", ""] {
            let err = load(&format!("symbol = [\"{sym}\"]\ntimeframe = [\"1m\"]")).unwrap_err();
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

    // — boundary and helper coverage —

    #[test]
    fn concurrency_boundary_is_inclusive_at_max() {
        let cfg = load(&format!(
            "symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]\nconcurrency = {MAX_CONCURRENCY}"
        ))
        .unwrap();
        assert_eq!(cfg.concurrency, MAX_CONCURRENCY);
    }

    #[test]
    fn concurrency_one_above_max_fails() {
        let err = load(&format!(
            "symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]\nconcurrency = {}",
            MAX_CONCURRENCY + 1
        ))
        .unwrap_err();
        assert!(err.to_string().contains("concurrency"));
    }

    #[test]
    fn resolved_end_keeps_a_past_end_untouched() {
        let cfg =
            load("symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]\nend = \"2020-05-05\"").unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        assert_eq!(
            cfg.resolved_end(today),
            chrono::NaiveDate::from_ymd_opt(2020, 5, 5).unwrap()
        );
    }

    #[test]
    fn resolved_end_saturates_at_the_minimum_date_instead_of_panicking() {
        let cfg = load("symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]").unwrap();
        // `NaiveDate - Duration` would overflow here; the subtraction saturates.
        assert_eq!(
            cfg.resolved_end(chrono::NaiveDate::MIN),
            chrono::NaiveDate::MIN
        );
    }

    #[test]
    fn is_valid_symbol_accepts_both_cases_and_quarterly_contracts() {
        for sym in ["BTCUSDT", "btcusdt", "1000PEPEUSDT", "BTCUSDT_250926", "A"] {
            assert!(is_valid_symbol(sym), "{sym} should be valid");
        }
    }

    #[test]
    fn is_valid_symbol_rejects_traversal_and_separators() {
        for sym in [
            "", "..", "../etc", "BTC/USDT", "BTC-USDT", "BTC USDT", "BTC.USDT", "BTCé",
        ] {
            assert!(!is_valid_symbol(sym), "{sym:?} should be rejected");
        }
    }

    #[test]
    fn lowercase_symbols_pass_validation_before_normalization() {
        // Regression guard: validation is case-insensitive, so a lowercase
        // symbol is accepted here and uppercased later by normalized_symbols.
        let cfg = load("symbol = [\"btcusdt\"]\ntimeframe = [\"1m\"]").unwrap();
        assert_eq!(cfg.symbol, vec!["btcusdt"]);
    }

    #[test]
    fn from_str_reports_the_valid_set() {
        let err = "7z".parse::<Timeframe>().unwrap_err();
        assert!(err.contains("unsupported timeframe"));
        assert!(err.contains("1m") && err.contains("1d"));
    }

    #[test]
    fn timeframe_ordering_follows_duration() {
        let mut tfs = vec![Timeframe::D1, Timeframe::M1, Timeframe::H1];
        tfs.sort();
        assert_eq!(tfs, vec![Timeframe::M1, Timeframe::H1, Timeframe::D1]);
        tfs.dedup();
        assert_eq!(tfs.len(), 3);
    }
}
