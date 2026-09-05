use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::AppError;

/// Accepted timeframe values, matching Binance daily kline archives.
const VALID_TIMEFRAMES: [&str; 12] = [
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d",
];

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub symbol: Vec<String>,
    pub timeframe: Vec<String>,
    pub start: Option<chrono::NaiveDate>,
    pub end: Option<chrono::NaiveDate>,
    #[serde(default = "default_output_dir")]
    pub output_dir: PathBuf,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
}

fn default_output_dir() -> PathBuf {
    PathBuf::from("data")
}

fn default_concurrency() -> usize {
    16
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self, AppError> {
        let raw = std::fs::read_to_string(path).map_err(|e| AppError::Config(e.to_string()))?;
        let cfg: Self = toml::from_str(&raw).map_err(|e| AppError::Config(e.to_string()))?;
        cfg.validate()
    }

    fn validate(self) -> Result<Self, AppError> {
        if self.symbol.is_empty() {
            return Err(AppError::Config(
                "symbol must contain at least one symbol".into(),
            ));
        }
        if !self.timeframe.is_empty()
            && let Some(bad) = self
                .timeframe
                .iter()
                .find(|tf| !VALID_TIMEFRAMES.contains(&tf.as_str()))
        {
            return Err(AppError::Config(format!(
                "unsupported timeframe: {bad}, valid values: {VALID_TIMEFRAMES:?}"
            )));
        }
        if self.concurrency == 0 {
            return Err(AppError::Config(
                "concurrency must be greater than zero".into(),
            ));
        }
        Ok(self)
    }
}

/// A download task: source URL, destination path and identifying metadata.
#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub url: String,
    pub dest: PathBuf,
    pub pair: String,
    pub timeframe: String,
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
        AppConfig::load(&path)
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
        assert_eq!(cfg.timeframe, vec!["1m", "1d"]);
        assert_eq!(
            cfg.start,
            Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        );
        assert_eq!(
            cfg.end,
            Some(chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap())
        );
        assert_eq!(cfg.output_dir, PathBuf::from("/tmp/data"));
        assert_eq!(cfg.concurrency, 16);
    }

    #[test]
    fn load_minimal_config() {
        let cfg = load("symbol = [\"BTCUSDT\"]\ntimeframe = [\"1m\"]").unwrap();
        assert_eq!(cfg.symbol, vec!["BTCUSDT"]);
        assert_eq!(cfg.timeframe, vec!["1m"]);
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
    fn empty_timeframe_ok() {
        let cfg = load("symbol = [\"BTCUSDT\"]\ntimeframe = []").unwrap();
        assert!(cfg.timeframe.is_empty());
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
    fn nonexistent_path_fails() {
        assert!(AppConfig::load(Path::new("/nonexistent/config.toml")).is_err());
    }
}
