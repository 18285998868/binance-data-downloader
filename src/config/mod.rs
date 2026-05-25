pub(crate) mod cache;
pub(crate) mod retry;
pub mod default_symbols;
pub mod default_timeframes;
pub mod default_start_date;

use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum MarketType {
    #[serde(rename = "spot")]
    Spot,
    #[serde(rename = "futures")]
    Futures,
}

impl MarketType {
    pub fn rel_path_prefix(self) -> &'static str {
        match self {
            MarketType::Spot => "spot/daily/klines",
            MarketType::Futures => "futures/um/daily/klines",
        }
    }
    pub fn as_tag(self) -> &'static str {
        match self {
            MarketType::Spot => "spot",
            MarketType::Futures => "futures",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub symbols: Option<Vec<String>>,
    #[serde(default)]
    pub timeframes: Option<Vec<String>>,
    #[serde(default)]
    pub market_type: Vec<MarketType>,
    #[serde(default)]
    pub max_concurrent: Option<usize>,
    #[serde(default)]
    pub data_dir: Option<PathBuf>,
    #[serde(default)]
    pub proxy_url: Option<String>,
    #[serde(default)]
    pub log_level: Option<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
}

impl Config {
    pub fn resolved_max_concurrent(&self) -> usize { self.max_concurrent.unwrap_or(100) }
    pub fn resolved_data_dir(&self) -> PathBuf {
        self.data_dir.clone().unwrap_or_else(|| {
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join("data")
        })
    }
    pub fn resolved_timeframes(&self) -> Vec<String> {
        let all = crate::config::default_timeframes::ALL_TIMEFRAMES;
        match &self.timeframes {
            Some(tfs) if !tfs.is_empty() => tfs.clone(),
            _ => all.iter().map(|s| s.to_string()).collect(),
        }
    }
    pub fn market_types(&self) -> &[MarketType] { &self.market_type }
    pub fn resolved_start_date(&self) -> Option<chrono::NaiveDate> {
        self.start_date.as_ref().and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
    }
    pub fn resolved_end_date(&self) -> Option<chrono::NaiveDate> {
        self.end_date.as_ref().and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
    }
    pub fn resolved_log_level(&self) -> &str { self.log_level.as_deref().unwrap_or("debug") }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            symbols: None, timeframes: None, market_type: vec![MarketType::Futures],
            max_concurrent: None, data_dir: None, proxy_url: None,
            log_level: None, start_date: None, end_date: None,
        }
    }
}

pub fn load_config(path: Option<&Path>) -> Result<Config, crate::AppError> {
    let config_path = path.unwrap_or_else(|| Path::new("config/data-downloader/config.example.toml"));
    let content = std::fs::read_to_string(config_path).map_err(crate::AppError::Io)?;
    let mut config: Config = toml::from_str(&content)
        .map_err(|e| crate::AppError::Api(format!("config parse error: {}", e)))?;
    if config.market_type.is_empty() {
        config.market_type = vec![MarketType::Futures];
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn test_default_market_type() { let cfg = Config::default(); assert_eq!(cfg.market_types(), &[MarketType::Futures]); }
    #[test] fn test_resolved_max_concurrent_default() { assert_eq!(Config::default().resolved_max_concurrent(), 100); }
    #[test] fn test_resolved_max_concurrent_override() { let cfg = Config { max_concurrent: Some(42), ..Default::default() }; assert_eq!(cfg.resolved_max_concurrent(), 42); }
    #[test] fn test_resolved_start_date_none() { assert!(Config::default().resolved_start_date().is_none()); }
    #[test] fn test_resolved_start_date_valid() { let cfg = Config { start_date: Some("2023-01-01".into()), ..Default::default() }; assert_eq!(cfg.resolved_start_date(), Some(chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap())); }
    #[test] fn test_resolved_start_date_invalid() { let cfg = Config { start_date: Some("not-a-date".into()), ..Default::default() }; assert!(cfg.resolved_start_date().is_none()); }
    #[test] fn test_resolved_data_dir_default() { let d = Config::default().resolved_data_dir(); assert!(d.ends_with("data")); }
    #[test] fn test_resolved_data_dir_custom() { let cfg = Config { data_dir: Some(PathBuf::from("/tmp/custom_data")), ..Default::default() }; assert_eq!(cfg.resolved_data_dir(), PathBuf::from("/tmp/custom_data")); }
    #[test] fn test_resolved_log_level_default() { assert_eq!(Config::default().resolved_log_level(), "debug"); }
    #[test] fn test_resolved_log_level_custom() { let cfg = Config { log_level: Some("info".into()), ..Default::default() }; assert_eq!(cfg.resolved_log_level(), "info"); }
    #[test] fn test_market_type_spot_prefix() { assert_eq!(MarketType::Spot.rel_path_prefix(), "spot/daily/klines"); }
    #[test] fn test_market_type_futures_prefix() { assert_eq!(MarketType::Futures.rel_path_prefix(), "futures/um/daily/klines"); }
    #[test] fn test_market_type_spot_tag() { assert_eq!(MarketType::Spot.as_tag(), "spot"); }
    #[test] fn test_market_type_futures_tag() { assert_eq!(MarketType::Futures.as_tag(), "futures"); }
    #[test] fn test_deserialize_market_type_array() { let cfg: Config = toml::from_str(&(r#"market_type = ["futures", "spot"]"#.to_owned() + "\n")).unwrap(); assert_eq!(cfg.market_type, vec![MarketType::Futures, MarketType::Spot]); }
    #[test] fn test_deserialize_symbols() { let cfg: Config = toml::from_str(&(r#"symbols = ["ETHUSDT"]"#.to_owned() + "\n")).unwrap(); assert_eq!(cfg.symbols, Some(vec!["ETHUSDT".into()])); }
    #[test] fn test_deserialize_empty_config() { let cfg: Config = toml::from_str("").unwrap(); assert!(cfg.symbols.is_none()); assert!(cfg.market_type.is_empty()); }
}
