use serde::Deserialize;
use std::path::{Path, PathBuf};

use super::market_type::MarketType;
use crate::error::AppError;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub symbols: Option<Vec<String>>,
    pub timeframes: Option<Vec<String>>,
    pub market_type: Vec<MarketType>,
    pub max_concurrent: usize,
    pub data_dir: PathBuf,
    pub proxy_url: Option<String>,
    pub log_level: String,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            symbols: None,
            timeframes: Some(vec![
                "1m".into(),
                "3m".into(),
                "5m".into(),
                "15m".into(),
                "30m".into(),
                "1h".into(),
                "2h".into(),
                "4h".into(),
                "6h".into(),
                "8h".into(),
                "12h".into(),
                "1d".into(),
            ]),
            market_type: vec![MarketType::Futures],
            max_concurrent: 1000,
            data_dir: PathBuf::from("data"),
            proxy_url: None,
            log_level: "debug".into(),
            start_date: None,
            end_date: Some(
                chrono::Local::now()
                    .date_naive()
                    .format("%Y-%m-%d")
                    .to_string(),
            ),
        }
    }
}

impl Config {
    pub fn load(manifest_dir: &str) -> Result<Self, AppError> {
        let path = Path::new(manifest_dir).join("download.toml");
        let content = std::fs::read_to_string(path)?;
        let mut config: Config =
            toml::from_str(&content).map_err(|e| AppError::Parse(format!("config: {e}")))?;
        if config.market_type.is_empty() {
            config.market_type = vec![MarketType::Futures];
        }
        Ok(config)
    }
}
