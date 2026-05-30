use serde::Deserialize;
use std::path::PathBuf;

use super::market_type::MarketType;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub symbols: Option<Vec<String>>,
    pub timeframes: Option<Vec<String>>,
    pub market_type: Vec<MarketType>,
    pub max_concurrent: usize,
    pub data_dir: PathBuf,
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
    pub fn load() -> anyhow::Result<Self> {
        let manifest = env!("CARGO_MANIFEST_DIR");
        let path = std::path::Path::new(manifest).join("config/config.toml");
        let content = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&content)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_symbols_is_none() {
        assert!(Config::default().symbols.is_none());
    }

    #[test]
    fn default_timeframes_has_12_entries() {
        assert_eq!(Config::default().timeframes.unwrap().len(), 12);
    }

    #[test]
    fn default_market_type_is_futures() {
        assert_eq!(Config::default().market_type, vec![MarketType::Futures]);
    }

    #[test]
    fn default_end_date_is_today() {
        let cfg = Config::default();
        let today = chrono::Local::now()
            .date_naive()
            .format("%Y-%m-%d")
            .to_string();
        assert_eq!(cfg.end_date, Some(today));
    }

    #[test]
    fn parse_full_toml() {
        let toml_str = r#"
            symbols = ["BTCUSDT"]
            timeframes = ["1h", "4h"]
            market_type = ["spot", "futures"]
            max_concurrent = 500
            data_dir = "/tmp/data"
            start_date = "2024-01-01"
            end_date = "2024-12-31"
        "#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.symbols, Some(vec!["BTCUSDT".into()]));
        assert_eq!(config.timeframes, Some(vec!["1h".into(), "4h".into()]));
        assert_eq!(
            config.market_type,
            vec![MarketType::Spot, MarketType::Futures]
        );
        assert_eq!(config.max_concurrent, 500);
        assert_eq!(config.start_date, Some("2024-01-01".into()));
        assert_eq!(config.end_date, Some("2024-12-31".into()));
    }

    #[test]
    fn partial_toml_uses_defaults() {
        let toml_str = "data_dir = \"custom\"\n";
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.data_dir, PathBuf::from("custom"));
        assert_eq!(config.max_concurrent, 1000);
        assert!(config.symbols.is_none());
    }

    #[test]
    fn empty_toml_uses_all_defaults() {
        let config: Config = toml::from_str("").unwrap();
        assert_eq!(config.max_concurrent, 1000);
        assert_eq!(config.data_dir, PathBuf::from("data"));
    }

    #[test]
    fn explicit_empty_market_type() {
        let toml_str = "market_type = []\n";
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.market_type.is_empty());
    }
}
