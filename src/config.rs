use std::path::Path;

use config::Config;
use serde::Deserialize;

use crate::AppError;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub symbol: Option<Vec<String>>,
    pub timeframe: Option<Vec<String>>,
    pub max_concurrent: usize,
    pub start: Option<String>,
    pub end: Option<String>,
    pub dir: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            symbol: None,
            timeframe: Some(vec![
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
            max_concurrent: 32,
            start: None,
            end: None,
            dir: "data".into(),
        }
    }
}

impl AppConfig {
    pub fn load(path: Option<&Path>) -> Result<Self, AppError> {
        let default_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("config")
            .join("config.toml");
        let p = path.unwrap_or(&default_path);
        Config::builder()
            .add_source(config::File::from(p))
            .build()
            .map_err(|e| AppError::Config(e.to_string()))?
            .try_deserialize()
            .map_err(|e| AppError::Config(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn load_default_path() {
        let cfg = AppConfig::load(None).unwrap();
        assert_eq!(cfg.symbol, None);
        assert_eq!(cfg.max_concurrent, 32);
        assert_eq!(cfg.dir, "data");
    }

    #[test]
    fn load_custom_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "symbol = [\"BTCUSDT\"]").unwrap();
        writeln!(f, "dir = \"/tmp/data\"").unwrap();
        drop(f);

        let cfg = AppConfig::load(Some(&path)).unwrap();
        assert_eq!(cfg.symbol, Some(vec!["BTCUSDT".into()]));
        assert_eq!(cfg.dir, "/tmp/data");
    }

    #[test]
    fn load_invalid_path() {
        let r = AppConfig::load(Some(Path::new("/nonexistent/config.toml")));
        assert!(r.is_err());
    }

    #[test]
    fn app_config_default() {
        let cfg = AppConfig::default();
        assert_eq!(cfg.max_concurrent, 32);
        assert!(cfg.symbol.is_none());
        assert!(cfg.timeframe.is_some());
        assert!(cfg.start.is_none());
    }
}
