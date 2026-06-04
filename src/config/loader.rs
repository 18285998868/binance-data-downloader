use std::path::Path;

use config::Config;
use serde::Deserialize;

use crate::AppError;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
/// Runtime configuration for a batch download run.
pub struct BatchConfig {
    pub symbol: Vec<String>,
    pub timeframe: Vec<String>,
    pub max_concurrent: usize,
    pub start: Option<String>,
    pub end: Option<String>,
    pub dir: String,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            symbol: Vec::new(),
            timeframe: vec![
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
            ],
            max_concurrent: 256,
            start: None,
            end: None,
            dir: "data".into(),
        }
    }
}

/// Load BatchConfig from a TOML file. When path is None, defaults to CARGO_MANIFEST_DIR/config/config.toml.
pub fn load_cfg(path: Option<&Path>) -> Result<BatchConfig, AppError> {
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


#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn load_cfg_default_path() {
        let cfg = load_cfg(None).unwrap();
        assert!(!cfg.timeframe.is_empty());
        assert_eq!(cfg.max_concurrent, 256);
        assert_eq!(cfg.dir, "data");
    }

    #[test]
    fn load_cfg_custom_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "symbol = [\"BTCUSDT\"]").unwrap();
        writeln!(f, "dir = \"/tmp/data\"").unwrap();
        drop(f);

        let cfg = load_cfg(Some(&path)).unwrap();
        assert_eq!(cfg.symbol, vec!["BTCUSDT"]);
        assert_eq!(cfg.dir, "/tmp/data");
    }

    #[test]
    fn load_cfg_invalid_path() {
        let r = load_cfg(Some(Path::new("/nonexistent/config.toml")));
        assert!(r.is_err());
    }

    #[test]
    fn batch_config_default() {
        let cfg = BatchConfig::default();
        assert_eq!(cfg.max_concurrent, 256);
        assert!(cfg.timeframe.contains(&"1m".to_string()));
        assert!(cfg.start.is_none());
    }
}
