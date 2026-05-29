use serde::Deserialize;
use std::path::{Path, PathBuf};

use super::Error;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub data_dir: PathBuf,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, Error> {
        let content = std::fs::read_to_string(path)?;
        let config: Config =
            toml::from_str(&content).map_err(|e| Error::Parse(format!("config: {e}")))?;
        if config.data_dir.as_os_str().is_empty() {
            return Err(Error::Parse("data_dir is required".into()));
        }
        if !config.data_dir.exists() {
            return Err(Error::Data(format!(
                "data_dir not found: {}",
                config.data_dir.display()
            )));
        }
        Ok(config)
    }
}
