use std::path::Path;

use common::config::{load_toml, load_toml_from};

use super::BatchConfig;
use crate::AppError;

pub fn load_batch_config() -> Result<BatchConfig, AppError> {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    load_toml(manifest, "config.toml").map_err(|e| AppError::Config(e.to_string()))
}

pub fn load_batch_config_from(path: &Path) -> Result<BatchConfig, AppError> {
    load_toml_from(path).map_err(|e| AppError::Config(e.to_string()))
}
