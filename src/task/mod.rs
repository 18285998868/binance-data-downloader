pub mod builder;
pub mod download;
pub mod get_local_files;
pub mod get_start;
pub mod get_symbols;
pub mod urls;

use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub url: String,
    pub dest: PathBuf,
    pub pair: String,
    pub timeframe: String,
}
