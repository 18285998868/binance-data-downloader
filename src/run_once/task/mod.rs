pub mod local_file;
pub mod urls;

#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub url: String,
    pub dest: std::path::PathBuf,
    pub pair: String,
    pub timeframe: String,
}
