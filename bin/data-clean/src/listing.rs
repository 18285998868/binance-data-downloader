use std::path::Path;

use super::Error;

/// Recursively list all .parquet files under `data_dir`, sorted lexicographically.
pub fn list_parquet_files(data_dir: &Path) -> Result<Vec<std::path::PathBuf>, Error> {
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(data_dir) {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("walkdir error (skipping): {e}");
                continue;
            }
        };
        if !entry.file_type().is_file() {
            continue;
        }
        if entry.path().extension().and_then(|s| s.to_str()) != Some("parquet") {
            continue;
        }
        files.push(entry.into_path());
    }
    files.sort();
    Ok(files)
}
