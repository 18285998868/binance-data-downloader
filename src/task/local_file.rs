use std::collections::HashSet;
use std::path::Path;

/// Scans `data_dir` recursively for existing `.zip` and `.parquet` files.
///
/// Returns a set of normalized paths (with extensions stripped) so the
/// downloader can skip files already on disk.
pub async fn scan_local_files(data_dir: &Path) -> Result<HashSet<String>, crate::AppError> {
    let dir = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut files = HashSet::new();
        for e in walkdir::WalkDir::new(&dir)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if !e.file_type().is_file() {
                continue;
            }
            let rel = e.path().strip_prefix(&dir).unwrap_or(e.path());
            let s = rel.to_string_lossy().to_string();
            if !s.ends_with(".zip") && !s.ends_with(".parquet") {
                continue;
            }
            let stripped = s
                .strip_suffix(".parquet")
                .or_else(|| s.strip_suffix(".zip"))
                .unwrap_or(&s)
                .to_string();
            files.insert(stripped);
        }
        Ok::<_, crate::AppError>(files)
    })
    .await
    .map_err(|e| crate::AppError::Join(format!("spawn: {e}")))?
}
