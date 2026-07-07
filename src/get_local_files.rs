use std::collections::HashSet;
use std::path::Path;

use crate::AppError;

/// Scan data_dir for existing .zip files.
/// Also removes stale .tmp files during the same walk.
pub async fn scan_local_files(data_dir: &Path) -> Result<HashSet<String>, AppError> {
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
            let s = rel.to_string_lossy();

            if s.ends_with(".tmp") {
                if let Err(err) = std::fs::remove_file(e.path()) {
                    tracing::warn!(
                        path = %e.path().display(),
                        "failed to remove tmp file: {err}",
                    );
                }
                continue;
            }

            if s.ends_with(".zip") {
                files.insert(s.into_owned());
            }
        }
        Ok::<_, AppError>(files)
    })
    .await
    .map_err(AppError::from)?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scan_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn scan_finds_zip_files() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("futures/um/daily/klines/BTCUSDT/1m");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("BTCUSDT-1m-2024-01-01.zip"), b"zip").unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert_eq!(files.len(), 1);
        assert!(files.contains("futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip"));
    }

    #[tokio::test]
    async fn scan_ignores_non_data_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("readme.txt"), b"hello").unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn scan_removes_tmp_files() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("futures/um/daily/klines/BTCUSDT/1m");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("BTCUSDT-1m-2024-01-01.zip"), b"zip").unwrap();
        std::fs::write(data.join("BTCUSDT-1m-2024-01-01.tmp"), b"tmp").unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert_eq!(files.len(), 1);
        assert!(files.contains("futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip"));
        assert!(!data.join("BTCUSDT-1m-2024-01-01.tmp").exists());
    }
}
