//! ZIP archive validation shared by `--verify` and `binance-data-verify`.
//!
//! Only the central directory is read. That catches truncated or structurally
//! corrupt archives without decompressing every CSV payload.

use std::path::Path;

use crate::AppError;
use crate::error::FailureReport;

/// Validate that `path` is a readable ZIP archive with at least one entry.
///
/// This is the synchronous building block used by the downloader's
/// `spawn_blocking` path and by the standalone verifier. It does not
/// decompress payloads.
///
/// # Errors
///
/// Returns [`AppError::Io`] when the file cannot be opened and
/// [`AppError::Data`] when the central directory is unreadable or empty.
pub fn verify_zip(path: &Path) -> Result<(), AppError> {
    let file = std::fs::File::open(path)?;
    let archive = zip::ZipArchive::new(file).map_err(|error| {
        AppError::Data(FailureReport::new(format!(
            "invalid zip archive {}: {error}",
            path.display()
        )))
    })?;
    if archive.is_empty() {
        return Err(AppError::Data(FailureReport::new(format!(
            "zip archive {} holds no entry",
            path.display()
        ))));
    }
    Ok(())
}

/// Async wrapper for the download path.
pub(crate) async fn verify_zip_archive(path: &Path) -> Result<(), AppError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || verify_zip(&path)).await??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifies_a_valid_zip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("valid.zip");
        let mut data = Vec::new();
        {
            let mut writer = zip::ZipWriter::new(std::io::Cursor::new(&mut data));
            writer
                .start_file("row.csv", zip::write::SimpleFileOptions::default())
                .unwrap();
            std::io::Write::write_all(&mut writer, b"a,b\n").unwrap();
            writer.finish().unwrap();
        }
        std::fs::write(&path, data).unwrap();
        verify_zip(&path).unwrap();
    }

    #[test]
    fn rejects_a_non_zip_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broken.zip");
        std::fs::write(&path, b"not a zip").unwrap();
        let err = verify_zip(&path).unwrap_err();
        assert!(err.to_string().contains("invalid zip archive"));
    }

    #[test]
    fn rejects_an_empty_zip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.zip");
        let mut data = Vec::new();
        {
            let writer = zip::ZipWriter::new(std::io::Cursor::new(&mut data));
            writer.finish().unwrap();
        }
        std::fs::write(&path, data).unwrap();
        let err = verify_zip(&path).unwrap_err();
        assert!(err.to_string().contains("holds no entry"));
    }
}
