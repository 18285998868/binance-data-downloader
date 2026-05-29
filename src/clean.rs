use std::path::Path;

/// Removes all `.tmp` files left from interrupted parquet writes.
pub async fn cleanup_tmp(data_dir: &Path) {
    let d = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        for entry in walkdir::WalkDir::new(&d).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file()
                && entry.path().extension().is_some_and(|x| x == "tmp")
                && let Err(err) = std::fs::remove_file(entry.path())
            {
                tracing::warn!(
                    path = %entry.path().display(),
                    "failed to remove tmp file: {err}",
                );
            }
        }
    })
    .await
    .ok();
}
