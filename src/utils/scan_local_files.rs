use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;

use crate::AppError;

/// Scan `data_dir` for existing `.zip` files.
/// Also removes stale tmp files this tool created (`{name}.zip.{pid}.tmp`)
/// during the same walk, except those that belong to a still-running process
/// (see [`belongs_to_live_process`]). Foreign `.tmp` files are left alone.
///
/// Returns 128-bit hashes of the relative paths (see [`rel_path_hash`])
/// rather than the paths themselves: the set can hold millions of entries
/// (≈200-300 MB as `String`s, ≈35 MB as `u128`s for 1.8M files).
///
/// # Errors
///
/// Returns [`AppError::Join`] if the blocking scanner task fails to join.
pub async fn scan_local_files(data_dir: &Path) -> Result<HashSet<u128>, AppError> {
    let dir = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        if !dir.exists() {
            return Ok(HashSet::new());
        }
        let mut files = HashSet::new();
        for e in walkdir::WalkDir::new(&dir) {
            let e = match e {
                Ok(e) => e,
                Err(err) => {
                    tracing::warn!(path = %dir.display(), "walkdir entry failed: {err}");
                    continue;
                }
            };
            if !e.file_type().is_file() {
                continue;
            }
            let rel = e.path().strip_prefix(&dir).unwrap_or(e.path());
            let s = rel.to_string_lossy();

            if s.ends_with(".tmp") {
                if !is_our_tmp(&s) {
                    continue;
                }
                if belongs_to_live_process(e.path()) {
                    continue;
                }
                if let Err(err) = std::fs::remove_file(e.path()) {
                    tracing::warn!(
                        path = %e.path().display(),
                        "failed to remove tmp file: {err}",
                    );
                }
                continue;
            }

            if s.ends_with(".zip") {
                files.insert(rel_path_hash(&s));
            }
        }
        Ok::<_, AppError>(files)
    })
    .await
    .map_err(AppError::from)?
}

/// Returns true for tmp files this tool creates: `{name}.zip.{pid}.tmp`.
///
/// Any other `.tmp` file in the output tree belongs to somebody else and must
/// not be deleted (writers create `{dest}.{pid}.tmp` before the atomic
/// rename).
fn is_our_tmp(rel: &str) -> bool {
    let Some(base) = rel.strip_suffix(".tmp") else {
        return false;
    };
    let Some((stem, pid)) = base.rsplit_once('.') else {
        return false;
    };
    stem.ends_with(".zip") && !pid.is_empty() && pid.bytes().all(|b| b.is_ascii_digit())
}

/// Returns true when a `.tmp` file may belong to an in-flight download of a
/// still-running process, judging by its `.{pid}.tmp` suffix.
///
/// Such files must not be deleted: a second run against the same output
/// directory would otherwise break the first run's atomic rename.
fn belongs_to_live_process(path: &Path) -> bool {
    let Some(pid) = path
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_suffix(".tmp"))
        .and_then(|s| s.rsplit('.').next())
        .and_then(|p| p.parse::<u32>().ok())
    else {
        return false;
    };
    #[cfg(target_os = "linux")]
    {
        Path::new(&format!("/proc/{pid}")).exists()
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        false
    }
}

/// Hash of a relative path as stored in the scanned file set.
///
/// The set is queried once per candidate download and can hold millions of
/// entries, so only a 128-bit hash is kept instead of the path itself (see
/// [`scan_local_files`] for the memory trade-off). A collision would skip one
/// file; two independent SipHash-1-3 passes over the path make this
/// practically impossible, and the downstream kline reader checks for gaps
/// anyway. Both inserts and lookups happen within the same process, so the
/// hash only needs to be stable for the duration of a run.
#[must_use]
pub(crate) fn rel_path_hash(rel: &str) -> u128 {
    let mut first = DefaultHasher::new();
    rel.hash(&mut first);
    let mut second = DefaultHasher::new();
    second.write_u8(0xa5);
    rel.hash(&mut second);
    (u128::from(first.finish()) << 64) | u128::from(second.finish())
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
    async fn scan_missing_dir_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        let files = scan_local_files(&missing).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn scan_finds_zip_files() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("binance/data/um/daily/klines/BTCUSDT/1m");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("BTCUSDT-1m-2024-01-01.zip"), b"zip").unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert_eq!(files.len(), 1);
        assert!(files.contains(&rel_path_hash(
            "binance/data/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip"
        )));
    }

    #[test]
    fn rel_path_hash_distinguishes_paths() {
        assert_ne!(rel_path_hash("a.zip"), rel_path_hash("b.zip"));
    }

    #[tokio::test]
    async fn scan_ignores_non_data_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("readme.txt"), b"hello").unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn scan_ignores_foreign_tmp_files() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("binance/data/um/daily/klines/BTCUSDT/1m");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("BTCUSDT-1m-2024-01-01.zip"), b"zip").unwrap();
        // Not our tmp shape (`{name}.zip.{pid}.tmp`): belongs to someone else.
        std::fs::write(data.join("BTCUSDT-1m-2024-01-01.tmp"), b"tmp").unwrap();
        std::fs::write(data.join("BTCUSDT-1m-2024-01-01.zip.tmp"), b"tmp").unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert_eq!(files.len(), 1);
        assert!(files.contains(&rel_path_hash(
            "binance/data/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip"
        )));
        assert!(data.join("BTCUSDT-1m-2024-01-01.tmp").exists());
        assert!(data.join("BTCUSDT-1m-2024-01-01.zip.tmp").exists());
    }

    #[test]
    fn is_our_tmp_shape() {
        assert!(is_our_tmp("a/b/x.zip.123.tmp"));
        assert!(is_our_tmp("x.zip.0.tmp"));
        assert!(!is_our_tmp("a/b/x.tmp")); // bare, no pid
        assert!(!is_our_tmp("a/b/x.zip.tmp")); // no pid
        assert!(!is_our_tmp("a/b/x.zip.12a.tmp")); // non-numeric pid
        assert!(!is_our_tmp("a/b/x.txt")); // not a tmp file
    }

    #[tokio::test]
    async fn scan_removes_dead_process_tmp_files() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("binance/data/um/daily/klines/BTCUSDT/1m");
        std::fs::create_dir_all(&data).unwrap();
        // PID 0 (backed by no process) stands in for a dead writer.
        let stale = data.join("BTCUSDT-1m-2024-01-01.zip.0.tmp");
        std::fs::write(&stale, b"stale").unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert!(files.is_empty());
        assert!(!stale.exists());
    }

    #[tokio::test]
    async fn scan_keeps_tmp_files_of_live_processes() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("binance/data/um/daily/klines/BTCUSDT/1m");
        std::fs::create_dir_all(&data).unwrap();
        // Our own PID is definitely alive: simulate our in-flight download.
        let live = data.join(format!(
            "BTCUSDT-1m-2024-01-01.zip.{}.tmp",
            std::process::id()
        ));
        std::fs::write(&live, b"in-flight").unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert!(files.is_empty());
        assert!(live.exists());
    }
}
