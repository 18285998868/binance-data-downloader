//! Single-instance guard: one run per output directory.
//!
//! The guard is an OS advisory lock (`flock` on Unix, `LockFileEx` on
//! Windows). The kernel releases it when the process exits, so a crashed run
//! cannot leave a permanently stale lock. The lock file itself is deliberately
//! not unlinked: removing it while another process has it open is a classic
//! lock-file race.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use crate::AppError;

/// Lock file created in the cache root while a run holds the directory.
const LOCK_FILE: &str = "run.lock";

/// Exclusive guard over one output directory.
///
/// The lock is taken with `try_lock` on an open file handle. The kernel
/// releases it when the handle is dropped or the process dies, which makes
/// crash recovery automatic and platform-independent. The file is kept on
/// disk after release so a second process never locks a different inode for
/// the same logical lock.
///
/// **Concurrent runs are not supported.** Two runs over the same `output_dir`
/// would contend on the same `redb` cache file and race on the same destination
/// files. This guard turns that into an immediate, explicit error. Multi-run
/// support is planned, not implemented.
#[derive(Debug)]
pub(crate) struct RunLock {
    /// Open handle that owns the advisory lock.
    file: std::fs::File,
    /// Path of the lock file, kept for diagnostics and tests.
    ///
    /// The kernel lock is owned by `file`; this path is not used to decide
    /// ownership, so it is allowed to be unused outside tests.
    #[allow(dead_code)]
    path: PathBuf,
}

impl RunLock {
    /// Take the lock for `output_dir`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Data`] when another live run already holds the
    /// directory, and [`AppError::Io`] when the lock file cannot be created or
    /// locked.
    pub(crate) async fn acquire(output_dir: &Path) -> Result<Self, AppError> {
        let dir = output_dir.join(".cache");
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(AppError::Io)?;
        let path = dir.join(LOCK_FILE);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(AppError::Io)?;

        match file.try_lock() {
            Ok(()) => {
                write_owner(&file).map_err(AppError::Io)?;
                Ok(Self { file, path })
            }
            Err(std::fs::TryLockError::WouldBlock) => Err(held_error(output_dir, &owner_of(&path))),
            Err(std::fs::TryLockError::Error(e)) => Err(AppError::Io(e)),
        }
    }
}

impl Drop for RunLock {
    fn drop(&mut self) {
        // Closing the file releases the kernel lock; unlocking explicitly makes
        // the intent obvious and does not rely on drop order. The file itself
        // stays on disk on purpose (see the module docs).
        let _ = self.file.unlock();
    }
}

/// Write this process's PID into the lock file for the diagnostic message.
fn write_owner(file: &std::fs::File) -> std::io::Result<()> {
    file.set_len(0)?;
    let mut file = file;
    file.write_all(format!("{}\n", std::process::id()).as_bytes())?;
    file.flush()?;
    file.sync_all()
}

/// The error reported when the output directory is already held by another run.
fn held_error(output_dir: &Path, owner: &str) -> AppError {
    AppError::Data(crate::error::FailureReport::new(format!(
        "another downloader run ({owner}) already holds {}; \
         concurrent runs over one output directory are not supported",
        output_dir.display()
    )))
}

/// Describe the current owner of the lock file, for the error message.
fn owner_of(path: &Path) -> String {
    match std::fs::read_to_string(path) {
        Ok(contents) => match contents.trim().parse::<u32>() {
            Ok(pid) => format!("pid {pid}"),
            Err(_) => "unidentified owner".to_owned(),
        },
        Err(_) => "unidentified owner".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquire_then_release_frees_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        {
            let lock = RunLock::acquire(dir.path()).await.unwrap();
            assert!(lock.path.exists());
        }
        // Dropped: the kernel lock is gone, so a second acquisition succeeds.
        RunLock::acquire(dir.path()).await.unwrap();
    }

    #[tokio::test]
    async fn second_acquire_while_held_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let _held = RunLock::acquire(dir.path()).await.unwrap();
        let err = RunLock::acquire(dir.path()).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("another downloader run"), "unexpected: {msg}");
        assert!(msg.contains("not supported"), "unexpected: {msg}");
    }

    #[tokio::test]
    async fn lock_file_left_by_a_previous_run_is_reused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".cache").join(LOCK_FILE);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        // A stale PID in the file is irrelevant: only the OS lock matters.
        std::fs::write(&path, "0\n").unwrap();

        let lock = RunLock::acquire(dir.path()).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(&lock.path).unwrap().trim(),
            std::process::id().to_string()
        );
    }

    #[tokio::test]
    async fn acquire_creates_the_cache_root() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("deep/nested/out");
        let lock = RunLock::acquire(&nested).await.unwrap();
        assert!(lock.path.starts_with(&nested));
        assert!(lock.path.exists());
    }

    #[tokio::test]
    async fn recorded_pid_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let lock = RunLock::acquire(dir.path()).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(&lock.path).unwrap().trim(),
            std::process::id().to_string()
        );
    }

    #[tokio::test]
    async fn lock_holds_across_clones_of_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        let _a = RunLock::acquire(dir.path()).await.unwrap();
        // A second guard on the same path must fail, proving it is not
        // re-entrant within one process either.
        assert!(RunLock::acquire(dir.path()).await.is_err());
    }

    #[tokio::test]
    async fn drop_keeps_the_lock_file_for_future_runs() {
        let dir = tempfile::tempdir().unwrap();
        let path;
        {
            let lock = RunLock::acquire(dir.path()).await.unwrap();
            path = lock.path.clone();
        }
        assert!(
            path.exists(),
            "the file must stay so the next run locks the same inode"
        );
        // And the next run can still acquire it.
        RunLock::acquire(dir.path()).await.unwrap();
    }
}
