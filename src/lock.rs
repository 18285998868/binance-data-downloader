//! Single-instance guard: one run per output directory.

use std::path::{Path, PathBuf};

use tokio::io::AsyncWriteExt;

use crate::AppError;

/// Lock file created in the cache root while a run holds the directory.
const LOCK_FILE: &str = "run.lock";

/// Age beyond which a lock whose owner cannot be identified is considered stale.
///
/// Mirrors the tmp-file heuristic in [`crate::scan`]: a PID check is the primary
/// signal, and wall-clock age is the fallback when the PID cannot be resolved.
const STALE_LOCK_WINDOW: std::time::Duration = std::time::Duration::from_mins(5);

/// Exclusive guard over one output directory.
///
/// Acquired with `create_new` (`O_EXCL`), so two processes racing to start
/// cannot both win. Released on drop, and reclaimed when a previous owner is
/// provably gone.
///
/// **Concurrent runs are not supported.** Two runs over the same `output_dir`
/// would contend on the same `redb` cache file — `Database::create` blocks on
/// the file lock — and race on the same destination files. This guard turns that
/// into an immediate, explicit error. Multi-run support is planned, not
/// implemented.
#[derive(Debug)]
pub(crate) struct RunLock {
    path: PathBuf,
}

impl RunLock {
    /// Take the lock for `output_dir`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Data`] when another live run already holds the
    /// directory, and [`AppError::Io`] when the lock file cannot be created,
    /// read or removed.
    pub(crate) async fn acquire(output_dir: &Path) -> Result<Self, AppError> {
        let dir = output_dir.join(".cache");
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(AppError::Io)?;
        let path = dir.join(LOCK_FILE);

        match create(&path).await {
            Ok(()) => return Ok(Self { path }),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(AppError::Io(e)),
        }

        // The lock exists: reclaim it only when the owner is provably gone.
        match reclaimable(&path).await {
            Reclaim::Yes(reason) => {
                tracing::warn!(path = %path.display(), reason, "reclaiming stale run lock");
                remove_stale(&path).await?;
                match create(&path).await {
                    Ok(()) => Ok(Self { path }),
                    // Another process won the reclaim race and now holds the
                    // lock: report it as a concurrent run, not as an I/O error.
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        Err(held_error(output_dir, &owner_of(&path).await))
                    }
                    Err(e) => Err(AppError::Io(e)),
                }
            }
            Reclaim::No(owner) => Err(held_error(output_dir, &owner)),
        }
    }
}

impl Drop for RunLock {
    fn drop(&mut self) {
        // A failed removal is not actionable: the next run reclaims stale locks.
        // `Drop` cannot await, so this is the one blocking call left in the
        // crate; it is a single `unlink` on the shutdown path.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// The error reported when the output directory is already held by another run.
fn held_error(output_dir: &Path, owner: &str) -> AppError {
    AppError::Data(crate::error::FailureReport::new(format!(
        "another downloader run ({owner}) already holds {}; \
         concurrent runs over one output directory are not supported",
        output_dir.display()
    )))
}

/// Delete a stale lock file, tolerating one that a concurrent reclaimer has
/// already removed.
///
/// A missing file is the expected outcome of losing the reclaim race, not an
/// I/O failure; the `O_EXCL` create that follows arbitrates who actually holds
/// the directory.
async fn remove_stale(path: &Path) -> Result<(), AppError> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(AppError::Io(e)),
    }
}

/// Describe the current owner of the lock file, for the error message.
async fn owner_of(path: &Path) -> String {
    match read_pid(path).await {
        Some(pid) => format!("pid {pid}"),
        None => "unidentified owner".to_owned(),
    }
}

/// Write this process's PID into a freshly created lock file.
async fn create(path: &Path) -> std::io::Result<()> {
    let mut f = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await?;
    f.write_all(format!("{}\n", std::process::id()).as_bytes())
        .await?;
    // `tokio::fs::File` buffers writes internally: without an explicit flush
    // the PID may still sit in the buffer when `sync_all` runs.
    f.flush().await?;
    f.sync_all().await
}

/// Whether an existing lock file may be taken over.
enum Reclaim {
    /// No live owner; the string explains why.
    Yes(&'static str),
    /// Someone (or something unknowable) still holds it.
    No(String),
}

/// Decide whether the lock at `path` is stale.
async fn reclaimable(path: &Path) -> Reclaim {
    let recorded = read_pid(path).await;
    if let Some(pid) = recorded {
        return match owner_is_alive(pid) {
            Some(true) => Reclaim::No(format!("pid {pid}")),
            Some(false) => Reclaim::Yes("owning process is gone"),
            None if older_than_stale_window(path).await => {
                Reclaim::Yes("owning process is gone and lock has aged out")
            }
            None => Reclaim::No(format!("pid {pid} of unknown liveness")),
        };
    }

    // No readable PID: fall back to age so a truncated lock cannot wedge the
    // directory forever.
    if older_than_stale_window(path).await {
        Reclaim::Yes("lock has no owner id and has aged out")
    } else {
        Reclaim::No("unidentified owner".to_owned())
    }
}

/// Parse the PID recorded in a lock file, if it holds a plain integer.
async fn read_pid(path: &Path) -> Option<u32> {
    tokio::fs::read_to_string(path)
        .await
        .ok()?
        .trim()
        .parse::<u32>()
        .ok()
}

/// Whether `pid` names a live process.
///
/// `None` means "cannot tell on this platform", which must never be treated as
/// "dead" — that would let two runs take the same directory at once. The
/// `Option` is load-bearing only off Linux, so the lint is platform-gated.
#[allow(clippy::unnecessary_wraps)]
fn owner_is_alive(pid: u32) -> Option<bool> {
    #[cfg(target_os = "linux")]
    {
        Some(Path::new(&format!("/proc/{pid}")).exists())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        None
    }
}

/// Whether the lock file has not been touched for [`STALE_LOCK_WINDOW`].
async fn older_than_stale_window(path: &Path) -> bool {
    tokio::fs::metadata(path)
        .await
        .and_then(|m| m.modified())
        .ok()
        .and_then(|m| m.elapsed().ok())
        .is_some_and(|age| age >= STALE_LOCK_WINDOW)
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
        // Dropped: a second acquisition must succeed.
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
    async fn stale_lock_from_a_dead_pid_is_reclaimed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".cache").join(LOCK_FILE);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        // PID 0 is never a normal owning process.
        std::fs::write(&path, "0\n").unwrap();

        let lock = RunLock::acquire(dir.path()).await.unwrap();
        assert!(lock.path.exists());
        assert_eq!(
            std::fs::read_to_string(&lock.path).unwrap().trim(),
            std::process::id().to_string()
        );
    }

    #[tokio::test]
    async fn lock_without_an_owner_id_ages_out() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".cache").join(LOCK_FILE);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "not-a-pid").unwrap();

        // Fresh but unidentifiable: must NOT be stolen (that would allow two runs).
        let err = RunLock::acquire(dir.path()).await.unwrap_err();
        assert!(err.to_string().contains("unidentified owner"));

        // Backdate past the window: now it is reclaimable.
        let f = std::fs::File::options().write(true).open(&path).unwrap();
        let old =
            std::time::SystemTime::now() - STALE_LOCK_WINDOW - std::time::Duration::from_secs(60);
        f.set_times(std::fs::FileTimes::new().set_modified(old))
            .unwrap();
        drop(f);
        RunLock::acquire(dir.path()).await.unwrap();
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
        assert_eq!(read_pid(&lock.path).await, Some(std::process::id()));
    }

    #[test]
    fn owner_liveness_of_self_is_true_on_linux() {
        #[cfg(target_os = "linux")]
        assert_eq!(owner_is_alive(std::process::id()), Some(true));
    }

    #[tokio::test]
    async fn lock_holds_across_clones_of_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        let _a = RunLock::acquire(dir.path()).await.unwrap();
        // A second guard on the same path must fail, proving it is not re-entrant.
        assert!(RunLock::acquire(dir.path()).await.is_err());
    }

    #[tokio::test]
    async fn a_lock_removed_by_a_concurrent_reclaimer_is_not_an_error() {
        // Regression guard for the reclaim race: when the stale lock vanishes
        // before it can be removed, the missing file must not surface as an
        // I/O error -- the `O_EXCL` create that follows arbitrates the race.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run.lock");
        // The file does not exist: `remove_stale` must tolerate that.
        remove_stale(&path).await.unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn remove_stale_deletes_an_existing_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run.lock");
        std::fs::write(&path, "0\n").unwrap();
        remove_stale(&path).await.unwrap();
        assert!(!path.exists());
    }
}
