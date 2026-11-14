//! Walk an output tree and validate every archive in it.
//!
//! The traversal is CPU-bound (ZIP central-directory parsing), so it runs on a
//! dedicated rayon pool whose size is bounded: `--jobs` is a request, not a
//! promise, and an absurd value must not exhaust memory by spawning threads.
//!
//! Failures are collected instead of printed, so the binary owns all user
//! output and a test can assert on the exact list.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

use binance_data_core::verify_zip;
use rayon::prelude::*;
use walkdir::WalkDir;

/// Upper bound on worker threads, as a multiple of the available CPUs.
///
/// Verification is CPU-bound, but the walk itself spends time waiting on the
/// filesystem; a small oversubscription is useful, an unbounded one is not.
const MAX_THREADS_PER_CPU: usize = 4;

/// A verification pass could not start.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum VerifyError {
    /// The root is missing or is not a directory.
    #[error("not a directory: {}", .0.display())]
    NotADirectory(PathBuf),
    /// The rayon pool rejected its configuration.
    #[error("cannot build verifier thread pool: {0}")]
    ThreadPool(#[from] rayon::ThreadPoolBuildError),
}

/// Outcome of one verification pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Summary {
    /// Archives inspected.
    pub seen: usize,
    /// One entry per failure: `<path>: <reason>` for a bad archive,
    /// `walk error: <reason>` for a directory entry that could not be read.
    ///
    /// Sorted, so two runs over the same tree produce the same output even
    /// though the walk is parallel.
    pub failures: Vec<String>,
}

impl Summary {
    /// Number of failures (bad archives plus unreadable directory entries).
    #[must_use]
    pub fn failed(&self) -> usize {
        self.failures.len()
    }

    /// True when nothing failed.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.failures.is_empty()
    }
}

/// Size of the verifier pool for a requested thread count.
///
/// `0` means "use the available CPUs"; any other value is clamped to
/// `1..=available * 4` so a typo cannot ask for millions of threads.
pub fn worker_threads(requested: usize) -> usize {
    let available = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    if requested == 0 {
        available
    } else {
        requested.min(available.saturating_mul(MAX_THREADS_PER_CPU))
    }
}

/// Verify every `.zip` below `root` with `jobs` worker threads.
///
/// Individual archives are validated through [`verify_zip`], which only reads
/// the central directory. Non-zip files are ignored.
///
/// # Errors
///
/// Returns [`VerifyError::NotADirectory`] when `root` is missing or not a
/// directory, and [`VerifyError::ThreadPool`] when the thread pool cannot be
/// built.
pub fn verify_tree(root: &Path, jobs: usize) -> Result<Summary, VerifyError> {
    if !root.is_dir() {
        return Err(VerifyError::NotADirectory(root.to_path_buf()));
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(worker_threads(jobs))
        .build()?;

    // Relaxed is enough: the counters are independent of each other and only
    // read after `install` has joined every worker.
    let seen = AtomicUsize::new(0);
    let failures = Mutex::new(Vec::new());

    pool.install(|| {
        WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .par_bridge()
            .for_each(|entry| match entry {
                Ok(entry) if is_zip(entry.path()) => {
                    seen.fetch_add(1, Ordering::Relaxed);
                    if let Err(error) = verify_zip(entry.path()) {
                        failures
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .push(format!("{}: {error}", entry.path().display()));
                    }
                }
                Ok(_) => {}
                Err(error) => failures
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(format!("walk error: {error}")),
            });
    });

    let mut failures = failures
        .into_inner()
        .unwrap_or_else(PoisonError::into_inner);
    failures.sort();

    Ok(Summary {
        seen: seen.load(Ordering::Relaxed),
        failures,
    })
}

/// True when `path` names a `.zip` file.
fn is_zip(path: &Path) -> bool {
    path.is_file()
        && path
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_jobs_uses_the_available_cpus() {
        let available = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        assert_eq!(worker_threads(0), available);
    }

    #[test]
    fn explicit_jobs_are_kept_within_the_bound() {
        let available = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        assert_eq!(worker_threads(1), 1);
        assert_eq!(worker_threads(available), available);
    }

    #[test]
    fn an_absurd_job_count_is_clamped() {
        let available = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        assert_eq!(
            worker_threads(usize::MAX),
            available.saturating_mul(MAX_THREADS_PER_CPU)
        );
    }

    #[test]
    fn a_missing_root_is_rejected() {
        let error = verify_tree(Path::new("/nonexistent/binance-data-verify"), 1).unwrap_err();
        assert!(
            matches!(error, VerifyError::NotADirectory(_)),
            "got {error:?}"
        );
    }

    #[test]
    fn a_file_root_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("file.zip");
        std::fs::write(&file, b"x").unwrap();
        let error = verify_tree(&file, 1).unwrap_err();
        assert!(
            matches!(error, VerifyError::NotADirectory(_)),
            "got {error:?}"
        );
    }

    #[test]
    fn an_empty_tree_reports_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let summary = verify_tree(dir.path(), 1).unwrap();
        assert_eq!(summary.seen, 0);
        assert!(summary.is_ok());
    }

    #[test]
    fn non_zip_files_are_ignored() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("notes.txt"), b"x").unwrap();
        let summary = verify_tree(dir.path(), 1).unwrap();
        assert_eq!(summary.seen, 0);
        assert!(summary.is_ok());
    }
}
