//! Walk an output tree and validate every archive in it.
//!
//! The traversal lives here, not in `main.rs`, so the binary entry point stays
//! limited to argument parsing and exit-code mapping.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use rayon::prelude::*;
use walkdir::WalkDir;

/// Outcome of one verification pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Summary {
    /// Archives inspected.
    pub(super) seen: usize,
    /// Archives or directory entries that failed.
    pub(super) failed: usize,
}

/// Verify every `.zip` below `root` with `jobs` worker threads.
///
/// Individual failures are reported to stderr as they are found; the summary
/// carries the counts so the caller decides the exit status.
pub(super) fn verify_tree(root: &Path, jobs: usize) -> Result<Summary> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(jobs)
        .build()
        .map_err(|error| anyhow::anyhow!("cannot build verifier thread pool: {error}"))?;

    // Relaxed is enough: the counters are independent of each other and only
    // read after `install` has joined every worker.
    let seen = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);

    pool.install(|| {
        WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .par_bridge()
            .for_each(|entry| match entry {
                Ok(entry)
                    if entry.file_type().is_file()
                        && entry.path().extension().is_some_and(|ext| ext == "zip") =>
                {
                    seen.fetch_add(1, Ordering::Relaxed);
                    if let Err(error) = bdd::verify_zip(entry.path()) {
                        failed.fetch_add(1, Ordering::Relaxed);
                        eprintln!("{}: {error}", entry.path().display());
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    eprintln!("walk error: {error}");
                }
            });
    });

    Ok(Summary {
        seen: seen.load(Ordering::Relaxed),
        failed: failed.load(Ordering::Relaxed),
    })
}
