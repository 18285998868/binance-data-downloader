//! Standalone verifier for already-downloaded archive files.
//!
//! The downloader's `--verify` flag only covers files fetched in the current
//! run. This binary exists for the other case: checking an existing output tree
//! after a disk event, a copy, or an earlier version of the tool.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::bail;
use clap::Parser;
use rayon::prelude::*;
use walkdir::WalkDir;

/// Verify every `.zip` under an output directory.
#[derive(Debug, Parser)]
#[command(
    name = "binance-data-verify",
    about = "Validate existing Binance kline zip archives (central directory only)."
)]
struct Args {
    /// Directory containing downloaded `.zip` files.
    #[arg(short, long)]
    dir: PathBuf,

    /// Worker threads; 0 uses the number of available CPUs.
    #[arg(long, default_value_t = 0, value_name = "N")]
    jobs: usize,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if !args.dir.is_dir() {
        bail!("not a directory: {}", args.dir.display());
    }
    let jobs = if args.jobs == 0 {
        std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
    } else {
        args.jobs
    };
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(jobs)
        .build()
        .map_err(|error| anyhow::anyhow!("cannot build verifier thread pool: {error}"))?;

    let seen = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);

    pool.install(|| {
        WalkDir::new(&args.dir)
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

    let seen = seen.load(Ordering::Relaxed);
    let failed = failed.load(Ordering::Relaxed);
    if failed > 0 {
        bail!("{failed} archive(s) failed verification (scanned {seen})");
    }
    if seen == 0 {
        println!("no .zip archives found under {}", args.dir.display());
        return Ok(());
    }
    println!("verified {seen} archive(s) under {}", args.dir.display());
    Ok(())
}
