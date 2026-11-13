//! Standalone verifier for already-downloaded archive files.
//!
//! The downloader's `--verify` flag only covers files fetched in the current
//! run. This binary exists for the other case: checking an existing output tree
//! after a disk event, a copy, or an earlier version of the tool.
//!
//! The entry point only parses arguments and maps the summary to an exit code;
//! the traversal and parallel verification live in [`tree`].

use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::Parser;

mod tree;

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

fn main() -> Result<()> {
    let args = Args::parse();
    if !args.dir.is_dir() {
        bail!("not a directory: {}", args.dir.display());
    }

    let jobs = if args.jobs == 0 {
        std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
    } else {
        args.jobs
    };

    let summary = tree::verify_tree(&args.dir, jobs)?;
    if summary.failed > 0 {
        bail!(
            "{} archive(s) failed verification (scanned {})",
            summary.failed,
            summary.seen
        );
    }
    if summary.seen == 0 {
        println!("no .zip archives found under {}", args.dir.display());
        return Ok(());
    }
    println!(
        "verified {} archive(s) under {}",
        summary.seen,
        args.dir.display()
    );
    Ok(())
}
