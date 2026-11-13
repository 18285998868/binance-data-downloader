//! Public facade over the download engine.
//!
//! The pipeline lives in `bdd-core`; this crate is the stable entry point
//! embedders depend on. Keeping the facade thin leaves room for future
//! host-facing APIs without changing the engine.
//!
//! The crate documentation is the project README, so that the published docs
//! and the repository README can never drift apart.
//!
//! ```no_run
//! # async fn demo() -> Result<(), bdd::AppError> {
//! bdd::run(std::path::Path::new("config.toml")).await?;
//! # Ok(())
//! # }
//! ```

#![doc = include_str!("../../../README.md")]

pub use bdd_core::{AppError, FailureReport, RunOptions, run, run_with_options, verify_zip};
