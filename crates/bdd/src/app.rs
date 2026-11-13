//! Pipeline orchestration: config -> prepare -> start dates -> downloads -> report.

use std::path::Path;

use crate::AppError;
use crate::cancel::CancelToken;
use crate::config;
use crate::download;
use crate::endpoints::Endpoints;
use crate::prepare;
use crate::report;
use crate::start_dates;

/// Options that tune a single run.
#[derive(Debug, Clone, Copy, Default)]
pub struct RunOptions {
    /// Validate every downloaded archive as a readable zip.
    ///
    /// Off by default: the magic-byte and `Content-Length` checks already
    /// reject truncated bodies and HTML error pages, and reading the central
    /// directory costs an extra pass over every file.
    pub verify: bool,
}

impl RunOptions {
    /// Options with every optional behaviour off.
    #[must_use]
    pub const fn new() -> Self {
        Self { verify: false }
    }

    /// Turn the zip validation on or off.
    #[must_use]
    pub const fn with_verify(mut self, verify: bool) -> Self {
        self.verify = verify;
        self
    }
}

/// Run the full batch download pipeline.
///
/// 1. Load config
/// 2. Scan existing local files (once)
/// 3. Resolve one start date per (symbol, job)
/// 4. Iterate symbol (serial) → job (serial), downloading missing files
///
/// # Errors
///
/// Returns [`AppError`] when the config is invalid, another run already holds
/// the output directory, or one or more symbol/job pairs fail (the summary
/// lists up to 20 failures). The batch aborts early after a run of
/// consecutive pair failures (likely a systemic outage) instead of retrying
/// every remaining pair.
///
/// A run cancelled with Ctrl-C is **not** an error: files already on disk stay
/// valid and a re-run resumes from them.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// # async fn demo() -> Result<(), bdd::AppError> {
/// // Downloads every symbol/job pair declared in `config.toml` into the
/// // configured output directory, skipping the files already on disk.
/// bdd::run(Path::new("config.toml")).await?;
/// # Ok(())
/// # }
/// ```
pub async fn run(config_path: &Path) -> Result<(), AppError> {
    run_with_options(config_path, &RunOptions::default()).await
}

/// Run the pipeline with explicit [`RunOptions`].
///
/// # Errors
///
/// Same as [`run`].
pub async fn run_with_options(config_path: &Path, options: &RunOptions) -> Result<(), AppError> {
    // Graceful shutdown: one Ctrl-C flag, observed both between pairs and by
    // every in-flight download, so a long batch stops promptly.
    let cancel = CancelToken::new();
    let _signal = prepare::signal::install(cancel.clone());
    run_pipeline(config_path, Endpoints::live(), cancel, options).await
}

/// The five pipeline steps, with their external dependencies passed in.
async fn run_pipeline(
    config_path: &Path,
    endpoints: Endpoints,
    cancel: CancelToken,
    options: &RunOptions,
) -> Result<(), AppError> {
    let cfg = config::load(config_path).await?;
    let prepared = prepare::run(&cfg, endpoints, cancel).await?;
    let dates = start_dates::resolve(&prepared, &cfg).await;
    let summary = download::run(&prepared, &cfg, &dates, options.verify).await;
    report::finish(&prepared, &cfg, &dates, &summary)
}

/// Test-only entry point: the same pipeline, redirected at a mock exchange.
///
/// Compiled only under `cfg(test)`; production has no way to reach it and no
/// configuration exposes the endpoints.
#[cfg(test)]
pub(crate) async fn run_with_endpoints(
    config_path: &Path,
    endpoints: Endpoints,
    cancel: CancelToken,
) -> Result<(), AppError> {
    run_pipeline(config_path, endpoints, cancel, &RunOptions::default()).await
}
