//! Step 2: acquire the run's exclusive resources.
//!
//! Everything here is per-run state that later steps borrow: the output
//! directory lock, the cache, the HTTP client, the local file index, the
//! resolved date window and the cancellation token.

pub(crate) mod client;
pub(crate) mod lock;
pub(crate) mod signal;

use crate::AppError;
use crate::cache::Cache;
use crate::cancel::CancelToken;
use crate::config::AppConfig;
use crate::dataset::DownloadJob;
use crate::endpoints::Endpoints;
use crate::scan::LocalFileSet;

use self::lock::RunLock;

/// Everything one run needs after configuration has been loaded.
pub(crate) struct PreparedRun {
    /// Shared HTTP client.
    pub(crate) client: reqwest::Client,
    /// Persistent start-date cache.
    pub(crate) cache: Cache,
    /// Relative paths already present on disk.
    pub(crate) local: LocalFileSet,
    /// Effective concurrency, capped by the descriptor limit.
    pub(crate) concurrency: usize,
    /// Inclusive end of the download window.
    pub(crate) end: chrono::NaiveDate,
    /// Normalized symbols (uppercased, sorted, deduplicated).
    pub(crate) symbols: Vec<String>,
    /// Expanded dataset jobs.
    pub(crate) jobs: Vec<DownloadJob>,
    /// Cooperative cancellation shared by every stage.
    pub(crate) cancel: CancelToken,
    /// Base URLs for this run.
    pub(crate) endpoints: Endpoints,
    /// Held until the run finishes; dropping it releases the output directory.
    _lock: RunLock,
}

/// Acquire every resource and derive the batch's fixed inputs.
///
/// # Errors
///
/// Returns [`AppError`] when the output directory is already locked by another
/// run, the HTTP client cannot be built, or the local file scan fails.
pub(crate) async fn run(
    cfg: &AppConfig,
    endpoints: Endpoints,
    cancel: CancelToken,
) -> Result<PreparedRun, AppError> {
    // Concurrent runs over one output directory are not supported: they would
    // contend on the same `redb` cache file and race on destination files. The
    // guard turns that into an immediate, explicit error.
    let lock = RunLock::acquire(&cfg.output_dir).await?;

    let cache = Cache::new(&cfg.output_dir);
    let client = client::build()?;
    let local = crate::scan::scan_local_files(&cfg.output_dir).await?;

    // The configured concurrency is a request; the descriptor limit has the
    // final say. Every in-flight download holds a socket *and* a temporary
    // file, so exceeding the limit turns into `EMFILE` and a wave of failed
    // downloads rather than a faster run.
    let concurrency = crate::limits::effective_concurrency(cfg.concurrency);
    if concurrency < cfg.concurrency {
        tracing::warn!(
            requested = cfg.concurrency,
            effective = concurrency,
            "concurrency reduced to fit the process descriptor limit",
        );
    }

    // The archive partitions days by UTC midnight, so "yesterday" must be UTC.
    let end = cfg.resolved_end(chrono::Utc::now().date_naive());
    let symbols = cfg.normalized_symbols();
    let jobs = cfg.jobs();

    tracing::info!(
        market = %cfg.market,
        symbols = symbols.len(),
        jobs = jobs.len(),
        total = symbols.len() * jobs.len(),
        concurrency,
        local_files = local.len(),
        "starting batch download",
    );
    if cfg.start.is_none() {
        tracing::info!(
            count = symbols.len(),
            "the first start-date of every symbol is resolved up front -- one request to \
             the market API and one bucket listing per job -- and cached for 30 days; \
             this can take a while on a fresh run",
        );
    }

    Ok(PreparedRun {
        client,
        cache,
        local,
        concurrency,
        end,
        symbols,
        jobs,
        cancel,
        endpoints,
        _lock: lock,
    })
}
