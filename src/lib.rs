pub mod config;
pub mod utils;
pub mod error;
pub mod task;

pub use error::AppError;

use futures::stream::{self, StreamExt};
use config::get_symbols;



/// Runs a batch download across all configured symbol/timeframe pairs.
///
/// Symbols and timeframes are resolved from config or auto-fetched from the
/// Binance exchangeInfo API. Pairs are processed in parallel across available
/// CPU cores; each pair uses file-level concurrency capped at 256.
pub async fn run() -> Result<(), AppError> {
    let config = config::load_cfg(None)?;
    let max_concurrent = config.max_concurrent.min(256);
    if config.max_concurrent > 256 {
        tracing::warn!(
            configured = config.max_concurrent,
            effective = 256,
            "capping max_concurrent to 256 (tokio blocking pool limit)",
        );
    }

    let client = get_symbols::build_client(max_concurrent)?;

    let symbols = get_symbols::resolve_symbols(&client, &config.symbol).await?;
    let timeframes = config.timeframe.clone();

    if symbols.is_empty() {
        tracing::warn!("no symbols resolved, nothing to download");
        return Ok(());
    }
    if timeframes.is_empty() {
        tracing::warn!("no timeframes configured, nothing to download");
        return Ok(());
    }

    let pairs: Vec<(String, String)> = symbols
        .iter()
        .flat_map(|s| timeframes.iter().map(move |t| (s.clone(), t.clone())))
        .collect();

    let n_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    tracing::info!(
        symbols = symbols.len(),
        timeframes = timeframes.len(),
        pairs = pairs.len(),
        parallelism = n_cores,
        max_concurrent = max_concurrent,
        "starting batch download",
    );

    let results: Vec<Option<String>> = stream::iter(pairs.into_iter().map(|(sym, tf)| {
        let client = client.clone();
        let cfg = config.clone();
        async move {
            tracing::info!(symbol = %sym, timeframe = %tf, "running pair");
            if let Err(e) =
                task::run_one_pair(&client, &sym, &tf, &cfg).await
            {
                tracing::error!(symbol = %sym, timeframe = %tf, "failed: {e}");
                Some(format!("{sym}/{tf}: {e}"))
            } else {
                None
            }
        }
    }))
    .buffer_unordered(n_cores)
    .collect()
    .await;

    let errors: Vec<String> = results.into_iter().flatten().collect();

    if errors.is_empty() {
        tracing::info!("batch download complete");
        Ok(())
    } else {
        tracing::error!(count = errors.len(), "batch download completed with errors");
        Err(AppError::Data(format!(
            "{} pair(s) failed: {}",
            errors.len(),
            errors.join("; ")
        )))
    }
}

