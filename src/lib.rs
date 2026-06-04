pub mod config;
pub mod error;
pub mod task;

pub use error::AppError;

use futures::stream::{self, StreamExt};

use config::get_symbols;

const DEFAULT_TIMEFRAMES: &[&str] = &[
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d",
];

/// Runs a batch download across all configured symbol/timeframe pairs.
///
/// Symbols and timeframes are resolved from config or auto-fetched from the
/// Binance exchangeInfo API. Pairs are processed in parallel across available
/// CPU cores; each pair uses file-level concurrency capped at 256.
pub async fn run() -> Result<(), AppError> {
    let config = config::load::load_batch_config()?;
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
    let timeframes = resolve_timeframes(&config.timeframe);

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

    let start = config.start.as_deref();
    let end = config.end.as_deref();
    let dir = Some(config.dir.as_str());
    let results: Vec<Option<String>> = stream::iter(pairs.into_iter().map(|(sym, tf)| {
        let client = client.clone();
        async move {
            tracing::info!(symbol = %sym, timeframe = %tf, "running pair");
            if let Err(e) =
                task::run_one_pair(&client, &sym, &tf, start, end, dir, max_concurrent).await
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

fn resolve_timeframes(configured: &[String]) -> Vec<String> {
    if !configured.is_empty() {
        return configured.to_vec();
    }
    DEFAULT_TIMEFRAMES.iter().map(|s| s.to_string()).collect()
}
