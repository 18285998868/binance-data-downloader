use std::sync::Arc;

use futures::stream::{self, StreamExt};

use crate::config;
use crate::AppError;

/// Run the full batch download pipeline.
///
/// 1. Load config
/// 2. Resolve symbols and timeframes
/// 3. Cartesian product → parallel per-pair downloads
pub async fn run() -> Result<(), AppError> {
    let cfg = Arc::new(config::AppConfig::load(None)?);
    let max_concurrent = cfg.max_concurrent.min(256);
    if cfg.max_concurrent > 256 {
        tracing::warn!(
            configured = cfg.max_concurrent,
            effective = 256,
            "capping max_concurrent to 256 (tokio blocking pool limit)",
        );
    }

    let client = common::http_client::build_client(max_concurrent)
        .map_err(|e| AppError::Data(e.to_string()))?;

    let symbols = crate::task::get_symbols::resolve_symbols(
        &client,
        cfg.symbol.as_deref().unwrap_or(&[]),
    )
    .await?;
    let timeframes = cfg.timeframe.clone().unwrap_or_default();

    if symbols.is_empty() {
        tracing::warn!("no symbols resolved, nothing to download");
        return Ok(());
    }
    if timeframes.is_empty() {
        tracing::warn!("no timeframes configured, nothing to download");
        return Ok(());
    }

    let pairs = build_pairs(&symbols, &timeframes);

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
        let cfg = Arc::clone(&cfg);
        async move {
            tracing::info!(symbol = %sym, timeframe = %tf, "running pair");
            if let Err(e) = crate::run_one::run_one(&client, &sym, &tf, &cfg).await {
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

    collect_errors(results)
}

fn build_pairs(symbols: &[String], timeframes: &[String]) -> Vec<(String, String)> {
    symbols
        .iter()
        .flat_map(|s| timeframes.iter().map(move |t| (s.clone(), t.clone())))
        .collect()
}

fn collect_errors(results: Vec<Option<String>>) -> Result<(), AppError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_pairs_cartesian_product() {
        let symbols = vec!["A".into(), "B".into()];
        let timeframes = vec!["1m".into(), "5m".into()];
        let pairs = build_pairs(&symbols, &timeframes);
        assert_eq!(pairs.len(), 4);
        assert!(pairs.contains(&("A".into(), "1m".into())));
        assert!(pairs.contains(&("A".into(), "5m".into())));
        assert!(pairs.contains(&("B".into(), "1m".into())));
        assert!(pairs.contains(&("B".into(), "5m".into())));
    }

    #[test]
    fn build_pairs_empty_symbols() {
        let pairs = build_pairs(&[], &["1d".into()]);
        assert!(pairs.is_empty());
    }

    #[test]
    fn build_pairs_empty_timeframes() {
        let pairs = build_pairs(&["BTCUSDT".into()], &[]);
        assert!(pairs.is_empty());
    }

    #[test]
    fn collect_errors_all_ok() {
        let results = vec![None, None, None];
        assert!(collect_errors(results).is_ok());
    }

    #[test]
    fn collect_errors_some_failed() {
        let results = vec![None, Some("ETHUSDT/1m: timeout".into()), None];
        let err = collect_errors(results).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("1 pair(s) failed"));
        assert!(msg.contains("ETHUSDT/1m"));
    }
}
