/// Orchestrates a batch download across resolved symbol/timeframe pairs.
///
/// Symbols and timeframes are resolved from config or auto-fetched from
/// Binance exchangeInfo (for symbols). Pairs are processed sequentially;
/// each pair uses file-level concurrency capped at 256.
pub use backoff::Error as BackoffError;
use backoff::ExponentialBackoff;
use backoff::future::retry;
use moka::future::Cache;
use serde::Deserialize;
use std::sync::LazyLock;

use crate::AppError;
use crate::run_once;

use super::load;

const EXCHANGE_INFO_URL: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";

static SYMBOL_CACHE: LazyLock<Cache<String, Vec<String>>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(10)
        .time_to_live(std::time::Duration::from_secs(3600))
        .build()
});

const DEFAULT_TIMEFRAMES: &[&str] = &[
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d",
];

#[derive(Deserialize)]
struct SymbolInfo {
    symbol: String,
    status: String,
}

#[derive(Deserialize)]
struct ExchangeInfoResponse {
    symbols: Vec<SymbolInfo>,
}

pub async fn run() -> Result<(), AppError> {
    let config = load::load_batch_config()?;
    let max_concurrent = config.max_concurrent.unwrap_or(1000).min(256);
    if max_concurrent > 256 {
        tracing::warn!(
            configured = max_concurrent,
            effective = 256,
            "capping max_concurrent to 256 (tokio blocking pool limit)",
        );
    }

    let client = build_client(max_concurrent)?;

    let symbols = resolve_symbols(&client, &config.symbol).await?;
    let timeframes = resolve_timeframes(&config.timeframe);

    if symbols.is_empty() {
        tracing::warn!("no symbols resolved, nothing to download");
        return Ok(());
    }
    if timeframes.is_empty() {
        tracing::warn!("no timeframes configured, nothing to download");
        return Ok(());
    }

    tracing::info!(
        symbols = symbols.len(),
        timeframes = timeframes.len(),
        max_concurrent = max_concurrent,
        "starting batch download",
    );

    let mut errors = Vec::new();
    for sym in &symbols {
        for tf in &timeframes {
            let run_cfg = run_once::RunOnceConfig {
                symbol: sym.clone(),
                timeframe: tf.clone(),
                start: config.start.clone(),
                end: config.end.clone(),
                dir: config.dir.clone(),
            };

            tracing::info!(symbol = %sym, timeframe = %tf, "running pair");
            if let Err(e) = run_once::run_once(&client, &run_cfg, max_concurrent).await {
                tracing::error!(symbol = %sym, timeframe = %tf, "failed: {e}");
                errors.push(format!("{sym}/{tf}: {e}"));
            }
        }
    }

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

async fn resolve_symbols(
    client: &reqwest::Client,
    configured: &Option<Vec<String>>,
) -> Result<Vec<String>, AppError> {
    if let Some(syms) = configured {
        if !syms.is_empty() {
            return Ok(syms.clone());
        }
    }
    if let Some(cached) = SYMBOL_CACHE.get("futures_trading").await {
        return Ok(cached);
    }
    let symbols = fetch_trading_symbols(client).await?;
    SYMBOL_CACHE
        .insert("futures_trading".into(), symbols.clone())
        .await;
    Ok(symbols)
}

async fn fetch_trading_symbols(client: &reqwest::Client) -> Result<Vec<String>, AppError> {
    let op = || {
        let c = client.clone();
        async move {
            let resp =
                c.get(EXCHANGE_INFO_URL)
                    .send()
                    .await
                    .map_err(|e| BackoffError::Transient {
                        err: AppError::Data(format!("exchange_info request: {e}")),
                        retry_after: None,
                    })?;

            resp.json::<ExchangeInfoResponse>().await.map_err(|e| {
                BackoffError::Permanent(AppError::Data(format!("exchange_info parse: {e}")))
            })
        }
    };

    let ExchangeInfoResponse { symbols } = retry(
        ExponentialBackoff {
            max_elapsed_time: Some(std::time::Duration::from_secs(30)),
            ..Default::default()
        },
        op,
    )
    .await?;

    Ok(symbols
        .into_iter()
        .filter(|s| s.status == "TRADING")
        .map(|s| s.symbol)
        .collect())
}

fn resolve_timeframes(configured: &Option<Vec<String>>) -> Vec<String> {
    if let Some(tfs) = configured {
        if !tfs.is_empty() {
            return tfs.clone();
        }
    }
    DEFAULT_TIMEFRAMES.iter().map(|s| s.to_string()).collect()
}

fn build_client(max_concurrent: usize) -> Result<reqwest::Client, AppError> {
    let b = reqwest::Client::builder()
        .user_agent("binance-data-downloader/0.1")
        .timeout(std::time::Duration::from_secs(30))
        .pool_max_idle_per_host(max_concurrent)
        .pool_idle_timeout(std::time::Duration::from_secs(30));
    b.build().map_err(AppError::Http)
}
