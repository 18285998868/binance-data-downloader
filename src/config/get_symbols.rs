use backoff::future::retry;
use moka::future::Cache;
use serde::Deserialize;
use std::sync::LazyLock;

use super::retry::{default_backoff, BackoffError};
use crate::AppError;

const EXCHANGE_INFO_URL: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";

static SYMBOL_CACHE: LazyLock<Cache<String, Vec<String>>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(10)
        .time_to_live(std::time::Duration::from_secs(3600))
        .build()
});

#[derive(Deserialize)]
struct SymbolInfo {
    symbol: String,
    status: String,
}

#[derive(Deserialize)]
struct ExchangeInfoResponse {
    symbols: Vec<SymbolInfo>,
}

pub async fn resolve_symbols(
    client: &reqwest::Client,
    configured: &[String],
) -> Result<Vec<String>, AppError> {
    if !configured.is_empty() {
        return Ok(configured.to_vec());
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
            let resp = c.get(EXCHANGE_INFO_URL).send().await.map_err(|e| {
                BackoffError::Transient {
                    err: AppError::Data(format!("exchange_info request: {e}")),
                    retry_after: None,
                }
            })?;

            resp.json::<ExchangeInfoResponse>().await.map_err(|e| {
                BackoffError::Permanent(AppError::Data(format!("exchange_info parse: {e}")))
            })
        }
    };

    let ExchangeInfoResponse { symbols } = retry(default_backoff(), op).await?;

    Ok(symbols
        .into_iter()
        .filter(|s| s.status == "TRADING")
        .map(|s| s.symbol)
        .collect())
}

pub fn build_client(max_concurrent: usize) -> Result<reqwest::Client, AppError> {
    let b = reqwest::Client::builder()
        .user_agent("binance-data-downloader/0.1")
        .timeout(std::time::Duration::from_secs(30))
        .pool_max_idle_per_host(max_concurrent)
        .pool_idle_timeout(std::time::Duration::from_secs(30));
    b.build().map_err(AppError::Http)
}
