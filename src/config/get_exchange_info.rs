use backoff::Error as BackoffError;
use backoff::future::retry;
use moka::future::Cache;
use serde::Deserialize;
use std::sync::LazyLock;

use crate::AppError;
use crate::utils::default_backoff::default_backoff;

const EXCHANGE_INFO_URL: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";

static EXCHANGE_INFO_CACHE: LazyLock<Cache<String, Vec<String>>> = LazyLock::new(|| {
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

/// Fetch and cache the list of TRADING symbols from Binance exchangeInfo.
pub async fn get_trading_symbols(client: &reqwest::Client) -> Result<Vec<String>, AppError> {
    if let Some(cached) = EXCHANGE_INFO_CACHE.get("trading").await {
        return Ok(cached);
    }
    let symbols = fetch_trading_symbols(client).await?;
    EXCHANGE_INFO_CACHE
        .insert("trading".into(), symbols.clone())
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

    let ExchangeInfoResponse { symbols } = retry(default_backoff(), op).await?;

    Ok(symbols
        .into_iter()
        .filter(|s| s.status == "TRADING")
        .map(|s| s.symbol)
        .collect())
}
