use backoff::Error as BackoffError;
use backoff::ExponentialBackoff;
use backoff::future::retry;
use futures::future::try_join_all;
use std::path::Path;
use std::time::Duration;

use super::store::Store;
use crate::config::market_type::MarketType;

const CACHE_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

pub async fn resolve_symbols(
    symbols: &Option<Vec<String>>,
    client: &reqwest::Client,
    market_types: &[MarketType],
    data_dir: &Path,
) -> Result<Vec<String>, crate::AppError> {
    if let Some(s) = symbols
        && !s.is_empty()
    {
        return Ok(s.clone());
    }
    if market_types.is_empty() {
        return Ok(Vec::new());
    }

    let cache_dir = data_dir.join(".cache");
    let results: Vec<Vec<String>> = try_join_all(market_types.iter().map(|&mt| {
        let c = client.clone();
        let cd = cache_dir.clone();
        async move {
            let key = format!("symbols_{}", mt.as_tag());

            if let Ok(Some(data)) = Store::get_async::<Vec<String>>(&cd, &key).await
                && !data.is_empty()
            {
                return Ok::<_, crate::AppError>(data);
            }

            let symbols = fetch_trading_symbols(&c, mt).await?;
            let _ = Store::set_async(&cd, &key, symbols.clone(), CACHE_TTL).await;
            Ok(symbols)
        }
    }))
    .await?;

    let mut all: Vec<String> = results.into_iter().flatten().collect();
    all.sort();
    all.dedup();
    Ok(all)
}

#[derive(serde::Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}
#[derive(serde::Deserialize)]
struct SymbolInfo {
    symbol: String,
    status: String,
}

async fn fetch_trading_symbols(
    client: &reqwest::Client,
    mt: MarketType,
) -> Result<Vec<String>, crate::AppError> {
    let url = match mt {
        MarketType::Spot => "https://api.binance.com/api/v3/exchangeInfo",
        MarketType::Futures => "https://fapi.binance.com/fapi/v1/exchangeInfo",
    };

    let op = || {
        let c = client.clone();
        async move {
            let r = match c.get(url).send().await {
                Ok(r) => r,
                Err(e) => {
                    return Err(BackoffError::Transient {
                        err: crate::AppError::from(e),
                        retry_after: None,
                    });
                }
            };
            let t = match r.text().await {
                Ok(t) => t,
                Err(e) => {
                    return Err(BackoffError::Transient {
                        err: crate::AppError::from(e),
                        retry_after: None,
                    });
                }
            };
            let info: ExchangeInfo = match serde_json::from_str(&t) {
                Ok(info) => info,
                Err(e) => {
                    return Err(BackoffError::Permanent(crate::AppError::Data(format!(
                        "exchangeInfo: {e}"
                    ))));
                }
            };
            Ok(info)
        }
    };

    let info = retry(ExponentialBackoff::default(), op).await?;

    Ok(info
        .symbols
        .into_iter()
        .filter(|s| s.status == "TRADING")
        .map(|s| s.symbol)
        .collect())
}
