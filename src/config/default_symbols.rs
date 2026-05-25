use std::path::Path;
use std::time::Duration;
use futures::future::try_join_all;
use crate::config::cache::JsonFileCache;
use crate::config::MarketType;

const CACHE_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Resolves symbols: if the user provided a non-empty list, return it.
/// Otherwise fetches TRADING symbols from Binance exchangeInfo for each
/// configured market type **in parallel**, unions them, and caches the result
/// (1-week TTL).
pub async fn resolve_symbols(
    symbols: &Option<Vec<String>>,
    client: &reqwest::Client,
    market_types: &[MarketType],
    data_dir: &Path,
) -> Result<Vec<String>, crate::AppError> {
    if let Some(s) = symbols && !s.is_empty() {
        return Ok(s.clone());
    }
    if market_types.is_empty() {
        return Ok(Vec::new());
    }
    let results: Vec<Vec<String>> = try_join_all(
        market_types.iter().map(|&mt| fetch_and_cache(client, mt, data_dir)),
    )
    .await?;
    let mut all: Vec<String> = results.into_iter().flatten().collect();
    all.sort();
    all.dedup();
    Ok(all)
}

async fn fetch_and_cache(
    client: &reqwest::Client,
    market_type: MarketType,
    data_dir: &Path,
) -> Result<Vec<String>, crate::AppError> {
    let name = format!("symbols_{}", market_type.as_tag());
    let mut cache = JsonFileCache::<Vec<String>>::new(data_dir, &name);
    cache.load_with_expiry(CACHE_TTL).await;
    if !cache.data.is_empty() {
        return Ok(cache.data);
    }
    let symbols = fetch_trading_symbols(client, market_type).await?;
    cache.data = symbols;
    cache.mark_changed();
    cache.save().await;
    Ok(cache.data)
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
    let r = client
        .get(url)
        .send()
        .await
        .map_err(crate::AppError::Http)?;
    let t = r.text().await.map_err(crate::AppError::Http)?;
    let info: ExchangeInfo = serde_json::from_str(&t)
        .map_err(|e| crate::AppError::Api(format!("exchangeInfo: {e}")))?;
    Ok(info
        .symbols
        .into_iter()
        .filter(|s| s.status == "TRADING")
        .map(|s| s.symbol)
        .collect())
}
