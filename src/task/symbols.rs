use backoff::Error as BackoffError;
use backoff::ExponentialBackoff;
use backoff::future::retry;
use futures::future::try_join_all;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use super::store;
use crate::config::market_type::MarketType;

const CACHE_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Resolves the list of symbols to download.
///
/// If `symbols` is `Some` and non-empty, returns it directly.
/// Otherwise fetches all TRADING symbols from Binance exchangeInfo
/// API with exponential backoff, cached in sled with a 7-day TTL.
pub async fn resolve_symbols(
    symbols: &Option<Vec<String>>,
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
    let store = store::Store::open_async(&cache_dir).await?;

    let results: Vec<Vec<String>> = try_join_all(market_types.iter().map(|&mt| {
        let store = Arc::clone(&store);
        async move {
            let key = format!("symbols_{}", mt.as_tag());

            if let Ok(Some(data)) = store::get_cached::<Vec<String>>(&store, &key).await
                && !data.is_empty()
            {
                return Ok::<_, crate::AppError>(data);
            }

            let symbols = fetch_trading_symbols(mt).await?;
            let _ = store::set_cached(&store, &key, symbols.clone(), CACHE_TTL).await;
            Ok(symbols)
        }
    }))
    .await?;

    let mut all: Vec<String> = results.into_iter().flatten().collect();
    all.sort();
    all.dedup();
    Ok(all)
}

async fn fetch_trading_symbols(mt: MarketType) -> Result<Vec<String>, crate::AppError> {
    let market = match mt {
        MarketType::Spot => common::venue::binance::Market::Spot,
        MarketType::Futures => common::venue::binance::Market::Futures,
    };

    let op = || {
        async move {
            common::venue::binance::exchange_info(market, None)
                .await
                .map_err(|e| {
                    BackoffError::Transient {
                        err: crate::AppError::Data(e.to_string()),
                        retry_after: None,
                    }
                })
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
