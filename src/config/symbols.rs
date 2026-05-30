use backoff::Error as BackoffError;
use backoff::ExponentialBackoff;
use backoff::future::retry;
use futures::future::try_join_all;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use super::Config;
use super::market_type::MarketType;

#[derive(Deserialize)]
struct SymbolInfo {
    symbol: String,
    status: String,
}

#[derive(Deserialize)]
struct ExchangeInfoResponse {
    symbols: Vec<SymbolInfo>,
}

impl Config {
    pub async fn resolve_symbols(&self) -> Result<Vec<String>, crate::AppError> {
        if let Some(s) = &self.symbols
            && !s.is_empty()
        {
            return Ok(s.clone());
        }
        if self.market_type.is_empty() {
            return Ok(Vec::new());
        }

        static CACHE: LazyLock<Mutex<HashMap<String, Vec<String>>>> =
            LazyLock::new(|| Mutex::new(HashMap::new()));

        let results: Vec<Vec<String>> = try_join_all(self.market_type.iter().map(|&mt| {
            let key = format!("symbols_{}", mt.as_tag());
            async move {
                {
                    let cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(data) = cache.get(&key) {
                        return Ok::<_, crate::AppError>(data.clone());
                    }
                }
                let symbols = fetch_trading_symbols(mt).await?;
                CACHE
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(key, symbols.clone());
                Ok(symbols)
            }
        }))
        .await?;

        let mut all: Vec<String> = results.into_iter().flatten().collect();
        all.sort();
        all.dedup();
        Ok(all)
    }
}

async fn fetch_trading_symbols(mt: MarketType) -> Result<Vec<String>, crate::AppError> {
    let op = || async {
        let resp =
            reqwest::get(mt.exchange_info_url())
                .await
                .map_err(|e| BackoffError::Transient {
                    err: crate::AppError::Data(format!("exchange_info request: {e}")),
                    retry_after: None,
                })?;

        resp.json::<ExchangeInfoResponse>().await.map_err(|e| {
            BackoffError::Permanent(crate::AppError::Data(format!("exchange_info parse: {e}")))
        })
    };

    let ExchangeInfoResponse { symbols } = retry(ExponentialBackoff::default(), op).await?;

    Ok(symbols
        .into_iter()
        .filter(|s| s.status == "TRADING")
        .map(|s| s.symbol)
        .collect())
}
