use backoff::Error as BackoffError;
use backoff::ExponentialBackoff;
use backoff::future::retry;
use futures::stream::{self, StreamExt};
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use super::market_type::MarketType;

/// Maps `market_type:symbol` keys to their earliest available date.
pub type StartDateMap = HashMap<String, chrono::NaiveDate>;

/// Builds the cache key for a (market_type, symbol) pair.
pub fn start_date_key(mt: MarketType, pair: &str) -> String {
    format!("{}:{}", mt.as_tag(), pair)
}

/// Resolves the earliest available date for each symbol on each market type.
///
/// Checks in-memory cache first; fetches from Binance klines API
/// (with backoff) for any missing entries, then caches the result.
pub async fn resolve_start_dates(
    symbols: &[String],
    client: &reqwest::Client,
    market_types: &[MarketType],
) -> Result<StartDateMap, crate::AppError> {
    static CACHE: LazyLock<Mutex<HashMap<String, String>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    let mut map = HashMap::new();
    let mut misses: Vec<(String, MarketType)> = Vec::new();

    for pair in symbols {
        for &mt in market_types {
            let key = start_date_key(mt, pair);
            let cached = CACHE
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&key)
                .cloned();
            if let Some(d) = cached
                && let Ok(date) = chrono::NaiveDate::parse_from_str(&d, "%Y-%m-%d")
            {
                map.insert(key, date);
            } else {
                misses.push((pair.clone(), mt));
            }
        }
    }

    if !misses.is_empty() {
        let fetched: Vec<_> = stream::iter(misses)
            .map(|(pair, mt)| {
                let c = client.clone();
                async move { fetch_first_date(&c, mt, &pair).await.map(|d| (pair, mt, d)) }
            })
            .buffer_unordered(10)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;

        let mut cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
        for (pair, mt, date) in &fetched {
            let key = start_date_key(*mt, pair);
            cache.insert(key.clone(), date.format("%Y-%m-%d").to_string());
            map.insert(key, *date);
        }
    }

    Ok(map)
}

/// Calls Binance klines API with `startTime=0` to get the first
/// available kline date for a (market_type, symbol) pair.
async fn fetch_first_date(
    client: &reqwest::Client,
    mt: MarketType,
    pair: &str,
) -> Result<chrono::NaiveDate, crate::AppError> {
    let url = mt.klines_url(pair);

    let op = || {
        let c = client.clone();
        let u = url.clone();
        async move {
            let r = match c.get(&u).send().await {
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
            let k: Vec<Vec<serde_json::Value>> = match serde_json::from_str(&t) {
                Ok(k) => k,
                Err(e) => {
                    return Err(BackoffError::Permanent(crate::AppError::Api(e.to_string())));
                }
            };
            let first = match k.first() {
                Some(f) => f,
                None => {
                    return Err(BackoffError::Permanent(crate::AppError::Api(
                        "no klines".into(),
                    )));
                }
            };
            let first_elem = match first.first() {
                Some(v) => v,
                None => {
                    return Err(BackoffError::Permanent(crate::AppError::Api(
                        "empty kline".into(),
                    )));
                }
            };
            let ms = match first_elem.as_u64() {
                Some(ms) => ms,
                None => {
                    return Err(BackoffError::Permanent(crate::AppError::Api(
                        "open_time missing".into(),
                    )));
                }
            };
            let dt = match chrono::DateTime::from_timestamp_millis(ms as i64) {
                Some(dt) => dt,
                None => {
                    return Err(BackoffError::Permanent(crate::AppError::Date(format!(
                        "invalid timestamp: {ms}"
                    ))));
                }
            };
            Ok(dt.date_naive())
        }
    };

    retry(ExponentialBackoff::default(), op).await
}
