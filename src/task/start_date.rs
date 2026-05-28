use backoff::Error as BackoffError;
use backoff::ExponentialBackoff;
use backoff::future::retry;
use futures::stream::{self, StreamExt};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use super::store;
use crate::config::market_type::MarketType;

const CACHE_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

pub type StartDateMap = HashMap<String, chrono::NaiveDate>;

pub fn start_date_key(mt: MarketType, pair: &str) -> String {
    format!("{}:{}", mt.as_tag(), pair)
}

pub async fn resolve_start_dates(
    symbols: &[String],
    client: &reqwest::Client,
    market_types: &[MarketType],
    data_dir: &Path,
) -> Result<StartDateMap, crate::AppError> {
    let cache_dir = data_dir.join(".cache");
    let store = store::Store::open_async(&cache_dir).await?;

    let mut map = HashMap::new();
    let mut misses: Vec<(String, MarketType)> = Vec::new();

    for pair in symbols {
        for &mt in market_types {
            let key = start_date_key(mt, pair);
            if let Ok(Some(d)) = store::get_cached::<String>(&store, &key).await
                && let Ok(date) = chrono::NaiveDate::parse_from_str(&d, "%Y-%m-%d")
            {
                map.insert(key, date);
                continue;
            }
            misses.push((pair.clone(), mt));
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

        for (pair, mt, date) in &fetched {
            let key = start_date_key(*mt, pair);
            let _ = store::set_cached(
                &store,
                &key,
                date.format("%Y-%m-%d").to_string(),
                CACHE_TTL,
            )
            .await;
            map.insert(key, *date);
        }
    }

    Ok(map)
}

async fn fetch_first_date(
    client: &reqwest::Client,
    mt: MarketType,
    pair: &str,
) -> Result<chrono::NaiveDate, crate::AppError> {
    let url = match mt {
        MarketType::Spot => format!(
            "https://api.binance.com/api/v3/klines?symbol={pair}&interval=1m&limit=1&startTime=0"
        ),
        MarketType::Futures => format!(
            "https://fapi.binance.com/fapi/v1/klines?symbol={pair}&interval=1m&limit=1&startTime=0"
        ),
    };

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
                Err(e) => return Err(BackoffError::Permanent(crate::AppError::Api(e.to_string()))),
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
