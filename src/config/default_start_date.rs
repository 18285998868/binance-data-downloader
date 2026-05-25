use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use futures::stream::{self, StreamExt};
use crate::config::cache::JsonFileCache;
use crate::config::retry::{RetryConfig, retry};
use crate::config::MarketType;

const CACHE_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Map from `market_type:symbol` → first trading date.
pub type StartDateMap = HashMap<String, chrono::NaiveDate>;

/// Composite key for the start-date map.
pub fn start_date_key(market_type: MarketType, pair: &str) -> String {
    format!("{}:{}", market_type.as_tag(), pair)
}

/// Resolves start dates for every (symbol, market_type) combination.
///
/// Uses a shared JSON cache (`start_dates.json`) with 1-week expiry.
/// Missing entries are fetched from the Binance klines endpoint
/// (`startTime=0`) at 10 concurrent requests.
pub async fn resolve_start_dates(
    symbols: &[String],
    client: &reqwest::Client,
    market_types: &[MarketType],
    data_dir: &Path,
) -> Result<StartDateMap, crate::AppError> {
    let mut cache = JsonFileCache::<HashMap<String, String>>::new(data_dir, "start_dates");
    cache.load_with_expiry(CACHE_TTL).await;

    let mut map = HashMap::new();
    let mut misses: Vec<(String, MarketType)> = Vec::new();

    for pair in symbols {
        for &mt in market_types {
            let key = start_date_key(mt, pair);
            if let Some(d) = cache_get(&cache.data, mt, pair) {
                map.insert(key, d);
            } else {
                misses.push((pair.clone(), mt));
            }
        }
    }

    if !misses.is_empty() {
        let fetched: Vec<_> = stream::iter(misses)
            .map(|(pair, mt)| {
                let c = client.clone();
                async move {
                    fetch_first_date(&c, mt, &pair)
                        .await
                        .map(|d| (pair, mt, d))
                }
            })
            .buffer_unordered(10)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;

        for (pair, mt, date) in &fetched {
            cache_set(&mut cache.data, *mt, pair, *date);
            cache.mark_changed();
            map.insert(start_date_key(*mt, pair), *date);
        }
        cache.save().await;
    }

    Ok(map)
}

// ---- internal helpers ----

fn cache_get(
    cache: &HashMap<String, String>,
    mt: MarketType,
    pair: &str,
) -> Option<chrono::NaiveDate> {
    cache
        .get(&start_date_key(mt, pair))
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
}

fn cache_set(
    cache: &mut HashMap<String, String>,
    mt: MarketType,
    pair: &str,
    date: chrono::NaiveDate,
) {
    cache.insert(start_date_key(mt, pair), date.format("%Y-%m-%d").to_string());
}

async fn fetch_first_date(
    client: &reqwest::Client,
    mt: MarketType,
    pair: &str,
) -> Result<chrono::NaiveDate, crate::AppError> {
    let url = match mt {
        MarketType::Spot => format!(
            "https://api.binance.com/api/v3/klines?symbol={}&interval=1m&limit=1&startTime=0",
            pair
        ),
        MarketType::Futures => format!(
            "https://fapi.binance.com/fapi/v1/klines?symbol={}&interval=1m&limit=1&startTime=0",
            pair
        ),
    };
    retry(
        || {
            let c = client.clone();
            let u = url.clone();
            async move {
                let r = c.get(&u).send().await.map_err(crate::AppError::Http)?;
                let t = r.text().await.map_err(crate::AppError::Http)?;
                let k: Vec<Vec<serde_json::Value>> = serde_json::from_str(&t)
                    .map_err(|e| crate::AppError::Api(e.to_string()))?;
                let first = k
                    .first()
                    .ok_or_else(|| crate::AppError::Api("no klines".into()))?;
                let ms = first[0]
                    .as_u64()
                    .ok_or_else(|| crate::AppError::Api("open_time missing".into()))?;
                chrono::NaiveDate::from_num_days_from_ce_opt(
                    719163 + (ms / 1000 / 86400) as i32,
                )
                .ok_or_else(|| crate::AppError::Date(format!("invalid ts: {ms}")))
            }
        },
        &RetryConfig::default(),
        |e| matches!(e, crate::AppError::Http(_)),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_spot() {
        assert_eq!(start_date_key(MarketType::Spot, "BTCUSDT"), "spot:BTCUSDT");
    }

    #[test]
    fn test_key_futures() {
        assert_eq!(
            start_date_key(MarketType::Futures, "ETHUSDT"),
            "futures:ETHUSDT"
        );
    }

    #[test]
    fn test_cache_roundtrip() {
        let mut map = HashMap::new();
        let date = chrono::NaiveDate::from_ymd_opt(2023, 6, 15).unwrap();
        cache_set(&mut map, MarketType::Spot, "SOLUSDT", date);
        assert_eq!(map.get("spot:SOLUSDT").unwrap(), "2023-06-15");
        assert_eq!(cache_get(&map, MarketType::Spot, "SOLUSDT"), Some(date));
    }

    #[test]
    fn test_cache_missing() {
        assert!(cache_get(&HashMap::new(), MarketType::Futures, "NONEXIST").is_none());
    }
}
