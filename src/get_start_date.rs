use backoff::Error as BackoffError;
use backoff::future::retry;

use crate::AppError;
use crate::backoff::default_backoff;
use crate::cache;

const CACHE_DOMAIN: &str = "start_date";

fn klines_url(symbol: &str, timeframe: &str) -> String {
    format!(
        "https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={timeframe}&limit=1&startTime=0"
    )
}

/// Fetch (or cache-lookup) the earliest kline open time for a symbol/timeframe.
pub async fn get_first_kline_date(
    client: &reqwest::Client,
    symbol: &str,
    timeframe: &str,
) -> Result<chrono::NaiveDate, AppError> {
    let key = format!("{symbol}:{timeframe}");
    if let Some(cached) = cache::read(CACHE_DOMAIN, &key, None) {
        let s = std::str::from_utf8(&cached).map_err(|e| AppError::Data(e.to_string()))?;
        let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|e| AppError::Date(e.to_string()))?;
        return Ok(d);
    }
    let date = fetch_first_kline(client, symbol, timeframe).await?;
    cache::write(
        CACHE_DOMAIN,
        &key,
        date.format("%Y-%m-%d").to_string().as_bytes(),
    );
    Ok(date)
}

async fn fetch_first_kline(
    client: &reqwest::Client,
    symbol: &str,
    timeframe: &str,
) -> Result<chrono::NaiveDate, AppError> {
    let url = klines_url(symbol, timeframe);

    let op = || {
        let c = client.clone();
        let u = url.clone();
        async move {
            let r = match c.get(&u).send().await {
                Ok(r) => r,
                Err(e) => {
                    return Err(BackoffError::Transient {
                        err: AppError::from(e),
                        retry_after: None,
                    });
                }
            };
            let t = match r.text().await {
                Ok(t) => t,
                Err(e) => {
                    return Err(BackoffError::Transient {
                        err: AppError::from(e),
                        retry_after: None,
                    });
                }
            };
            let k: Vec<Vec<serde_json::Value>> = match serde_json::from_str(&t) {
                Ok(k) => k,
                Err(e) => return Err(BackoffError::Permanent(AppError::Api(e.to_string()))),
            };
            let first = match k.first() {
                Some(f) => f,
                None => {
                    return Err(BackoffError::Permanent(AppError::Api("no klines".into())));
                }
            };
            let first_elem = match first.first() {
                Some(v) => v,
                None => {
                    return Err(BackoffError::Permanent(AppError::Api("empty kline".into())));
                }
            };
            let ms = match first_elem.as_u64() {
                Some(ms) => ms,
                None => {
                    return Err(BackoffError::Permanent(AppError::Api(
                        "open_time missing".into(),
                    )));
                }
            };
            let dt = match chrono::DateTime::from_timestamp_millis(ms as i64) {
                Some(dt) => dt,
                None => {
                    return Err(BackoffError::Permanent(AppError::Date(format!(
                        "invalid timestamp: {ms}"
                    ))));
                }
            };
            Ok(dt.date_naive())
        }
    };

    retry(default_backoff(), op).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn klines_url_format() {
        let url = klines_url("BTCUSDT", "1m");
        assert!(url.contains("BTCUSDT"));
        assert!(url.contains("interval=1m"));
        assert!(url.contains("startTime=0"));
        assert!(url.contains("limit=1"));
    }

    #[test]
    fn klines_url_different_symbol_timeframe() {
        let url = klines_url("ETHUSDT", "5m");
        assert!(url.contains("ETHUSDT"));
        assert!(url.contains("interval=5m"));
    }

    #[test]
    fn klines_url_start_time_zero() {
        let url = klines_url("BTCUSDT", "1d");
        assert!(url.contains("startTime=0"));
    }

    #[test]
    fn date_parse_round_trip() {
        let date_str = "2024-01-15";
        let date = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d").unwrap();
        assert_eq!(date.format("%Y-%m-%d").to_string(), date_str);
    }

    #[test]
    fn date_parse_leap_year() {
        let date = chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        assert_eq!(date.format("%Y-%m-%d").to_string(), "2024-02-29");
    }
}
