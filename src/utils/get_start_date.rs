use backoff::Error as BackoffError;
use backoff::future::retry;

use crate::AppError;
use crate::utils::backoff::{RETRY_WINDOW, clamp_retry_after, default_backoff, parse_retry_after};
use crate::utils::cache::Cache;

const CACHE_DOMAIN: &str = "start_date";
/// Cached first-kline dates are considered stale after 30 days.
const CACHE_TTL_SECS: u64 = 30 * 24 * 60 * 60;

/// Binance USDⓈ-M futures API base. Kept as a parameter elsewhere so tests
/// can point at a local mock server.
const FAPI_BASE: &str = "https://fapi.binance.com";

fn klines_url(base: &str, symbol: &str, timeframe: &str) -> String {
    format!("{base}/fapi/v1/klines?symbol={symbol}&interval={timeframe}&limit=1&startTime=0")
}

/// Parse a cached `YYYY-MM-DD` start date. Returns `None` for entries that are
/// not valid UTF-8 or not a date; the caller then treats them as a miss.
fn parse_cached_date(raw: &[u8]) -> Option<chrono::NaiveDate> {
    let s = std::str::from_utf8(raw).ok()?;
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

/// Fetch (or cache-lookup, TTL 30 days, via `cache`) the earliest kline open
/// time for a symbol/timeframe.
///
/// # Errors
///
/// Returns [`AppError::UnknownSymbol`] for unknown symbols,
/// [`AppError::Api`] for other Binance API failures, and [`AppError::Date`]
/// for unparseable timestamps. A corrupt cache entry is treated as a cache
/// miss (warn + refetch), never an error.
pub async fn get_first_kline_date(
    client: &reqwest::Client,
    cache: &Cache,
    symbol: &str,
    timeframe: &str,
) -> Result<chrono::NaiveDate, AppError> {
    let key = format!("{symbol}:{timeframe}");
    if let Some(cached) = cache.read(CACHE_DOMAIN, &key, Some(CACHE_TTL_SECS)) {
        match parse_cached_date(&cached) {
            Some(date) => return Ok(date),
            None => tracing::warn!(
                symbol,
                timeframe,
                "ignoring unreadable cached start date, refetching",
            ),
        }
    }
    let date = fetch_first_kline(client, FAPI_BASE, symbol, timeframe).await?;
    if let Err(e) = cache.write(
        CACHE_DOMAIN,
        &key,
        date.format("%Y-%m-%d").to_string().as_bytes(),
    ) {
        tracing::warn!(symbol, timeframe, "failed to cache start date: {e}");
    }
    Ok(date)
}

/// One retried GET against `{base}/fapi/v1/klines`; `base` is injectable so
/// tests can target a mock server.
async fn fetch_first_kline(
    client: &reqwest::Client,
    base: &str,
    symbol: &str,
    timeframe: &str,
) -> Result<chrono::NaiveDate, AppError> {
    let url = klines_url(base, symbol, timeframe);

    let deadline = std::time::Instant::now() + RETRY_WINDOW;
    let op = || {
        let c = client.clone();
        let u = url.clone();
        async move {
            // Hard stop: `Retry-After` overrides bypass backoff's own
            // `max_elapsed_time` (see [`RETRY_WINDOW`]); the window is
            // enforced here as well, and the override itself is clamped to
            // the remaining window via `clamp_retry_after`.
            if std::time::Instant::now() >= deadline {
                return Err(BackoffError::Permanent(AppError::Api(format!(
                    "retry window elapsed for {u}"
                ))));
            }
            let r = match c.get(&u).send().await {
                Ok(r) => r,
                Err(e) => {
                    return Err(BackoffError::Transient {
                        err: AppError::from(e),
                        retry_after: None,
                    });
                }
            };
            let status = r.status();
            if !status.is_success() {
                return if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    || status.is_server_error()
                {
                    Err(BackoffError::Transient {
                        err: AppError::Api(format!("HTTP {status} for {u}")),
                        retry_after: clamp_retry_after(parse_retry_after(&r), deadline),
                    })
                } else {
                    // Binance returns error objects with 4xx statuses, e.g.
                    // `{"code":-1121,"msg":"Invalid symbol."}`; surface
                    // unknown symbols distinctly so callers can tell a typo
                    // from a transient outage.
                    let code = r
                        .text()
                        .await
                        .ok()
                        .and_then(|body| serde_json::from_str::<serde_json::Value>(&body).ok())
                        .and_then(|v| v.get("code").and_then(serde_json::Value::as_i64));
                    if code == Some(-1121) {
                        Err(BackoffError::Permanent(AppError::UnknownSymbol {
                            symbol: symbol.to_string(),
                        }))
                    } else {
                        Err(BackoffError::Permanent(AppError::Api(format!(
                            "HTTP {status} for {u}"
                        ))))
                    }
                };
            }
            let body = match r.text().await {
                Ok(body) => body,
                Err(e) => {
                    return Err(BackoffError::Transient {
                        err: AppError::from(e),
                        retry_after: None,
                    });
                }
            };
            let json: serde_json::Value = match serde_json::from_str(&body) {
                Ok(json) => json,
                Err(e) => return Err(BackoffError::Permanent(AppError::Api(e.to_string()))),
            };
            let Some(klines) = json.as_array() else {
                // Binance reports failures (e.g. an unknown symbol) as an
                // object like `{"code": -1121, "msg": "Invalid symbol."}`.
                let code = json.get("code").and_then(serde_json::Value::as_i64);
                if code == Some(-1121) {
                    return Err(BackoffError::Permanent(AppError::UnknownSymbol {
                        symbol: symbol.to_string(),
                    }));
                }
                let msg = json
                    .get("msg")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unexpected response shape");
                return Err(BackoffError::Permanent(AppError::Api(match code {
                    Some(code) => format!("api error {code}: {msg} for {u}"),
                    None => format!("{msg} for {u}"),
                })));
            };
            let Some(first) = klines.first().and_then(serde_json::Value::as_array) else {
                return Err(BackoffError::Permanent(AppError::Api(format!(
                    "no klines for {u}"
                ))));
            };
            let Some(open_time) = first.first().and_then(serde_json::Value::as_u64) else {
                return Err(BackoffError::Permanent(AppError::Api(format!(
                    "open time missing for {u}"
                ))));
            };
            let Ok(ms) = i64::try_from(open_time) else {
                return Err(BackoffError::Permanent(AppError::Date(format!(
                    "timestamp out of range: {open_time}"
                ))));
            };
            let Some(dt) = chrono::DateTime::from_timestamp_millis(ms) else {
                return Err(BackoffError::Permanent(AppError::Date(format!(
                    "invalid timestamp: {ms}"
                ))));
            };
            Ok(dt.date_naive())
        }
    };

    retry(default_backoff(), op).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// A well-formed `fapi` klines response whose first open time is
    /// 2024-01-01T00:00:00Z.
    const KLINE_2024_01_01: &str = r#"[[1704067200000,"1.0","2.0","0.5","1.5","100.0",1704070799999,"150.0",42,"60.0","90.0","0"]]"#;

    #[test]
    fn klines_url_format() {
        let url = klines_url(FAPI_BASE, "BTCUSDT", "1m");
        assert!(url.contains("BTCUSDT"));
        assert!(url.contains("interval=1m"));
        assert!(url.contains("startTime=0"));
        assert!(url.contains("limit=1"));
    }

    #[test]
    fn klines_url_different_symbol_timeframe() {
        let url = klines_url(FAPI_BASE, "ETHUSDT", "5m");
        assert!(url.contains("ETHUSDT"));
        assert!(url.contains("interval=5m"));
    }

    #[test]
    fn klines_url_start_time_zero() {
        let url = klines_url(FAPI_BASE, "BTCUSDT", "1d");
        assert!(url.contains("startTime=0"));
    }

    #[tokio::test]
    async fn fetch_first_kline_success() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(KLINE_2024_01_01))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let date = fetch_first_kline(&client, &server.uri(), "BTCUSDT", "1m")
            .await
            .unwrap();
        assert_eq!(date, chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
    }

    #[tokio::test]
    async fn fetch_first_kline_unknown_symbol_is_distinct() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_string(r#"{"code":-1121,"msg":"Invalid symbol."}"#),
            )
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let err = fetch_first_kline(&client, &server.uri(), "NOPEUSDT", "1m")
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::UnknownSymbol { .. }));
    }

    #[tokio::test]
    async fn fetch_first_kline_retries_on_429_then_succeeds() {
        #[derive(Default)]
        struct First429Then200 {
            calls: std::sync::atomic::AtomicUsize,
        }
        impl wiremock::Respond for First429Then200 {
            fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
                if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(429).insert_header("Retry-After", "0")
                } else {
                    ResponseTemplate::new(200).set_body_string(KLINE_2024_01_01)
                }
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(First429Then200::default())
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let date = fetch_first_kline(&client, &server.uri(), "BTCUSDT", "1m")
            .await
            .unwrap();
        assert_eq!(date, chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
    }

    #[tokio::test]
    async fn fetch_first_kline_bad_json_is_api_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let err = fetch_first_kline(&client, &server.uri(), "BTCUSDT", "1m")
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::Api(_)));
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

    #[test]
    fn parse_cached_date_round_trip() {
        assert_eq!(
            parse_cached_date(b"2024-01-15"),
            Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap())
        );
    }

    #[test]
    fn parse_cached_date_rejects_corrupt_entries() {
        assert_eq!(parse_cached_date(b"not-a-date"), None);
        assert_eq!(parse_cached_date(&[0xff, 0xfe]), None);
    }
}
