//! Resolution and caching of a symbol's first kline date via the market API.
//!
//! The `1m` klines endpoint of the configured market is used as a symbol
//! existence check and as a lower bound for the first archive date. Spot,
//! USDⓈ-M and COIN-M use different bases and paths; the market value selects
//! them.

use crate::AppError;
use crate::backoff::{
    RETRY_WINDOW, RetryError, default_retry_policy, is_transient_status, parse_retry_after,
    retry_operation,
};
use crate::cache::Cache;
use crate::dataset::Market;

const CACHE_DOMAIN: &str = "start_date";

/// Binance error code for an unrecognised symbol.
const UNKNOWN_SYMBOL_CODE: i64 = -1121;

fn klines_url(base: &str, market: Market, symbol: &str, interval: &str) -> String {
    format!(
        "{base}{}?symbol={symbol}&interval={interval}&limit=1&startTime=0",
        market.klines_path()
    )
}

/// Binance error envelope, e.g. `{"code":-1121,"msg":"Invalid symbol."}`.
#[derive(Debug, serde::Deserialize)]
struct ApiErrorBody {
    /// Binance numeric error code.
    code: i64,
    /// Human-readable message.
    msg: String,
}

/// A single kline row: `[open_time, open, high, low, close, volume, ...]`.
///
/// Only the leading open time is needed; the remaining fields are ignored, so
/// they are captured as [`serde_json::Value`] rather than typed.
type KlineRow = Vec<serde_json::Value>;

/// Parse a cached `YYYY-MM-DD` start date. Returns `None` for entries that are
/// not valid UTF-8 or not a date; the caller then treats them as a miss.
fn parse_cached_date(raw: &[u8]) -> Option<chrono::NaiveDate> {
    let s = std::str::from_utf8(raw).ok()?;
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

/// Fetch (or cache-lookup, TTL 30 days, via `cache`) the earliest kline open
/// time for a symbol.
///
/// The cache key includes the market: the same symbol can exist on several
/// markets with different listing dates.
///
/// # Errors
///
/// Returns [`AppError::UnknownSymbol`] for unknown symbols,
/// [`AppError::Api`] for other Binance API failures, and [`AppError::Date`]
/// for unparseable timestamps. A corrupt cache entry is treated as a cache
/// miss (warn + refetch), never an error.
#[tracing::instrument(skip(client, cache), fields(market = %market, symbol = %symbol))]
pub(crate) async fn get_first_kline_date(
    client: &reqwest::Client,
    cache: &Cache,
    market: Market,
    api_base: &str,
    symbol: &str,
) -> Result<chrono::NaiveDate, AppError> {
    let key = format!("{}:{symbol}", market.as_path());
    match cache
        .read_async(CACHE_DOMAIN, &key, Some(crate::cache::CACHE_TTL_SECS))
        .await
    {
        Ok(Some(cached)) => {
            if let Some(date) = parse_cached_date(&cached) {
                return Ok(date);
            }
            tracing::warn!(
                market = %market,
                symbol,
                "ignoring unreadable cached start date, refetching",
            );
        }
        Ok(None) => {}
        Err(e) => tracing::warn!(
            market = %market,
            symbol,
            error = ?e,
            "cache read failed, treating as miss",
        ),
    }
    let date = fetch_first_kline(client, api_base, market, symbol).await?;
    if let Err(e) = cache
        .write_async(
            CACHE_DOMAIN,
            &key,
            date.format("%Y-%m-%d").to_string().as_bytes(),
        )
        .await
    {
        tracing::warn!(
            market = %market,
            symbol,
            error = ?e,
            "failed to cache start date"
        );
    }
    Ok(date)
}

/// One retried GET against the market's klines endpoint; `base` is injectable
/// so tests can target a mock server.
async fn fetch_first_kline(
    client: &reqwest::Client,
    base: &str,
    market: Market,
    symbol: &str,
) -> Result<chrono::NaiveDate, AppError> {
    let url = klines_url(base, market, symbol, "1m");

    let deadline = std::time::Instant::now() + RETRY_WINDOW;
    let op = || {
        let c = client.clone();
        let u = url.clone();
        async move {
            // Do not start another attempt after the retry window. The shared
            // retry loop caps every backon/Retry-After delay at the same
            // deadline; this check also covers a retry that became ready just
            // at the cutoff. An already in-flight request is bounded by the
            // client's 300-second whole-request timeout instead.
            if std::time::Instant::now() >= deadline {
                return Err(RetryError::permanent(AppError::Api {
                    status: None,
                    message: format!("retry window elapsed for {u}"),
                }));
            }
            let r = match c.get(&u).send().await {
                Ok(r) => r,
                Err(e) => {
                    return Err(RetryError::transient(AppError::from(e), None));
                }
            };
            let status = r.status();
            if !status.is_success() {
                return if is_transient_status(status) {
                    Err(RetryError::transient(
                        AppError::Api {
                            status: Some(status.as_u16()),
                            message: format!("HTTP {status} for {u}"),
                        },
                        parse_retry_after(&r),
                    ))
                } else {
                    // Binance returns error objects with 4xx statuses, e.g.
                    // `{"code":-1121,"msg":"Invalid symbol."}`; surface
                    // unknown symbols distinctly so callers can tell a typo
                    // from a transient outage.
                    let body = r.text().await.ok();
                    if is_unknown_symbol_body(body.as_deref()) {
                        Err(RetryError::permanent(AppError::UnknownSymbol {
                            symbol: symbol.to_string(),
                        }))
                    } else {
                        Err(RetryError::permanent(AppError::Api {
                            status: Some(status.as_u16()),
                            message: format!("HTTP {status} for {u}"),
                        }))
                    }
                };
            }
            let body = match r.text().await {
                Ok(body) => body,
                Err(e) => {
                    return Err(RetryError::transient(AppError::from(e), None));
                }
            };
            // Binance reports failures (e.g. an unknown symbol) as an object
            // like `{"code": -1121, "msg": "Invalid symbol."}` with a 200
            // status, so the error envelope is checked before the kline array.
            let klines = match serde_json::from_str::<Vec<KlineRow>>(&body) {
                Ok(rows) => rows,
                Err(parse_err) => {
                    if let Some(err) = api_error_from_body(&body, symbol) {
                        return Err(RetryError::permanent(err));
                    }
                    // Not klines and not an envelope: the body is unparseable.
                    return Err(RetryError::permanent(AppError::Json(parse_err)));
                }
            };
            let Some(row) = klines.first() else {
                return Err(RetryError::permanent(AppError::Api {
                    status: None,
                    message: format!("no klines for {u}"),
                }));
            };
            let Some(open_time) = row.first().and_then(serde_json::Value::as_u64) else {
                return Err(RetryError::permanent(AppError::Api {
                    status: None,
                    message: format!("open time missing for {u}"),
                }));
            };
            let Ok(ms) = i64::try_from(open_time) else {
                return Err(RetryError::permanent(AppError::Date(format!(
                    "timestamp out of range: {open_time}"
                ))));
            };
            let Some(dt) = chrono::DateTime::from_timestamp_millis(ms) else {
                return Err(RetryError::permanent(AppError::Date(format!(
                    "invalid timestamp: {ms}"
                ))));
            };
            Ok(dt.date_naive())
        }
    };

    retry_operation(default_retry_policy(), deadline, op).await
}

/// Whether a non-success response body is Binance's "invalid symbol" envelope.
fn is_unknown_symbol_body(body: Option<&str>) -> bool {
    body.and_then(|b| serde_json::from_str::<ApiErrorBody>(b).ok())
        .is_some_and(|e| e.code == UNKNOWN_SYMBOL_CODE)
}

/// Turn a non-kline JSON body into an error, if it is a Binance error envelope.
fn api_error_from_body(body: &str, symbol: &str) -> Option<AppError> {
    let err = serde_json::from_str::<ApiErrorBody>(body).ok()?;
    if err.code == UNKNOWN_SYMBOL_CODE {
        return Some(AppError::UnknownSymbol {
            symbol: symbol.to_owned(),
        });
    }
    Some(AppError::Api {
        status: None,
        message: format!("api error {}: {}", err.code, err.msg),
    })
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
        let url = klines_url(Market::Um.api_base(), Market::Um, "BTCUSDT", "1m");
        assert!(url.starts_with("https://fapi.binance.com/fapi/v1/klines?"));
        assert!(url.contains("BTCUSDT"));
        assert!(url.contains("interval=1m"));
        assert!(url.contains("startTime=0"));
        assert!(url.contains("limit=1"));
    }

    #[test]
    fn klines_url_uses_the_market_specific_path() {
        let spot = klines_url(Market::Spot.api_base(), Market::Spot, "BTCUSDT", "1m");
        assert!(spot.starts_with("https://api.binance.com/api/v3/klines?"));
        let cm = klines_url(Market::Cm.api_base(), Market::Cm, "BTCUSD_PERP", "1m");
        assert!(cm.starts_with("https://dapi.binance.com/dapi/v1/klines?"));
    }

    #[test]
    fn klines_url_different_symbol_timeframe() {
        let url = klines_url(Market::Um.api_base(), Market::Um, "ETHUSDT", "5m");
        assert!(url.contains("ETHUSDT"));
        assert!(url.contains("interval=5m"));
    }

    #[test]
    fn klines_url_start_time_zero() {
        let url = klines_url(Market::Um.api_base(), Market::Um, "BTCUSDT", "1d");
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
        let date = fetch_first_kline(&client, &server.uri(), Market::Um, "BTCUSDT")
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
        let err = fetch_first_kline(&client, &server.uri(), Market::Um, "NOPEUSDT")
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
                if self
                    .calls
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    == 0
                {
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
        let date = fetch_first_kline(&client, &server.uri(), Market::Um, "BTCUSDT")
            .await
            .unwrap();
        assert_eq!(date, chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
    }

    #[tokio::test]
    async fn fetch_first_kline_retries_on_408_then_succeeds() {
        #[derive(Default)]
        struct First408Then200 {
            calls: std::sync::atomic::AtomicUsize,
        }
        impl wiremock::Respond for First408Then200 {
            fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
                if self
                    .calls
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    == 0
                {
                    ResponseTemplate::new(408).insert_header("Retry-After", "0")
                } else {
                    ResponseTemplate::new(200).set_body_string(KLINE_2024_01_01)
                }
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(First408Then200::default())
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let date = fetch_first_kline(&client, &server.uri(), Market::Um, "BTCUSDT")
            .await
            .unwrap();
        assert_eq!(date, chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
    }

    #[tokio::test]
    async fn fetch_first_kline_bad_json_is_a_json_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let err = fetch_first_kline(&client, &server.uri(), Market::Um, "BTCUSDT")
            .await
            .unwrap_err();
        // An unparseable body is neither klines nor an error envelope, so it
        // surfaces through the dedicated Json variant.
        assert!(matches!(err, AppError::Json(_)), "got {err:?}");
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

    #[tokio::test]
    async fn fetch_first_kline_object_error_body_is_api_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"code":-2013,"msg":"No klines for this request."}"#),
            )
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let err = fetch_first_kline(&client, &server.uri(), Market::Um, "BTCUSDT")
            .await
            .unwrap_err();
        match err {
            AppError::Api { message, .. } => {
                assert!(message.contains("-2013"), "unexpected message: {message}");
                assert!(
                    message.contains("No klines"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected Api, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fetch_first_kline_unknown_symbol_envelope_with_status_200() {
        // Binance can answer 200 with an error object; it must still be
        // classified as UnknownSymbol rather than a generic Api error.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"code":-1121,"msg":"Invalid symbol."}"#),
            )
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let err = fetch_first_kline(&client, &server.uri(), Market::Um, "NOPEUSDT")
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::UnknownSymbol { .. }), "got {err:?}");
        assert!(err.to_string().contains("NOPEUSDT"));
    }

    #[tokio::test]
    async fn is_unknown_symbol_body_recognises_only_the_code() {
        assert!(is_unknown_symbol_body(Some(
            r#"{"code":-1121,"msg":"Invalid symbol."}"#
        )));
        assert!(!is_unknown_symbol_body(Some(
            r#"{"code":-2013,"msg":"No klines."}"#
        )));
        assert!(!is_unknown_symbol_body(Some("not json")));
        assert!(!is_unknown_symbol_body(None));
    }

    #[tokio::test]
    async fn fetch_first_kline_empty_array_is_api_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let err = fetch_first_kline(&client, &server.uri(), Market::Um, "BTCUSDT")
            .await
            .unwrap_err();
        match err {
            AppError::Api { message, .. } => assert!(message.contains("no klines")),
            other => panic!("expected Api, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fetch_first_kline_missing_open_time_is_api_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("[[]]"))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let err = fetch_first_kline(&client, &server.uri(), Market::Um, "BTCUSDT")
            .await
            .unwrap_err();
        match err {
            AppError::Api { message, .. } => assert!(message.contains("open time")),
            other => panic!("expected Api, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fetch_first_kline_retries_on_500_then_succeeds() {
        struct First500Then200 {
            calls: std::sync::atomic::AtomicUsize,
        }
        impl wiremock::Respond for First500Then200 {
            fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
                if self
                    .calls
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    == 0
                {
                    ResponseTemplate::new(503)
                } else {
                    ResponseTemplate::new(200).set_body_string(KLINE_2024_01_01)
                }
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(First500Then200 {
                calls: std::sync::atomic::AtomicUsize::new(0),
            })
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let date = fetch_first_kline(&client, &server.uri(), Market::Um, "BTCUSDT")
            .await
            .unwrap();
        assert_eq!(date, chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
    }

    #[tokio::test]
    async fn get_first_kline_date_is_served_from_cache_without_network() {
        let dir = tempfile::tempdir().unwrap();
        let cache = Cache::new(dir.path());
        cache
            .write_async(CACHE_DOMAIN, "futures/um:BTCUSDT", b"2024-01-01")
            .await
            .unwrap();

        // A client with no reachable endpoint proves the cache short-circuits:
        // any HTTP attempt would fail rather than return this date.
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_millis(50))
            .build()
            .unwrap();
        let date = get_first_kline_date(
            &client,
            &cache,
            Market::Um,
            Market::Um.api_base(),
            "BTCUSDT",
        )
        .await
        .unwrap();
        assert_eq!(date, chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
    }

    #[tokio::test]
    async fn fetch_first_kline_propagates_unknown_symbol() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_string(r#"{"code":-1121,"msg":"Invalid symbol."}"#),
            )
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let err = fetch_first_kline(&client, &server.uri(), Market::Um, "NOPEUSDT")
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::UnknownSymbol { .. }));
        assert!(err.to_string().contains("NOPEUSDT"));
    }
}
