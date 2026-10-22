//! The archive's own bucket listing: when did a symbol/timeframe start?
//!
//! `data.binance.vision` is an S3 bucket, and the bucket listing answers
//! "which files exist?" directly. The first key under a symbol/timeframe
//! prefix is therefore the **first day the archive actually has data for** —
//! the exact bound the downloader needs, and the only way to avoid requesting
//! days that were never published.
//!
//! Why this matters: the `fapi` API reports the symbol's *first kline* date,
//! which for the oldest symbols predates the archive by months (BTCUSDT's first
//! kline is 2019-09-08, while its first archive file — of *every* timeframe —
//! is 2019-12-31). Starting from the API's date therefore requests ~114 days
//! per symbol that are guaranteed to 404.
//!
//! An empty listing is **not** an error: it means the archive holds no file
//! for that symbol/timeframe *yet* (a freshly listed symbol), which is a
//! normal state and not the same as "the symbol does not exist".

use std::collections::HashMap;

use futures::StreamExt;

use crate::AppError;
use crate::backoff::{
    RETRY_WINDOW, RetryError, default_retry_policy, is_transient_status, parse_retry_after,
    retry_operation,
};
use crate::cache::Cache;
use crate::config::Timeframe;

/// Prefix of the USDⓈ-M daily klines inside the archive bucket.
pub(crate) const KLINES_PREFIX: &str = "data/futures/um/daily/klines";

/// The bucket's public endpoint. Kept as a parameter elsewhere so tests can
/// point the whole pipeline at a mock server.
pub(crate) const S3_BASE: &str = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision";

/// Cache domain of the archive-derived start dates.
const CACHE_DOMAIN: &str = "archive_start";

/// How many listing requests one symbol issues at a time.
const FETCH_CONCURRENCY: usize = 4;

/// Build the bucket-listing URL for one symbol/timeframe.
///
/// `max-keys=1` is deliberate: S3 lists keys in binary order, so the single
/// returned key is the *earliest* file. One request, ~650 bytes of XML.
fn listing_url(base: &str, symbol: &str, timeframe: &str) -> String {
    format!("{base}?delimiter=/&max-keys=1&prefix={KLINES_PREFIX}/{symbol}/{timeframe}/")
}

/// The first day the archive holds a file for `symbol`/`timeframe`.
///
/// Returns `Ok(None)` when the archive holds no file for it yet.
///
/// # Errors
///
/// Returns [`AppError::Api`] when the listing request fails or answers with a
/// non-success status, and [`AppError::Data`] when the response is not a
/// parseable listing.
pub(crate) async fn first_archive_date(
    client: &reqwest::Client,
    base: &str,
    symbol: &str,
    timeframe: &str,
) -> Result<Option<chrono::NaiveDate>, AppError> {
    let url = listing_url(base, symbol, timeframe);
    let deadline = std::time::Instant::now() + RETRY_WINDOW;
    let op = || {
        let c = client.clone();
        let u = url.clone();
        async move {
            // The shared retry loop stops scheduling after the deadline; this
            // guard also avoids starting an attempt that was queued just before
            // the cutoff.
            if std::time::Instant::now() >= deadline {
                return Err(RetryError::permanent(AppError::Api {
                    status: None,
                    message: format!("retry window elapsed for {u}"),
                }));
            }
            let resp = match c.get(&u).send().await {
                Ok(r) => r,
                Err(e) => {
                    return Err(RetryError::transient(AppError::from(e), None));
                }
            };
            let status = resp.status();
            if status != reqwest::StatusCode::OK {
                let err = AppError::Api {
                    status: Some(status.as_u16()),
                    message: format!("HTTP {status} for {u}"),
                };
                return Err(if is_transient_status(status) {
                    RetryError::transient(err, parse_retry_after(&resp))
                } else {
                    RetryError::permanent(err)
                });
            }
            match resp.text().await {
                Ok(body) => Ok(body),
                Err(e) => Err(RetryError::transient(AppError::from(e), None)),
            }
        }
    };

    let body = retry_operation(default_retry_policy(), deadline, op).await?;
    parse_first_key_date(&body).map_err(|e| {
        AppError::Data(crate::error::FailureReport::new(format!(
            "archive listing is unparseable: {e}"
        )))
    })
}

/// Extract the date of the first `<Key>` of an S3 listing.
///
/// `Ok(None)` means the listing holds no key at all.
fn parse_first_key_date(body: &str) -> Result<Option<chrono::NaiveDate>, String> {
    let doc = roxmltree::Document::parse(body).map_err(|e| e.to_string())?;
    let Some(key) = doc
        .descendants()
        .find(|node| node.tag_name().name() == "Key")
        .and_then(|node| node.text())
    else {
        return Ok(None);
    };
    date_from_archive_key(key).map(Some)
}

/// Extract `YYYY-MM-DD` from an archive key such as
/// `data/.../BTCUSDT_250926-1d-2024-01-01.zip`.
///
/// The symbol charset is `[A-Za-z0-9_]` (validated in the config), so the date
/// is always the last three dash-separated components of the file name.
fn date_from_archive_key(key: &str) -> Result<chrono::NaiveDate, String> {
    let name = key.rsplit('/').next().unwrap_or(key);
    let stem = name
        .strip_suffix(".zip")
        .ok_or_else(|| format!("not a zip key: {key}"))?;
    let mut parts = stem.rsplit('-');
    let mut next = |what: &str| -> Result<&str, String> {
        parts
            .next()
            .ok_or_else(|| format!("no {what} in archive key: {key}"))
    };
    let day = next("day")?;
    let month = next("month")?;
    let year = next("year")?;
    let date = format!("{year}-{month}-{day}");
    chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d")
        .map_err(|e| format!("bad date in archive key {key}: {e}"))
}

/// The first day the archive holds a file, per requested timeframe.
///
/// One lookup per timeframe, each cached for 30 days. `None` means the
/// archive holds no file for that timeframe *yet* — a normal state for a
/// freshly listed symbol, and not the same as "the symbol does not exist".
pub(crate) async fn archive_dates(
    client: &reqwest::Client,
    cache: &Cache,
    s3_base: &str,
    symbol: &str,
    timeframes: &[Timeframe],
) -> HashMap<Timeframe, Option<chrono::NaiveDate>> {
    let mut pending = futures::stream::iter(timeframes.iter().copied())
        .map(|tf| {
            let c = client.clone();
            let cache = cache.clone();
            let base = s3_base.to_owned();
            let sym = symbol.to_owned();
            async move {
                (
                    tf,
                    cached_first_archive_date(&c, &cache, &base, &sym, tf).await,
                )
            }
        })
        .buffer_unordered(FETCH_CONCURRENCY);

    let mut dates = HashMap::with_capacity(timeframes.len());
    let mut to_cache: Vec<(String, Vec<u8>)> = Vec::new();
    while let Some((timeframe, looked_up)) = pending.next().await {
        dates.insert(timeframe, looked_up.date);
        if let Some(entry) = looked_up.pending {
            to_cache.push(entry);
        }
    }

    // One transaction for the whole symbol: a fresh run resolves every
    // timeframe, and one fsync per entry would dominate the pre-flight.
    if !to_cache.is_empty()
        && let Err(e) = cache.write_many_async(CACHE_DOMAIN, &to_cache).await
    {
        tracing::warn!(symbol, error = ?e, "failed to cache archive dates");
    }
    dates
}

/// A looked-up archive date, plus the cache entry to store when it was not
/// cached yet.
struct ArchiveDate {
    /// The first day the archive holds a file, when it holds one.
    date: Option<chrono::NaiveDate>,
    /// The entry to store, when the value was freshly fetched.
    pending: Option<(String, Vec<u8>)>,
}

/// The first archive date of one symbol/timeframe, cached for 30 days.
///
/// `date` is `None` when the archive holds no file yet **or** when the listing
/// could not be read. The former is deliberately *not* cached: the archive
/// publishes daily, so tomorrow the same symbol may well have files.
async fn cached_first_archive_date(
    client: &reqwest::Client,
    cache: &Cache,
    s3_base: &str,
    symbol: &str,
    timeframe: Timeframe,
) -> ArchiveDate {
    let key = format!("{symbol}:{timeframe}");
    match cache
        .read_async(CACHE_DOMAIN, &key, Some(crate::cache::CACHE_TTL_SECS))
        .await
    {
        Ok(Some(raw)) => {
            if let Some(date) = parse_cached_date(&raw) {
                return ArchiveDate {
                    date: Some(date),
                    pending: None,
                };
            }
            tracing::warn!(
                symbol,
                timeframe = %timeframe,
                "ignoring unreadable cached archive date, refetching",
            );
        }
        Ok(None) => {}
        Err(e) => tracing::warn!(
            symbol,
            timeframe = %timeframe,
            error = ?e,
            "archive-date cache read failed, treating as miss",
        ),
    }

    match first_archive_date(client, s3_base, symbol, timeframe.as_str()).await {
        Ok(Some(date)) => {
            let payload = date.format("%Y-%m-%d").to_string().into_bytes();
            ArchiveDate {
                date: Some(date),
                pending: Some((key, payload)),
            }
        }
        Ok(None) => ArchiveDate {
            date: None,
            pending: None,
        },
        Err(e) => {
            tracing::warn!(
                symbol,
                timeframe = %timeframe,
                error = ?e,
                "archive listing failed; keeping the API-derived start date",
            );
            ArchiveDate {
                date: None,
                pending: None,
            }
        }
    }
}

/// Parse a cached `YYYY-MM-DD` date. `None` for entries that are not valid
/// UTF-8 or not a date; the caller then treats them as a miss.
fn parse_cached_date(raw: &[u8]) -> Option<chrono::NaiveDate> {
    let s = std::str::from_utf8(raw).ok()?;
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// A listing holding one key, as S3 answers `max-keys=1`.
    fn listing(key: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>data.binance.vision</Name>
<Prefix>data/futures/um/daily/klines/BTCUSDT/1d/</Prefix>
<MaxKeys>1</MaxKeys>
<Delimiter>/</Delimiter>
<IsTruncated>true</IsTruncated>
<Contents><Key>{key}</Key><LastModified>2022-03-21T08:57:54.000Z</LastModified></Contents>
</ListBucketResult>"#
        )
    }

    /// A listing with no key at all (the prefix holds nothing).
    const EMPTY_LISTING: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>data.binance.vision</Name>
<Prefix>data/futures/um/daily/klines/NEWUSDT/1d/</Prefix>
<MaxKeys>1</MaxKeys>
<IsTruncated>false</IsTruncated>
</ListBucketResult>"#;

    #[test]
    fn listing_url_encodes_the_prefix_and_caps_the_page() {
        let url = listing_url(S3_BASE, "BTCUSDT", "1d");
        assert!(url.starts_with(S3_BASE));
        assert!(url.contains("prefix=data/futures/um/daily/klines/BTCUSDT/1d/"));
        // `max-keys=1` is what keeps the listing to a single, earliest key.
        assert!(url.contains("max-keys=1"));
    }

    #[test]
    fn parses_the_date_of_the_first_key() {
        let date = parse_first_key_date(&listing(
            "data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2019-12-31.zip",
        ))
        .unwrap()
        .unwrap();
        assert_eq!(date, chrono::NaiveDate::from_ymd_opt(2019, 12, 31).unwrap());
    }

    #[test]
    fn an_empty_listing_yields_no_date() {
        assert_eq!(parse_first_key_date(EMPTY_LISTING).unwrap(), None);
    }

    #[test]
    fn a_malformed_body_is_an_error() {
        assert!(parse_first_key_date("not xml at all").is_err());
    }

    #[test]
    fn a_key_without_a_zip_suffix_is_an_error() {
        assert!(date_from_archive_key("data/x/BTCUSDT-1d-2019-12-31.txt").is_err());
    }

    #[test]
    fn a_key_with_a_non_date_tail_is_an_error() {
        assert!(date_from_archive_key("data/x/BTCUSDT-1d-nope.zip").is_err());
    }

    #[test]
    fn quarterly_contract_symbols_are_parsed() {
        // The symbol charset allows `_`, so the date is still the last three
        // dash-separated components.
        let date = date_from_archive_key(
            "data/futures/um/daily/klines/BTCUSDT_250926/1d/BTCUSDT_250926-1d-2024-09-26.zip",
        )
        .unwrap();
        assert_eq!(date, chrono::NaiveDate::from_ymd_opt(2024, 9, 26).unwrap());
    }

    #[tokio::test]
    async fn first_archive_date_reads_the_listing() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(listing(
                "data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2019-12-31.zip",
            )))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let date = first_archive_date(&client, &server.uri(), "BTCUSDT", "1d")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(date, chrono::NaiveDate::from_ymd_opt(2019, 12, 31).unwrap());
    }

    #[tokio::test]
    async fn first_archive_date_reports_an_empty_listing_as_none() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(EMPTY_LISTING))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        assert_eq!(
            first_archive_date(&client, &server.uri(), "NEWUSDT", "1d")
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn first_archive_date_reports_a_server_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let err = first_archive_date(&client, &server.uri(), "BTCUSDT", "1d")
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::Api { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn first_archive_date_retries_429_then_succeeds() {
        /// Answers 429 (+ `Retry-After`) on the first call, then a listing.
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
                    ResponseTemplate::new(200).set_body_string(listing(
                        "data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2019-12-31.zip",
                    ))
                }
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(First429Then200 {
                calls: std::sync::atomic::AtomicUsize::new(0),
            })
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let date = first_archive_date(&client, &server.uri(), "BTCUSDT", "1d")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(date, chrono::NaiveDate::from_ymd_opt(2019, 12, 31).unwrap());
    }
}
