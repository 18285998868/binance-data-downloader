//! The archive's own bucket listing: when did a symbol/job start?
//!
//! `data.binance.vision` is an S3 bucket, and the bucket listing answers
//! "which files exist?" directly. The first key under a symbol/job prefix is
//! therefore the **first day the archive actually has data for** — the exact
//! bound the downloader needs, and the only way to avoid requesting days that
//! were never published.
//!
//! Why this matters: the exchange API reports the symbol's *first kline* date,
//! which for the oldest symbols predates the archive by months (BTCUSDT's first
//! kline is 2019-09-08, while its first archive file — of *every* timeframe —
//! is 2019-12-31). Starting from the API's date therefore requests ~114 days
//! per symbol that are guaranteed to 404.
//!
//! An empty listing is **not** an error: it means the archive holds no file
//! for that symbol/job *yet* (a freshly listed symbol), which is a normal state
//! and not the same as "the symbol does not exist".

use std::collections::HashMap;

use futures::StreamExt;

use crate::AppError;
use crate::backoff::{
    RETRY_WINDOW, RetryError, default_retry_policy, is_transient_status, parse_retry_after,
    retry_operation,
};
use crate::cache::Cache;
use crate::dataset::{DownloadJob, Market, Period};

/// Cache domain of the archive-derived start dates.
const CACHE_DOMAIN: &str = "archive_start";

/// How many listing requests one symbol issues at a time.
const FETCH_CONCURRENCY: usize = 4;

/// Build the bucket-listing URL for one symbol/job prefix.
///
/// `max-keys=1` is deliberate: S3 lists keys in binary order, so the single
/// returned key is the *earliest* file. One request, ~650 bytes of XML.
fn listing_url(base: &str, prefix: &str) -> String {
    format!("{base}?delimiter=/&max-keys=1&prefix={prefix}")
}

/// The first day the archive holds a file under `prefix`.
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
    prefix: &str,
    period: Period,
) -> Result<Option<chrono::NaiveDate>, AppError> {
    let url = listing_url(base, prefix);
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
    parse_first_key_date(&body, period).map_err(|e| {
        AppError::Data(crate::error::FailureReport::new(format!(
            "archive listing is unparseable: {e}"
        )))
    })
}

/// Extract the date of the first `<Key>` of an S3 listing.
///
/// `Ok(None)` means the listing holds no key at all.
fn parse_first_key_date(body: &str, period: Period) -> Result<Option<chrono::NaiveDate>, String> {
    let doc = roxmltree::Document::parse(body).map_err(|e| e.to_string())?;
    let Some(key) = doc
        .descendants()
        .find(|node| node.tag_name().name() == "Key")
        .and_then(|node| node.text())
    else {
        return Ok(None);
    };
    date_from_archive_key(key, period).map(Some)
}

/// Extract the date from an archive key such as
/// `data/.../BTCUSDT_250926-1d-2024-01-01.zip` (daily) or
/// `data/.../BTCUSDT-fundingRate-2024-01.zip` (monthly).
///
/// The symbol charset is `[A-Za-z0-9_]` (validated in the config), so the date
/// is always the last three (daily) or two (monthly) dash-separated components.
fn date_from_archive_key(key: &str, period: Period) -> Result<chrono::NaiveDate, String> {
    let name = key.rsplit('/').next().unwrap_or(key);
    let stem = name
        .strip_suffix(".zip")
        .ok_or_else(|| format!("not a zip key: {key}"))?;
    period
        .parse_key_date(stem)
        .map_err(|e| format!("bad date in archive key {key}: {e}"))
}

/// The first day the archive holds a file, per requested job.
///
/// One lookup per job, each cached for 30 days. `None` means the archive holds
/// no file for that job *yet* — a normal state for a freshly listed symbol, and
/// not the same as "the symbol does not exist".
pub(crate) async fn archive_dates(
    client: &reqwest::Client,
    cache: &Cache,
    s3_base: &str,
    market: Market,
    symbol: &str,
    jobs: &[DownloadJob],
) -> HashMap<DownloadJob, Option<chrono::NaiveDate>> {
    let mut pending = futures::stream::iter(jobs.iter().copied())
        .map(|job| {
            let c = client.clone();
            let cache = cache.clone();
            let base = s3_base.to_owned();
            let sym = symbol.to_owned();
            async move {
                (
                    job,
                    cached_first_archive_date(&c, &cache, &base, market, &sym, job).await,
                )
            }
        })
        .buffer_unordered(FETCH_CONCURRENCY);

    let mut dates = HashMap::with_capacity(jobs.len());
    let mut to_cache: Vec<(String, Vec<u8>)> = Vec::new();
    while let Some((job, looked_up)) = pending.next().await {
        dates.insert(job, looked_up.date);
        if let Some(entry) = looked_up.pending {
            to_cache.push(entry);
        }
    }

    // One transaction for the whole symbol: a fresh run resolves every job,
    // and one fsync per entry would dominate the pre-flight.
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

/// The first archive date of one symbol/job, cached for 30 days.
///
/// `date` is `None` when the archive holds no file yet **or** when the listing
/// could not be read. The former is deliberately *not* cached: the archive
/// publishes daily, so tomorrow the same symbol may well have files.
async fn cached_first_archive_date(
    client: &reqwest::Client,
    cache: &Cache,
    s3_base: &str,
    market: Market,
    symbol: &str,
    job: DownloadJob,
) -> ArchiveDate {
    let key = format!("{}:{job}:{symbol}", market.as_path());
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
                market = %market,
                symbol,
                job = %job,
                "ignoring unreadable cached archive date, refetching",
            );
        }
        Ok(None) => {}
        Err(e) => tracing::warn!(
            market = %market,
            symbol,
            job = %job,
            error = ?e,
            "archive-date cache read failed, treating as miss",
        ),
    }

    let prefix = format!("{}/", job.relative_dir(market, symbol));
    match first_archive_date(client, s3_base, &prefix, job.period).await {
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
                market = %market,
                symbol,
                job = %job,
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
    use crate::dataset::{DatasetKind, Interval};
    use crate::endpoints::S3_BASE;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn daily_klines() -> DownloadJob {
        DownloadJob {
            kind: DatasetKind::Klines,
            interval: Some(Interval::D1),
            period: Period::Daily,
        }
    }

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
        let prefix = "data/futures/um/daily/klines/BTCUSDT/1d/";
        let url = listing_url(S3_BASE, prefix);
        assert!(url.starts_with(S3_BASE));
        assert!(url.contains(&format!("prefix={prefix}")));
        // `max-keys=1` is what keeps the listing to a single, earliest key.
        assert!(url.contains("max-keys=1"));
    }

    #[test]
    fn parses_the_date_of_the_first_daily_key() {
        let body = listing("data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2019-12-31.zip");
        let parsed = parse_first_key_date(&body, Period::Daily).unwrap().unwrap();
        assert_eq!(parsed, date(2019, 12, 31));
    }

    #[test]
    fn parses_the_month_of_the_first_monthly_key() {
        let body =
            listing("data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-01.zip");
        let parsed = parse_first_key_date(&body, Period::Monthly)
            .unwrap()
            .unwrap();
        assert_eq!(parsed, date(2024, 1, 1));
    }

    #[test]
    fn an_empty_listing_yields_no_date() {
        assert_eq!(
            parse_first_key_date(EMPTY_LISTING, Period::Daily).unwrap(),
            None
        );
    }

    #[test]
    fn a_malformed_body_is_an_error() {
        assert!(parse_first_key_date("not xml at all", Period::Daily).is_err());
    }

    #[test]
    fn a_key_without_a_zip_suffix_is_an_error() {
        assert!(date_from_archive_key("data/x/BTCUSDT-1d-2019-12-31.txt", Period::Daily).is_err());
    }

    #[test]
    fn a_key_with_a_non_date_tail_is_an_error() {
        assert!(date_from_archive_key("data/x/BTCUSDT-1d-nope.zip", Period::Daily).is_err());
    }

    #[test]
    fn quarterly_contract_symbols_are_parsed() {
        // The symbol charset allows `_`, so the date is still the last three
        // dash-separated components.
        let parsed = date_from_archive_key(
            "data/futures/um/daily/klines/BTCUSDT_250926/1d/BTCUSDT_250926-1d-2024-09-26.zip",
            Period::Daily,
        )
        .unwrap();
        assert_eq!(parsed, date(2024, 9, 26));
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
        let parsed = first_archive_date(
            &client,
            &server.uri(),
            "data/futures/um/daily/klines/BTCUSDT/1d/",
            Period::Daily,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(parsed, date(2019, 12, 31));
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
            first_archive_date(
                &client,
                &server.uri(),
                "data/futures/um/daily/klines/NEWUSDT/1d/",
                Period::Daily,
            )
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
        let err = first_archive_date(
            &client,
            &server.uri(),
            "data/futures/um/daily/klines/BTCUSDT/1d/",
            Period::Daily,
        )
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
        let parsed = first_archive_date(
            &client,
            &server.uri(),
            "data/futures/um/daily/klines/BTCUSDT/1d/",
            Period::Daily,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(parsed, date(2019, 12, 31));
    }

    #[tokio::test]
    async fn archive_dates_keys_results_by_job() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(listing(
                "data/futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2019-12-31.zip",
            )))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = Cache::new(cache_dir.path());
        let job = daily_klines();
        let dates = archive_dates(
            &client,
            &cache,
            &server.uri(),
            Market::Um,
            "BTCUSDT",
            &[job],
        )
        .await;
        assert_eq!(dates.get(&job).copied().flatten(), Some(date(2019, 12, 31)));
    }
}
