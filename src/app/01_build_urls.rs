/// A download URL with its relative storage path and metadata.
pub struct DownloadUrl {
    pub url: String,
    pub rel_path: String,
    pub pair: String,
    pub timeframe: String,
}

/// Server-side path on data.binance.vision (fixed by the exchange).
const SERVER_PREFIX: &str = "data/futures/um/daily/klines";
/// Local storage root below output_dir (our choice).
const LOCAL_PREFIX: &str = "binance/data/um/daily/klines";

/// Generate daily kline download URLs for a date range.
pub fn build_urls(
    pair: &str,
    tf: &str,
    date_start: chrono::NaiveDate,
    date_end: chrono::NaiveDate,
) -> Vec<DownloadUrl> {
    if date_start > date_end {
        return Vec::new();
    }
    let n_days = ((date_end - date_start).num_days() as usize).max(1);
    let mut result = Vec::with_capacity(n_days);

    let mut cursor = date_start;
    while cursor <= date_end {
        let date_str = cursor.format("%Y-%m-%d").to_string();
        let filename = format!("{pair}-{tf}-{date_str}.zip");
        let server_rel = format!("{SERVER_PREFIX}/{pair}/{tf}/{filename}");
        let local_rel = format!("{LOCAL_PREFIX}/{pair}/{tf}/{filename}");
        let url = format!("https://data.binance.vision/{server_rel}");
        result.push(DownloadUrl {
            url,
            rel_path: local_rel,
            pair: pair.to_string(),
            timeframe: tf.to_string(),
        });
        cursor += chrono::Duration::days(1);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn build_urls_single_day() {
        let urls = build_urls(
            "BTCUSDT",
            "1m",
            chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        );
        assert_eq!(urls.len(), 1);
        assert!(urls[0].url.contains("BTCUSDT"));
        assert!(urls[0].url.contains("2024-01-02"));
        assert!(urls[0].url.starts_with("https://data.binance.vision/data/"));
        assert!(urls[0].url.ends_with(".zip"));
        assert!(urls[0].url.contains("data/futures/um/daily/klines/"));
        assert!(
            urls[0]
                .rel_path
                .starts_with("binance/data/um/daily/klines/")
        );
    }

    #[test]
    fn build_urls_multi_day() {
        let urls = build_urls(
            "BTCUSDT",
            "1m",
            chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        );
        assert_eq!(urls.len(), 3);
        assert!(urls[0].url.contains("2024-01-01"));
        assert!(urls[1].url.contains("2024-01-02"));
        assert!(urls[2].url.contains("2024-01-03"));
    }

    #[test]
    fn build_urls_rel_path_format() {
        let urls = build_urls(
            "ETHUSDT",
            "5m",
            chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
        );
        assert_eq!(
            urls[0].rel_path,
            "binance/data/um/daily/klines/ETHUSDT/5m/ETHUSDT-5m-2024-06-15.zip"
        );
    }

    #[test]
    fn build_urls_empty_range() {
        assert!(build_urls("BTCUSDT", "1m", date(2024, 6, 1), date(2024, 5, 31),).is_empty());
    }
}
