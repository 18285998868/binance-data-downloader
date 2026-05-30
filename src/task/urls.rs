use crate::config::Config;
/// Generates Binance data download URLs within a date range.
///
/// Produces the Cartesian product of (symbol × market_type × timeframe × date),
/// then filters each symbol's date range by its cached start_date (or config override).
use crate::config::start_date::{StartDateMap, start_date_key};

/// A single download URL with metadata for bookkeeping.
pub struct DownloadUrl {
    pub url: String,
    pub rel_path: String,
    pub pair: String,
    pub timeframe: String,
}

/// Builds all download URLs for a symbol batch within `[chunk_start, chunk_end]`.
///
/// Each symbol's start date is the max of its cached start_date and `chunk_start`.
/// The end date is the min of `chunk_end` and the config's end_date.
#[allow(clippy::too_many_arguments)]
pub fn build_urls(
    symbols: &[String],
    timeframes: &[String],
    config: &Config,
    start_dates: &StartDateMap,
    chunk_start: chrono::NaiveDate,
    chunk_end: chrono::NaiveDate,
    config_sd: Option<chrono::NaiveDate>,
    config_ed: Option<chrono::NaiveDate>,
    one_year_ago: chrono::NaiveDate,
) -> Vec<DownloadUrl> {
    let max_dates_per_sym = config.market_type.len()
        * timeframes.len()
        * ((chunk_end - chunk_start).num_days() as usize).max(1);
    let mut result = Vec::with_capacity(symbols.len() * max_dates_per_sym);

    for sym in symbols {
        for &mt in &config.market_type {
            let prefix = mt.rel_path_prefix();
            let key = start_date_key(mt, sym);
            let sym_start = config_sd
                .or_else(|| start_dates.get(&key).copied())
                .unwrap_or(one_year_ago);
            let date_start = sym_start.max(chunk_start);
            let date_end = config_ed.map(|ed| ed.min(chunk_end)).unwrap_or(chunk_end);

            let mut cursor = date_start;
            while cursor <= date_end {
                let date_str = cursor.format("%Y-%m-%d").to_string();
                for tf in timeframes {
                    let filename = format!("{sym}-{tf}-{date_str}.zip");
                    let rel = format!("{prefix}/{sym}/{tf}/{filename}");
                    let url = format!("https://data.binance.vision/data/{rel}");
                    result.push(DownloadUrl {
                        url,
                        rel_path: rel,
                        pair: sym.clone(),
                        timeframe: tf.clone(),
                    });
                }
                cursor += chrono::Duration::days(1);
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MarketType;
    use std::collections::HashMap;

    fn test_config() -> Config {
        Config {
            market_type: vec![MarketType::Futures],
            timeframes: Some(vec!["1m".into(), "5m".into()]),
            start_date: Some("2024-01-01".into()),
            end_date: Some("2024-01-03".into()),
            data_dir: "./data".into(),
            ..Default::default()
        }
    }

    #[test]
    fn build_urls_within_chunk() {
        let config = test_config();
        let start_dates = HashMap::new();
        let symbols = vec!["BTCUSDT".into()];
        let timeframes = config.timeframes.as_deref().unwrap_or(&[]);

        let urls = build_urls(
            &symbols,
            timeframes,
            &config,
            &start_dates,
            chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 3).unwrap()),
            chrono::NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(),
        );

        // 1 symbol x 1 type x 2 tfs x 1 day = 2 URLs
        assert_eq!(urls.len(), 2);
        assert!(urls[0].url.contains("BTCUSDT"));
        assert!(urls[0].url.contains("2024-01-02"));
    }

    #[test]
    fn build_urls_chunk_respects_config_end_date() {
        let config = test_config();
        let start_dates = HashMap::new();
        let symbols = vec!["BTCUSDT".into()];
        let timeframes = config.timeframes.as_deref().unwrap_or(&[]);

        let urls = build_urls(
            &symbols,
            timeframes,
            &config,
            &start_dates,
            chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 10).unwrap(), // beyond config end_date
            Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 3).unwrap()),
            chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
        );

        // Should be capped at config.end_date = 2024-01-03
        // 1 × 1 × 2 × 3 days = 6 URLs
        assert_eq!(urls.len(), 6);
        assert!(!urls.iter().any(|u| u.url.contains("2024-01-04")));
    }

    #[test]
    fn build_urls_empty_symbols() {
        let config = test_config();
        let urls = build_urls(
            &[],
            &["1m".into()],
            &config,
            &HashMap::new(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 3).unwrap()),
            chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
        );
        assert!(urls.is_empty());
    }
}
