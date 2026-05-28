use super::start_date::StartDateMap;
use super::start_date::start_date_key;
use crate::config::Config;

pub struct DownloadUrl {
    pub url: String,
    pub rel_path: String,
    pub pair: String,
    pub timeframe: String,
    pub date_str: String,
}

pub fn build_urls(
    symbols: &[String],
    timeframes: &[String],
    config: &Config,
    start_dates: &StartDateMap,
    end_date: chrono::NaiveDate,
) -> Vec<DownloadUrl> {
    let config_sd = config
        .start_date
        .as_ref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
    let one_year_ago = chrono::Local::now().date_naive() - chrono::Duration::days(365);
    let estimated_per_sym = config.market_type.len() * timeframes.len() * 366;
    let mut result = Vec::with_capacity(symbols.len() * estimated_per_sym);

    for sym in symbols {
        for &mt in &config.market_type {
            let prefix = mt.rel_path_prefix();
            let key = start_date_key(mt, sym);
            let sd = config_sd
                .or_else(|| start_dates.get(&key).copied())
                .unwrap_or(one_year_ago);
            for tf in timeframes {
                let mut d = sd;
                while d <= end_date {
                    let ds = d.format("%Y-%m-%d").to_string();
                    let filename = format!("{sym}-{tf}-{ds}.zip");
                    let rel = format!("{prefix}/{sym}/{tf}/{filename}");
                    let url = format!("https://data.binance.vision/data/{rel}");
                    result.push(DownloadUrl {
                        url,
                        rel_path: rel,
                        pair: sym.clone(),
                        timeframe: tf.clone(),
                        date_str: ds,
                    });
                    d += chrono::Duration::days(1);
                }
            }
        }
    }
    result
}
