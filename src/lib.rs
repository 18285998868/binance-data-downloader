pub mod config;
pub mod error;
pub mod download;

pub use config::load_config;
pub use error::AppError;
use config::Config;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

pub struct DownloadUrl {
    pub url: String,
    pub rel_path: String,
    pub pair: String,
    pub timeframe: String,
    pub date_str: String,
}

#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub url: String,
    pub dest: PathBuf,
    pub pair: String,
    pub timeframe: String,
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

pub async fn run(config: &Config) -> Result<(), AppError> {
    let data_dir = config.resolved_data_dir();
    let client = build_client(config)?;
    cleanup_tmp(&data_dir).await;

    // 1. Resolve symbols (config or default, cached 1 week)
    let symbols = config::default_symbols::resolve_symbols(
        &config.symbols,
        &client,
        config.market_types(),
        &data_dir,
    )
    .await?;

    // 2. Resolve timeframes (config or default)
    let timeframes = config::default_timeframes::resolve_timeframes(&config.timeframes);

    // 3. Resolve start dates per (symbol, market_type), cached 1 week
    let start_dates = config::default_start_date::resolve_start_dates(
        &symbols,
        &client,
        config.market_types(),
        &data_dir,
    )
    .await?;

    // 4. Resolve end date (config or today - 1 day)
    let end_date = config
        .resolved_end_date()
        .unwrap_or_else(|| chrono::Local::now().date_naive() - chrono::Duration::days(1));

    // 5. Scan local files once (used both for dedup and .tmp cleanup hint)
    let local = scan_local_files(&data_dir).await?;

    // 6. Cartesian product + dedup + task build — one streaming pass
    //
    //    Instead of building an intermediate Vec<DownloadUrl> for the full
    //    product (could be millions of entries), we generate URLs lazily
    //    with an iterator chain, filter against the local file set, and
    //    collect directly into Vec<DownloadTask>.
    let tasks: Vec<DownloadTask> = urls(&symbols, &timeframes, config, &start_dates, end_date)
        .filter(|dl| {
            let no_ext = dl.rel_path.trim_end_matches(".zip");
            !local.contains(no_ext)
        })
        .map(|u| DownloadTask {
            url: u.url,
            dest: data_dir.join(&u.rel_path),
            pair: u.pair,
            timeframe: u.timeframe,
        })
        .collect();

    // 7. Execute downloads
    if !tasks.is_empty() {
        log::info!("downloading {} files", tasks.len());
        download::run_downloads(&client, tasks, config.resolved_max_concurrent()).await?;
    } else {
        log::info!("all files already downloaded");
    }

    // 8. Final temp-file cleanup (catches orphan .parquet.tmp from crashes)
    cleanup_tmp(&data_dir).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Cartesian-product URL generator (iterator to avoid allocating all URLs)
// ---------------------------------------------------------------------------

fn urls<'a>(
    symbols: &'a [String],
    timeframes: &'a [String],
    config: &'a Config,
    start_dates: &'a config::default_start_date::StartDateMap,
    end_date: chrono::NaiveDate,
) -> impl Iterator<Item = DownloadUrl> + 'a {
    let config_sd = config.resolved_start_date();
    symbols
        .iter()
        .flat_map(move |sym| {
            let sym_path = strip_perp(sym);
            config.market_types().iter().copied().flat_map(move |mt| {
                let prefix = mt.rel_path_prefix();
                let key = config::default_start_date::start_date_key(mt, sym);
                let first = start_dates
                    .get(&key)
                    .copied()
                    .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(2017, 1, 1).unwrap());
                let sd = config_sd.unwrap_or(first);
                timeframes.iter().flat_map(move |tf| {
                    let mut d = sd;
                    std::iter::from_fn(move || {
                        if d > end_date {
                            return None;
                        }
                        let ds = d.format("%Y-%m-%d").to_string();
                        let filename = format!("{sym}-{tf}-{ds}.zip");
                        let rel = format!("{prefix}/{sym_path}/{tf}/{filename}");
                        let url = format!("https://data.binance.vision/data/{rel}");
                        let item = DownloadUrl {
                            url,
                            rel_path: rel,
                            pair: sym.clone(),
                            timeframe: tf.clone(),
                            date_str: ds,
                        };
                        d += chrono::Duration::days(1);
                        Some(item)
                    })
                })
            })
        })
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn strip_perp(symbol: &str) -> &str {
    symbol.trim_end_matches("-PERP")
}

async fn scan_local_files(data_dir: &Path) -> Result<HashSet<String>, AppError> {
    let dir = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut files = HashSet::new();
        for e in walkdir::WalkDir::new(&dir)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if !e.file_type().is_file() {
                continue;
            }
            let rel = e.path().strip_prefix(&dir).unwrap_or(e.path());
            let s = rel.to_string_lossy().to_string();
            if !s.ends_with(".zip") && !s.ends_with(".parquet") {
                continue;
            }
            files.insert(
                s.trim_end_matches(".parquet")
                    .trim_end_matches(".zip")
                    .to_string(),
            );
        }
        Ok::<_, AppError>(files)
    })
    .await
    .map_err(|e| AppError::Api(format!("spawn: {e}")))?
}

fn build_client(config: &Config) -> Result<reqwest::Client, AppError> {
    let pool = config.resolved_max_concurrent();
    let mut b = reqwest::Client::builder()
        .user_agent("binance-data-downloader/0.1")
        .timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(pool)
        .pool_idle_timeout(Duration::from_secs(30));
    if let Some(ref p) = config.proxy_url
        && let Ok(proxy) = reqwest::Proxy::all(p)
    {
        b = b.proxy(proxy);
    }
    b.build().map_err(AppError::Http)
}

async fn cleanup_tmp(data_dir: &Path) {
    let d = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        for e in walkdir::WalkDir::new(&d)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if e.file_type().is_file()
                && e.path().extension().is_some_and(|x| x == "tmp")
            {
                let _ = std::fs::remove_file(e.path());
            }
        }
    })
    .await
    .ok();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_perp_noop() {
        assert_eq!(strip_perp("BTCUSDT"), "BTCUSDT");
    }

    #[test]
    fn test_strip_perp_removes_suffix() {
        assert_eq!(strip_perp("ETHUSDT-PERP"), "ETHUSDT");
    }

    #[test]
    fn test_urls_single_sym_tf() {
        let symbols = vec!["BTCUSDT".to_string()];
        let tfs = vec!["1d".to_string()];
        let cfg = Config {
            start_date: Some("2024-01-01".into()),
            end_date: Some("2024-01-02".into()),
            ..Default::default()
        };
        let mut sd = std::collections::HashMap::new();
        sd.insert(
            "futures:BTCUSDT".into(),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        );
        let end = chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let all: Vec<_> = urls(&symbols, &tfs, &cfg, &sd, end).collect();
        assert_eq!(all.len(), 2);
        assert!(all[0].url.contains("data.binance.vision"));
        assert!(all[0].url.ends_with(".zip"));
        assert_eq!(all[0].pair, "BTCUSDT");
        assert_eq!(all[0].date_str, "2024-01-01");
        assert_eq!(all[1].date_str, "2024-01-02");
    }

    #[test]
    fn test_urls_spot() {
        let symbols = vec!["BTCUSDT".to_string()];
        let tfs = vec!["1h".to_string()];
        let cfg = Config {
            start_date: Some("2024-01-01".into()),
            end_date: Some("2024-01-01".into()),
            market_type: vec![config::MarketType::Spot],
            ..Default::default()
        };
        let sd = std::collections::HashMap::new();
        let end = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let all: Vec<_> = urls(&symbols, &tfs, &cfg, &sd, end).collect();
        assert_eq!(all.len(), 1);
        assert!(all[0].rel_path.starts_with("spot/daily/klines"));
    }

    #[test]
    fn test_urls_futures() {
        let symbols = vec!["BTCUSDT".to_string()];
        let tfs = vec!["1h".to_string()];
        let cfg = Config {
            start_date: Some("2024-01-01".into()),
            end_date: Some("2024-01-01".into()),
            ..Default::default()
        };
        let sd = std::collections::HashMap::new();
        let end = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let all: Vec<_> = urls(&symbols, &tfs, &cfg, &sd, end).collect();
        assert!(all[0].rel_path.starts_with("futures/um/daily/klines"));
    }

    #[test]
    fn test_filter_urls_removes_existing() {
        let urls = vec![
            DownloadUrl {
                url: "a".into(),
                rel_path: "futures/um/daily/klines/BTCUSDT/1d/file.zip".into(),
                pair: "BTCUSDT".into(),
                timeframe: "1d".into(),
                date_str: "2024-01-01".into(),
            },
            DownloadUrl {
                url: "b".into(),
                rel_path: "futures/um/daily/klines/BTCUSDT/1d/file2.zip".into(),
                pair: "BTCUSDT".into(),
                timeframe: "1d".into(),
                date_str: "2024-01-02".into(),
            },
        ];
        let mut local = HashSet::<String>::new();
        local.insert("futures/um/daily/klines/BTCUSDT/1d/file".into());
        let pending: Vec<_> = urls
            .into_iter()
            .filter(|dl| {
                let no_ext = dl.rel_path.trim_end_matches(".zip");
                !local.contains(no_ext)
            })
            .collect();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].date_str, "2024-01-02");
    }

    #[test]
    fn test_filter_urls_all_new() {
        let urls = vec![DownloadUrl {
            url: "a".into(),
            rel_path: "file1.zip".into(),
            pair: "X".into(),
            timeframe: "1d".into(),
            date_str: "2024-01-01".into(),
        }];
        let local = HashSet::<String>::new();
        let pending: Vec<_> = urls
            .into_iter()
            .filter(|dl| {
                let no_ext = dl.rel_path.trim_end_matches(".zip");
                !local.contains(no_ext)
            })
            .collect();
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn test_filter_urls_all_existing() {
        let urls = vec![DownloadUrl {
            url: "a".into(),
            rel_path: "file1.zip".into(),
            pair: "X".into(),
            timeframe: "1d".into(),
            date_str: "2024-01-01".into(),
        }];
        let mut local = HashSet::<String>::new();
        local.insert("file1".into());
        let pending: Vec<_> = urls
            .into_iter()
            .filter(|dl| {
                let no_ext = dl.rel_path.trim_end_matches(".zip");
                !local.contains(no_ext)
            })
            .collect();
        assert!(pending.is_empty());
    }
}
