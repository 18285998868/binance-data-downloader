#[path = "02_build_download_tasks.rs"]
pub mod build_download_tasks;
#[path = "01_build_urls.rs"]
pub mod build_urls;
#[path = "03_download/mod.rs"]
pub mod download;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use futures::stream::{StreamExt, iter as stream_iter};

use crate::AppError;
use crate::config::AppConfig;

/// Run the full batch download pipeline.
///
/// 1. Load config
/// 2. Scan existing local files (once)
/// 3. Iterate symbol (serial) → timeframe (serial), downloading missing files
pub async fn run(config_path: &Path) -> Result<(), AppError> {
    let cfg = AppConfig::load(config_path)?;
    crate::utils::cache::set_base_dir(&cfg.output_dir)?;

    let client = reqwest::Client::builder()
        .user_agent("binance-data-downloader/0.1")
        .timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(256)
        .pool_idle_timeout(Duration::from_secs(30))
        .build()?;

    let local = crate::utils::scan_local_files::scan_local_files(&cfg.output_dir).await?;

    let concurrency = cfg.concurrency;
    let end = cfg.resolved_end(chrono::Local::now().date_naive());

    tracing::info!(
        symbols = cfg.symbol.len(),
        timeframes = cfg.timeframe.len(),
        total = cfg.symbol.len() * cfg.timeframe.len(),
        concurrency,
        "starting batch download",
    );

    if !cfg.symbol.is_empty() && cfg.start.is_none() {
        tracing::info!(
            count = cfg.symbol.len(),
            "first start-date per symbol is resolved by an individual request to the \
             Binance API and cached for 30 days; this can take a while on a fresh run",
        );
    }

    let mut failed_pairs = 0;
    let mut errors = Vec::new();

    // Fetch one start-date per symbol up front, 8 in flight at most.
    let sym_starts: HashMap<String, chrono::NaiveDate> = {
        let mut map = HashMap::new();
        let mut pending = stream_iter(cfg.symbol.iter().map(|sym| {
            let c = client.clone();
            let s = sym.clone();
            async move {
                (
                    s.clone(),
                    crate::utils::get_start_date::get_first_kline_date(&c, &s, "1m").await,
                )
            }
        }))
        .buffer_unordered(8);
        while let Some((sym, res)) = pending.next().await {
            match res {
                Ok(d) => {
                    map.insert(sym, d);
                }
                Err(e) => {
                    failed_pairs += cfg.timeframe.len();
                    tracing::error!(symbol = %sym, "start-date fetch failed: {e}");
                    if errors.len() < MAX_BATCH_ERRORS {
                        errors.push(format!("{sym}/*: {e}"));
                    }
                }
            }
        }
        map
    };
    tracing::info!(
        resolved = sym_starts.len(),
        total = cfg.symbol.len(),
        "start-dates resolved",
    );

    for symbol in &cfg.symbol {
        let Some(sym_start) = sym_starts.get(symbol) else {
            continue;
        };
        for timeframe in &cfg.timeframe {
            tracing::info!(symbol = %symbol, timeframe = %timeframe, "running pair");
            let ctx = PairCtx {
                sym_start: *sym_start,
                end,
                cfg: &cfg,
                local: &local,
            };
            if let Err(e) = run_one(&client, symbol, timeframe, &ctx).await {
                failed_pairs += 1;
                tracing::error!(symbol = %symbol, timeframe = %timeframe, "failed: {e}");
                if errors.len() < MAX_BATCH_ERRORS {
                    errors.push(format!("{symbol}/{timeframe}: {e}"));
                }
            }
        }
    }

    if failed_pairs == 0 {
        tracing::info!("batch download complete");
        Ok(())
    } else {
        tracing::error!(count = failed_pairs, "batch download completed with errors");
        let truncated = if errors.len() < failed_pairs {
            "\n(additional failures not listed)"
        } else {
            ""
        };
        Err(AppError::Data(format!(
            "{} pair(s) failed: {}{}",
            failed_pairs,
            errors.join("; "),
            truncated,
        )))
    }
}

/// Maximum number of pair-level error details retained in the batch summary.
const MAX_BATCH_ERRORS: usize = 20;

/// Per-pair context: resolved dates plus shared borrows.
struct PairCtx<'a> {
    sym_start: chrono::NaiveDate,
    end: chrono::NaiveDate,
    cfg: &'a AppConfig,
    local: &'a HashSet<String>,
}

/// Download one symbol/timeframe pair.
async fn run_one(
    client: &reqwest::Client,
    symbol: &str,
    timeframe: &str,
    ctx: &PairCtx<'_>,
) -> Result<(), AppError> {
    if let Some(cfg_start) = ctx.cfg.start
        && cfg_start > ctx.end
    {
        tracing::warn!(
            symbol = %symbol,
            timeframe = %timeframe,
            start = %cfg_start,
            end = %ctx.end,
            "config start after end date, skipping",
        );
        return Ok(());
    }

    let start = resolve_start(ctx.cfg.start, ctx.sym_start);

    if start > ctx.end {
        tracing::warn!(
            symbol = %symbol,
            timeframe = %timeframe,
            start = %start,
            end = %ctx.end,
            "start date after end date, skipping",
        );
        return Ok(());
    }

    tracing::info!(
        symbol = %symbol,
        timeframe = %timeframe,
        start = %start,
        end = %ctx.end,
        "processing symbol-timeframe pair",
    );

    let tasks = build_download_tasks::build_download_tasks(
        symbol,
        timeframe,
        start,
        ctx.end,
        ctx.local,
        &ctx.cfg.output_dir,
    );

    if !tasks.is_empty() {
        tracing::debug!(
            files = tasks.len(),
            start = %start,
            end = %ctx.end,
            "downloading all",
        );
        let stats = download::run_downloads(client, tasks, ctx.cfg.concurrency).await?;
        tracing::info!(
            symbol = %symbol,
            timeframe = %timeframe,
            ok = stats.ok,
            not_found = stats.not_found,
            failed = stats.failed,
            total = stats.total,
            "pair download complete",
        );
    } else {
        tracing::debug!(
            symbol = %symbol,
            timeframe = %timeframe,
            start = %start,
            end = %ctx.end,
            "no files to download, all cached or out of range",
        );
    }

    Ok(())
}

fn resolve_start(
    start: Option<chrono::NaiveDate>,
    sym_start: chrono::NaiveDate,
) -> chrono::NaiveDate {
    start.map(|cs| cs.max(sym_start)).unwrap_or(sym_start)
}

#[cfg(test)]
mod tests {
    use super::resolve_start;

    #[test]
    fn resolve_start_date_prefers_cfg() {
        let sym = chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let cfg = chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(resolve_start(Some(cfg), sym), cfg);
    }

    #[test]
    fn resolve_start_date_prefers_sym_when_later() {
        let sym = chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let cfg = Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        assert_eq!(resolve_start(cfg, sym), sym);
    }

    #[test]
    fn resolve_start_date_no_cfg() {
        let sym = chrono::NaiveDate::from_ymd_opt(2023, 6, 15).unwrap();
        let cfg: Option<chrono::NaiveDate> = None;
        assert_eq!(resolve_start(cfg, sym), sym);
    }
}
