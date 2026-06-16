use std::collections::HashSet;
use std::path::PathBuf;

use crate::AppError;
use crate::config::AppConfig;

/// Download and convert one symbol/timeframe pair.
///
/// Steps: resolve start date, build task list from pre-scanned local files, run downloads.
pub async fn run_one(
    client: &reqwest::Client,
    symbol: &str,
    timeframe: &str,
    cfg: &AppConfig,
    local: &HashSet<String>,
) -> Result<(), AppError> {
    let data_dir = PathBuf::from(&cfg.dir);
    let end = cfg
        .end
        .as_deref()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
        .unwrap_or_else(|| chrono::Local::now().date_naive() - chrono::Duration::days(1));

    let start = resolve_start(client, symbol, timeframe, cfg.start.as_deref()).await?;

    if start > end {
        tracing::warn!(
            symbol = %symbol,
            timeframe = %timeframe,
            start = %start,
            end = %end,
            "start date after end date, skipping",
        );
        return Ok(());
    }

    tracing::info!(
        symbol = %symbol,
        timeframe = %timeframe,
        start = %start,
        end = %end,
        "processing symbol-timeframe pair",
    );

    let tasks =
        crate::task::builder::build_download_tasks(symbol, timeframe, start, end, local, &data_dir);

    if !tasks.is_empty() {
        tracing::debug!(
            files = tasks.len(),
            start = %start,
            end = %end,
            "downloading all",
        );
        crate::task::download::run_downloads(client, tasks).await?;
    } else {
        tracing::debug!(
            symbol = %symbol,
            timeframe = %timeframe,
            start = %start,
            end = %end,
            "no files to download, all cached or out of range",
        );
    }

    Ok(())
}

async fn resolve_start(
    client: &reqwest::Client,
    symbol: &str,
    timeframe: &str,
    start: Option<&str>,
) -> Result<chrono::NaiveDate, AppError> {
    let sym_start = crate::task::get_start::get_first_kline_date(client, symbol, timeframe).await?;
    let cfg_start = start.and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
    Ok(resolve_start_date(sym_start, cfg_start))
}

/// Pick the later of symbol-first-kline-date and user-configured-start.
fn resolve_start_date(
    sym_start: chrono::NaiveDate,
    cfg_start: Option<chrono::NaiveDate>,
) -> chrono::NaiveDate {
    cfg_start.map(|cs| cs.max(sym_start)).unwrap_or(sym_start)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_start_date_prefers_cfg() {
        let sym = chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let cfg = Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        assert_eq!(resolve_start_date(sym, cfg), cfg.unwrap());
    }

    #[test]
    fn resolve_start_date_prefers_sym_when_later() {
        let sym = chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let cfg = Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        assert_eq!(resolve_start_date(sym, cfg), sym);
    }

    #[test]
    fn resolve_start_date_no_cfg() {
        let sym = chrono::NaiveDate::from_ymd_opt(2023, 6, 15).unwrap();
        assert_eq!(resolve_start_date(sym, None), sym);
    }
}
