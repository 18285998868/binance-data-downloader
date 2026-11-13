//! Combining the API and archive dates into the day a pair starts at.

use crate::AppError;
use crate::dataset::{DatasetKind, DownloadJob};

///
/// For ordinary datasets the API date is a floor: the archive cannot predate
/// the symbol. `indexPriceKlines` is the exception — it is keyed by the
/// underlying index, which can exist before the configured contract.
pub(super) fn resolve_job_start(
    job: DownloadJob,
    api_date: chrono::NaiveDate,
    archive_date: Option<chrono::NaiveDate>,
) -> chrono::NaiveDate {
    match archive_date {
        Some(date) if job.kind == DatasetKind::IndexPriceKlines => date,
        Some(date) => api_date.max(date),
        None => api_date,
    }
}

/// Pick the first day to download: the later of `config.start` and the
/// symbol's own start date, so dates before the listing are never requested.
pub(crate) fn resolve_start(
    start: Option<chrono::NaiveDate>,
    symbol_start: chrono::NaiveDate,
) -> chrono::NaiveDate {
    start.map_or(symbol_start, |configured| configured.max(symbol_start))
}

/// Decide whether a failed start-date fetch may degrade to `cfg.start`.
///
/// With an explicit `config.start`, a transient failure is recoverable: the
/// run proceeds from that date. An unknown-symbol error stays fatal, so typos
/// cannot masquerade as success.
pub(super) fn start_fallback(
    err: &AppError,
    cfg_start: Option<chrono::NaiveDate>,
) -> Option<chrono::NaiveDate> {
    match (cfg_start, err) {
        (Some(start), e) if !matches!(e, AppError::UnknownSymbol { .. }) => Some(start),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{Interval, Period};
    use chrono::NaiveDate;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn klines_job(interval: Interval) -> DownloadJob {
        DownloadJob {
            kind: DatasetKind::Klines,
            interval: Some(interval),
            period: Period::Daily,
        }
    }

    fn index_job() -> DownloadJob {
        DownloadJob {
            kind: DatasetKind::IndexPriceKlines,
            interval: Some(Interval::M1),
            period: Period::Daily,
        }
    }

    #[test]
    fn index_jobs_take_the_earlier_archive_date() {
        // BTCUSD_PERP was listed 2020-08-11, but the BTCUSD index archive
        // starts 2020-06-30. Clamping to the contract's API date would silently
        // drop the first six weeks of index history.
        assert_eq!(
            resolve_job_start(index_job(), date(2020, 8, 11), Some(date(2020, 6, 30))),
            date(2020, 6, 30)
        );
    }

    #[test]
    fn ordinary_jobs_clamp_the_archive_to_the_api_date() {
        assert_eq!(
            resolve_job_start(
                klines_job(Interval::D1),
                date(2020, 8, 11),
                Some(date(2020, 6, 30))
            ),
            date(2020, 8, 11)
        );
    }

    #[test]
    fn a_missing_archive_date_uses_the_api_date() {
        assert_eq!(
            resolve_job_start(index_job(), date(2020, 8, 11), None),
            date(2020, 8, 11)
        );
    }

    #[test]
    fn resolve_start_date_prefers_cfg() {
        let sym = date(2023, 1, 1);
        let cfg = date(2024, 1, 1);
        assert_eq!(resolve_start(Some(cfg), sym), cfg);
    }

    #[test]
    fn resolve_start_date_prefers_sym_when_later() {
        let sym = date(2025, 1, 1);
        let cfg = Some(date(2024, 1, 1));
        assert_eq!(resolve_start(cfg, sym), sym);
    }

    #[test]
    fn resolve_start_date_no_cfg() {
        let sym = date(2023, 6, 15);
        let cfg: Option<chrono::NaiveDate> = None;
        assert_eq!(resolve_start(cfg, sym), sym);
    }

    #[test]
    fn start_fallback_used_when_cfg_start_is_set() {
        let start = date(2024, 1, 1);
        let err = AppError::Api {
            status: None,
            message: "network down".into(),
        };
        assert_eq!(start_fallback(&err, Some(start)), Some(start));
    }

    #[test]
    fn start_fallback_rejected_for_unknown_symbol() {
        let start = date(2024, 1, 1);
        let err = AppError::UnknownSymbol {
            symbol: "NOPEUSDT".into(),
        };
        assert_eq!(start_fallback(&err, Some(start)), None);
    }

    #[test]
    fn start_fallback_needs_cfg_start() {
        let err = AppError::Api {
            status: None,
            message: "network down".into(),
        };
        assert_eq!(start_fallback(&err, None), None);
    }
}
