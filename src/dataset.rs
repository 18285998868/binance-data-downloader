//! Archive dataset model: market, period, dataset kind and interval.
//!
//! Every value here maps **one-to-one** onto a segment of the public archive
//! path (`https://data.binance.vision/data/...`) and, by design, onto the
//! local storage path below `output_dir`. There is deliberately no second
//! layout: what the web page shows is what lands on disk.

use std::fmt;
use std::str::FromStr;

use chrono::{Datelike, Months, NaiveDate};
use serde::Deserialize;

/// Binance market, matching the archive path segment.
///
/// | market | archive path |
/// | --- | --- |
/// | spot | `data/spot` |
/// | USDⓈ-M futures | `data/futures/um` |
/// | COIN-M futures | `data/futures/cm` |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Market {
    /// Spot market.
    Spot,
    /// USDⓈ-M perpetual/delivery futures.
    Um,
    /// COIN-M perpetual/delivery futures.
    Cm,
}

impl Market {
    /// Canonical path segment, e.g. `futures/um`.
    #[must_use]
    pub(crate) const fn as_path(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Um => "futures/um",
            Self::Cm => "futures/cm",
        }
    }

    /// Archive root below the bucket, e.g. `data/futures/um`.
    ///
    /// The same string is used as the local root below `output_dir`, which is
    /// what keeps the on-disk tree aligned with the web page.
    #[must_use]
    pub(crate) const fn archive_root(self) -> &'static str {
        match self {
            Self::Spot => "data/spot",
            Self::Um => "data/futures/um",
            Self::Cm => "data/futures/cm",
        }
    }

    /// Exchange REST base used to resolve a symbol's first kline.
    #[must_use]
    pub(crate) const fn api_base(self) -> &'static str {
        match self {
            Self::Spot => "https://api.binance.com",
            Self::Um => "https://fapi.binance.com",
            Self::Cm => "https://dapi.binance.com",
        }
    }

    /// Klines endpoint path on [`Self::api_base`].
    #[must_use]
    pub(crate) const fn klines_path(self) -> &'static str {
        match self {
            Self::Spot => "/api/v3/klines",
            Self::Um => "/fapi/v1/klines",
            Self::Cm => "/dapi/v1/klines",
        }
    }
}

impl fmt::Display for Market {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_path())
    }
}

impl FromStr for Market {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            // Canonical archive paths, plus short aliases.
            "spot" => Ok(Self::Spot),
            "futures/um" | "um" => Ok(Self::Um),
            "futures/cm" | "cm" => Ok(Self::Cm),
            other => Err(format!(
                "unsupported market: {other}, valid values: spot, futures/um, futures/cm"
            )),
        }
    }
}

impl<'de> Deserialize<'de> for Market {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Archive cadence: daily or monthly files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Period {
    /// One file per day, named `YYYY-MM-DD`.
    Daily,
    /// One file per month, named `YYYY-MM`.
    Monthly,
}

impl Period {
    /// All accepted periods.
    pub(crate) const ALL: [Self; 2] = [Self::Daily, Self::Monthly];

    /// Archive path segment and file suffix, e.g. `daily`.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Daily => "daily",
            Self::Monthly => "monthly",
        }
    }

    /// First cursor value for a period that contains `date`.
    ///
    /// Daily keeps the exact day; monthly rounds down to the first day of the
    /// month, because a monthly file always holds the whole month.
    #[must_use]
    pub(crate) fn start_of_period(self, date: NaiveDate) -> NaiveDate {
        match self {
            Self::Daily => date,
            Self::Monthly => date.with_day(1).unwrap_or(date),
        }
    }

    /// Last **complete** period start inside `[.., end]`.
    ///
    /// Daily returns `end`. Monthly returns the first day of `end`'s month
    /// only when `end` is that month's last day; otherwise the previous month,
    /// so a partially published month is never requested.
    #[must_use]
    pub(crate) fn last_complete_start(self, end: NaiveDate) -> NaiveDate {
        match self {
            Self::Daily => end,
            Self::Monthly => {
                let first = self.start_of_period(end);
                match next_month(first) {
                    Some(next) => {
                        let last_day = next.pred_opt().unwrap_or(end);
                        if end == last_day {
                            first
                        } else {
                            previous_month(first).unwrap_or(first)
                        }
                    }
                    None => first,
                }
            }
        }
    }

    /// Advance one period; `None` at the end of the representable range.
    #[must_use]
    pub(crate) fn advance(self, date: NaiveDate) -> Option<NaiveDate> {
        match self {
            Self::Daily => date.checked_add_days(chrono::Days::new(1)),
            Self::Monthly => next_month(date),
        }
    }

    /// File-name date component.
    #[must_use]
    pub(crate) fn format(self, date: NaiveDate) -> String {
        match self {
            Self::Daily => date.format("%Y-%m-%d").to_string(),
            Self::Monthly => date.format("%Y-%m").to_string(),
        }
    }

    /// Iterate the period starts covered by a `start..=end` window.
    ///
    /// Monthly windows are aligned to complete months: a `start` in the middle
    /// of a month downloads that whole month, while an `end` in the middle of
    /// a month stops at the previous month.
    pub(crate) fn dates(self, start: NaiveDate, end: NaiveDate) -> impl Iterator<Item = NaiveDate> {
        let mut cursor = Some(self.start_of_period(start));
        let last = self.last_complete_start(end);
        std::iter::from_fn(move || {
            let current = cursor?;
            if current > last {
                cursor = None;
                return None;
            }
            cursor = self.advance(current);
            Some(current)
        })
    }

    /// Extract the date of an archive file stem.
    ///
    /// `BTCUSDT-1m-2024-01-02` (daily) and `BTCUSDT-1m-2024-01` (monthly) both
    /// end in the date component; symbols never contain `-`.
    pub(crate) fn parse_key_date(self, stem: &str) -> Result<NaiveDate, String> {
        // Symbols are validated to contain no `-` and dataset/interval names
        // are dash-free, so the component count identifies the cadence:
        // `SYMBOL[-INTERVAL]-KIND-YYYY-MM[-DD]`.
        let expected = match self {
            Self::Daily => 5,
            Self::Monthly => 4,
        };
        if stem.split('-').count() != expected {
            return Err(format!(
                "wrong number of components for {self} archive key: {stem}"
            ));
        }
        let mut parts = stem.rsplit('-');
        let mut next = |what: &str| -> Result<&str, String> {
            parts
                .next()
                .ok_or_else(|| format!("no {what} in archive key: {stem}"))
        };
        match self {
            Self::Daily => {
                let day = next("day")?;
                let month = next("month")?;
                let year = next("year")?;
                NaiveDate::parse_from_str(&format!("{year}-{month}-{day}"), "%Y-%m-%d")
                    .map_err(|e| format!("bad date in archive key {stem}: {e}"))
            }
            Self::Monthly => {
                let month = next("month")?;
                let year = next("year")?;
                NaiveDate::parse_from_str(&format!("{year}-{month}-01"), "%Y-%m-%d")
                    .map_err(|e| format!("bad month in archive key {stem}: {e}"))
            }
        }
    }
}

impl fmt::Display for Period {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Period {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|period| period.as_str() == s)
            .ok_or_else(|| format!("unsupported period: {s}, valid values: daily, monthly"))
    }
}

impl<'de> Deserialize<'de> for Period {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

fn next_month(date: NaiveDate) -> Option<NaiveDate> {
    date.checked_add_months(Months::new(1))
}

fn previous_month(date: NaiveDate) -> Option<NaiveDate> {
    date.checked_sub_months(Months::new(1))
}

/// Kline interval, matching archive directory names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Interval {
    /// 1-second klines (spot only).
    S1,
    /// 1-minute klines.
    M1,
    /// 3-minute klines.
    M3,
    /// 5-minute klines.
    M5,
    /// 15-minute klines.
    M15,
    /// 30-minute klines.
    M30,
    /// 1-hour klines.
    H1,
    /// 2-hour klines.
    H2,
    /// 4-hour klines.
    H4,
    /// 6-hour klines.
    H6,
    /// 8-hour klines.
    H8,
    /// 12-hour klines.
    H12,
    /// 1-day klines.
    D1,
    /// 3-day klines (monthly archives only).
    D3,
    /// 1-week klines (monthly archives only).
    W1,
    /// 1-month klines (monthly archives only).
    MO1,
}

impl Interval {
    /// All accepted intervals, ordered by duration.
    pub(crate) const ALL: [Self; 16] = [
        Self::S1,
        Self::M1,
        Self::M3,
        Self::M5,
        Self::M15,
        Self::M30,
        Self::H1,
        Self::H2,
        Self::H4,
        Self::H6,
        Self::H8,
        Self::H12,
        Self::D1,
        Self::D3,
        Self::W1,
        Self::MO1,
    ];

    /// Archive directory name, e.g. `1m`.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::S1 => "1s",
            Self::M1 => "1m",
            Self::M3 => "3m",
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::M30 => "30m",
            Self::H1 => "1h",
            Self::H2 => "2h",
            Self::H4 => "4h",
            Self::H6 => "6h",
            Self::H8 => "8h",
            Self::H12 => "12h",
            Self::D1 => "1d",
            Self::D3 => "3d",
            Self::W1 => "1w",
            Self::MO1 => "1mo",
        }
    }

    /// Whether the archive publishes this interval for a market/period.
    ///
    /// `3d`, `1w` and `1mo` are monthly-only: their daily files exist only on
    /// period boundaries and are fully covered by the monthly archive, so the
    /// downloader deliberately does not request them.
    #[must_use]
    pub(crate) fn available_for(self, market: Market, period: Period) -> bool {
        match (market, period) {
            (Market::Spot, Period::Daily) => self <= Self::D1,
            (Market::Spot, Period::Monthly) => true,
            (_, Period::Daily) => self >= Self::M1 && self <= Self::D1,
            (_, Period::Monthly) => self != Self::S1,
        }
    }
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Interval {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|interval| interval.as_str() == s)
            .ok_or_else(|| {
                let valid: Vec<&str> = Self::ALL.iter().map(|i| i.as_str()).collect();
                format!("unsupported interval: {s}, valid values: {valid:?}")
            })
    }
}

impl<'de> Deserialize<'de> for Interval {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// One archive dataset family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum DatasetKind {
    /// Aggregate trades.
    AggTrades,
    /// Individual trades.
    Trades,
    /// Klines.
    Klines,
    /// Order-book depth snapshots.
    BookDepth,
    /// Best bid/ask ticker.
    BookTicker,
    /// Funding rates.
    FundingRate,
    /// Liquidation snapshots.
    LiquidationSnapshot,
    /// Futures metrics.
    Metrics,
    /// Index-price klines.
    IndexPriceKlines,
    /// Mark-price klines.
    MarkPriceKlines,
    /// Premium-index klines.
    PremiumIndexKlines,
}

impl DatasetKind {
    /// All accepted datasets.
    pub(crate) const ALL: [Self; 11] = [
        Self::AggTrades,
        Self::Trades,
        Self::Klines,
        Self::BookDepth,
        Self::BookTicker,
        Self::FundingRate,
        Self::LiquidationSnapshot,
        Self::Metrics,
        Self::IndexPriceKlines,
        Self::MarkPriceKlines,
        Self::PremiumIndexKlines,
    ];

    /// Archive directory name, exactly as the web page spells it.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::AggTrades => "aggTrades",
            Self::Trades => "trades",
            Self::Klines => "klines",
            Self::BookDepth => "bookDepth",
            Self::BookTicker => "bookTicker",
            Self::FundingRate => "fundingRate",
            Self::LiquidationSnapshot => "liquidationSnapshot",
            Self::Metrics => "metrics",
            Self::IndexPriceKlines => "indexPriceKlines",
            Self::MarkPriceKlines => "markPriceKlines",
            Self::PremiumIndexKlines => "premiumIndexKlines",
        }
    }

    /// Whether files of this dataset carry an interval directory.
    #[must_use]
    pub(crate) const fn needs_interval(self) -> bool {
        matches!(
            self,
            Self::Klines
                | Self::IndexPriceKlines
                | Self::MarkPriceKlines
                | Self::PremiumIndexKlines
        )
    }

    /// Whether the archive publishes this dataset for a market/period.
    #[must_use]
    pub(crate) const fn supports(self, market: Market, period: Period) -> bool {
        let market_ok = match self {
            Self::AggTrades | Self::Trades | Self::Klines => true,
            Self::BookDepth
            | Self::BookTicker
            | Self::FundingRate
            | Self::Metrics
            | Self::IndexPriceKlines
            | Self::MarkPriceKlines
            | Self::PremiumIndexKlines => matches!(market, Market::Um | Market::Cm),
            Self::LiquidationSnapshot => matches!(market, Market::Cm),
        };
        let period_ok = match self {
            // `bookDepth`, `metrics` and `liquidationSnapshot` are daily-only.
            Self::BookDepth | Self::Metrics | Self::LiquidationSnapshot => {
                matches!(period, Period::Daily)
            }
            // `fundingRate` is monthly-only.
            Self::FundingRate => matches!(period, Period::Monthly),
            _ => true,
        };
        market_ok && period_ok
    }
}

impl fmt::Display for DatasetKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for DatasetKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.as_str() == s)
            .ok_or_else(|| {
                let valid: Vec<&str> = Self::ALL.iter().map(|k| k.as_str()).collect();
                format!("unsupported dataset type: {s}, valid values: {valid:?}")
            })
    }
}

impl<'de> Deserialize<'de> for DatasetKind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// One concrete download job: dataset family + period + optional interval.
///
/// A `[[dataset]]` entry with two periods and three intervals expands into six
/// jobs (or two, for datasets without intervals).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct DownloadJob {
    /// Dataset family.
    pub(crate) kind: DatasetKind,
    /// Kline interval; `None` for datasets without an interval directory.
    pub(crate) interval: Option<Interval>,
    /// Archive cadence.
    pub(crate) period: Period,
}

impl DownloadJob {
    /// The symbol used by the archive for this job.
    ///
    /// `indexPriceKlines` is keyed by the **underlying index**, not by the
    /// contract: `BTCUSD_PERP` lives under `BTCUSD`, and a dated contract such
    /// as `BTCUSDT_250926` lives under `BTCUSDT`. Every other dataset uses the
    /// configured symbol verbatim.
    #[must_use]
    pub(crate) fn web_symbol(self, market: Market, symbol: &str) -> &str {
        if self.kind == DatasetKind::IndexPriceKlines && matches!(market, Market::Um | Market::Cm) {
            underlying_index_symbol(symbol)
        } else {
            symbol
        }
    }

    /// Archive file name for `symbol` on `date`.
    #[must_use]
    pub(crate) fn file_name(self, market: Market, symbol: &str, date: NaiveDate) -> String {
        let symbol = self.web_symbol(market, symbol);
        let date = self.period.format(date);
        match self.interval {
            Some(interval) => format!("{symbol}-{interval}-{date}.zip"),
            None => format!("{symbol}-{}-{date}.zip", self.kind),
        }
    }

    /// Directory relative to the archive root, e.g.
    /// `data/futures/um/daily/klines/BTCUSDT/1m`.
    #[must_use]
    pub(crate) fn relative_dir(self, market: Market, symbol: &str) -> String {
        let symbol = self.web_symbol(market, symbol);
        let mut dir = format!(
            "{}/{}/{}/{}",
            market.archive_root(),
            self.period,
            self.kind,
            symbol
        );
        if let Some(interval) = self.interval {
            dir.push('/');
            dir.push_str(interval.as_str());
        }
        dir
    }

    /// Path relative to the archive root **and** to `output_dir`.
    #[must_use]
    pub(crate) fn rel_path(self, market: Market, symbol: &str, date: NaiveDate) -> String {
        format!(
            "{}/{}",
            self.relative_dir(market, symbol),
            self.file_name(market, symbol, date)
        )
    }
}

/// Strip the contract suffix (`_PERP`, `_250926`, ...) to get the index
/// symbol used by the archive's `indexPriceKlines` tree.
fn underlying_index_symbol(symbol: &str) -> &str {
    match symbol.rsplit_once('_') {
        Some((base, suffix))
            if suffix == "PERP"
                || (suffix.len() == 6 && suffix.bytes().all(|b| b.is_ascii_digit())) =>
        {
            base
        }
        _ => symbol,
    }
}

impl fmt::Display for DownloadJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.interval {
            Some(interval) => write!(f, "{}/{interval}/{}", self.kind, self.period),
            None => write!(f, "{}/{}", self.kind, self.period),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn market_parses_canonical_paths_and_short_aliases() {
        assert_eq!("spot".parse::<Market>().unwrap(), Market::Spot);
        assert_eq!("futures/um".parse::<Market>().unwrap(), Market::Um);
        assert_eq!("futures/cm".parse::<Market>().unwrap(), Market::Cm);
        assert_eq!("um".parse::<Market>().unwrap(), Market::Um);
        assert_eq!("cm".parse::<Market>().unwrap(), Market::Cm);
        assert!("fapi".parse::<Market>().is_err());
        assert_eq!(Market::Um.to_string(), "futures/um");
        assert_eq!(Market::Um.archive_root(), "data/futures/um");
    }

    #[test]
    fn market_api_endpoints_follow_the_market() {
        assert_eq!(Market::Spot.api_base(), "https://api.binance.com");
        assert_eq!(Market::Um.api_base(), "https://fapi.binance.com");
        assert_eq!(Market::Cm.api_base(), "https://dapi.binance.com");
        assert_eq!(Market::Spot.klines_path(), "/api/v3/klines");
        assert_eq!(Market::Um.klines_path(), "/fapi/v1/klines");
        assert_eq!(Market::Cm.klines_path(), "/dapi/v1/klines");
    }

    #[test]
    fn period_round_trips() {
        for period in Period::ALL {
            assert_eq!(period.as_str().parse::<Period>().unwrap(), period);
            assert_eq!(period.to_string(), period.as_str());
        }
        assert!("weekly".parse::<Period>().is_err());
    }

    #[test]
    fn daily_dates_cover_every_day() {
        let dates: Vec<_> = Period::Daily
            .dates(date(2024, 1, 1), date(2024, 1, 3))
            .collect();
        assert_eq!(
            dates,
            vec![date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]
        );
    }

    #[test]
    fn monthly_dates_only_cover_complete_months() {
        let dates: Vec<_> = Period::Monthly
            .dates(date(2024, 1, 15), date(2024, 3, 15))
            .collect();
        assert_eq!(dates, vec![date(2024, 1, 1), date(2024, 2, 1)]);
    }

    #[test]
    fn monthly_dates_include_the_end_month_when_it_is_complete() {
        let dates: Vec<_> = Period::Monthly
            .dates(date(2024, 1, 1), date(2024, 3, 31))
            .collect();
        assert_eq!(
            dates,
            vec![date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)]
        );
    }

    #[test]
    fn monthly_dates_handle_a_leap_february() {
        let dates: Vec<_> = Period::Monthly
            .dates(date(2024, 2, 1), date(2024, 2, 29))
            .collect();
        assert_eq!(dates, vec![date(2024, 2, 1)]);
    }

    #[test]
    fn monthly_dates_skip_an_incomplete_leading_month() {
        let dates: Vec<_> = Period::Monthly
            .dates(date(2024, 1, 15), date(2024, 1, 20))
            .collect();
        assert_eq!(dates, Vec::<NaiveDate>::new());
    }

    #[test]
    fn monthly_dates_stop_at_the_representable_maximum() {
        let dates: Vec<_> = Period::Monthly
            .dates(NaiveDate::MAX, NaiveDate::MAX)
            .collect();
        assert_eq!(dates.len(), 1);
    }

    #[test]
    fn daily_key_parsing() {
        assert_eq!(
            Period::Daily
                .parse_key_date("BTCUSDT-1m-2024-01-02")
                .unwrap(),
            date(2024, 1, 2)
        );
        assert_eq!(
            Period::Daily
                .parse_key_date("BTCUSDT_250926-1d-2024-09-26")
                .unwrap(),
            date(2024, 9, 26)
        );
        assert!(Period::Daily.parse_key_date("BTCUSDT-1m-2024-01").is_err());
    }

    #[test]
    fn monthly_key_parsing() {
        assert_eq!(
            Period::Monthly
                .parse_key_date("BTCUSDT-fundingRate-2024-01")
                .unwrap(),
            date(2024, 1, 1)
        );
        assert!(
            Period::Monthly
                .parse_key_date("BTCUSDT-1m-2024-01-02")
                .is_err()
        );
    }

    #[test]
    fn interval_availability_matches_the_archive() {
        assert!(Interval::S1.available_for(Market::Spot, Period::Daily));
        assert!(Interval::S1.available_for(Market::Spot, Period::Monthly));
        assert!(!Interval::S1.available_for(Market::Um, Period::Daily));
        assert!(!Interval::S1.available_for(Market::Cm, Period::Monthly));
        assert!(Interval::D3.available_for(Market::Um, Period::Monthly));
        assert!(!Interval::D3.available_for(Market::Um, Period::Daily));
        assert!(!Interval::MO1.available_for(Market::Spot, Period::Daily));
        assert!(Interval::MO1.available_for(Market::Spot, Period::Monthly));
    }

    #[test]
    fn dataset_support_matrix() {
        assert!(DatasetKind::AggTrades.supports(Market::Spot, Period::Daily));
        assert!(DatasetKind::BookDepth.supports(Market::Um, Period::Daily));
        assert!(!DatasetKind::BookDepth.supports(Market::Um, Period::Monthly));
        assert!(!DatasetKind::BookDepth.supports(Market::Spot, Period::Daily));
        assert!(DatasetKind::BookTicker.supports(Market::Cm, Period::Monthly));
        assert!(DatasetKind::FundingRate.supports(Market::Um, Period::Monthly));
        assert!(!DatasetKind::FundingRate.supports(Market::Um, Period::Daily));
        assert!(DatasetKind::LiquidationSnapshot.supports(Market::Cm, Period::Daily));
        assert!(!DatasetKind::LiquidationSnapshot.supports(Market::Um, Period::Daily));
        assert!(DatasetKind::Metrics.supports(Market::Cm, Period::Daily));
        assert!(!DatasetKind::Metrics.supports(Market::Cm, Period::Monthly));
        assert!(DatasetKind::IndexPriceKlines.supports(Market::Um, Period::Monthly));
        assert!(!DatasetKind::IndexPriceKlines.supports(Market::Spot, Period::Daily));
    }

    #[test]
    fn job_file_names_and_paths_mirror_the_web_layout() {
        let klines = DownloadJob {
            kind: DatasetKind::Klines,
            interval: Some(Interval::M1),
            period: Period::Daily,
        };
        assert_eq!(
            klines.rel_path(Market::Um, "BTCUSDT", date(2024, 1, 2)),
            "data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-02.zip"
        );

        let funding = DownloadJob {
            kind: DatasetKind::FundingRate,
            interval: None,
            period: Period::Monthly,
        };
        assert_eq!(
            funding.rel_path(Market::Um, "BTCUSDT", date(2024, 1, 1)),
            "data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-01.zip"
        );

        let spot = DownloadJob {
            kind: DatasetKind::AggTrades,
            interval: None,
            period: Period::Daily,
        };
        assert_eq!(
            spot.rel_path(Market::Spot, "BTCUSDT", date(2024, 1, 2)),
            "data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01-02.zip"
        );
    }

    #[test]
    fn index_price_klines_use_the_underlying_symbol() {
        let index = DownloadJob {
            kind: DatasetKind::IndexPriceKlines,
            interval: Some(Interval::M1),
            period: Period::Daily,
        };
        assert_eq!(
            index.rel_path(Market::Cm, "BTCUSD_PERP", date(2024, 1, 2)),
            "data/futures/cm/daily/indexPriceKlines/BTCUSD/1m/BTCUSD-1m-2024-01-02.zip"
        );
        assert_eq!(
            index.rel_path(Market::Um, "BTCUSDT_250926", date(2024, 9, 26)),
            "data/futures/um/daily/indexPriceKlines/BTCUSDT/1m/BTCUSDT-1m-2024-09-26.zip"
        );

        // Mark-price files keep the contract symbol; only the index tree is
        // keyed by the underlying.
        let mark = DownloadJob {
            kind: DatasetKind::MarkPriceKlines,
            interval: Some(Interval::M1),
            period: Period::Daily,
        };
        assert_eq!(
            mark.rel_path(Market::Cm, "BTCUSD_PERP", date(2024, 1, 2)),
            "data/futures/cm/daily/markPriceKlines/BTCUSD_PERP/1m/BTCUSD_PERP-1m-2024-01-02.zip"
        );
    }

    #[test]
    fn job_display_is_stable() {
        assert_eq!(
            DownloadJob {
                kind: DatasetKind::Klines,
                interval: Some(Interval::M1),
                period: Period::Daily,
            }
            .to_string(),
            "klines/1m/daily"
        );
        assert_eq!(
            DownloadJob {
                kind: DatasetKind::FundingRate,
                interval: None,
                period: Period::Monthly,
            }
            .to_string(),
            "fundingRate/monthly"
        );
    }
}
