use serde::Deserialize;

/// Configuration for a batch download run.
///
/// Loaded from `config/config.toml`. All fields are optional;
/// defaults are resolved at runtime (symbols/timeframes from exchangeInfo,
/// start from first kline, end from yesterday, dir = "data").
#[derive(Debug, Clone, Deserialize)]
pub struct BatchConfig {
    /// Symbols to download. Empty/None = auto-fetch all TRADING pairs from exchangeInfo.
    pub symbol: Option<Vec<String>>,
    /// Timeframes to download. Empty/None = default set (1m, 3m, 5m, ..., 1d).
    pub timeframe: Option<Vec<String>>,
    /// Max concurrent file downloads per symbol/timeframe pair (default 1000, capped at 256).
    pub max_concurrent: Option<usize>,
    /// Start date (YYYY-MM-DD). None = first kline date from exchange.
    pub start: Option<String>,
    /// End date (YYYY-MM-DD). None = yesterday.
    pub end: Option<String>,
    /// Data output directory. None = ./data.
    pub dir: Option<String>,
}
