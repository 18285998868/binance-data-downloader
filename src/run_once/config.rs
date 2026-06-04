use serde::Deserialize;

/// Configuration for a single symbol/timeframe download.
///
/// Typically constructed by `run_batch` for each pair; also loadable
/// from `config/run_once.toml` for standalone use.
#[derive(Debug, Clone, Deserialize)]
pub struct RunOnceConfig {
    /// Trading pair symbol (e.g. "BTCUSDT").
    pub symbol: String,
    /// Kline interval (e.g. "1m", "1h", "1d").
    pub timeframe: String,
    /// Start date (YYYY-MM-DD). None = first kline date from exchange.
    pub start: Option<String>,
    /// End date (YYYY-MM-DD). None = yesterday.
    pub end: Option<String>,
    /// Data output directory. None = ./data.
    pub dir: Option<String>,
}
