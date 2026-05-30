use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum MarketType {
    #[serde(rename = "spot")]
    Spot,
    #[serde(rename = "futures")]
    Futures,
}

impl MarketType {
    /// Returns the path prefix used in Binance public data URL structure.
    pub fn rel_path_prefix(self) -> &'static str {
        match self {
            MarketType::Spot => "spot/daily/klines",
            MarketType::Futures => "futures/um/daily/klines",
        }
    }

    /// Returns a short tag string for cache keys and logging.
    pub fn as_tag(self) -> &'static str {
        match self {
            MarketType::Spot => "spot",
            MarketType::Futures => "futures",
        }
    }

    pub fn exchange_info_url(self) -> &'static str {
        match self {
            MarketType::Spot => "https://api.binance.com/api/v3/exchangeInfo",
            MarketType::Futures => "https://fapi.binance.com/fapi/v1/exchangeInfo",
        }
    }

    pub fn klines_url(self, pair: &str) -> String {
        let base = match self {
            MarketType::Spot => "https://api.binance.com/api/v3/klines",
            MarketType::Futures => "https://fapi.binance.com/fapi/v1/klines",
        };
        format!("{base}?symbol={pair}&interval=1m&limit=1&startTime=0")
    }
}
