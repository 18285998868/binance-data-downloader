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
}
