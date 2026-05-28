use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum MarketType {
    #[serde(rename = "spot")]
    Spot,
    #[serde(rename = "futures")]
    Futures,
}

impl MarketType {
    pub fn rel_path_prefix(self) -> &'static str {
        match self {
            MarketType::Spot => "spot/daily/klines",
            MarketType::Futures => "futures/um/daily/klines",
        }
    }

    pub fn as_tag(self) -> &'static str {
        match self {
            MarketType::Spot => "spot",
            MarketType::Futures => "futures",
        }
    }
}
