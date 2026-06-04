use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BatchConfig {
    pub symbol: Vec<String>,
    pub timeframe: Vec<String>,
    pub max_concurrent: usize,
    pub start: Option<String>,
    pub end: Option<String>,
    pub dir: String,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            symbol: Vec::new(),
            timeframe: Vec::new(),
            max_concurrent: 256,
            start: None,
            end: None,
            dir: "data".into(),
        }
    }
}
