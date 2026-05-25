/// All Binance kline intervals supported by the data downloader.
pub const ALL_TIMEFRAMES: &[&str] = &[
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d",
];

/// Returns the user-provided timeframe list, or the full `ALL_TIMEFRAMES`
/// default when none were specified.
pub fn resolve_timeframes(timeframes: &Option<Vec<String>>) -> Vec<String> {
    if let Some(tfs) = timeframes && !tfs.is_empty() {
        return tfs.clone();
    }
    ALL_TIMEFRAMES.iter().map(|s| s.to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_list() {
        assert_eq!(ALL_TIMEFRAMES.len(), 12);
        assert_eq!(ALL_TIMEFRAMES[0], "1m");
        assert_eq!(ALL_TIMEFRAMES[11], "1d");
    }

    #[test]
    fn test_custom() {
        let input = Some(vec!["1h".into(), "4h".into()]);
        assert_eq!(resolve_timeframes(&input), vec!["1h", "4h"]);
    }

    #[test]
    fn test_empty_falls_back() {
        let input = Some(vec![]);
        assert_eq!(resolve_timeframes(&input).len(), 12);
    }

    #[test]
    fn test_none_falls_back() {
        assert_eq!(resolve_timeframes(&None).len(), 12);
    }
}
