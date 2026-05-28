/// Default Binance kline column names (12 columns).
const DEFAULT_COLUMNS: [&str; 12] = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
];

/// Generates a default CSV header row for `count` columns.
pub fn default_header(count: usize) -> Vec<String> {
    match count {
        11 | 12 => DEFAULT_COLUMNS[..count.min(DEFAULT_COLUMNS.len())]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        n => (0..n).map(|i| format!("column_{i}")).collect(),
    }
}

/// Detects whether the raw CSV bytes include a header row.
pub fn detect_header(bytes: &[u8]) -> bool {
    let mut lines = bytes.split(|b| *b == b'\n');
    let first = match lines.next() {
        Some(line) if !line.is_empty() => line,
        _ => return true,
    };
    let first_field = first.split(|b| *b == b',').next().unwrap_or(b"");
    let s = std::str::from_utf8(first_field).unwrap_or("");
    s.trim().parse::<f64>().is_err()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_header_11_cols() {
        let h = default_header(11);
        assert_eq!(h.len(), 11);
        assert_eq!(h[0], "open_time");
        assert_eq!(h[10], "taker_buy_quote_volume");
    }

    #[test]
    fn default_header_12_cols() {
        let h = default_header(12);
        assert_eq!(h.len(), 12);
        assert_eq!(h[11], "ignore");
    }

    #[test]
    fn default_header_unknown_cols() {
        let h = default_header(5);
        assert_eq!(
            h,
            vec!["column_0", "column_1", "column_2", "column_3", "column_4"]
        );
    }

    #[test]
    fn detect_header_present() {
        let data = b"open_time,open,high,low,close\n1,2,3,4,5\n";
        assert!(detect_header(data));
    }

    #[test]
    fn detect_header_absent() {
        let data = b"1,2,3,4,5\n6,7,8,9,10\n";
        assert!(!detect_header(data));
    }

    #[test]
    fn detect_header_empty() {
        assert!(detect_header(b""));
    }

    #[test]
    fn detect_header_partial_float() {
        let data = b"abc,def\n1,2\n";
        assert!(detect_header(data));
    }
}
