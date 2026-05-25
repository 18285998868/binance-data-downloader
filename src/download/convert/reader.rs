use polars::prelude::*;

pub async fn csv_to_dataframe(bytes: &[u8]) -> Result<DataFrame, String> {
    let has_header = detect_header(bytes);
    let opts = CsvReadOptions::default().with_infer_schema_length(Some(0));

    if has_header {
        opts.into_reader_with_file_handle(std::io::Cursor::new(bytes))
            .finish()
            .map_err(|e| e.to_string())
    } else {
        let content = std::str::from_utf8(bytes).map_err(|e| e.to_string())?;
        let num_cols = content
            .lines()
            .next()
            .map(|line| line.split(',').count())
            .unwrap_or(0);
        let header = default_header(num_cols).join(",");
        let csv_content = format!("{}\n{}", header, content);
        opts.into_reader_with_file_handle(std::io::Cursor::new(csv_content))
            .finish()
            .map_err(|e| e.to_string())
    }
}

/// Casts columns to their correct types *without cloning the entire DataFrame*.
pub fn convert_types(df: &DataFrame) -> Result<DataFrame, String> {
    let mut series = Vec::with_capacity(df.width());
    for name in df.get_column_names() {
        let name: &str = name.as_ref();
        if name == "ignore" {
            continue;
        }
        let target = match name {
            "open_time" | "close_time" | "count" => DataType::Int64,
            _ => DataType::Float64,
        };
        let s = df
            .column(name)
            .map_err(|e| e.to_string())?
            .cast(&target)
            .map_err(|e| format!("cast {name} to {target:?}: {e}"))?;
        series.push(s);
    }
    DataFrame::new(series).map_err(|e| e.to_string())
}

const DEFAULT_COLUMNS: [&str; 12] = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "count", "taker_buy_volume",
    "taker_buy_quote_volume", "ignore",
];

fn default_header(count: usize) -> Vec<String> {
    match count {
        11 | 12 => DEFAULT_COLUMNS[..count.min(DEFAULT_COLUMNS.len())]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        n => (0..n).map(|i| format!("column_{}", i)).collect(),
    }
}

fn detect_header(bytes: &[u8]) -> bool {
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
    fn test_default_header_11() {
        let h = default_header(11);
        assert_eq!(h.len(), 11);
        assert_eq!(h[0], "open_time");
        assert_eq!(h[10], "taker_buy_quote_volume");
    }

    #[test]
    fn test_default_header_12() {
        let h = default_header(12);
        assert_eq!(h.len(), 12);
        assert_eq!(h[11], "ignore");
    }

    #[test]
    fn test_default_header_unknown() {
        let h = default_header(5);
        assert_eq!(h, vec!["column_0", "column_1", "column_2", "column_3", "column_4"]);
    }

    #[test]
    fn test_default_header_zero() {
        let h = default_header(0);
        assert!(h.is_empty());
    }

    #[test]
    fn test_detect_header_with_header() {
        assert!(detect_header(b"open_time,open,high,low,close\n"));
    }

    #[test]
    fn test_detect_header_without_header() {
        assert!(!detect_header(b"1234567890,100.0,200.0,50.0,75.0\n"));
    }

    #[test]
    fn test_detect_header_empty() {
        assert!(detect_header(b""));
    }

    #[test]
    fn test_detect_header_only_newline() {
        assert!(detect_header(b"\n"));
    }

    #[test]
    fn test_convert_types_drops_ignore() {
        let df = df!(
            "open_time" => &[1000i64],
            "close" => &[100.0f64],
            "ignore" => &[0i64],
        ).unwrap();

        let result = convert_types(&df).unwrap();
        assert!(result.column("ignore").is_err());
        assert!(result.column("open_time").is_ok());
        assert!(result.column("close").is_ok());
    }

    #[test]
    fn test_convert_types_types() {
        let df = df!(
            "open_time" => &["1000"],
            "open" => &["100.5"],
            "close_time" => &["2000"],
            "count" => &["42"],
            "volume" => &["10.0"],
        ).unwrap();
        let result = convert_types(&df).unwrap();
        assert_eq!(result.column("open_time").unwrap().dtype(), &DataType::Int64);
        assert_eq!(result.column("close_time").unwrap().dtype(), &DataType::Int64);
        assert_eq!(result.column("count").unwrap().dtype(), &DataType::Int64);
        assert_eq!(result.column("open").unwrap().dtype(), &DataType::Float64);
        assert_eq!(result.column("volume").unwrap().dtype(), &DataType::Float64);
    }
}
