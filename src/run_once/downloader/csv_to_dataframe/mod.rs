pub mod set_column_types;
pub mod table_title;

pub use set_column_types::convert_types;

use polars::prelude::*;

/// Parses CSV bytes into a Polars DataFrame.
///
/// Auto-detects whether the CSV has a header row. If not, generates
/// default Binance kline column names (open_time, open, high, ...).
pub fn csv_to_dataframe_sync(bytes: &[u8]) -> Result<DataFrame, crate::AppError> {
    let has_header = table_title::detect_header(bytes);
    let opts = CsvReadOptions::default().with_infer_schema_length(Some(0));

    if has_header {
        opts.into_reader_with_file_handle(std::io::Cursor::new(bytes))
            .finish()
            .map_err(|e| crate::AppError::Data(format!("csv parse: {e}")))
    } else {
        let content =
            std::str::from_utf8(bytes).map_err(|e| crate::AppError::Utf8(e.to_string()))?;
        let num_cols = content
            .lines()
            .next()
            .map(|line| line.split(",").count())
            .unwrap_or(0);
        let header = table_title::default_header(num_cols).join(",");
        let csv_content = format!("{header}\n{content}");
        opts.into_reader_with_file_handle(std::io::Cursor::new(csv_content.as_bytes()))
            .finish()
            .map_err(|e| crate::AppError::Data(format!("csv parse: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_csv_with_header() {
        let csv = b"open_time,open,high,low,close\n1,100.0,101.0,99.0,100.5\n";
        let df = csv_to_dataframe_sync(csv).unwrap();
        assert_eq!(df.width(), 5);
        assert_eq!(df.height(), 1);
    }

    #[test]
    fn parse_csv_without_header() {
        let csv = b"1,100.0,101.0,99.0,100.5\n2,101.0,102.0,100.0,101.5\n";
        let df = csv_to_dataframe_sync(csv).unwrap();
        assert_eq!(df.width(), 5);
        assert_eq!(df.height(), 2);
    }

    #[test]
    fn parse_empty_csv_returns_error() {
        let csv = b"";
        assert!(csv_to_dataframe_sync(csv).is_err());
    }
}
