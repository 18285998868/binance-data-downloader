pub mod set_column_types;
pub mod table_title;

pub use set_column_types::convert_types;

use polars::prelude::*;

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
