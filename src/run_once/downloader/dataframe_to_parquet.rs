use polars::prelude::*;
use std::path::Path;

/// Writes a DataFrame to Parquet at `path` using atomic rename.
///
/// Writes to a `.parquet.tmp` file first, then atomically renames
/// to the final path. This prevents partial/corrupt files on crash.
pub fn write_parquet_sync(path: &Path, mut df: DataFrame) -> Result<(), crate::AppError> {
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p).map_err(crate::AppError::Io)?;
    }
    let tmp = path.with_extension("parquet.tmp");
    let _ = std::fs::remove_file(&tmp);
    {
        let file = std::fs::File::create(&tmp).map_err(crate::AppError::Io)?;
        ParquetWriter::new(file)
            .finish(&mut df)
            .map_err(|e| crate::AppError::Data(format!("parquet write: {e}")))?;
    }
    std::fs::rename(&tmp, path).map_err(crate::AppError::Io)?;
    Ok(())
}
