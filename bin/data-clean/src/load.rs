use polars::prelude::*;
use std::path::Path;

use super::Error;

/// Read a single Parquet file into a `DataFrame`.
pub fn read_parquet(path: &Path) -> Result<DataFrame, Error> {
    let file =
        std::fs::File::open(path).map_err(|e| Error::io(path.display().to_string(), e))?;
    let df = ParquetReader::new(file)
        .finish()
        .map_err(|e| Error::Data(format!("read {}: {e}", path.display())))?;
    log::debug!("read {}: {} rows", path.display(), df.height());
    Ok(df)
}
