use polars::prelude::*;
use std::path::Path;

use super::Error;

/// Atomically overwrite a Parquet file with the given `DataFrame`.
pub fn overwrite_parquet(path: &Path, mut df: DataFrame) -> Result<(), Error> {
    let tmp = path.with_extension("parquet.tmp");
    let _ = std::fs::remove_file(&tmp);
    {
        let file = std::fs::File::create(&tmp)?;
        ParquetWriter::new(file)
            .finish(&mut df)
            .map_err(|e| Error::Data(format!("write {}: {e}", path.display())))?;
    }
    std::fs::rename(&tmp, path)?;
    log::info!("overwrote {} ({} rows)", path.display(), df.height());
    Ok(())
}
