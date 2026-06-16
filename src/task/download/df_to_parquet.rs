use polars::prelude::*;
use std::path::Path;

/// Write a DataFrame to a .parquet file using an atomic rename (tmp -> final).
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

#[cfg(test)]
mod tests {
    use polars::prelude::*;

    #[test]
    fn parquet_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        let df = df!(
            "open_time" => &[1i64, 2],
            "close" => &[100.5f64, 101.2],
        )
        .unwrap();
        super::write_parquet_sync(&path, df).unwrap();
        assert!(path.exists());

        let file = std::fs::File::open(&path).unwrap();
        let loaded = ParquetReader::new(file).finish().unwrap();
        assert_eq!(loaded.height(), 2);
    }

    #[test]
    fn parquet_creates_parent_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/deep/test.parquet");
        let df = df!("a" => &[1i64]).unwrap();
        super::write_parquet_sync(&path, df).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn parquet_tmp_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("clean.parquet");

        let df = df!("x" => &[1i64]).unwrap();
        super::write_parquet_sync(&path, df).unwrap();
        let tmp = path.with_extension("parquet.tmp");
        assert!(!tmp.exists(), "tmp file should be removed after rename");
    }
}
