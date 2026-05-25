use std::path::Path;
use polars::prelude::*;

pub async fn write_parquet(path: &Path, mut df: DataFrame) -> Result<(), String> {
    if let Some(p) = path.parent() {
        tokio::fs::create_dir_all(p).await.map_err(|e| e.to_string())?;
    }
    let tmp = path.with_extension("parquet.tmp");
    let _ = tokio::fs::remove_file(&tmp).await;
    {
        let file = std::fs::File::create(&tmp).map_err(|e| e.to_string())?;
        ParquetWriter::new(file)
            .finish(&mut df)
            .map_err(|e| e.to_string())?;
    }
    tokio::fs::rename(&tmp, path)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}
