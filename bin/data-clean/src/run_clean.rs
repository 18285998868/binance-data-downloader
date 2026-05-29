use super::{clean, config::Config, listing, load, save};

/// Run the full cleaning pipeline over all Parquet files in `config.data_dir`.
pub fn run(config: &Config) -> Result<(), super::Error> {
    let files = listing::list_parquet_files(&config.data_dir)?;
    if files.is_empty() {
        log::warn!("no .parquet files found in {}", config.data_dir.display());
        return Ok(());
    }
    log::info!(
        "found {} .parquet files in {}",
        files.len(),
        config.data_dir.display()
    );
    for path in &files {
        let df = load::read_parquet(path)?;
        if df.height() == 0 {
            log::warn!("skip empty: {}", path.display());
            continue;
        }
        let df = clean::clean_dataframe(df)?;
        save::overwrite_parquet(path, df)?;
    }
    Ok(())
}
