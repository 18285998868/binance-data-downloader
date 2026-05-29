pub mod clean;
pub mod config;
pub mod error;
mod listing;
mod load;
mod run_clean;
mod save;

pub use clean::clean_dataframe;
pub use config::Config;
pub use error::Error;

/// Run the full cleaning pipeline over all Parquet files in `config.data_dir`.
pub fn run_clean(config: &Config) -> Result<(), Error> {
    run_clean::run(config)
}
