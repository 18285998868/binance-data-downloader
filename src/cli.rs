use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "binance-data-downloader")]
pub struct Cli {
    #[arg(short = 'c', long = "config")]
    pub config: Option<PathBuf>,
}

pub fn parse() -> Cli {
    Cli::parse()
}
