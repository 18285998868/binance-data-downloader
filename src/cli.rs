use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "binance-data-downloader")]
pub struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short = 'c', long = "config")]
    pub config: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_config_flag() {
        let cli = Cli::try_parse_from(["app", "-c", "foo.toml"]).unwrap();
        assert_eq!(cli.config.to_str().unwrap(), "foo.toml");
    }

    #[test]
    fn parses_long_config_flag() {
        let cli = Cli::try_parse_from(["app", "--config", "bar.toml"]).unwrap();
        assert_eq!(cli.config.to_str().unwrap(), "bar.toml");
    }
}
