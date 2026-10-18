//! Command-line interface: the `-c/--config` argument of the binary.

use clap::Parser;
use std::path::PathBuf;

/// Command-line arguments accepted by the `binance-data-downloader` binary.
#[derive(Debug, Clone, Parser)]
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

    #[test]
    fn missing_config_flag_is_rejected() {
        assert!(Cli::try_parse_from(["app"]).is_err());
    }
}
