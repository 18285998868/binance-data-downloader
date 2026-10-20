//! Command-line interface: the `-c/--config` argument of the binary.

use clap::Parser;
use std::path::PathBuf;

/// Command-line arguments accepted by the `binance-data-downloader` binary.
///
/// The example below is a doctest; `long_about` keeps it out of `--help`,
/// which would otherwise render the raw doctest source.
///
/// # Examples
///
/// ```
/// use clap::Parser;
/// use bdd::Cli;
///
/// # fn main() -> Result<(), clap::Error> {
/// let cli = Cli::try_parse_from(["binance-data-downloader", "-c", "config.toml"])?;
/// assert_eq!(cli.config, std::path::Path::new("config.toml"));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Parser)]
#[command(
    name = "binance-data-downloader",
    long_about = "Command-line arguments accepted by the `binance-data-downloader` binary."
)]
pub struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short = 'c', long = "config")]
    pub config: PathBuf,
    /// Validate every downloaded archive as a readable zip.
    ///
    /// Off by default: the magic-byte and content-length checks already reject
    /// truncated bodies and HTML error pages.
    #[arg(long = "verify")]
    pub verify: bool,
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

    #[test]
    fn help_is_available() {
        let err = Cli::try_parse_from(["app", "--help"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);
    }

    #[test]
    fn an_unknown_flag_is_rejected() {
        assert!(Cli::try_parse_from(["app", "-c", "x.toml", "--nope"]).is_err());
    }
}
