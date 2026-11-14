//! Configuration errors, optionally tied to the file they came from.

use std::fmt;
use std::path::{Path, PathBuf};

use crate::AppError;

/// A configuration failure.
///
/// The path is kept separate from the message so [`AppError::Config`] can render
/// it exactly once, and callers that only need the human-readable reason do not
/// have to strip the path back out.
#[derive(Debug)]
pub(crate) struct ConfigError {
    path: Option<PathBuf>,
    message: String,
}

impl ConfigError {
    /// A schema or semantic error, before the file path is attached.
    pub(super) fn invalid(message: impl Into<String>) -> Self {
        Self {
            path: None,
            message: message.into(),
        }
    }

    /// An error reading the configuration file.
    pub(super) fn io(path: &Path, error: &std::io::Error) -> Self {
        Self {
            path: Some(path.to_path_buf()),
            message: error.to_string(),
        }
    }

    /// Attach the configuration file path, unless one is already present.
    pub(super) fn with_path(mut self, path: &Path) -> Self {
        if self.path.is_none() {
            self.path = Some(path.to_path_buf());
        }
        self
    }

    /// Split into the fields [`AppError::Config`] stores.
    pub(crate) fn into_parts(self) -> (Option<PathBuf>, String) {
        (self.path, self.message)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.path {
            Some(path) => write!(f, "{}: {}", path.display(), self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<ConfigError> for AppError {
    fn from(error: ConfigError) -> Self {
        let (path, message) = error.into_parts();
        Self::Config { path, message }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendering_includes_the_path_once() {
        let error = ConfigError::invalid("bad value").with_path(Path::new("config.toml"));
        assert_eq!(error.to_string(), "config.toml: bad value");
        match AppError::from(error) {
            AppError::Config { path, message } => {
                assert_eq!(path.as_deref(), Some(Path::new("config.toml")));
                assert_eq!(message, "bad value");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn rendering_without_a_path_is_the_message_alone() {
        assert_eq!(ConfigError::invalid("bad value").to_string(), "bad value");
    }
}
