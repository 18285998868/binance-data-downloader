use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error{context}: {source}")]
    Io {
        source: std::io::Error,
        context: String,
    },

    #[error("Data error: {0}")]
    Data(String),

    #[error("Parse error: {0}")]
    Parse(String),
}

impl Error {
    pub(crate) fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        let context = context.into();
        let context = if context.is_empty() {
            String::new()
        } else {
            format!(" ({})", context)
        };
        Error::Io { source, context }
    }
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Error::Io {
            source,
            context: String::new(),
        }
    }
}
