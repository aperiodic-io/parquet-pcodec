use std::fmt;

/// Everything that can go wrong in this crate.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Reading or writing Parquet failed.
    Parquet(parquet::errors::ParquetError),
    /// An Arrow operation failed.
    Arrow(arrow_schema::ArrowError),
    /// Reading or writing a file failed.
    Io(std::io::Error),
    /// The request or the input file is not valid.
    Invalid(String),
}

/// `Result` with this crate's [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parquet(e) => write!(f, "parquet: {e}"),
            Self::Arrow(e) => write!(f, "arrow: {e}"),
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Invalid(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Parquet(e) => Some(e),
            Self::Arrow(e) => Some(e),
            Self::Io(e) => Some(e),
            Self::Invalid(_) => None,
        }
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Self::Parquet(e)
    }
}

impl From<arrow_schema::ArrowError> for Error {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Self::Arrow(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

pub(crate) fn invalid(msg: impl Into<String>) -> Error {
    Error::Invalid(msg.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn display_names_the_layer() {
        let p = Error::from(parquet::errors::ParquetError::General("boom".into()));
        assert_eq!(p.to_string(), "parquet: Parquet error: boom");
        assert!(p.source().is_some());
        let a = Error::from(arrow_schema::ArrowError::ComputeError("bad".into()));
        assert_eq!(a.to_string(), "arrow: Compute error: bad");
        assert!(a.source().is_some());
        let i = Error::from(std::io::Error::other("disk"));
        assert_eq!(i.to_string(), "io: disk");
        assert!(i.source().is_some());
        let v = invalid("nope");
        assert_eq!(v.to_string(), "nope");
        assert!(v.source().is_none());
    }
}
