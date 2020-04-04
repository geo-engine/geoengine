use crate::error::Error;
use snafu::Snafu;

#[derive(Debug, PartialEq, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum FeatureCollectionError {
    #[snafu(display("Arrow internal error: {:?}", source))]
    ArrowInternal {
        source: arrow::error::ArrowError,
    },

    CannotAccessReservedColumn {
        name: String,
    },

    ColumnDoesNotExist {
        name: String,
    },

    ColumnAlreadyExists {
        name: String,
    },

    UnmatchedLength {
        a: usize,
        b: usize,
    },
}

impl From<FeatureCollectionError> for Error {
    fn from(error: FeatureCollectionError) -> Self {
        Error::FeatureCollection { source: error }
    }
}
