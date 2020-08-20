use arrow::error::ArrowError;
use snafu::Snafu;

use crate::error::Error;
use crate::primitives::PrimitivesError;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum FeatureCollectionError {
    #[snafu(display("Arrow internal error: {:?}", source))]
    ArrowInternal {
        source: ArrowError,
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

    Primitives {
        source: PrimitivesError,
    },

    UnmatchedLength {
        a: usize,
        b: usize,
    },

    UnmatchedSchema {
        a: Vec<String>,
        b: Vec<String>,
    },

    WrongDataType,
}

impl From<FeatureCollectionError> for Error {
    fn from(error: FeatureCollectionError) -> Self {
        Error::FeatureCollection { source: error }
    }
}

impl From<ArrowError> for FeatureCollectionError {
    fn from(source: ArrowError) -> Self {
        FeatureCollectionError::ArrowInternal { source }
    }
}

impl From<PrimitivesError> for FeatureCollectionError {
    fn from(source: PrimitivesError) -> Self {
        FeatureCollectionError::Primitives { source }
    }
}
