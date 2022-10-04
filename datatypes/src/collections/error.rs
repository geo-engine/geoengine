use arrow::error::ArrowError;
use snafu::prelude::*;

use crate::error::Error;
use crate::primitives::PrimitivesError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
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

    ColumnDuplicate {
        name: String,
    },

    EmptyPredicate,

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

    MissingColumnArray,

    MissingTime,
    MissingGeo,
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
