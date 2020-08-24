use crate::error::Error;
use arrow::error::ArrowError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum PrimitivesError {
    UnallowedEmpty,
    UnclosedPolygonRing,
    #[snafu(display("Arrow internal error: {:?}", source))]
    ArrowInternal {
        source: ArrowError,
    },
}

impl From<PrimitivesError> for Error {
    fn from(error: PrimitivesError) -> Self {
        Error::Primitives { source: error }
    }
}

impl From<ArrowError> for PrimitivesError {
    fn from(source: ArrowError) -> Self {
        PrimitivesError::ArrowInternal { source }
    }
}
