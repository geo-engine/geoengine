use crate::error::Error;
use snafu::Snafu;

#[derive(Debug, PartialEq, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum PrimitivesError {
    UnallowedEmpty,
    UnclosedPolygonRing,
}

impl From<PrimitivesError> for Error {
    fn from(error: PrimitivesError) -> Self {
        Error::Primitives { source: error }
    }
}
