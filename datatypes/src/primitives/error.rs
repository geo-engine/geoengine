use crate::error::Error;
use arrow::error::ArrowError;
use snafu::Snafu;

use super::TimeInstance;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum PrimitivesError {
    UnallowedEmpty,
    UnclosedPolygonRing,
    InvalidSpatialResolution {
        value: f64,
    },
    #[snafu(display("Arrow internal error: {:?}", source))]
    ArrowInternal {
        source: ArrowError,
    },
    InvalidConversion,

    #[snafu(display("Time instance must be between {} and {}, but is {}", min.inner(), max.inner(), is))]
    InvalidTimeInstance {
        min: TimeInstance,
        max: TimeInstance,
        is: i64,
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
