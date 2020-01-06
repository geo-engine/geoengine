use arrow::error::ArrowError;
use snafu::Snafu;

use crate::primitives::TimeInterval;

#[derive(Debug, PartialEq, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    // #[snafu(display("Arrow internal error: {:?}", source))]
    #[snafu(display("Arrow internal error: {:?}", error))]
    ArrowInternalError {
        // #[snafu(source(false))]
        // source: arrow::error::ArrowError,
        error: arrow::error::ArrowError,
    },

    #[snafu(display("Field is reserved or already in use: {}", name))]
    FieldNameConflict { name: String },

    #[snafu(display("Start `{}` must be before end `{}`", start, end))]
    TimeIntervalEndBeforeStart { start: i64, end: i64 },

    #[snafu(display(
        "{} cannot be unioned with {} since the intervals are neither intersecting nor contiguous",
        i1,
        i2
    ))]
    TimeIntervalUnmatchedIntervals { i1: TimeInterval, i2: TimeInterval },

    #[snafu(display(
        "Mask length ≠ collection length ({} ≠ {})",
        mask_length,
        collection_length
    ))]
    MaskLengthDoesNotMatchCollectionLength {
        mask_length: usize,
        collection_length: usize,
    },

    #[snafu(display("FeatureCollection exception: {}", details))]
    FeatureCollection { details: String },

    #[snafu(display("FeatureData exception: {}", details))]
    FeatureData { details: String },

    #[snafu(display("FeatureCollectionBuilder exception: {}", details))]
    FeatureCollectionBuilderException { details: String },
}

impl From<arrow::error::ArrowError> for Error {
    fn from(arrow_error: ArrowError) -> Self {
        Error::ArrowInternalError {
            // source: arrow_error,
            error: arrow_error,
        }
    }
}
