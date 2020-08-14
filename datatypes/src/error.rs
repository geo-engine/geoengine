use arrow::error::ArrowError;
use snafu::Snafu;

use crate::collections::FeatureCollectionError;
use crate::{
    primitives::{Coordinate2D, PrimitivesError, TimeInterval},
    raster::RasterDataType,
};
use serde::export::Formatter;

#[derive(Debug, PartialEq, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("Arrow internal error: {:?}", source))]
    ArrowInternal {
        source: ArrowWrappedError,
    },

    #[snafu(display("Field is reserved or already in use: {}", name))]
    ColumnNameConflict {
        name: String,
    },

    #[snafu(display("Start `{}` must be before end `{}`", start, end))]
    TimeIntervalEndBeforeStart {
        start: i64,
        end: i64,
    },

    #[snafu(display(
        "{} cannot be unioned with {} since the intervals are neither intersecting nor contiguous",
        i1,
        i2
    ))]
    TimeIntervalUnmatchedIntervals {
        i1: TimeInterval,
        i2: TimeInterval,
    },

    #[snafu(display(
        "{} is not a valid index in the dimension {} with size {}",
        index,
        dimension,
        dimension_size
    ))]
    GridIndexOutOfBounds {
        index: usize,
        dimension: usize,
        dimension_size: usize,
    },

    #[snafu(display(
        "Dimension capacity  ≠ data capacity ({} ≠ {})",
        dimension_cap,
        data_cap
    ))]
    DimnsionCapacityDoesNotMatchDataCapacity {
        dimension_cap: usize,
        data_cap: usize,
    },

    #[snafu(display(
        "The conditions ll.x <= ur.x && ll.y <= ur.y are not met by ll:{} ur:{}",
        lower_left_coordinate,
        upper_right_coordinate
    ))]
    InvalidBoundingBox {
        lower_left_coordinate: Coordinate2D,
        upper_right_coordinate: Coordinate2D,
    },

    #[snafu(display(
        "Mask length ≠ collection length ({} ≠ {})",
        mask_length,
        collection_length
    ))]
    MaskLengthDoesNotMatchCollectionLength {
        mask_length: usize,
        collection_length: usize,
    },

    FeatureCollection {
        source: FeatureCollectionError,
    },

    #[snafu(display("FeatureData exception: {}", details))]
    FeatureData {
        details: String,
    },

    #[snafu(display("FeatureCollectionBuilder exception: {}", details))]
    FeatureCollectionBuilder {
        details: String,
    },

    #[snafu(display("Plot exception: {}", details))]
    Plot {
        details: String,
    },

    #[snafu(display("Colorizer exception: {}", details))]
    Colorizer {
        details: String,
    },

    Primitives {
        source: PrimitivesError,
    },

    Blit {
        details: String,
    },
    #[snafu(display("NonMatchingRasterTypes: a=\"{:?}\", b=\"{:?}\"", a, b))]
    NonMatchingRasterTypes {
        a: RasterDataType,
        b: RasterDataType,
    },

    #[snafu(display("InvalidProjectionString: {}", projection_string))]
    InvalidProjectionString {
        projection_string: String,
    },

    #[snafu(display("ParseU32: {}", source))]
    ParseU32 {
        source: <u32 as std::str::FromStr>::Err,
    },
}

#[derive(Debug)]
pub struct ArrowWrappedError {
    error: arrow::error::ArrowError,
}

impl AsRef<arrow::error::ArrowError> for ArrowWrappedError {
    fn as_ref(&self) -> &ArrowError {
        &self.error
    }
}

impl std::fmt::Display for ArrowWrappedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        self.error.fmt(f)
    }
}

impl std::error::Error for ArrowWrappedError {}

impl From<arrow::error::ArrowError> for ArrowWrappedError {
    fn from(error: ArrowError) -> Self {
        Self { error }
    }
}

impl PartialEq for ArrowWrappedError {
    fn eq(&self, other: &Self) -> bool {
        match (self.as_ref(), other.as_ref()) {
            (
                arrow::error::ArrowError::MemoryError(e1),
                arrow::error::ArrowError::MemoryError(e2),
            )
            | (
                arrow::error::ArrowError::ParseError(e1),
                arrow::error::ArrowError::ParseError(e2),
            )
            | (
                arrow::error::ArrowError::SchemaError(e1),
                arrow::error::ArrowError::SchemaError(e2),
            )
            | (
                arrow::error::ArrowError::ComputeError(e1),
                arrow::error::ArrowError::ComputeError(e2),
            )
            | (arrow::error::ArrowError::CsvError(e1), arrow::error::ArrowError::CsvError(e2))
            | (arrow::error::ArrowError::JsonError(e1), arrow::error::ArrowError::JsonError(e2))
            | (arrow::error::ArrowError::IoError(e1), arrow::error::ArrowError::IoError(e2))
            | (
                arrow::error::ArrowError::InvalidArgumentError(e1),
                arrow::error::ArrowError::InvalidArgumentError(e2),
            )
            | (
                arrow::error::ArrowError::ParquetError(e1),
                arrow::error::ArrowError::ParquetError(e2),
            ) => e1 == e2,
            (arrow::error::ArrowError::DivideByZero, arrow::error::ArrowError::DivideByZero)
            | (
                arrow::error::ArrowError::DictionaryKeyOverflowError,
                arrow::error::ArrowError::DictionaryKeyOverflowError,
            ) => true,
            (arrow::error::ArrowError::ExternalError(_), _)
            | (ArrowError::MemoryError(_), _)
            | (ArrowError::ParseError(_), _)
            | (ArrowError::SchemaError(_), _)
            | (ArrowError::ComputeError(_), _)
            | (ArrowError::DivideByZero, _)
            | (ArrowError::CsvError(_), _)
            | (ArrowError::JsonError(_), _)
            | (ArrowError::IoError(_), _)
            | (ArrowError::InvalidArgumentError(_), _)
            | (ArrowError::ParquetError(_), _)
            | (ArrowError::DictionaryKeyOverflowError, _) => false,
        }
    }
}

impl From<arrow::error::ArrowError> for Error {
    fn from(arrow_error: ArrowError) -> Self {
        Error::ArrowInternal {
            source: arrow_error.into(),
        }
    }
}
