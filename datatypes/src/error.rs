use snafu::Snafu;

use crate::collections::FeatureCollectionError;
use crate::{
    primitives::{Coordinate2D, PrimitivesError, TimeInterval},
    raster::RasterDataType,
};
use std::convert::Infallible;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    #[snafu(display("Arrow internal error: {:?}", source))]
    ArrowInternal {
        source: arrow::error::ArrowError,
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
        "{:?} is not a valid index in the bounds {:?}, {:?} ",
        index,
        min_index,
        max_index,
    ))]
    GridIndexOutOfBounds {
        index: Vec<isize>,
        min_index: Vec<isize>,
        max_index: Vec<isize>,
    },

    #[snafu(display("Invalid GridIndex ({:?}), reason: \"{}\".", grid_index, description))]
    InvalidGridIndex {
        grid_index: Vec<usize>,
        description: &'static str,
    },

    #[snafu(display("Invalid raster operation. Reason: \"{}\".", description))]
    InvalidRasterOperation {
        description: &'static str,
    },

    #[snafu(display(
        "Dimension capacity  ≠ data capacity ({} ≠ {})",
        dimension_cap,
        data_cap
    ))]
    DimensionCapacityDoesNotMatchDataCapacity {
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
    #[snafu(display(
        "Invalid Grid bounds: Each eleemnt in {:?} must be <= the corresponding element in {:?}.",
        min,
        max
    ))]
    InvalidGridBounds {
        min: Vec<isize>,
        max: Vec<isize>,
    },
    #[snafu(display("InvalidSpatialReferenceString: {}", spatial_reference_string))]
    InvalidSpatialReferenceString {
        spatial_reference_string: String,
    },

    #[snafu(display("ParseU32: {}", source))]
    ParseU32 {
        source: <u32 as std::str::FromStr>::Err,
    },
    InvalidTypedGridConversion,
    InvalidTypedValueConversion,
}

impl From<arrow::error::ArrowError> for Error {
    fn from(source: arrow::error::ArrowError) -> Self {
        Error::ArrowInternal { source }
    }
}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        unreachable!("This function cannot be called on a non-failing type")
    }
}
